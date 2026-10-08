//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// arrow_batch_stats.h
//
//
//===----------------------------------------------------------------------===//

// Header-only, C and C++, on the nanoarrow C API.
//
// Computes, while an Arrow IPC file or stream is written (or from one already written),
// the per-file record batch statistics RETURN_STATS reports, in particular
// peak_read_memory_bytes: an upper bound on the memory a reader that allocates in powers
// of two (arrow-java's DefaultRoundingPolicy) needs at any point while it decodes the
// output, also large enough to size an arena that aligns each allocation to 64 bytes.
//
// Contract: a streaming observer.
//   ArrowBatchStatsInit(&stats, schema, file_format, &error)      once, from the schema
//   ArrowBatchStatsObserve(&stats, batch, metadata_length,        once per record batch
//                          body_length, compressed)
//     (or ArrowBatchStatsMeasure(batch, &m) while the view is valid, then
//      ArrowBatchStatsAdd(&stats, &m, metadata_length, body_length, compressed))
//   ArrowBatchStatsPeakReadMemoryBytes(&stats)                    at the end
// batch is the ArrowArrayView of the record batch as written: its buffer views hold the
// uncompressed buffer sizes, and a validity buffer the writer left out has size 0.
// metadata_length and body_length are the block's (the message's encapsulated metadata,
// prefix and padding included, and its body as stored).
//
// Cost: no allocation, no data access. Observe walks the batch's buffer sizes once,
// O(number of buffers), and the state is a handful of integers.
//
// The reader model it bounds, verified against arrow-java 16.1 with the netty and unsafe
// allocators (test/java):
//   * a stream reader allocates an uncompressed body as one buffer; a file reader
//     (ArrowFileReader) reads the batch metadata in that same allocation
//   * a compressed body is read into one scratch buffer for the whole message, then
//     every buffer is decompressed into its own allocation
//   * every allocation is rounded up to a power of two, and at least 2 bytes
//   * a reader that keeps a bitmap for every field allocates ceil(length / 8) bytes for
//     each bitmap the writer left out because the field has no nulls
//   * loading batch i releases batch i - 1 field by field, so at most two adjacent
//     batches are resident at once

#ifndef ARROW_BATCH_STATS_H_INCLUDED
#define ARROW_BATCH_STATS_H_INCLUDED

#include <stdint.h>
#include <string.h>

#include "nanoarrow/nanoarrow.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ARROW_BATCH_STATS_MIN_ESTIMATE 64
#define ARROW_BATCH_STATS_ALLOCATION_ALIGNMENT 64

struct ArrowBatchStats {
  // Set by ArrowBatchStatsInit
  int file_format;
  int64_t allocations_per_batch;
  // Accumulated by ArrowBatchStatsObserve
  int64_t record_batch_count;
  int64_t row_count;
  int64_t total_compressed_size;
  int64_t total_uncompressed_size;
  int64_t previous_batch_peak;
  int64_t max_batch_pair_peak;
};

static inline int64_t ArrowBatchStatsPad8(int64_t size) {
  return (size + 7) & ~(int64_t)7;
}

// The smallest power of two that is at least size, 1 for sizes up to 1
static inline int64_t ArrowBatchStatsRoundUpToPowerOfTwo(int64_t size) {
  int64_t result = 1;
  while (result < size) {
    result <<= 1;
  }
  return result;
}

// Buffers an array of this field holds, its children's included
static inline ArrowErrorCode ArrowBatchStatsBufferCount(const struct ArrowSchema* field,
                                                        int64_t* count,
                                                        struct ArrowError* error) {
  struct ArrowSchemaView view;
  NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&view, field, error));
  for (int i = 0; i < NANOARROW_MAX_FIXED_BUFFERS; i++) {
    if (view.layout.buffer_type[i] != NANOARROW_BUFFER_TYPE_NONE) {
      (*count)++;
    }
  }
  for (int64_t c = 0; c < field->n_children; c++) {
    NANOARROW_RETURN_NOT_OK(ArrowBatchStatsBufferCount(field->children[c], count, error));
  }
  return NANOARROW_OK;
}

// schema is the record batch schema (a struct). file_format is non-zero for the IPC file
// format, which a reader decodes with ArrowFileReader.
static inline ArrowErrorCode ArrowBatchStatsInit(struct ArrowBatchStats* stats,
                                                 const struct ArrowSchema* schema,
                                                 int file_format,
                                                 struct ArrowError* error) {
  memset(stats, 0, sizeof(*stats));
  stats->file_format = file_format;
  int64_t buffers = 0;
  NANOARROW_RETURN_NOT_OK(ArrowBatchStatsBufferCount(schema, &buffers, error));
  // Every buffer of a batch, plus the block or scratch buffer its message is read into,
  // plus one of margin. The root struct's own validity buffer is counted but never
  // allocated, so this overcounts by one more.
  stats->allocations_per_batch = buffers + 2;
  return NANOARROW_OK;
}

// One pass over the fields below view: the padded uncompressed body size, the sum of
// every buffer allocated on its own at the next power of two, and the bitmaps a reader
// allocates for fields written without one
static inline void ArrowBatchStatsWalk(const struct ArrowArrayView* view, int64_t* padded,
                                       int64_t* allocations, int64_t* dropped_validity) {
  for (int64_t c = 0; c < view->n_children; c++) {
    const struct ArrowArrayView* child = view->children[c];
    for (int64_t b = 0; b < NANOARROW_MAX_FIXED_BUFFERS; b++) {
      if (child->layout.buffer_type[b] == NANOARROW_BUFFER_TYPE_NONE) {
        continue;
      }
      const int64_t size = ArrowBatchStatsPad8(child->buffer_views[b].size_bytes);
      *padded += size;
      *allocations += ArrowBatchStatsRoundUpToPowerOfTwo(size);
    }
    if (child->layout.buffer_type[0] == NANOARROW_BUFFER_TYPE_VALIDITY &&
        child->buffer_views[0].size_bytes == 0 && child->length > 0) {
      const int64_t bitmap = (child->length + 7) / 8;
      *dropped_validity += ArrowBatchStatsRoundUpToPowerOfTwo(bitmap < 2 ? 2 : bitmap);
    }
    ArrowBatchStatsWalk(child, padded, allocations, dropped_validity);
  }
}

// What one batch contributes, taken from its ArrowArrayView. Split from
// ArrowBatchStatsAdd so a writer can measure while the batch's view is valid and add
// once the message's block lengths are known (possibly on another thread, under a lock).
struct ArrowBatchMeasure {
  int64_t length;
  int64_t uncompressed_bytes;
  int64_t buffer_allocations;
  int64_t dropped_validity;
};

static inline void ArrowBatchStatsMeasure(const struct ArrowArrayView* batch,
                                          struct ArrowBatchMeasure* out) {
  out->length = batch->length;
  out->uncompressed_bytes = 0;
  out->buffer_allocations = 0;
  out->dropped_validity = 0;
  ArrowBatchStatsWalk(batch, &out->uncompressed_bytes, &out->buffer_allocations,
                      &out->dropped_validity);
}

static inline void ArrowBatchStatsAdd(struct ArrowBatchStats* stats,
                                      const struct ArrowBatchMeasure* batch,
                                      int64_t metadata_length, int64_t body_length,
                                      int compressed) {
  const int64_t message_bytes = metadata_length + body_length;
  stats->record_batch_count++;
  stats->row_count += batch->length;
  stats->total_compressed_size += body_length;
  stats->total_uncompressed_size += batch->uncompressed_bytes;

  int64_t peak;
  if (compressed) {
    peak = ArrowBatchStatsRoundUpToPowerOfTwo(message_bytes) + batch->buffer_allocations;
  } else {
    peak = ArrowBatchStatsRoundUpToPowerOfTwo(stats->file_format ? message_bytes
                                                                 : body_length);
  }
  peak += batch->dropped_validity;
  if (peak + stats->previous_batch_peak > stats->max_batch_pair_peak) {
    stats->max_batch_pair_peak = peak + stats->previous_batch_peak;
  }
  stats->previous_batch_peak = peak;
}

// Measure and add in one call, for a caller that has the view and the block together
static inline void ArrowBatchStatsObserve(struct ArrowBatchStats* stats,
                                          const struct ArrowArrayView* batch,
                                          int64_t metadata_length, int64_t body_length,
                                          int compressed) {
  struct ArrowBatchMeasure measure;
  ArrowBatchStatsMeasure(batch, &measure);
  ArrowBatchStatsAdd(stats, &measure, metadata_length, body_length, compressed);
}

static inline int64_t ArrowBatchStatsPeakReadMemoryBytes(
    const struct ArrowBatchStats* stats) {
  const int64_t pair = stats->max_batch_pair_peak > ARROW_BATCH_STATS_MIN_ESTIMATE
                           ? stats->max_batch_pair_peak
                           : ARROW_BATCH_STATS_MIN_ESTIMATE;
  return pair +
         (ARROW_BATCH_STATS_ALLOCATION_ALIGNMENT - 1) * stats->allocations_per_batch;
}

#ifdef __cplusplus
}
#endif

#endif  // ARROW_BATCH_STATS_H_INCLUDED
