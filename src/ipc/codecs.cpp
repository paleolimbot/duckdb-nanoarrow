#include "ipc/codecs.hpp"

#include <cinttypes>

#include "lz4frame.h"
#include "nanoarrow_errors.hpp"
#include "zstd.h"

namespace duckdb {
namespace ext_nanoarrow {

namespace {

//===----------------------------------------------------------------------===//
// Decompression
//
// nanoarrow's own zstd/lz4 support is compiled in only when nanoarrow is built
// against those libraries, and DuckDB's bundled zstd header (which lives in a C++
// namespace) can't be used from nanoarrow's C sources anyway. Instead we plug our
// own decompression functions into nanoarrow's serial decompressor.
//===----------------------------------------------------------------------===//

ArrowErrorCode DuckDBDecompressZstd(struct ArrowBufferView src, uint8_t* dst,
                                    int64_t dst_size, struct ArrowError* error) {
  size_t code = duckdb_zstd::ZSTD_decompress((void*)dst, (size_t)dst_size, src.data.data,
                                             src.size_bytes);
  if (duckdb_zstd::ZSTD_isError(code)) {
    ArrowErrorSet(error,
                  "ZSTD_decompress([buffer with %" PRId64
                  " bytes] -> [buffer with %" PRId64 " bytes]) failed with error '%s'",
                  src.size_bytes, dst_size, duckdb_zstd::ZSTD_getErrorName(code));
    return EIO;
  }

  if (dst_size != static_cast<int64_t>(code)) {
    ArrowErrorSet(error,
                  "Expected decompressed size of %" PRId64 " bytes but got %" PRId64
                  " bytes",
                  dst_size, static_cast<int64_t>(code));
    return EIO;
  }

  return NANOARROW_OK;
}

ArrowErrorCode DuckDBDecompressLZ4(struct ArrowBufferView src, uint8_t* dst,
                                   int64_t dst_size, struct ArrowError* error) {
  LZ4F_dctx* ctx = nullptr;
  size_t code = LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION);
  if (LZ4F_isError(code)) {
    ArrowErrorSet(error, "LZ4F_createDecompressionContext() failed with error '%s'",
                  LZ4F_getErrorName(code));
    return EIO;
  }

  size_t dst_capacity = static_cast<size_t>(dst_size);
  size_t src_size = static_cast<size_t>(src.size_bytes);
  code = LZ4F_decompress(ctx, dst, &dst_capacity, src.data.data, &src_size, nullptr);
  LZ4F_freeDecompressionContext(ctx);

  if (LZ4F_isError(code)) {
    ArrowErrorSet(error,
                  "LZ4F_decompress([buffer with %" PRId64
                  " bytes] -> [buffer with %" PRId64 " bytes]) failed with error '%s'",
                  src.size_bytes, dst_size, LZ4F_getErrorName(code));
    return EIO;
  }

  if (static_cast<int64_t>(dst_capacity) != dst_size) {
    ArrowErrorSet(error,
                  "Expected decompressed size of %" PRId64 " bytes but got %" PRId64
                  " bytes",
                  dst_size, static_cast<int64_t>(dst_capacity));
    return EIO;
  }

  // The Arrow IPC format requires each compressed buffer to be exactly one LZ4 frame
  if (code != 0 || static_cast<int64_t>(src_size) != src.size_bytes) {
    ArrowErrorSet(error,
                  "Expected a single complete LZ4 frame in buffer with %" PRId64
                  " bytes but %" PRId64 " bytes were consumed",
                  src.size_bytes, static_cast<int64_t>(src_size));
    return EIO;
  }

  return NANOARROW_OK;
}

}  // namespace

nanoarrow::ipc::UniqueDecoder NewDuckDBArrowDecoder() {
  // We could also define a decompressor that uses threads to parallelize
  // decompression for batches with many columns.
  nanoarrow::ipc::UniqueDecompressor decompressor;
  NANOARROW_THROW_NOT_OK(ArrowIpcSerialDecompressor(decompressor.get()));
  NANOARROW_THROW_NOT_OK(ArrowIpcSerialDecompressorSetFunction(
      decompressor.get(), NANOARROW_IPC_COMPRESSION_TYPE_ZSTD, DuckDBDecompressZstd));
  NANOARROW_THROW_NOT_OK(ArrowIpcSerialDecompressorSetFunction(
      decompressor.get(), NANOARROW_IPC_COMPRESSION_TYPE_LZ4_FRAME, DuckDBDecompressLZ4));

  nanoarrow::ipc::UniqueDecoder decoder;
  NANOARROW_THROW_NOT_OK(ArrowIpcDecoderInit(decoder.get()));
  // The decoder takes ownership of the decompressor
  NANOARROW_THROW_NOT_OK(
      ArrowIpcDecoderSetDecompressor(decoder.get(), decompressor.get()));
  return decoder;
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
