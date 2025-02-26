#include "ipc/stream_reader/base_stream_reader.hpp"
#include <cinttypes>
#include <iostream>
#include "zstd.h"

namespace duckdb {
namespace ext_nanoarrow {

// A version of ArrowDecompressZstd that uses DuckDB's C++ name-specified
// zstd.h header that doesn't work with a C compiler
static ArrowErrorCode DuckDBDecompressZstd(struct ArrowBufferView src, uint8_t* dst,
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

// Create an ArrowIpcDecoder() with the appropriate decompressor set.
// We could also define a decompressor that uses threads to parellelize
// decompression for batches with many columns.
nanoarrow::ipc::UniqueDecoder IPCStreamReader::NewDuckDBArrowDecoder() {
  nanoarrow::ipc::UniqueDecompressor decompressor;
  NANOARROW_THROW_NOT_OK(ArrowIpcSerialDecompressor(decompressor.get()));
  NANOARROW_THROW_NOT_OK(ArrowIpcSerialDecompressorSetFunction(
      decompressor.get(), NANOARROW_IPC_COMPRESSION_TYPE_ZSTD, DuckDBDecompressZstd));

  nanoarrow::ipc::UniqueDecoder decoder;
  NANOARROW_THROW_NOT_OK(ArrowIpcDecoderInit(decoder.get()));
  NANOARROW_THROW_NOT_OK(
      ArrowIpcDecoderSetDecompressor(decoder.get(), decompressor.get()));
  // Bug in nanoarrow!
  decompressor->release = nullptr;
  return decoder;
}

const ArrowSchema* IPCStreamReader::GetBaseSchema() {
  if (base_schema->release) {
    return base_schema.get();
  }

  ReadNextMessage({NANOARROW_IPC_MESSAGE_TYPE_SCHEMA}, /*end_of_stream_ok*/ false);

  if (decoder->feature_flags & NANOARROW_IPC_FEATURE_DICTIONARY_REPLACEMENT) {
    throw IOException("This stream uses unsupported feature DICTIONARY_REPLACEMENT");
  }

  // Decode the schema
  THROW_NOT_OK(IOException, &error,
               ArrowIpcDecoderDecodeSchema(decoder.get(), base_schema.get(), &error));

  // Set up the decoder to decode batches
  THROW_NOT_OK(InternalException, &error,
               ArrowIpcDecoderSetEndianness(decoder.get(), decoder->endianness));
  THROW_NOT_OK(InternalException, &error,
               ArrowIpcDecoderSetSchema(decoder.get(), base_schema.get(), &error));

  return base_schema.get();
}

bool IPCStreamReader::HasProjection() const { return !projected_fields.empty(); }

const ArrowSchema* IPCStreamReader::GetOutputSchema() {
  if (HasProjection()) {
    return projected_schema.get();
  } else {
    return GetBaseSchema();
  }
}

bool IPCStreamReader::GetNextBatch(ArrowArray* out) {
  // When nanoarrow supports dictionary batches, we'd accept either a
  // RecordBatch or DictionaryBatch message, recording the dictionary batch
  // (or possibly ignoring it if it is for a field that we don't care about),
  // but looping until we end up with a RecordBatch in the decoder.
  ArrowIpcMessageType message_type =
      ReadNextMessage({NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH});
  if (message_type == NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED) {
    out->release = nullptr;
    return false;
  }

  // Use the ArrowIpcSharedBuffer if we have thread safety (i.e., if this was
  // compiled with a compiler that supports C11 atomics, i.e., not gcc 4.8 or
  // MSVC)
  bool thread_safe_shared = ArrowIpcSharedBufferIsThreadSafe();
  struct ArrowBufferView body_view = AllocatedDataView(cur_ptr, cur_size);
  nanoarrow::UniqueBuffer body_shared = GetUniqueBuffer();
  UniqueSharedBuffer shared;
  NANOARROW_THROW_NOT_OK(ArrowIpcSharedBufferInit(&shared.data, body_shared.get()));
  nanoarrow::UniqueArray array;
  if (HasProjection()) {
    NANOARROW_THROW_NOT_OK(ArrowArrayInitFromType(array.get(), NANOARROW_TYPE_STRUCT));
    NANOARROW_THROW_NOT_OK(
        ArrowArrayAllocateChildren(array.get(), GetOutputSchema()->n_children));

    if (thread_safe_shared) {
      for (int64_t i = 0; i < array->n_children; i++) {
        THROW_NOT_OK(InternalException, &error,
                     ArrowIpcDecoderDecodeArrayFromShared(
                         decoder.get(), &shared.data, projected_fields[i],
                         array->children[i], NANOARROW_VALIDATION_LEVEL_FULL, &error));
      }
    } else {
      for (int64_t i = 0; i < array->n_children; i++) {
        THROW_NOT_OK(InternalException, &error,
                     ArrowIpcDecoderDecodeArray(decoder.get(), body_view,
                                                projected_fields[i], array->children[i],
                                                NANOARROW_VALIDATION_LEVEL_FULL, &error));
      }
    }

    D_ASSERT(array->n_children > 0);
    array->length = array->children[0]->length;
    array->null_count = 0;
  } else if (thread_safe_shared) {
    THROW_NOT_OK(
        InternalException, &error,
        ArrowIpcDecoderDecodeArrayFromShared(decoder.get(), &shared.data, -1, array.get(),
                                             NANOARROW_VALIDATION_LEVEL_FULL, &error));
  } else {
    THROW_NOT_OK(InternalException, &error,
                 ArrowIpcDecoderDecodeArray(decoder.get(), body_view, -1, array.get(),
                                            NANOARROW_VALIDATION_LEVEL_FULL, &error));
  }

  ArrowArrayMove(array.get(), out);
  return true;
}

void IPCStreamReader::SetColumnProjection(const vector<string>& column_names) {
  if (column_names.empty()) {
    throw InternalException("Can't request zero fields projected from IpcStreamReader");
  }

  // Ensure we have a file schema to work with
  GetBaseSchema();

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  NANOARROW_THROW_NOT_OK(ArrowSchemaSetTypeStruct(
      schema.get(), UnsafeNumericCast<int64_t>(column_names.size())));

  // The ArrowArray builder needs the flattened field index, which we need to
  // keep track of.
  unordered_map<string, pair<int64_t, const ArrowSchema*>> name_to_flat_field_map;

  // Duplicate column names are in theory fine as long as they are not queried,
  // so we need to make a list of them to check.
  unordered_set<string> duplicate_column_names;

  // Loop over columns to build the field map
  int64_t field_count = 0;
  for (int64_t i = 0; i < base_schema->n_children; i++) {
    const ArrowSchema* column = base_schema->children[i];
    string name;
    if (!column->name) {
      name = "";
    } else {
      name = column->name;
    }

    if (name_to_flat_field_map.find(name) != name_to_flat_field_map.end()) {
      duplicate_column_names.insert(name);
    }

    name_to_flat_field_map.insert({name, {field_count, column}});

    field_count += CountFields(column);
  }

  // Loop over projected column names to build the projection information
  int64_t output_column_index = 0;
  for (const auto& column_name : column_names) {
    if (duplicate_column_names.find(column_name) != duplicate_column_names.end()) {
      throw InternalException(string("Field '") + column_name +
                              "' refers to a duplicate column name in IPC file schema");
    }

    auto field_id_item = name_to_flat_field_map.find(column_name);
    if (field_id_item == name_to_flat_field_map.end()) {
      throw InternalException(string("Field '") + column_name +
                              "' does not exist in IPC file schema");
    }

    // Record the flat field index for this column
    projected_fields.push_back(field_id_item->second.first);

    // Record the Schema for this column
    NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(field_id_item->second.second,
                                               schema->children[output_column_index]));

    ++output_column_index;
  }
  projected_schema = std::move(schema);
}

idx_t IPCStreamReader::DecodeMetadata() const {
  idx_t metadata_size;
  if (!Radix::IsLittleEndian()) {
    metadata_size = static_cast<int32_t>(BSWAP32(message_prefix.metadata_size));
  } else {
    metadata_size = message_prefix.metadata_size;
  }

  if (metadata_size < 0) {
    throw IOException(std::string("Expected metadata size >= 0 but got " +
                                  std::to_string(metadata_size)));
  }
  return metadata_size + sizeof(message_prefix);
}

ArrowIpcMessageType IPCStreamReader::DecodeMessage() {
  auto message_header_size = DecodeMetadata();
  if (DecodeHeader(message_header_size)) {
    return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
  }
  DecodeBody();
  return decoder->message_type;
}

ArrowIpcMessageType IPCStreamReader::ReadNextMessage(
    vector<ArrowIpcMessageType> expected_types, bool end_of_stream_ok) {
  ArrowIpcMessageType actual_type = ReadNextMessage();
  if (end_of_stream_ok && actual_type == NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED) {
    return actual_type;
  }

  for (const auto expected_type : expected_types) {
    if (expected_type == actual_type) {
      return actual_type;
    }
  }

  std::stringstream expected_types_label;
  for (size_t i = 0; i < expected_types.size(); i++) {
    if (i > 0) {
      expected_types_label << " or ";
    }

    expected_types_label << MessageTypeString(expected_types[i]);
  }

  string actual_type_label;
  if (actual_type == NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED) {
    actual_type_label = "end of stream";
  } else {
    actual_type_label = MessageTypeString(actual_type);
  }

  throw IOException(string("Expected ") + expected_types_label.str() +
                    " Arrow IPC message but got " + actual_type_label);
}

int64_t IPCStreamReader::CountFields(const ArrowSchema* schema) {
  int64_t n_fields = 1;
  for (int64_t i = 0; i < schema->n_children; i++) {
    n_fields += CountFields(schema->children[i]);
  }
  return n_fields;
}

ArrowBufferView IPCStreamReader::AllocatedDataView(const_data_ptr_t data, int64_t size) {
  ArrowBufferView view;
  view.data.data = data;
  view.size_bytes = size;
  return view;
}

nanoarrow::UniqueBuffer IPCStreamReader::AllocatedDataToOwningBuffer(
    shared_ptr<AllocatedData> data) {
  nanoarrow::UniqueBuffer out;
  nanoarrow::BufferInitWrapped(out.get(), data, data->get(),
                               UnsafeNumericCast<int64_t>(data->GetSize()));
  return out;
}

const char* IPCStreamReader::MessageTypeString(ArrowIpcMessageType message_type) {
  switch (message_type) {
    case NANOARROW_IPC_MESSAGE_TYPE_SCHEMA:
      return "Schema";
    case NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH:
      return "RecordBatch";
    case NANOARROW_IPC_MESSAGE_TYPE_DICTIONARY_BATCH:
      return "DictionaryBatch";
    case NANOARROW_IPC_MESSAGE_TYPE_TENSOR:
      return "Tensor";
    case NANOARROW_IPC_MESSAGE_TYPE_SPARSE_TENSOR:
      return "SparseTensor";
    case NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED:
      return "Uninitialized";
    default:
      return "";
  }
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
