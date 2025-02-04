//===----------------------------------------------------------------------===//
//                         DuckDB - NanoArrow
//
// writer/arrow_stream_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "writer/column_data_collection_serializer.hpp"
namespace duckdb {
namespace ext_nanoarrow {

struct ArrowStreamWriter {
  ArrowStreamWriter(ClientContext& context, FileSystem& fs, const string& file_path,
                    const vector<LogicalType>& logical_types,
                    const vector<string>& column_names,
                    const vector<pair<string, string>>& metadata)
      : options(context.GetClientProperties()),
        allocator(BufferAllocator::Get(context)),
        serializer(options, allocator),
        file_name(file_path),
        logical_types(logical_types) {
    InitSchema(logical_types, column_names, metadata);
    InitOutputFile(fs, file_path);
  }

  void InitSchema(const vector<LogicalType>& logical_types,
                  const vector<string>& column_names,
                  const vector<pair<string, string>>& metadata) {
    nanoarrow::UniqueSchema tmp_schema;
    ArrowConverter::ToArrowSchema(tmp_schema.get(), logical_types, column_names, options);

    if (metadata.empty()) {
      ArrowSchemaMove(tmp_schema.get(), schema.get());
    } else {
      nanoarrow::UniqueBuffer metadata_packed;
      NANOARROW_THROW_NOT_OK(
          ArrowMetadataBuilderInit(metadata_packed.get(), tmp_schema->metadata));
      ArrowStringView key;
      ArrowStringView value;
      for (const auto& item : metadata) {
        key = {item.first.data(), static_cast<int64_t>(item.first.size())};
        key = {item.second.data(), static_cast<int64_t>(item.second.size())};
        NANOARROW_THROW_NOT_OK(
            ArrowMetadataBuilderAppend(metadata_packed.get(), key, value));
      }

      NANOARROW_THROW_NOT_OK(ArrowSchemaDeepCopy(tmp_schema.get(), schema.get()));
      NANOARROW_THROW_NOT_OK(ArrowSchemaSetMetadata(
          schema.get(), reinterpret_cast<char*>(metadata_packed->data)));
    }

    serializer.Init(schema.get(), logical_types);
  }

  void InitOutputFile(FileSystem& fs, const string& file_path) {
    writer = make_uniq<BufferedFileWriter>(
        fs, file_path.c_str(),
        FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
  }

  void WriteSchema() {
    serializer.SerializeSchema();
    serializer.Flush(*writer);
  }

  unique_ptr<ColumnDataCollectionSerializer> NewSerializer() {
    auto serializer = make_uniq<ColumnDataCollectionSerializer>(options, allocator);
    serializer->Init(schema.get(), logical_types);
    return serializer;
  }

  void Flush(ColumnDataCollection& buffer) {
    serializer.Serialize(buffer);
    buffer.Reset();
    serializer.Flush(*writer);
    ++row_group_count;
  }

  void Flush(ColumnDataCollectionSerializer& serializer) {
    serializer.Flush(*writer);
    ++row_group_count;
  }

  void Finalize() {
    uint8_t end_of_stream[] = {0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00};
    writer->WriteData(end_of_stream, sizeof(end_of_stream));
    writer->Close();
  }

  idx_t NumberOfRowGroups() { return row_group_count; }

  idx_t FileSize() { return writer->GetTotalWritten(); }

 private:
  ClientProperties options;
  Allocator& allocator;
  ColumnDataCollectionSerializer serializer;
  string file_name;
  vector<LogicalType> logical_types;
  unique_ptr<BufferedFileWriter> writer;
  idx_t row_group_count{0};
  nanoarrow::UniqueSchema schema;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
