//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// writer/column_data_collection_serializer.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "arrow_batch_stats.h"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/async_file_writer.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_properties.hpp"
#include "ipc/codecs.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"
#include "nanoarrow_errors.hpp"

namespace duckdb {
class ArrowAppender;

namespace ext_nanoarrow {

class ColumnDataCollectionSerializer {
 public:
  ColumnDataCollectionSerializer(ClientProperties options, Allocator& allocator,
                                 ArrowIpcCompressionOptions compression = {},
                                 bool track_body_size = false);

  void Init(const ArrowSchema* schema, const vector<LogicalType>& logical_types);

  void SerializeSchema(const ArrowSchema* schema, idx_t reserved_size = 0);

  void SerializeFooter(nanoarrow::UniqueSchema schema,
                       const vector<ArrowIpcFileBlock>& blocks);

  idx_t Serialize(ArrowAppender& appender);

  idx_t Serialize(const ColumnDataCollection& buffer);

  ArrowIpcFileBlock Flush(BufferedFileWriter& writer);
  //! Hands the message to the async writer, which frees it once written
  ArrowIpcFileBlock Flush(AsyncFileWriter& writer);

  //! What the last serialized batch contributes to the file's record batch statistics,
  //! measured from the batch as written. Only set when tracking body sizes.
  const ArrowBatchMeasure& BatchMeasure() const { return batch_measure; }

  nanoarrow::UniqueBuffer GetHeader();

  nanoarrow::UniqueBuffer GetBody();

 private:
  ClientProperties options;
  Allocator& allocator;
  ArrowIpcCompressionOptions compression;
  bool track_body_size;
  ArrowBatchMeasure batch_measure{};
  unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_types;
  nanoarrow::ipc::UniqueEncoder encoder;
  nanoarrow::UniqueArrayView chunk_view;
  nanoarrow::UniqueBuffer header;
  nanoarrow::UniqueBuffer body;
  ArrowError error{};
};

// The writer does not emit dictionary messages or support view layouts
nanoarrow::UniqueSchema CreateArrowIpcSchema(const vector<LogicalType>& types,
                                             const vector<Identifier>& names,
                                             ClientProperties& options);

}  // namespace ext_nanoarrow
}  // namespace duckdb
