//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// file_scanner/arrow_file_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/stream_factory.hpp"

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/parallel/async_result.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! This class refers to an Arrow File Scan
class ArrowFileScan : public BaseFileReader {
 public:
  explicit ArrowFileScan(ClientContext& context, const string& file_name);
  ~ArrowFileScan() override {
    // Release is done by the arrow scanner
    schema_root.arrow_schema.release = nullptr;
  };

  //! Factory of this stream
  unique_ptr<FileIPCStreamFactory> factory;

  string GetReaderType() const override;

  const vector<string>& GetNames();
  const vector<LogicalType>& GetTypes();
  ArrowSchemaWrapper schema_root;
  ArrowTableSchema arrow_table;

  bool TryInitializeScan(ClientContext& context, GlobalTableFunctionState& gstate,
                         LocalTableFunctionState& lstate) override;
  AsyncResult Scan(ClientContext& context, GlobalTableFunctionState& global_state,
                   LocalTableFunctionState& local_state, DataChunk& chunk) override;
  double GetProgressInFile(ClientContext& context) override;

  shared_ptr<BaseUnionData> GetUnionData(idx_t file_idx) override;

 private:
  vector<string> names;
  vector<LogicalType> types;
};
}  // namespace ext_nanoarrow
}  // namespace duckdb
