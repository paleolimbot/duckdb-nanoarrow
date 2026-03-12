//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// file_scanner/arrow_multi_file_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "duckdb/function/table/arrow.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! We might have arrow specific options one day
class ArrowFileReaderOptions : public BaseFileReaderOptions {};

class ArrowFileScan;

//! The Arrow Local File State, basically refers to the Scan of one Arrow File
//! This is done by calling the Arrow Scan directly on one file.
struct ArrowFileLocalState : public LocalTableFunctionState {
 public:
  explicit ArrowFileLocalState(ExecutionContext& execution_context)
      : execution_context(execution_context) {};
  //! Factory Pointer
  shared_ptr<ArrowFileScan> file_scan;

  ExecutionContext& execution_context;

  //! Each local state refers to an Arrow Scan on a local file
  unique_ptr<ArrowScanFunctionData> local_arrow_function_data;
  unique_ptr<TableFunctionInitInput> init_input;
  unique_ptr<GlobalTableFunctionState> local_arrow_global_state;
  unique_ptr<LocalTableFunctionState> local_arrow_local_state;
  unique_ptr<TableFunctionInput> table_function_input;
};

struct ArrowFileGlobalState : public GlobalTableFunctionState {
 public:
  ArrowFileGlobalState(ClientContext& context_p, idx_t total_file_count,
                       const MultiFileBindData& bind_data,
                       MultiFileGlobalState& global_state)
      : global_state(global_state), context(context_p) {};

  ~ArrowFileGlobalState() override = default;

  const MultiFileGlobalState& global_state;
  ClientContext& context;
  set<idx_t> files;
};

struct ArrowMultiFileInfo : MultiFileReaderInterface {
  unique_ptr<BaseFileReaderOptions> InitializeOptions(
      ClientContext& context, optional_ptr<TableFunctionInfo> info) override;

  static unique_ptr<MultiFileReaderInterface> CreateInterface(ClientContext& context);

  bool ParseCopyOption(ClientContext& context, const string& key,
                       const vector<Value>& values, BaseFileReaderOptions& options,
                       vector<string>& expected_names,
                       vector<LogicalType>& expected_types) override;

  bool ParseOption(ClientContext& context, const string& key, const Value& val,
                   MultiFileOptions& file_options,
                   BaseFileReaderOptions& options) override;

  void FinalizeCopyBind(ClientContext& context, BaseFileReaderOptions& options,
                        const vector<string>& expected_names,
                        const vector<LogicalType>& expected_types) override;

  unique_ptr<TableFunctionData> InitializeBindData(
      MultiFileBindData& multi_file_data,
      unique_ptr<BaseFileReaderOptions> options) override;

  //! This is where the actual binding must happen, so in this function we either:
  //! 1. union_by_name = False. We set the schema/name depending on the first file
  //! 2. union_by_name = True.
  void BindReader(ClientContext& context, vector<LogicalType>& return_types,
                  vector<string>& names, MultiFileBindData& bind_data) override;

  void FinalizeBindData(MultiFileBindData& multi_file_data) override;

  void GetBindInfo(const TableFunctionData& bind_data, BindInfo& info) override;

  optional_idx MaxThreads(const MultiFileBindData& bind_data_p,
                          const MultiFileGlobalState& global_state,
                          FileExpandResult expand_result) override;

  unique_ptr<GlobalTableFunctionState> InitializeGlobalState(
      ClientContext& context, MultiFileBindData& bind_data,
      MultiFileGlobalState& global_state) override;

  unique_ptr<LocalTableFunctionState> InitializeLocalState(
      ExecutionContext& context, GlobalTableFunctionState& function_state) override;

  shared_ptr<BaseFileReader> CreateReader(ClientContext& context,
                                          GlobalTableFunctionState& gstate,
                                          BaseUnionData& union_data,
                                          const MultiFileBindData& bind_data_p) override;

  shared_ptr<BaseFileReader> CreateReader(ClientContext& context,
                                          GlobalTableFunctionState& gstate,
                                          const OpenFileInfo& file_info, idx_t file_idx,
                                          const MultiFileBindData& bind_data) override;

  shared_ptr<BaseFileReader> CreateReader(ClientContext& context,
                                          const OpenFileInfo& file,
                                          BaseFileReaderOptions& options,
                                          const MultiFileOptions& file_options) override;

  void FinishReading(ClientContext& context, GlobalTableFunctionState& global_state,
                     LocalTableFunctionState& local_state) override;

  unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData& bind_data,
                                            idx_t file_count) override;

  void GetVirtualColumns(ClientContext& context, MultiFileBindData& bind_data,
                         virtual_column_map_t& result) override;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
