#include "file_scanner/arrow_multi_file_info.hpp"

#include "duckdb/common/bind_helpers.hpp"
#include "file_scanner/arrow_file_scan.hpp"
#include "ipc/stream_factory.hpp"

namespace duckdb {
namespace ext_nanoarrow {

unique_ptr<BaseFileReaderOptions> ArrowMultiFileInfo::InitializeOptions(
    ClientContext& context, optional_ptr<TableFunctionInfo> info) {
  return make_uniq<ArrowFileReaderOptions>();
}

bool ArrowMultiFileInfo::ParseCopyOption(ClientContext& context, const string& key,
                                         const vector<Value>& values,
                                         BaseFileReaderOptions& options_p,
                                         vector<string>& expected_names,
                                         vector<LogicalType>& expected_types) {
  // We currently do not have any options for the scanner, so we always return false
  return false;
}

unique_ptr<MultiFileReaderInterface> ArrowMultiFileInfo::CreateInterface(
    ClientContext& context) {
  return make_uniq<ArrowMultiFileInfo>();
}

bool ArrowMultiFileInfo::ParseOption(ClientContext& context, const string& key,
                                     const Value& val, MultiFileOptions& file_options,
                                     BaseFileReaderOptions& options) {
  // We currently do not have any options for the scanner, so we always return false
  return false;
}

void ArrowMultiFileInfo::FinalizeCopyBind(ClientContext& context,
                                          BaseFileReaderOptions& options_p,
                                          const vector<string>& expected_names,
                                          const vector<LogicalType>& expected_types) {}

struct ArrowMultiFileData final : public TableFunctionData {
  ArrowMultiFileData() = default;

  unique_ptr<ArrowFileScan> file_scan;
};

unique_ptr<TableFunctionData> ArrowMultiFileInfo::InitializeBindData(
    MultiFileBindData& multi_file_data, unique_ptr<BaseFileReaderOptions> options_p) {
  return make_uniq<ArrowMultiFileData>();
}

void ArrowMultiFileInfo::BindReader(ClientContext& context,
                                    vector<LogicalType>& return_types,
                                    vector<string>& names, MultiFileBindData& bind_data) {
  ArrowFileReaderOptions options;
  auto& multi_file_list = *bind_data.file_list;
  if (!bind_data.file_options.union_by_name) {
    bind_data.reader_bind = bind_data.multi_file_reader->BindReader(
        context, return_types, names, *bind_data.file_list, bind_data, options,
        bind_data.file_options);

  } else {
    bind_data.reader_bind = bind_data.multi_file_reader->BindUnionReader(
        context, return_types, names, multi_file_list, bind_data, options,
        bind_data.file_options);
  }
  D_ASSERT(names.size() == return_types.size());
}

void ArrowMultiFileInfo::FinalizeBindData(MultiFileBindData& multi_file_data) {}

void ArrowMultiFileInfo::GetBindInfo(const TableFunctionData& bind_data, BindInfo& info) {
}

optional_idx ArrowMultiFileInfo::MaxThreads(const MultiFileBindData& bind_data_p,
                                            const MultiFileGlobalState& global_state,
                                            FileExpandResult expand_result) {
  if (expand_result == FileExpandResult::MULTIPLE_FILES) {
    // always launch max threads if we are reading multiple files
    return {};
  }
  // Otherwise, only one thread
  return 1;
}

unique_ptr<GlobalTableFunctionState> ArrowMultiFileInfo::InitializeGlobalState(
    ClientContext& context, MultiFileBindData& bind_data,
    MultiFileGlobalState& global_state) {
  return make_uniq<ArrowFileGlobalState>(
      context, bind_data.file_list->GetTotalFileCount(), bind_data, global_state);
}

unique_ptr<LocalTableFunctionState> ArrowMultiFileInfo::InitializeLocalState(
    ExecutionContext& context, GlobalTableFunctionState& function_state) {
  return make_uniq<ArrowFileLocalState>(context);
}

shared_ptr<BaseFileReader> ArrowMultiFileInfo::CreateReader(
    ClientContext& context, GlobalTableFunctionState& gstate_p, BaseUnionData& union_data,
    const MultiFileBindData& bind_data) {
  return make_shared_ptr<ArrowFileScan>(context, union_data.GetFileName());
}

shared_ptr<BaseFileReader> ArrowMultiFileInfo::CreateReader(
    ClientContext& context, GlobalTableFunctionState& gstate_p,
    const OpenFileInfo& file_info, idx_t file_idx, const MultiFileBindData& bind_data) {
  return make_shared_ptr<ArrowFileScan>(context, file_info.path);
}

shared_ptr<BaseFileReader> ArrowMultiFileInfo::CreateReader(
    ClientContext& context, const OpenFileInfo& file, BaseFileReaderOptions& options,
    const MultiFileOptions& file_options) {
  return make_shared_ptr<ArrowFileScan>(context, file.path);
}

void ArrowMultiFileInfo::FinishReading(ClientContext& context,
                                       GlobalTableFunctionState& global_state,
                                       LocalTableFunctionState& local_state) {}

unique_ptr<NodeStatistics> ArrowMultiFileInfo::GetCardinality(
    const MultiFileBindData& bind_data, idx_t file_count) {
  // TODO: Here is where we might set statistics, for optimizations if we have them
  // e.g., cardinality from the file footer
  return make_uniq<NodeStatistics>();
}

void ArrowMultiFileInfo::GetVirtualColumns(ClientContext&, MultiFileBindData&,
                                           virtual_column_map_t& result) {
  if (result.find(COLUMN_IDENTIFIER_EMPTY) != result.end()) {
    result.erase(COLUMN_IDENTIFIER_EMPTY);
  }
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
