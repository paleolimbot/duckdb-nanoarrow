#include "duckdb/function/table/arrow.hpp"

#include "file_scanner/arrow_file_scan.hpp"
#include "file_scanner/arrow_multi_file_info.hpp"
#include "ipc/stream_reader/ipc_file_stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {
struct ArrowFileLocalState;

ArrowFileScan::ArrowFileScan(ClientContext& context, const string& file_name)
    : BaseFileReader(OpenFileInfo(file_name)) {
  factory = make_uniq<FileIPCStreamFactory>(context, file_name);

  factory->InitReader();
  factory->GetFileSchema(schema_root);
  ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table,
                                               schema_root.arrow_schema);
  names = arrow_table.GetNames();
  types = arrow_table.GetTypes();
  if (types.empty()) {
    throw InvalidInputException("Provided table/dataframe must have at least one column");
  }
  columns = MultiFileColumnDefinition::ColumnsFromNamesAndTypes(names, types);
}

string ArrowFileScan::GetReaderType() const { return "ARROW"; }

const vector<string>& ArrowFileScan::GetNames() { return names; }
const vector<LogicalType>& ArrowFileScan::GetTypes() { return types; }

bool ArrowFileScan::TryInitializeScan(ClientContext& context,
                                      GlobalTableFunctionState& gstate_p,
                                      LocalTableFunctionState& lstate_p) {
  auto& gstate = gstate_p.Cast<ArrowFileGlobalState>();
  auto& lstate = lstate_p.Cast<ArrowFileLocalState>();
  if (gstate.files.find(file_list_idx.GetIndex()) != gstate.files.end()) {
    // Return false because we don't currently support more than one thread
    // scanning a file. In the future we may be able to support this by (e.g.)
    // reading the Arrow file footer or sending a thread to read ahead to scan
    // for RecordBatch messages.
    return false;
  }
  gstate.files.insert(file_list_idx.GetIndex());

  // lstate.file_scan = shared_ptr_cast<BaseFileReader, ArrowFileScan>(this);
  lstate.local_arrow_function_data = make_uniq<ArrowScanFunctionData>(
      &FileIPCStreamFactory::Produce, reinterpret_cast<uintptr_t>(factory.get()));
  lstate.local_arrow_function_data->schema_root = schema_root;
  lstate.local_arrow_function_data->arrow_table = arrow_table;
  if (!column_indexes.empty()) {
    lstate.init_input = make_uniq<TableFunctionInitInput>(
        *lstate.local_arrow_function_data, column_indexes,
        gstate.global_state.projection_ids, filters);
  } else {
    lstate.init_input = make_uniq<TableFunctionInitInput>(
        *lstate.local_arrow_function_data, gstate.global_state.column_indexes,
        gstate.global_state.projection_ids, filters);
  }
  lstate.local_arrow_global_state =
      ArrowTableFunction::ArrowScanInitGlobal(context, *lstate.init_input);
  lstate.local_arrow_local_state =
      ArrowTableFunction::ArrowScanInitLocal(lstate.execution_context, *lstate.init_input,
                                             lstate.local_arrow_global_state.get());
  lstate.table_function_input = make_uniq<TableFunctionInput>(
      lstate.local_arrow_function_data.get(), lstate.local_arrow_local_state.get(),
      lstate.local_arrow_global_state.get());
  return true;
}
AsyncResult ArrowFileScan::Scan(ClientContext& context,
                                GlobalTableFunctionState& global_state,
                                LocalTableFunctionState& local_state, DataChunk& chunk) {
  auto& lstate = local_state.Cast<ArrowFileLocalState>();
  ArrowTableFunction::ArrowScanFunction(context, *lstate.table_function_input, chunk);
  if (chunk.size() == 0) {
    return SourceResultType::FINISHED;
  }
  return SourceResultType::HAVE_MORE_OUTPUT;
}

double ArrowFileScan::GetProgressInFile(ClientContext& context) {
  if (!factory->reader) {
    return 100;
  }
  auto file_reader =
      static_cast<IPCFileStreamReader*>(factory->reader.get());
  return file_reader->GetProgress();
}

shared_ptr<BaseUnionData> ArrowFileScan::GetUnionData(idx_t file_idx) {
  auto data = make_shared_ptr<BaseUnionData>(OpenFileInfo(GetFileName()));
  data->names = GetNames();
  data->types = GetTypes();
  return data;
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
