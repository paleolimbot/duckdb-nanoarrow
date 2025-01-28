#include "read_arrow.hpp"

#include <inttypes.h>

#include <zstd.h>

#include "duckdb/common/radix.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "nanoarrow/nanoarrow.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

#include "nanoarrow_errors.hpp"
#include "ipc/stream_factory.hpp"
#include "ipc/stream_reader.hpp"

// read_arrow() implementation
//
// This version uses the ArrowIpcDecoder directly. instead of nanoarrow's
// ArrowArrayStream wrapper. This lets it use DuckDB's allocator at the
// expense of a bit more verbosity. Because we can apply the projection
// it reduces some of the verbosity of the actual DuckDB part (although the
// ArrayStreamReader from nanoarrow could support a projection, which
// would handle that too).
//
// I like this version better than the simpler one, and there are more parts
// that could get optimized here (whereas with the array stream version you
// don't have much control).

namespace duckdb {

namespace ext_nanoarrow {

struct ReadArrowStream {
  // Define the function. Unlike arrow_scan(), which takes integer pointers
  // as arguments, we keep the factory alive by making it a member of the bind
  // data (instead of as a Python object whose ownership is kept alive via the
  // DependencyItem mechanism).
  static TableFunction Function() {
    TableFunction fn("read_arrow", {LogicalType::VARCHAR}, Scan, Bind,
                     ArrowTableFunction::ArrowScanInitGlobal,
                     ArrowTableFunction::ArrowScanInitLocal);
    fn.cardinality = Cardinality;
    fn.projection_pushdown = true;
    fn.filter_pushdown = false;
    fn.filter_prune = false;
    return fn;
  }

  static unique_ptr<TableRef> ScanReplacement(ClientContext& context,
                                              ReplacementScanInput& input,
                                              optional_ptr<ReplacementScanData> data) {
    auto table_name = ReplacementScan::GetFullPath(input);
    if (!ReplacementScan::CanReplace(table_name, {"arrows", "arrow"})) {
      return nullptr;
    }

    auto table_function = make_uniq<TableFunctionRef>();
    vector<unique_ptr<ParsedExpression>> children;
    auto table_name_expr = make_uniq<ConstantExpression>(Value(table_name));
    children.push_back(std::move(table_name_expr));
    auto function_expr = make_uniq<FunctionExpression>("read_arrow", std::move(children));
    table_function->function = std::move(function_expr);

    if (!FileSystem::HasGlob(table_name)) {
      auto& fs = FileSystem::GetFileSystem(context);
      table_function->alias = fs.ExtractBaseName(table_name);
    }

    return std::move(table_function);
  }

  // Our FunctionData is the same as the ArrowScanFunctionData except we extend it
  // it to keep the ArrowIpcArrowArrayStreamFactory alive.
  struct Data : public ArrowScanFunctionData {
    Data(std::unique_ptr<ArrowIpcArrowArrayStreamFactory> factory)
        : ArrowScanFunctionData(ArrowIpcArrowArrayStreamFactory::Produce,
                                reinterpret_cast<uintptr_t>(factory.get())),
          factory(std::move(factory)) {}
    std::unique_ptr<ArrowIpcArrowArrayStreamFactory> factory;
  };

  // Our Bind() function is different from the arrow_scan because our input
  // is a filename (and their input is three pointer addresses).
  static unique_ptr<FunctionData> Bind(ClientContext& context,
                                       TableFunctionBindInput& input,
                                       vector<LogicalType>& return_types,
                                       vector<string>& names) {
    return BindInternal(context, input.inputs[0].GetValue<string>(), return_types, names);
  }

  static unique_ptr<FunctionData> BindCopy(ClientContext& context, CopyInfo& info,
                                           vector<string>& expected_names,
                                           vector<LogicalType>& expected_types) {
    return BindInternal(context, info.file_path, expected_types, expected_names);
  }

  static unique_ptr<FunctionData> BindInternal(ClientContext& context, std::string src,
                                               vector<LogicalType>& return_types,
                                               vector<string>& names) {
    auto stream_factory = make_uniq<ArrowIpcArrowArrayStreamFactory>(context, src);
    auto res = make_uniq<Data>(std::move(stream_factory));
    res->factory->InitReader();
    res->factory->GetFileSchema(res->schema_root);

    DBConfig& config = DatabaseInstance::GetDatabase(context).config;
    ArrowTableFunction::PopulateArrowTableType(config, res->arrow_table, res->schema_root,
                                               names, return_types);
    QueryResult::DeduplicateColumns(names);
    res->all_types = return_types;
    if (return_types.empty()) {
      throw InvalidInputException(
          "Provided table/dataframe must have at least one column");
    }

    return std::move(res);
  }

  static void Scan(ClientContext& context, TableFunctionInput& data_p,
                   DataChunk& output) {
    ArrowTableFunction::ArrowScanFunction(context, data_p, output);
  }

  // Identical to the ArrowTableFunction, but that version is marked protected
  static unique_ptr<NodeStatistics> Cardinality(ClientContext& context,
                                                const FunctionData* data) {
    return make_uniq<NodeStatistics>();
  }
};

// A version of ArrowDecompressZstd that uses DuckDB's C++ namespaceified
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

  if (dst_size != (int64_t)code) {
    ArrowErrorSet(error,
                  "Expected decompressed size of %" PRId64 " bytes but got %" PRId64
                  " bytes",
                  dst_size, (int64_t)code);
    return EIO;
  }

  return NANOARROW_OK;
}

// Create an ArrowIpcDecoder() with the appropriate decompressor set.
// We could also define a decompressor that uses threads to parellelize
// decompression for batches with many columns.
nanoarrow::ipc::UniqueDecoder IpcStreamReader::NewDuckDBArrowDecoder() {
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


unique_ptr<FunctionData> ReadArrowStreamBindCopy(ClientContext& context, CopyInfo& info,
                                                 vector<string>& expected_names,
                                                 vector<LogicalType>& expected_types) {
  return ReadArrowStream::BindCopy(context, info, expected_names, expected_types);
}

TableFunction ReadArrowStreamFunction() { return ReadArrowStream::Function(); }

void RegisterReadArrowStream(DatabaseInstance& db) {
  auto function = ReadArrowStream::Function();
  ExtensionUtil::RegisterFunction(db, function);
  function.name = "scan_arrow_ipc";
  ExtensionUtil::RegisterFunction(db, function);
  auto& config = DBConfig::GetConfig(db);
  config.replacement_scans.emplace_back(ReadArrowStream::ScanReplacement);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
