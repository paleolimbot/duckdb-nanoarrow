//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/codecs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "nanoarrow/nanoarrow_ipc.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! Creates an IPC decoder that can decompress zstd (using DuckDB's bundled zstd) and
//! lz4 (using nanoarrow's built-in codec) compressed RecordBatch bodies
nanoarrow::ipc::UniqueDecoder NewDuckDBArrowDecoder();

}  // namespace ext_nanoarrow
}  // namespace duckdb
