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

//! Creates an IPC decoder that can decompress zstd compressed RecordBatch bodies
//! (using DuckDB's bundled zstd)
nanoarrow::ipc::UniqueDecoder NewDuckDBArrowDecoder();

}  // namespace ext_nanoarrow
}  // namespace duckdb
