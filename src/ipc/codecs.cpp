#include "ipc/codecs.hpp"

#include <cinttypes>

#include "nanoarrow_errors.hpp"
#include "zstd.h"

namespace duckdb {
namespace ext_nanoarrow {

namespace {

//===----------------------------------------------------------------------===//
// Decompression
//
// nanoarrow's own zstd support is compiled in only when nanoarrow is built
// against zstd, and DuckDB's bundled zstd header (which lives in a C++ namespace)
// can't be used from nanoarrow's C sources anyway. Instead we plug our own
// decompression function into nanoarrow's serial decompressor.
//===----------------------------------------------------------------------===//

ArrowErrorCode DuckDBDecompressZstd(struct ArrowBufferView src, uint8_t* dst,
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

}  // namespace

nanoarrow::ipc::UniqueDecoder NewDuckDBArrowDecoder() {
  // We could also define a decompressor that uses threads to parallelize
  // decompression for batches with many columns.
  nanoarrow::ipc::UniqueDecompressor decompressor;
  NANOARROW_THROW_NOT_OK(ArrowIpcSerialDecompressor(decompressor.get()));
  NANOARROW_THROW_NOT_OK(ArrowIpcSerialDecompressorSetFunction(
      decompressor.get(), NANOARROW_IPC_COMPRESSION_TYPE_ZSTD, DuckDBDecompressZstd));

  nanoarrow::ipc::UniqueDecoder decoder;
  NANOARROW_THROW_NOT_OK(ArrowIpcDecoderInit(decoder.get()));
  // The decoder takes ownership of the decompressor
  NANOARROW_THROW_NOT_OK(
      ArrowIpcDecoderSetDecompressor(decoder.get(), decompressor.get()));
  return decoder;
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
