//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// ipc/stream_reader/ipc_buffer_stream_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ipc/stream_reader/base_stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

struct IPCBuffer {
  idx_t pos = 0;
  data_ptr_t ptr = nullptr;
  int64_t size = 0;
};

//! Buffer Stream
class IPCBufferStreamReader final : public IPCStreamReader {
 public:
  IPCBufferStreamReader(vector<ArrowIPCBuffer> buffers, Allocator& allocator);

  ArrowIpcMessageType ReadNextMessage() override;

 private:
  data_ptr_t ReadData(data_ptr_t ptr, idx_t size) override;
  bool DecodeHeader(idx_t message_header_size) override;
  void DecodeBody() override;
  nanoarrow::UniqueBuffer GetUniqueBuffer() override;
  vector<ArrowIPCBuffer> buffers;
  idx_t cur_idx = 0;
  IPCBuffer header;
  IPCBuffer body;
  IPCBuffer cur_buffer;
  bool initialized = false;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
