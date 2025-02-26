#include "ipc/stream_reader/ipc_buffer_stream_reader.hpp"

#include <iostream>

namespace duckdb {
namespace ext_nanoarrow {

IPCBufferStreamReader::IPCBufferStreamReader(vector<ArrowIPCBuffer> buffers,
                                             Allocator& allocator)
    : IPCStreamReader(allocator), buffers(std::move(buffers)) {}

ArrowIpcMessageType IPCBufferStreamReader::ReadNextMessage() {
  if ((!initialized && cur_idx == buffers.size()) || finished) {
    finished = true;
    return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
  }
  if (!initialized || cur_buffer.pos >= buffers[cur_idx].size) {
    if (initialized) {
      cur_idx++;
    }
    if (cur_idx >= buffers.size()) {
      finished = true;
      return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
    }
    cur_buffer.ptr = reinterpret_cast<data_ptr_t>(buffers[cur_idx].ptr);
    cur_buffer.size = static_cast<int64_t>(buffers[cur_idx].size);
    cur_buffer.pos = 0;
    initialized = true;
  }
  auto* message_prefix_ptr = reinterpret_cast<const ArrowIpcMessagePrefix*>(
      ReadData(reinterpret_cast<data_ptr_t>(&message_prefix), sizeof(message_prefix)));
  message_prefix = *message_prefix_ptr;
  return DecodeMessage();
}

data_ptr_t IPCBufferStreamReader::ReadData(data_ptr_t ptr, idx_t size) {
  D_ASSERT(size + cur_buffer.pos < cur_buffer.size);
  data_ptr_t cur_ptr = cur_buffer.ptr + cur_buffer.pos;
  cur_buffer.pos += size;
  return cur_ptr;
}

bool IPCBufferStreamReader::DecodeHeader(idx_t message_header_size) {
  // Our Header must contain the message prefix
  header.ptr =
      ReadData(header.ptr, message_prefix.metadata_size) - sizeof(message_prefix);
  header.size = message_header_size;
  const ArrowErrorCode decode_header_status = ArrowIpcDecoderDecodeHeader(
      decoder.get(), AllocatedDataView(header.ptr, header.size), &error);
  if (decode_header_status == ENODATA) {
    finished = true;
    return true;
  }
  THROW_NOT_OK(IOException, &error, decode_header_status);
  return false;
}

void IPCBufferStreamReader::DecodeBody() {
  if (decoder->body_size_bytes > 0) {
    body.ptr = ReadData(body.ptr, decoder->body_size_bytes);
  }
  if (body.ptr) {
    cur_ptr = body.ptr;
    cur_size = body.size;
  } else {
    cur_ptr = nullptr;
    cur_size = 0;
  }
}

nanoarrow::UniqueBuffer IPCBufferStreamReader::GetUniqueBuffer() {
  nanoarrow::UniqueBuffer out;
  nanoarrow::BufferInitWrapped(out.get(), body, body.ptr, body.size);
  return out;
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
