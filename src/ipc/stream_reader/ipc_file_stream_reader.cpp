

#include "ipc/stream_reader/ipc_file_stream_reader.hpp"

namespace duckdb {
namespace ext_nanoarrow {
IPCFileStreamReader::IPCFileStreamReader(FileSystem& fs, unique_ptr<FileHandle> handle,
                                         Allocator& allocator)
    : IPCStreamReader(allocator), file_reader(fs, std::move(handle)) {}

void IPCFileStreamReader::PopulateNames(vector<string>& names) {
  GetBaseSchema();
  for (int64_t i = 0; i < base_schema->n_children; i++) {
    const ArrowSchema* column = base_schema->children[i];
    if (!column->name) {
      names.push_back("");
    } else {
      names.push_back(column->name);
    }
  }
}

void IPCFileStreamReader::DecodeArray(nanoarrow::ipc::UniqueDecoder& decoder,
                                      ArrowArray* out, ArrowBufferView& body_view,
                                      ArrowError* error) {
  // Use the ArrowIpcSharedBuffer if we have thread safety (i.e., if this was
  // compiled with a compiler that supports C11 atomics, i.e., not gcc 4.8 or
  // MSVC)
  nanoarrow::UniqueArray array;
  THROW_NOT_OK(InternalException, error,
               ArrowIpcDecoderDecodeArray(decoder.get(), body_view, -1, array.get(),
                                          NANOARROW_VALIDATION_LEVEL_FULL, error));
  ArrowArrayMove(array.get(), out);
}

nanoarrow::UniqueBuffer IPCFileStreamReader::GetUniqueBuffer() {
  return AllocatedDataToOwningBuffer(message_body);
}
bool IPCFileStreamReader::DecodeHeader(const idx_t message_header_size) {
  if (message_header.GetSize() < message_header_size) {
    message_header = allocator.Allocate(message_header_size);
  }
  // Read the message header. I believe the fact that this loops and calls
  // the file handle's Read() method with relatively small chunks will ensure that
  // an attempt to read a very large message_header_size can be cancelled. If this
  // is not the case, we might want to implement our own buffering.
  std::memcpy(message_header.get(), &message_prefix, sizeof(message_prefix));
  ReadData(message_header.get() + sizeof(message_prefix), message_prefix.metadata_size);

  ArrowErrorCode decode_header_status = ArrowIpcDecoderDecodeHeader(
      decoder.get(),
      AllocatedDataView(message_header.get(),
                        static_cast<int64_t>(message_header.GetSize())),
      &error);
  if (decode_header_status == ENODATA) {
    finished = true;
    return true;
  }
  THROW_NOT_OK(IOException, &error, decode_header_status);
  return false;
}

void IPCFileStreamReader::DecodeBody() {
  if (decoder->body_size_bytes > 0) {
    EnsureInputStreamAligned();
    message_body =
        make_shared_ptr<AllocatedData>(allocator.Allocate(decoder->body_size_bytes));

    // Again, this is possibly a long running Read() call for a large body.
    // We could possibly be smarter about how we do this, particularly if we
    // are reading a small portion of the input from a seekable file.
    ReadData(message_body->get(), decoder->body_size_bytes);
  }
  if (message_body) {
    cur_ptr = message_body->get();
    cur_size = static_cast<int64_t>(message_body->GetSize());
  } else {
    cur_ptr = nullptr;
    cur_size = 0;
  }
}

data_ptr_t IPCFileStreamReader::ReadData(data_ptr_t ptr, idx_t size) {
  file_reader.ReadData(ptr, size);
  return ptr;
}

ArrowIpcMessageType IPCFileStreamReader::ReadNextMessage() {
  if (finished || file_reader.Finished()) {
    finished = true;
    return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
  }

  // If there is no more data to be read, we're done!
  try {
    EnsureInputStreamAligned();
    file_reader.ReadData(reinterpret_cast<data_ptr_t>(&message_prefix),
                         sizeof(message_prefix));
  } catch (SerializationException& e) {
    finished = true;
    return NANOARROW_IPC_MESSAGE_TYPE_UNINITIALIZED;
  }
  // If we're at the beginning of the read, and we see the Arrow file format
  // header bytes, skip them and try to read the stream anyway. This works because
  // there's a full stream within an Arrow file (including the EOS indicator, which
  // is key to success. This EOS indicator is unfortunately missing in Rust releases
  // prior to ~September 2024).
  //
  // When we support dictionary encoding we will possibly need to seek to the footer
  // here, parse that, and maybe lazily seek and read dictionaries for if/when they are
  // required.
  if (file_reader.CurrentOffset() == 8 &&
      std::memcmp("ARROW1\0\0", &message_prefix, 8) == 0) {
    return ReadNextMessage();
  }

  if (message_prefix.continuation_token != kContinuationToken) {
    throw IOException(std::string("Expected continuation token (0xFFFFFFFF) but got " +
                                  std::to_string(message_prefix.continuation_token)));
  }
  return DecodeMessage();
}

void IPCFileStreamReader::EnsureInputStreamAligned() {
  uint8_t padding[8];
  int padding_bytes = 8 - (file_reader.CurrentOffset() % 8);
  if (padding_bytes != 8) {
    file_reader.ReadData(padding, padding_bytes);
  }
  D_ASSERT((file_reader.CurrentOffset() % 8) == 0);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
