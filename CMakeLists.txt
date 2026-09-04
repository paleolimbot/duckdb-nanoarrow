cmake_minimum_required(VERSION 3.5)
include(FetchContent)

# Set extension name here
set(TARGET_NAME nanoarrow)

set(NANOARROW_IPC ON)
set(NANOARROW_NAMESPACE "DuckDBExt${TARGET_NAME}")
fetchcontent_declare(nanoarrow
                     URL "https://github.com/apache/arrow-nanoarrow/archive/9fad80292360d0f3978264c11ba865ccc94020d8.zip"
                     URL_HASH SHA256=7dae6a078981463c7cbf9d430c27056812cdac0963e368f3f1ea6736c5f8280f
)
# nanoarrow builds its own LZ4 decompressor but must be told where liblz4 lives
# In CI the library comes from vcpkg (see vcpkg.json), otherwise a system copy is found
# The extension links liblz4 transitively through nanoarrow_ipc (needs CMake 3.24 or newer)
find_path(LZ4_INCLUDE_DIR lz4frame.h REQUIRED)
find_library(LZ4_LIBRARY NAMES lz4 liblz4 REQUIRED)
get_filename_component(LZ4_LIBRARY_DIR "${LZ4_LIBRARY}" DIRECTORY)
set(NANOARROW_IPC_WITH_LZ4 ON)
set(LZ4_INCLUDE_DIRS "${LZ4_INCLUDE_DIR}")
set(LZ4_LIBRARY_DIRS "${LZ4_LIBRARY_DIR}")
set(LZ4_LIBRARIES "${LZ4_LIBRARY}")
fetchcontent_makeavailable(nanoarrow)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/file_scanner/arrow_file_scan.cpp
    src/file_scanner/arrow_multi_file_info.cpp
    src/ipc/array_stream.cpp
    src/ipc/codecs.cpp
    src/ipc/stream_factory.cpp
    src/ipc/stream_reader/base_stream_reader.cpp
    src/ipc/stream_reader/ipc_file_stream_reader.cpp
    src/ipc/stream_reader/ipc_buffer_stream_reader.cpp
    src/scanner/read_arrow.cpp
    src/scanner/scan_arrow_ipc.cpp
    src/nanoarrow_extension.cpp
    src/writer/arrow_stream_writer.cpp
    src/writer/column_data_collection_serializer.cpp
    src/writer/write_arrow_stream.cpp
    src/writer/to_arrow_ipc.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link nanoarrow in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} nanoarrow::nanoarrow nanoarrow::nanoarrow_ipc)
target_link_libraries(${LOADABLE_EXTENSION_NAME} nanoarrow::nanoarrow
                      nanoarrow::nanoarrow_ipc)

install(TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
