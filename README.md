# nanoarrow for DuckDB

This extension, nanoarrow, allows you to read Arrow IPC streams and files. It serves a similar purpose as the now-deprecated [Arrow DuckDB core extension](https://github.com/duckdb/arrow).
However, it comes with the added functionality to query Arrow IPC files and is much better tested. This extension is released as a DuckDB Community Extension.
For compatibility reasons with the previous Arrow core extension, this extension is also aliased as `arrow`.

You can install and load it as:

```sql
-- arrow would also be a suitable name
INSTALL nanoarrow FROM community;
LOAD nanoarrow;
```

## Usage
Below is a complete example of how to use our extension to read an Arrow IPC file.
In addition to our extension, you will also need the `httpfs` extension installed and loaded to fetch the data directly from GitHub.

```sql
LOAD httpfs;
LOAD nanoarrow;
SELECT
    commit, message
  FROM
    'https://github.com/apache/arrow-experiments/raw/refs/heads/main/data/arrow-commits/arrow-commits.arrows'
  LIMIT 10;
```

```
┌───────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────┐
│          commit           │                                          message                                          │
│          varchar          │                                          varchar                                          │
├───────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────┤
│ 49cdb0fe4e98fda19031c86…  │ GH-40370: [C++] Define ARROW_FORCE_INLINE for non-MSVC builds (#40372)                    │
│ 1d966e98e41ce817d1f8c51…  │ GH-40386: [Python] Fix except clauses (#40387)                                            │
│ 96f26a89bd73997f7532643…  │ GH-40227: [R] ensure executable files in `create_package_with_all_dependencies` (#40232)  │
│ ee1a8c39a55f3543a82fed9…  │ GH-40366: [C++] Remove const qualifier from Buffer::mutable_span_as (#40367)              │
│ 3d467ac7bfae03cf2db0980…  │ GH-20127: [Python][CI] Remove legacy hdfs tests from hdfs and hypothesis setup (#40363)   │
│ ef6ea6beed071ed070daf03…  │ GH-40345: [FlightRPC][C++][Java][Go] Add URI scheme to reuse connection (#40084)          │
│ 53e0c745ad491af98a5bf18…  │ GH-40153: [C++][Python] Fix test_gdb failures on 32-bit (#40293)                          │
│ 3ba6d286caad328b8572a3b…  │ GH-40059: [C++][Python] Basic conversion of RecordBatch to Arrow Tensor (#40064)          │
│ 4ce9a5edd2710fb8bf0c642…  │ GH-40153: [Python] Make `Tensor.__getbuffer__` work on 32-bit platforms (#40294)          │
│ 2445975162905bd8d9a42ff…  │ GH-40334: [C++][Gandiva] Add missing OpenSSL dependency to encrypt_utils_test.cc (#40338) │
├───────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────┤
│ 10 rows                                                                                                     2 columns │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

In the remainder of this section, we cover the supported parameters and usages for our IPC readers/writers.

### IPC Files

#### Write
Writing an Arrow IPC file is done using the COPY statement Below is a simple example of how you can use DuckDB to create such a file.

```sql
COPY (SELECT 42 as foofy, 'string' as stringy) TO "test.arrows";
```

Both `.arrows` and `.arrow` will be automatically recognized by DuckDB as Arrow IPC streams.
However, if you wish to use a different extension, you can manually specify the format using:

```sql
COPY (SELECT 42 as foofy, 'string' as stringy) TO "test.ipc" (FORMAT ARROWS);
```

The Copy function of the Copy To Arrow File operation accepts the following parameters:
* `row_group_size`: The size of a row group. By default, the value is 122,880. A lower value may reduce performance but can be beneficial for streaming. It is important to note that this value is not exact, a slightly higher value divisible by 2,048 (DuckDB's standard vector size) may be used as the actual row group size.
* `chunk_size`: An alias for the `row_group_size` parameter.
* `row_group_size_bytes`: The size of row groups in bytes.
* `row_groups_per_file`: The maximum number of row groups per file. If this option is set, multiple files can be generated in a single `COPY` call. This means the specified path will create a directory, and the `row_group_size` parameter will also be used to determine the partition sizes.
* `kv_metadata`: Key-value metadata to be added to the file schema.

If `row_group_size_bytes` and either `chunk_size` or `row_group_size` are used, the row groups will be defined by the smallest of these parameters.

#### Read
You can consume the file using the `read_arrow` scanner. For example, to read the file we just created, you could run:
```sql
FROM read_arrow('test.arrows');
```

Similar to the copy function, the extension also registers `.arrows` and `.arrow` as valid extensions for the Arrow IPC format. This means that a replacement scan can be applied if that is the file extension, so the following would also be a valid query:
```sql
FROM 'test.arrows';
```

Files and streams whose record batch bodies are compressed with `zstd` or `lz4` (the two codecs allowed by the Arrow IPC format) are read transparently.

Besides single-file reading, our extension also fully supports multi-file reading, including all valid multi-file options.

If we were to create a second test file using:
```sql
COPY (SELECT 42 as foofy, 'string' as stringy) TO "test_2.arrows" (FORMAT ARROWS);
```

We can then run a query that reads both files using a glob pattern or a list of file paths:

```sql
-- Glob
FROM read_arrow('*.arrows')

-- List
FROM read_arrow(['test.arrows','test_2.arrows'])
```

When reading multiple files, the following parameters are also supported:
* `union_by_name`: If the schemas of the files differ, setting `union_by_name` allows DuckDB to construct the schema by aligning columns with the same name.
* `filename`: If set to `True`, this will add a column with the name of the file that generated each row.
* `hive_partitioning`: Enables reading data from a Hive-partitioned dataset and applies partition filtering.
> [!NOTE]
> [Arrow IPC files (.arrow)](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format) and [Arrow IPC streams (.arrows)](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) are distinct but related formats. This extension can read both but only writes Arrow IPC Streams.
### IPC Stream Buffers
Similar to the old core Arrow extension, this extension also allows direct production and consumption of the Arrow IPC streaming format from in-memory buffers in both Python and Node.js.
In this section, we will demonstrate how to use the Python API, but you can find many tests that serve as examples for both [Node.js](https://github.com/paleolimbot/duckdb-nanoarrow/tree/main/test/nodejs) and [Python](https://github.com/paleolimbot/duckdb-nanoarrow/tree/main/test/python).

Our extension can create Arrow IPC buffers using the `to_arrow_ipc` function. This function returns two columns: one containing the serialized data as a `BLOB`, and another `BOOL` column indicating which tuples contain the header information of the messages. For example, consider the following table in our DuckDB database:
```python
import pyarrow as pa
import duckdb
import pyarrow.ipc as ipc

connection = duckdb.connect()
connection.execute("CREATE TABLE T (f0 integer, f1 varchar, f2 bool )")
connection.execute("INSERT INTO T values (1, 'foo', true),(2, 'bar', NULL), (3, 'baz', false), (4, NULL, true) ")
```

We can then obtain our buffers by simply issuing a `to_arrow_ipc` call, like this:

```python
buffers = connection.execute("FROM to_arrow_ipc((FROM T))").fetchall()
```
In this case, our buffers will contain two tuples: the first is the header of our message, and the second is the data. To convert this into an Arrow table, we simply concatenate the tuples and use the `ipc.RecordBatchStreamReader`. For example, you can read them as follows:


```python
batches = []
with pa.BufferReader(pa.py_buffer(buffers[0][0] + buffers[1][0])) as reader:
     stream_reader = ipc.RecordBatchStreamReader(reader)
     schema = stream_reader.schema
     batches.extend(stream_reader)
arrow_table = pa.Table.from_batches(batches, schema=schema)
```

To read buffers with DuckDB, you must use the Python function `from_arrow`. Continuing from our example, we would first need to convert our Arrow table into the Arrow IPC format.
```python
batch = arrow_table.to_batches()[0]
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, batch.schema) as writer:
     writer.write_batch(batch)
buffer = sink.getvalue()
buf_reader = pa.BufferReader(buffer)
msg_reader = ipc.MessageReader.open_stream(buf_reader)
```

After this, the following query will return a DuckDB relation with the deserialized Arrow IPC:

```python
connection.from_arrow(msg_reader)
```

## Building

To build the extension, clone the repository with submodules:

``` shell
git clone --recurse-submodules https://github.com/paleolimbot/duckdb-nanoarrow.git
```

...or if you forget to clone the submodules/you're using VSCode to do your checkout, you can run:

``` shell
git submodule init
git submodule update --checkout
```

A quick-and-dirty way to get your build up and running is to run `make`:

```sh
make

```
The main binaries that will be built are:

```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/nanoarrow/nanoarrow.duckdb_extension
```

- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `nanoarrow.duckdb_extension` is the loadable binary as it would be distributed.

If you'd like to use VSCode with the integration provided by the CMake/clangd extension, you
can run:

``` shell
cp CMakeUserPresets.json duckdb/
```

...and ensure that `.vscode/settings.json` contains:

``` json
{
    "cmake.sourceDirectory": "${workspaceFolder}/duckdb"
}
```

Then choose *Developer: Reload window* from the command palette and choose the
*Extension (Debug build)* preset.

## Running the extension

To run the extension code, simply start the shell with `./build/release/duckdb`
(if you're using `make` to build) or `./build/duckdb` (if you're using CMake
via VSCode).

Now we can use the features from the extension directly in DuckDB.

## Running the tests

Different tests can be created for DuckDB extensions. Tests are written in
SQL  `./test/sql`. These SQL tests can be run using `make test` (if using
make) or `./test_local.sh` (if using CMake via VSCode).

## Debugging

You can debug an interactive SQL session by launching it with `gdb` or `lldb`:

``` shell
lldb build/duckdb
```

...or you can use the CodeLLDB extension (Command Palette: *LLDB: Attach to process*)
to launch a VSCode interactive debugger launched in a terminal.
