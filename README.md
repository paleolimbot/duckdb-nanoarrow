# nanoarrow for DuckDB

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, nanoarrow, allows you to read Arrow IPC streams.


```sql
LOAD httpfs;
LOAD nanoarrow;
SELECT
    commit, message
  FROM
    read_arrow('https://github.com/apache/arrow-experiments/raw/refs/heads/main/data/arrow-commits/arrow-commits.arrows')
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

It is very new and proof-of-concept only at this point!

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
