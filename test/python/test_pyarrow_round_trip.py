import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.ipc as ipc
import pytest

# One column per type the writer supports, NULL on every seventh row
COLUMNS = {
    "p": "i % 3",
    "b": "i % 2 = 0",
    "t1": "(i % 256 - 128)::TINYINT",
    "t2": "(i % 65536 - 32768)::SMALLINT",
    "t4": "i::INTEGER",
    "t8": "(i * 1000000007)::BIGINT",
    "u1": "(i % 256)::UTINYINT",
    "u2": "(i % 65536)::USMALLINT",
    "u4": "i::UINTEGER",
    "u8": "(i * 1000000007)::UBIGINT",
    "h": "(i * 1000000007)::HUGEINT * 1000000007",
    "uh": "(i * 1000000007)::UHUGEINT",
    "f": "(i / 7.0)::FLOAT",
    "d": "i / 7.0",
    "dec4": "((i % 1000) / 10.0)::DECIMAL(4,1)",
    "dec9": "(i / 100.0)::DECIMAL(9,2)",
    "dec18": "(i / 100.0)::DECIMAL(18,3)",
    "dec38": "i::DECIMAL(38,10)",
    "s": "CASE WHEN i % 5 = 0 THEN '' ELSE repeat('ü€', i % 100) || i END",
    "bl": "CASE WHEN i % 5 = 0 THEN ''::BLOB ELSE '\\x00\\xFF'::BLOB || encode('bytes_' || i) END",
    "dt": "DATE '2000-01-01' + i::INTEGER",
    "tm": "TIME '00:00:00' + to_microseconds(i * 1000003)",
    "tmns": "make_timestamp_ns(946684800000000000 + i * 1000000000 + i % 997)::TIME_NS",
    "tss": "(TIMESTAMP '2000-01-01' + INTERVAL (i) SECOND)::TIMESTAMP_S",
    "ts": "TIMESTAMP '2000-01-01' + INTERVAL (i) SECOND + to_microseconds(i % 997)",
    "tsms": "(TIMESTAMP '2000-01-01' + INTERVAL (i) SECOND + to_milliseconds(i % 997))::TIMESTAMP_MS",
    "tsns": "make_timestamp_ns(946684800000000000 + i * 1000000000 + i % 997)",
    "tstzns": "(make_timestamp_ns(946684800000000000 + i * 1000000000 + i % 997)::VARCHAR || '+00')::TIMESTAMPTZ_NS",
    "tstz": "(TIMESTAMP '2000-01-01' + INTERVAL (i) SECOND + to_microseconds(i % 997))::TIMESTAMPTZ",
    "iv": "INTERVAL (i % 13) MONTH + INTERVAL (i) DAY + to_microseconds(i)",
    "uu": "('00000000-0000-0000-0000-' || lpad(i::VARCHAR, 12, '0'))::UUID",
    "bn": "(CASE WHEN i % 2 = 0 THEN '-' ELSE '' END || '12' || repeat('9', i % 60))::BIGNUM",
    "g": "('POINT(' || i || ' ' || i || ')')::GEOMETRY",
    "l": "[i, NULL, i + 1]",
    "ll": "[[i], [], NULL]",
    "st": "{'a': i, 'b': 'x' || i}",
    "m": "MAP {i: 'v' || i}",
    "arr": "[i, i + 1, i + 2]",
    "ls": "[{'k': i}]",
    "un": "CASE WHEN i % 2 = 0 THEN union_value(a := i::INTEGER)::UNION(a INTEGER, b VARCHAR)"
    " ELSE union_value(b := 'v' || i)::UNION(a INTEGER, b VARCHAR) END",
}


def create_source(connection, rows):
    columns = []
    for name, expression in COLUMNS.items():
        nullable = f"CASE WHEN i % 7 = 3 THEN NULL ELSE {expression} END"
        if name == "arr":
            nullable = f"({nullable})::INTEGER[3]"
        if name == "p":
            nullable = expression
        columns.append(f"{nullable} AS {name}")
    connection.execute(f"CREATE OR REPLACE TABLE source AS SELECT i, {', '.join(columns)} FROM range({rows}) t(i)")
    return pa.table(connection.execute("SELECT * FROM source ORDER BY i").arrow())


def assert_same_table(table, expected):
    table.validate(full=True)
    table = table.sort_by("i")
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert table.equals(expected)


def assert_footer_matches_stream(path, reader, row_group_size, rows):
    stream = ipc.open_stream(path.read_bytes()[8:])
    assert reader.schema.equals(stream.schema, check_metadata=True)
    batches = list(stream)
    assert reader.num_record_batches == len(batches)
    # The writer takes whole vectors of 2048 rows, so a smaller row group still holds one
    if rows > -(-row_group_size // 2048) * 2048:
        assert len(batches) > 1
    for index, batch in enumerate(batches):
        block = reader.get_batch(index)
        block.validate(full=True)
        assert block.equals(batch, check_metadata=True)


# The <namespace>:type of the types that take parameters, every other type is named as
# DuckDB spells it
TYPE_NAMES = {
    "decimal": "DECIMAL",
    "list": "LIST",
    "array": "ARRAY",
    "struct": "STRUCT",
    "tuple": "TUPLE",
    "map": "MAP",
    "union": "UNION",
    "geometry": "GEOMETRY",
    "null": "NULL",
}


def arrow_children(field):
    if pa.types.is_map(field.type):
        return [field.type.key_field, field.type.item_field]
    if pa.types.is_list(field.type) or pa.types.is_fixed_size_list(field.type):
        return [field.type.value_field]
    if pa.types.is_struct(field.type) or pa.types.is_union(field.type):
        return [field.type.field(index) for index in range(field.type.num_fields)]
    return []


def duckdb_children(duckdb_type):
    if duckdb_type.id in ("list", "array"):
        return [duckdb_type.children[0][1]]
    if duckdb_type.id in ("struct", "tuple", "map"):
        return [child for _, child in duckdb_type.children]
    if duckdb_type.id == "union":
        # The first child is the tag, which Arrow keeps in the type ids buffer
        return [child for _, child in duckdb_type.children[1:]]
    return []


def assert_type_metadata(field, expected, duckdb_type):
    """Checks the TYPE_METADATA_NAMESPACE 'VENDOR' keys of a field and its children, and
    that every other key is kept as the field has it without the option"""
    metadata = {key.decode(): value.decode() for key, value in field.metadata.items()}
    details = str(duckdb_type)
    keys = {"VENDOR:type": TYPE_NAMES.get(duckdb_type.id, details), "VENDOR:type_details": details}
    if duckdb_type.id == "decimal":
        parameters = dict(duckdb_type.children)
        keys["VENDOR:type_precision"] = str(parameters["precision"])
        keys["VENDOR:type_scale"] = str(parameters["scale"])
    assert {key: value for key, value in metadata.items() if key.startswith("VENDOR:")} == keys, field
    others = {key: value for key, value in metadata.items() if not key.startswith("VENDOR:")}
    assert others == {key.decode(): value.decode() for key, value in (expected.metadata or {}).items()}
    assert field.type.equals(expected.type, check_metadata=False)
    children = arrow_children(field)
    expected_children = arrow_children(expected)
    duckdb_types = duckdb_children(duckdb_type)
    assert len(children) == len(expected_children) == len(duckdb_types), field
    for child, expected_child, duckdb_child in zip(children, expected_children, duckdb_types):
        assert_type_metadata(child, expected_child, duckdb_child)


def assert_same_table_with_type_metadata(connection, table, expected, source="source"):
    table.validate(full=True)
    table = table.sort_by("i")
    assert table.equals(expected)
    # The writer stores an empty schema metadata map where pyarrow has none
    assert (table.schema.metadata or {}) == (expected.schema.metadata or {})
    relation = connection.sql(f"FROM {source}")
    for name, duckdb_type in zip(relation.columns, relation.types):
        assert_type_metadata(table.schema.field(name), expected.schema.field(name), duckdb_type)


ROWS = [0, 1, 2049, 5000, 150000]
ROW_GROUP_SIZES = [1000, 122880]
COMPRESSIONS = ["uncompressed", "zstd"]


@pytest.mark.parametrize("compression", COMPRESSIONS)
@pytest.mark.parametrize("row_group_size", ROW_GROUP_SIZES)
@pytest.mark.parametrize("rows", ROWS)
def test_pyarrow_reads_file(connection, tmp_path, rows, row_group_size, compression):
    expected = create_source(connection, rows)
    path = tmp_path / "types.arrow"
    connection.execute(
        f"COPY source TO '{path}' (FORMAT arrow, ROW_GROUP_SIZE {row_group_size}, COMPRESSION '{compression}')"
    )
    reader = ipc.open_file(path)
    assert_same_table(reader.read_all(), expected)
    assert_footer_matches_stream(path, reader, row_group_size, rows)


@pytest.mark.parametrize("compression", COMPRESSIONS)
@pytest.mark.parametrize("row_group_size", ROW_GROUP_SIZES)
@pytest.mark.parametrize("rows", ROWS)
def test_pyarrow_reads_stream(connection, tmp_path, rows, row_group_size, compression):
    expected = create_source(connection, rows)
    path = tmp_path / "types.arrows"
    connection.execute(
        f"COPY source TO '{path}' (FORMAT arrows, ROW_GROUP_SIZE {row_group_size}, COMPRESSION '{compression}')"
    )
    assert_same_table(ipc.open_stream(path).read_all(), expected)


def test_pyarrow_reads_parallel_write(connection, tmp_path):
    expected = create_source(connection, 150000)
    connection.execute("SET threads=4")
    connection.execute("SET preserve_insertion_order=false")
    path = tmp_path / "parallel.arrow"
    connection.execute(f"COPY source TO '{path}' (FORMAT arrow, ROW_GROUP_SIZE 10000)")
    reader = ipc.open_file(path)
    assert_same_table(reader.read_all(), expected)
    assert_footer_matches_stream(path, reader, 10000, 150000)


def test_pyarrow_reads_large_buffers(connection, tmp_path):
    connection.execute("SET arrow_large_buffer_size=true")
    expected = create_source(connection, 5000)
    path = tmp_path / "large.arrow"
    connection.execute(f"COPY source TO '{path}' (FORMAT arrow)")
    table = ipc.open_file(path).read_all()
    assert table.schema.field("s").type == pa.large_string()
    assert_same_table(table, expected)


@pytest.mark.parametrize("format_name", ["arrow", "arrows"])
def test_pyarrow_reads_lossless_conversion(connection, tmp_path, format_name):
    connection.execute("SET arrow_lossless_conversion=true")
    expected = create_source(connection, 5000)
    path = tmp_path / f"lossless.{format_name}"
    connection.execute(f"COPY source TO '{path}' (FORMAT {format_name})")
    reader = ipc.open_file(path) if format_name == "arrow" else ipc.open_stream(path)
    table = reader.read_all()
    assert table.schema.field("uu").type.extension_name == "arrow.uuid"
    assert table.schema.field("h").type.storage_type == pa.binary(16)
    assert_same_table(table, expected)


def test_pyarrow_reads_many_files(connection, tmp_path):
    expected = create_source(connection, 20000)
    connection.execute(
        f"COPY source TO '{tmp_path / 'many'}' (FORMAT arrow, ROW_GROUP_SIZE 1000, ROW_GROUPS_PER_FILE 1)"
    )
    dataset = ds.dataset(tmp_path / "many", format="arrow")
    assert len(dataset.files) > 1
    assert_same_table(dataset.to_table(), expected)


def test_pyarrow_reads_partitions(connection, tmp_path):
    expected = create_source(connection, 5000)
    connection.execute(f"COPY source TO '{tmp_path / 'parts'}' (FORMAT arrow, PARTITION_BY (p))")
    dataset = ds.dataset(tmp_path / "parts", format="arrow", partitioning="hive")
    table = dataset.to_table()
    # The partition value is inferred from the path, so it is cast back to the column's type
    table = table.set_column(table.schema.get_field_index("p"), "p", table.column("p").cast(pa.int64()))
    assert_same_table(table.select(expected.column_names), expected)


@pytest.mark.parametrize("lossless_conversion", [False, True])
@pytest.mark.parametrize("format_name", ["arrow", "arrows"])
def test_pyarrow_reads_type_metadata(connection, tmp_path, format_name, lossless_conversion):
    connection.execute(f"SET arrow_lossless_conversion={lossless_conversion}")
    expected = create_source(connection, 5000)
    path = tmp_path / f"type_metadata.{format_name}"
    connection.execute(f"COPY source TO '{path}' (FORMAT {format_name}, TYPE_METADATA_NAMESPACE 'VENDOR')")
    reader = ipc.open_file(path) if format_name == "arrow" else ipc.open_stream(path)
    assert_same_table_with_type_metadata(connection, reader.read_all(), expected)
    if format_name == "arrow":
        assert_footer_matches_stream(path, reader, 122880, 5000)


def test_pyarrow_reads_type_metadata_in_many_files(connection, tmp_path):
    expected = create_source(connection, 20000)
    connection.execute(
        f"COPY source TO '{tmp_path / 'many'}' (FORMAT arrow, ROW_GROUP_SIZE 1000, ROW_GROUPS_PER_FILE 1, "
        "TYPE_METADATA_NAMESPACE 'VENDOR')"
    )
    files = sorted((tmp_path / "many").iterdir())
    assert len(files) > 1
    for path in files:
        assert ipc.open_file(path).schema.equals(ipc.open_file(files[0]).schema, check_metadata=True)
    dataset = ds.dataset(tmp_path / "many", format="arrow")
    assert_same_table_with_type_metadata(connection, dataset.to_table(), expected)


# Types COLUMNS does not have, whose type_details differ from their type name or whose
# type has no parameters to read back
TYPE_METADATA_COLUMNS = {
    "j": "('{\"n\": ' || i || '}')::JSON",
    "bt": "('1' || (i % 2)::VARCHAR || '1')::BIT",
    "ttz": "('01:02:03+0' || (i % 9)::VARCHAR)::TIMETZ",
    "gcrs": "('POINT(' || i || ' 1)')::GEOMETRY('OGC:CRS84')",
    "n": "NULL",
    "tu": "(i, 'x' || i)",
}


def create_type_metadata_source(connection, rows):
    """Every type, alone and as the children of deep structs, lists, maps, arrays and unions"""
    scalars = {**COLUMNS, **TYPE_METADATA_COLUMNS}
    nullable = {name: f"CASE WHEN i % 7 = 3 THEN NULL ELSE {expression} END" for name, expression in scalars.items()}
    # A NULL child of a struct is written with a null count pyarrow rejects, so NULL is only
    # checked as a column of its own
    children = {name: expression for name, expression in nullable.items() if name != "n"}
    every = "{" + ", ".join(f"'{name}': {expression}" for name, expression in children.items()) + "}"
    columns = dict(nullable)
    columns["deep_struct"] = f"{{'every': {every}, 'inner': {{'every': {every}, 'inner': {{'every': {every}}}}}}}"
    columns["deep_list"] = f"[[{every}], [], NULL]"
    columns["deep_map"] = f"MAP {{'k' || i: [{{'every': {every}}}]}}"
    columns["deep_array"] = f"array_value(array_value({every}, {every}))"
    columns["deep_union"] = f"union_value(every := {{'inner': [{every}]}})"
    columns["deep_mixed"] = f"{{'maps': [MAP {{i: array_value({{'every': [{every}]}})}}]}}"
    selects = ", ".join(f"{expression} AS {name}" for name, expression in columns.items())
    connection.execute(f"CREATE OR REPLACE TABLE type_source AS SELECT i, {selects} FROM range({rows}) t(i)")
    return pa.table(connection.execute("SELECT * FROM type_source ORDER BY i").arrow())


@pytest.mark.parametrize("format_name", ["arrow", "arrows"])
def test_pyarrow_reads_type_metadata_of_deep_types(connection, tmp_path, format_name):
    expected = create_type_metadata_source(connection, 100)
    path = tmp_path / f"deep_types.{format_name}"
    connection.execute(f"COPY type_source TO '{path}' (FORMAT {format_name}, TYPE_METADATA_NAMESPACE 'VENDOR')")
    reader = ipc.open_file(path) if format_name == "arrow" else ipc.open_stream(path)
    assert_same_table_with_type_metadata(connection, reader.read_all(), expected, "type_source")
