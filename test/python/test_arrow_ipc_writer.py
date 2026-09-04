import pytest
import pyarrow as pa
import duckdb
import pyarrow.ipc as ipc


def create_table(connection):
    connection.execute("CREATE TABLE T (f0 integer, f1 varchar, f2 bool )")
    connection.execute("INSERT INTO T values (1, 'foo', true),(2, 'bar', NULL), (3, 'baz', false), (4, NULL, true) ")


def tables_match(result):
    print(result)
    assert result == [(1, 'foo', True), (2, 'bar', None), (3, 'baz', False), (4, None, True)]


class TestArrowIPCBufferWriter(object):
    def test_round_trip(self, connection):
        create_table(connection)
        buffers = connection.execute("FROM to_arrow_ipc((FROM T))").fetchall()
        buffer = pa.py_buffer(buffers[0][0] + buffers[1][0])
        with pa.BufferReader(buffer) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            tables_match(connection.from_arrow(msg_reader).fetchall())

    def test_arrow_read_duck_buffers(self, connection):
        create_table(connection)
        buffers = connection.execute("FROM to_arrow_ipc((FROM T))").fetchall()
        arrow_buffers = []
        # We have to concatenate the schema to the data
        arrow_buffers.append(pa.py_buffer(buffers[0][0] + buffers[1][0]))
        assert buffers[0][1] == True
        assert buffers[1][1] == False
        batches = []
        with pa.BufferReader(arrow_buffers[0]) as reader:
            stream_reader = ipc.RecordBatchStreamReader(reader)
            schema = stream_reader.schema
            batches.extend(stream_reader)
        arrow_table = pa.Table.from_batches(batches, schema=schema)
        tables_match(connection.execute("FROM arrow_table").fetchall())

    def test_round_trip_multiple_batches(self, connection):
        # Big enough to be split over several IPC record batches, so the
        # serializer runs more than once for a single query. A batch holds
        # 120 * STANDARD_VECTOR_SIZE rows, so keep this comfortably above that
        # for any vector size the build might use.
        source = """
            SELECT i,
                   i::VARCHAR AS s,
                   i % 7 = 0 AS f,
                   CASE WHEN i % 11 = 0 THEN NULL ELSE i * 2 END AS n
            FROM range(1200000) t(i)
        """
        buffers = connection.execute(f"FROM to_arrow_ipc(({source}))").fetchall()
        assert len(buffers) > 2, "expected more than one data message"
        assert [header for _, header in buffers] == [True] + [False] * (len(buffers) - 1)

        schema_message = buffers[0][0]
        batches = []
        for payload, _ in buffers[1:]:
            with pa.BufferReader(pa.py_buffer(schema_message + payload)) as reader:
                stream_reader = ipc.RecordBatchStreamReader(reader)
                schema = stream_reader.schema
                batches.extend(stream_reader)

        arrow_table = pa.Table.from_batches(batches, schema=schema)
        assert connection.execute(
            f"""
            SELECT (SELECT count(*) FROM arrow_table),
                   (SELECT count(*) FROM (({source}) EXCEPT ALL FROM arrow_table)),
                   (SELECT count(*) FROM (FROM arrow_table EXCEPT ALL ({source})))
            """
        ).fetchone() == (1200000, 0, 0)

    def test_dictionary_round_trip(self, connection):
        connection.execute(
            """
            CREATE TABLE dict_t AS
            SELECT CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END AS cat, i
            FROM range(6) t(i)
            """
        )
        buffers = connection.execute("FROM to_arrow_ipc((FROM dict_t))").fetchall()

        arrow_buffers = [pa.py_buffer(buffers[0][0] + b[0]) for b in buffers[1:]]
        batches = []
        for buf in arrow_buffers:
            with pa.BufferReader(buf) as reader:
                stream_reader = ipc.RecordBatchStreamReader(reader)
                batches.extend(stream_reader)

        arrow_table = pa.Table.from_batches(batches, schema=stream_reader.schema)
        result = connection.execute("FROM arrow_table").fetchall()
        assert result == [("even", 0), ("odd", 1), ("even", 2), ("odd", 3), ("even", 4), ("odd", 5)]

    def test_dictionary_to_arrow_ipc_from_pyarrow(self, connection):
        indices = pa.array([0, 1, 1, 0])
        dictionary = pa.array(["low", "high"])
        dict_arr = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_arr], names=["priority"])

        buffers = connection.execute("FROM to_arrow_ipc((FROM arrow_table))").fetchall()
        schema_msg = buffers[0][0]

        batches = []
        for payload, _ in buffers[1:]:
            with pa.BufferReader(pa.py_buffer(schema_msg + payload)) as reader:
                stream_reader = ipc.RecordBatchStreamReader(reader)
                batches.extend(stream_reader)

        result_table = pa.Table.from_batches(batches, schema=stream_reader.schema)
        assert result_table.column("priority").to_pylist() == ["low", "high", "high", "low"]


class TestArrowIPCCompression(object):
    @pytest.mark.parametrize("compression", ["zstd", "lz4"])
    def test_pyarrow_writes_duckdb_reads(self, connection, compression, tmp_path):
        arrow_table = pa.table(
            {
                'f0': pa.array([1, 2, 3, 4], pa.int32()),
                'f1': ['foo', 'bar', 'baz', None],
                'f2': [True, None, False, True],
            }
        )
        path = str(tmp_path / f"pyarrow_{compression}.arrows")
        options = ipc.IpcWriteOptions(compression=compression)
        with pa.OSFile(path, 'wb') as sink, ipc.new_stream(sink, arrow_table.schema, options=options) as writer:
            writer.write_table(arrow_table)
        tables_match(connection.execute(f"FROM read_arrow('{path}')").fetchall())
