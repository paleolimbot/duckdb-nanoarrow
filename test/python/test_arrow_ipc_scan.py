import pytest
import pyarrow as pa
import duckdb
import pyarrow.ipc as ipc


def get_record_batch():
    data = [pa.array([1, 2, 3, 4]), pa.array(['foo', 'bar', 'baz', None]), pa.array([True, None, False, True])]

    return pa.record_batch(data, names=['f0', 'f1', 'f2'])


def tables_match(result):
    assert result == [
        (1, 'foo', True),
        (2, 'bar', None),
        (3, 'baz', False),
        (4, None, True),
        (1, 'foo', True),
        (2, 'bar', None),
        (3, 'baz', False),
        (4, None, True),
        (1, 'foo', True),
        (2, 'bar', None),
        (3, 'baz', False),
        (4, None, True),
        (1, 'foo', True),
        (2, 'bar', None),
        (3, 'baz', False),
        (4, None, True),
        (1, 'foo', True),
        (2, 'bar', None),
        (3, 'baz', False),
        (4, None, True),
    ]


class TestArrowIPCBufferRead(object):
    def test_single_buffer(self, connection):
        batch = get_record_batch()
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            for i in range(5):
                writer.write_batch(batch)
        buffer = sink.getvalue()
        with pa.BufferReader(buffer) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            tables_match(connection.from_arrow(msg_reader).fetchall())

    def test_multi_buffers(self, connection):
        batch = get_record_batch()
        sink = pa.BufferOutputStream()

        with pa.ipc.new_stream(sink, batch.schema) as writer:
            for _ in range(5):  # Write 5 batches into one stream
                writer.write_batch(batch)

        buffer = sink.getvalue()

        with pa.BufferReader(buffer) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            tables_match(connection.from_arrow(msg_reader).fetchall())

    def test_replacement_scan(self, connection):

        batch = get_record_batch()
        sink = pa.BufferOutputStream()

        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)

        buffer = sink.getvalue()

        with pa.BufferReader(buffer) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            with pytest.raises(
                duckdb.InvalidInputException,
                match="not suitable for replacement scans",
            ):
                connection.execute("FROM msg_reader")

    def test_dictionary_single_buffer(self, connection):
        indices = pa.array([0, 1, 0, 2, None, 1])
        dictionary = pa.array(["apple", "banana", "cherry"])
        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        batch = pa.record_batch([dict_array], names=["fruit"])

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)

        buffer = sink.getvalue()
        with pa.BufferReader(buffer) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            res = connection.from_arrow(msg_reader).fetchall()
            assert res == [("apple",), ("banana",), ("apple",), ("cherry",), (None,), ("banana",)]

    def test_dictionary_multi_batch_stream(self, connection):
        indices1 = pa.array([0, 1])
        indices2 = pa.array([2, 0, None])
        dictionary = pa.array(["alpha", "beta", "gamma"])

        batch1 = pa.record_batch([pa.DictionaryArray.from_arrays(indices1, dictionary)], names=["codes"])
        batch2 = pa.record_batch([pa.DictionaryArray.from_arrays(indices2, dictionary)], names=["codes"])

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch1.schema) as writer:
            writer.write_batch(batch1)
            writer.write_batch(batch2)

        buffer = sink.getvalue()
        with pa.BufferReader(buffer) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            res = connection.from_arrow(msg_reader).fetchall()
            assert res == [("alpha",), ("beta",), ("gamma",), ("alpha",), (None,)]

    def test_dictionary_unsigned_indices(self, connection):
        indices = pa.array([0, 1, 2], type=pa.uint8())
        dictionary = pa.array(["x", "y", "z"])
        batch = pa.record_batch([pa.DictionaryArray.from_arrays(indices, dictionary)], names=["v"])

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)

        buffer = sink.getvalue()
        with pa.BufferReader(buffer) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            assert connection.from_arrow(msg_reader).fetchall() == [("x",), ("y",), ("z",)]

    def test_dictionary_null_in_values(self, connection):
        # Index points to a slot in the dictionary that is itself NULL
        indices = pa.array([0, 1, 2])
        dictionary = pa.array(["apple", None, "cherry"])
        batch = pa.record_batch([pa.DictionaryArray.from_arrays(indices, dictionary)], names=["fruit"])

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)

        with pa.BufferReader(sink.getvalue()) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            assert connection.from_arrow(msg_reader).fetchall() == [("apple",), (None,), ("cherry",)]

    def test_dictionary_shared_id(self, connection):
        # Two columns using the same dictionary
        dictionary = pa.array(["red", "green", "blue"])
        dict_type = pa.dictionary(pa.int32(), dictionary.type)

        field1 = pa.field("color1", dict_type)
        field2 = pa.field("color2", dict_type)
        schema = pa.schema([field1, field2])

        arr1 = pa.DictionaryArray.from_arrays(pa.array([0, 1]), dictionary)
        arr2 = pa.DictionaryArray.from_arrays(pa.array([2, 0]), dictionary)
        batch = pa.record_batch([arr1, arr2], schema=schema)

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_batch(batch)

        with pa.BufferReader(sink.getvalue()) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            assert connection.from_arrow(msg_reader).fetchall() == [("red", "blue"), ("green", "red")]

    @pytest.mark.skip(reason="Nested dictionaries trigger a DuckDB Arrow conversion ownership error")
    def test_dictionary_inside_struct(self, connection):
        # Nested dictionary inside a Struct
        dictionary = pa.array(["low", "high"])
        dict_arr = pa.DictionaryArray.from_arrays(pa.array([0, 1]), dictionary)
        struct_arr = pa.StructArray.from_arrays([dict_arr], names=["level"])
        batch = pa.record_batch([struct_arr], names=["info"])

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)

        with pa.BufferReader(sink.getvalue()) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            assert connection.from_arrow(msg_reader).fetchall() == [({'level': 'low'},), ({'level': 'high'},)]

    def test_dictionary_numeric_values(self, connection):
        indices = pa.array([0, 1, 0, None, 1], type=pa.int8())
        dictionary = pa.array([100.55, 200.75], type=pa.float64())
        dict_arr = pa.DictionaryArray.from_arrays(indices, dictionary)
        batch = pa.record_batch([dict_arr], names=["amounts"])

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)

        with pa.BufferReader(sink.getvalue()) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            assert connection.from_arrow(msg_reader).fetchall() == [
                (100.55,), (200.75,), (100.55,), (None,), (200.75,)
            ]

    def test_dictionary_sliced_array(self, connection):
        indices = pa.array([0, 1, 2, 0, 1])
        dictionary = pa.array(["a", "b", "c"])
        full_arr = pa.DictionaryArray.from_arrays(indices, dictionary)

        # Take a slice starting at index 2 with length 2 -> indices [2, 0] -> ["c", "a"]
        sliced_arr = full_arr.slice(2, 2)
        batch = pa.record_batch([sliced_arr], names=["code"])

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)

        with pa.BufferReader(sink.getvalue()) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            assert connection.from_arrow(msg_reader).fetchall() == [("c",), ("a",)]

    @pytest.mark.skip(reason="nanoarrow cannot encode dictionary arrays")
    def test_duckdb_enum_to_arrow_ipc(self, connection):
        connection.execute("CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok');")
        connection.execute("CREATE TABLE enum_t (id INT, m mood);")
        connection.execute("INSERT INTO enum_t VALUES (1, 'happy'), (2, 'ok'), (3, NULL), (4, 'sad');")

        buffers = connection.execute("FROM to_arrow_ipc((FROM enum_t))").fetchall()
        schema_msg = buffers[0][0]

        batches = []
        for payload, _ in buffers[1:]:
            with pa.BufferReader(pa.py_buffer(schema_msg + payload)) as reader:
                stream_reader = ipc.RecordBatchStreamReader(reader)
                batches.extend(stream_reader)

        result_table = pa.Table.from_batches(batches, schema=stream_reader.schema)
        # Verify schema field is dictionary-encoded
        assert pa.types.is_dictionary(result_table.schema.field("m").type)
        assert connection.execute("FROM result_table").fetchall() == [
            (1, "happy"), (2, "ok"), (3, None), (4, "sad")
        ]

    def test_dictionary_empty_batch(self, connection):
        indices = pa.array([], type=pa.int32())
        dictionary = pa.array(["unused"], type=pa.string())
        dict_arr = pa.DictionaryArray.from_arrays(indices, dictionary)
        batch = pa.record_batch([dict_arr], names=["empty_col"])

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)

        with pa.BufferReader(sink.getvalue()) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            assert connection.from_arrow(msg_reader).fetchall() == []

    def test_dictionary_large_string_values(self, connection):
        indices = pa.array([0, 1, 0])
        dictionary = pa.array(["v1", "v2"], type=pa.large_string())
        dict_arr = pa.DictionaryArray.from_arrays(indices, dictionary)
        batch = pa.record_batch([dict_arr], names=["l_str"])

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)

        with pa.BufferReader(sink.getvalue()) as buf_reader:
            msg_reader = ipc.MessageReader.open_stream(buf_reader)
            assert connection.from_arrow(msg_reader).fetchall() == [("v1",), ("v2",), ("v1",)]
