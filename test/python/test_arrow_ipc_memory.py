import gc

import psutil

# One call emits several IPC record batches. The leak this guards against scaled
# with the number of batches that got serialized, so a multi-batch result makes
# it show up quickly.
LEAK_QUERY = """
    SELECT count(*), sum(octet_length(ipc))
    FROM to_arrow_ipc((SELECT i, i % 7 = 0 AS flag FROM range(1000000) t(i)))
"""

# Long enough for the allocator to settle before the window opens. How many
# calls that takes varies by machine, so err on the generous side.
WARMUP_ITERATIONS = 8
MEASURED_ITERATIONS = 10
# to_arrow_ipc used to retain ~10 MiB of native memory per call for this query,
# so the measured window grew by 96.6 MiB. Once the appender's array, the IPC
# encoder and the bound schema are released, what is left is allocator drift:
# a few MiB at most, and it does not accumulate. That leaves this threshold
# comfortably clear of both outcomes.
MAX_GROWTH_MIB = 32


def rss_mib():
    return psutil.Process().memory_info().rss / (1024 * 1024)


class TestArrowIPCMemory(object):
    def test_to_arrow_ipc_does_not_retain_memory(self, connection):
        connection.execute("SET threads=1")

        # The aggregate is computed inside DuckDB, so the blobs never cross into
        # Python and anything we measure is native memory held by the extension.
        expected = connection.execute(LEAK_QUERY).fetchone()
        assert expected[0] > 2, "query is expected to emit several IPC messages"

        for _ in range(WARMUP_ITERATIONS):
            connection.execute(LEAK_QUERY).fetchone()
        gc.collect()
        before = rss_mib()

        for _ in range(MEASURED_ITERATIONS):
            assert connection.execute(LEAK_QUERY).fetchone() == expected
        gc.collect()
        growth = rss_mib() - before

        assert growth < MAX_GROWTH_MIB, (
            f"RSS grew by {growth:.1f} MiB over {MEASURED_ITERATIONS} to_arrow_ipc calls "
            f"({growth / MEASURED_ITERATIONS:.1f} MiB per call)"
        )
