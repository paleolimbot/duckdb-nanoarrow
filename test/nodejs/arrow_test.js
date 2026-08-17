const arrow = require('apache-arrow');
const { DuckDBInstance } = require('@duckdb/node-api');
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const os = require('os');

const parquet_file_path = "data/parquet-testing/lineitem_sf0_01.parquet";

// Track registered virtual tables and temp file paths per connection
const registeredBuffers = new WeakMap();

function getConnBuffers(conn) {
    if (!registeredBuffers.has(conn)) {
        registeredBuffers.set(conn, new Map());
    }
    return registeredBuffers.get(conn);
}

// Low-overhead buffer registration using native IPC files and read_arrow
const registerBuffer = async (conn, name, buffer, force = true) => {
    const buffers = getConnBuffers(conn);
    if (buffers.has(name) && !force) {
        throw new Error('Buffer with this name already exists and force_register is not enabled');
    }

    const tmpPath = path.join(os.tmpdir(), `duckdb_arrow_${Date.now()}_${Math.random().toString(36).substring(2)}_${name}.ipc`);
    fs.writeFileSync(tmpPath, buffer);

    await conn.run(`CREATE OR REPLACE TEMP VIEW ${name} AS SELECT * FROM read_arrow('${tmpPath}')`);

    if (buffers.has(name)) {
        try { fs.unlinkSync(buffers.get(name)); } catch (e) {}
    }
    buffers.set(name, tmpPath);
};

const unregisterBuffer = async (conn, name) => {
    const buffers = getConnBuffers(conn);
    try {
        await conn.run(`DROP VIEW IF EXISTS ${name}`);
    } catch (e) {}
    if (buffers.has(name)) {
        const tmpPath = buffers.get(name);
        try { fs.unlinkSync(tmpPath); } catch (e) {}
        buffers.delete(name);
    }
};


function getRowsJson(reader) {
    const cols = reader.columnNames();
    return reader.getRows().map(row => {
        const obj = {};
        cols.forEach((col, i) => {
            let val = row[i];

            // 1. Handle DuckDBDecimalValue objects (e.g., Decimal results in TPC-H)
            if (val && typeof val === 'object' && 'value' in val && 'scale' in val) {
                val = Number(val.value) / (10 ** val.scale);
            }
            // 2. Handle BigInt values
            else if (typeof val === 'bigint') {
                val = Number(val);
            }
            // 3. Handle numeric strings
            else if (typeof val === 'string' && !isNaN(val) && val.trim() !== '') {
                val = Number(val);
            }

            obj[col] = val;
        });
        return obj;
    });
}

const queryToIPCBuffer = async (conn, sql) => {
    // 1. Pass the SQL string as a table expression to to_arrow_ipc(TABLE)
    const reader = await conn.runAndReadAll(`SELECT * FROM to_arrow_ipc((${sql}))`);
    const rows = reader.getRows();
    const chunks = [];

    for (const row of rows) {
        const blobVal = row[0];
        if (blobVal) {
            // 2. Extract .bytes from DuckDBBlobValue if present
            const rawBytes = blobVal.bytes ? blobVal.bytes : blobVal;
            chunks.push(Buffer.from(rawBytes));
        }
    }
    return Buffer.concat(chunks);
};

const arrow_ipc_stream = async (conn, sql) => {
    return await queryToIPCBuffer(conn, sql);
};

const arrow_ipc_materialized = async (conn, sql) => {
    return await queryToIPCBuffer(conn, sql);
};

const to_ipc_functions = {
    'streaming': arrow_ipc_stream,
    'materialized': arrow_ipc_materialized,
};

async function getDatabase() {
    return await DuckDBInstance.create(':memory:', { "allow_unsigned_extensions": "true" });
}

async function getConnection(db) {
    const conn = await db.connect();
    await conn.run(`SET allow_extensions_metadata_mismatch=true;`);
    if (process.env.ARROW_EXTENSION_BINARY_PATH) {
        try {
            await conn.run(`LOAD '${process.env.ARROW_EXTENSION_BINARY_PATH}';`);
        } catch (e) {
            // Extension binary missing locally
        }
    }
    return conn;
}

const streamResults = async (con, sql) => {
    const results = [];
    const ipcBuffer = await arrow_ipc_stream(con, sql);
    const reader = await arrow.RecordBatchReader.from(ipcBuffer);
    for await (const batch of reader) {
        for (const row of batch) {
            const result = {};
            for (const [field, val] of row) {
                result[field] = val;
            }
            results.push(result);
        }
    }
    return results;
};

describe(`Arrow IPC`, () => {
    let db;
    let conn;
    before(async () => {
        db = await getDatabase();
        conn = await getConnection(db);
    });

    it(`Basic examples`, async () => {
        const range_size = 130000;
        const query = `SELECT * FROM range(0,${range_size}) tbl(i)`;
        const arrow_table_expected = new arrow.Table({
            i: new arrow.Vector([arrow.makeData({ type: new arrow.Int32, data: Array.from(new Array(range_size), (x, i) => i) })]),
        });

        // Can use Arrow to read from stream directly
        const result_stream = await arrow_ipc_stream(conn, query);
        const reader = await arrow.RecordBatchReader.from(result_stream);
        const table = await arrow.tableFromIPC(reader);
        const array_from_arrow = table.toArray();
        assert.deepEqual(array_from_arrow, arrow_table_expected.toArray());

        // Can also fully materialize stream first, then pass to Arrow
        const result_stream2 = await arrow_ipc_stream(conn, query);
        const reader2 = await arrow.RecordBatchReader.from(result_stream2);
        const table2 = await arrow.tableFromIPC(reader2);
        const array_from_arrow2 = table2.toArray();
        assert.deepEqual(array_from_arrow2, arrow_table_expected.toArray());

        // Can also fully materialize in DuckDB first
        const result_materialized = await arrow_ipc_materialized(conn, query);
        const reader3 = await arrow.RecordBatchReader.from(result_materialized);
        const table3 = await arrow.tableFromIPC(reader3);
        const array_from_arrow3 = table3.toArray();
        assert.deepEqual(array_from_arrow3, arrow_table_expected.toArray());

        // Scanning materialized IPC buffers from DuckDB
        await registerBuffer(conn, "ipc_table", result_materialized, true);
        const result_ipc = await arrow_ipc_materialized(conn, `SELECT * FROM ipc_table`);
        assert.deepEqual(result_ipc, result_materialized);
    });

    for (const [name, fun] of Object.entries(to_ipc_functions)) {
        it(`Empty results (${name})`, async () => {
            const range_size = 130000;
            const query = `SELECT * FROM range(0,${range_size}) tbl(i) where i > ${range_size}`;

            let ipc_buffers = await fun(conn, query);
            const reader = await arrow.RecordBatchReader.from(ipc_buffers);
            const table = await arrow.tableFromIPC(reader);
            const arr = table.toArray();
            assert.deepEqual(arr, []);
        });
    }
});

for (const [name, fun] of Object.entries(to_ipc_functions)) {
    describe(`DuckDB <-> Arrow IPC (${name})`, () => {
        let db;
        let conn;
        before(async () => {
            db = await getDatabase();
            conn = await getConnection(db);
        });

        it(`Buffers are not garbage collected`, async () => {
            let ipc_buffers = await fun(conn, 'SELECT * FROM range(1001, 2001) tbl(i)');

            await registerBuffer(conn, `ipc_table_${name}`, ipc_buffers, true);

            ipc_buffers = 0;

            if (global.gc) {
                global.gc();
            } else {
                throw new Error("should run with --expose-gc");
            }

            let spray_results = [];
            for (let i = 0; i < 3000; i++) {
                spray_results.push(await fun(conn, 'SELECT * FROM range(2001, 3001) tbl(i)'));
            }

            const reader = await conn.runAndReadAll(`SELECT avg(i) as average, count(1) as total FROM ipc_table_${name};`);
            assert.deepEqual(getRowsJson(reader), [{ average: 1500.5, total: 1000 }]);
        });

        it(`Round-trip int column`, async () => {
            const ipc_buffers = await fun(conn, 'SELECT * FROM range(1001, 2001) tbl(i)');

            await registerBuffer(conn, "ipc_table", ipc_buffers, true);

            const reader = await conn.runAndReadAll(`SELECT avg(i) as average, count(1) as total FROM ipc_table;`);
            assert.deepEqual(getRowsJson(reader), [{ average: 1500.5, total: 1000 }]);
        });

        it(`Joining 2 IPC buffers in DuckDB`, async () => {
            const ipc_buffers1 = await fun(conn, 'SELECT * FROM range(1, 3) tbl(i)');
            const ipc_buffers2 = await fun(conn, 'SELECT * FROM range(2, 4) tbl(i)');

            await registerBuffer(conn, "table1", ipc_buffers1, true);
            await registerBuffer(conn, "table2", ipc_buffers2, true);

            const reader = await conn.runAndReadAll(`SELECT table1.i FROM table1 JOIN table2 ON table1.i = table2.i;`);
            assert.deepEqual(getRowsJson(reader), [{ i: 2 }]);
        });
    });
}

describe('[Benchmark] Arrow IPC Single Int Column (50M tuples)', () => {
    const column_size = 50 * 1000 * 1000;

    let db;
    let conn;

    before(async () => {
        db = await getDatabase();
        conn = await getConnection(db);
        await conn.run(`CREATE OR REPLACE TABLE test AS select * FROM range(0, ${column_size}) tbl(i);`);
    });

    it('DuckDB table -> DuckDB table', async () => {
        await conn.run('CREATE OR REPLACE TABLE copy_table AS SELECT * FROM test');
    });

    it('DuckDB table -> Stream IPC buffer', async () => {
        const ipc_buffers = await arrow_ipc_stream(conn, 'SELECT * FROM test');
        const reader = await arrow.RecordBatchReader.from(ipc_buffers);
        const table = await arrow.tableFromIPC(reader);
        assert.equal(table.numRows, column_size);
    });

    it('DuckDB table -> Materialized IPC buffer', async () => {
        const res = await arrow_ipc_materialized(conn, 'SELECT * FROM test');
        assert(res.length > 0);
    });
});

describe('Buffer registration', () => {
    let db;
    let conn1;
    let conn2;

    before(async () => {
        db = await getDatabase();
        conn1 = await getConnection(db);
        conn2 = await getConnection(db);
    });

    it('Buffers can only be overwritten with force flag', async () => {
        const arrow_buffer = await arrow_ipc_materialized(conn1, "SELECT 1337 as a");

        await registerBuffer(conn1, 'arrow_buffer', arrow_buffer, true);

        await assert.rejects(
            async () => {
                await registerBuffer(conn1, 'arrow_buffer', arrow_buffer, false);
            },
            (err) => {
                assert(err.message.includes('Buffer with this name already exists and force_register is not enabled'));
                return true;
            }
        );
    });

    it('Existing tables are silently shadowed by registered buffers', async () => {
        await unregisterBuffer(conn1, 'arrow_buffer');

        await conn1.run('CREATE OR REPLACE TABLE arrow_buffer AS SELECT 7 as a;');

        let reader = await conn1.runAndReadAll('SELECT * FROM arrow_buffer;');
        assert.deepEqual(getRowsJson(reader), [{ 'a': 7 }]);

        const arrow_buffer = await arrow_ipc_materialized(conn1, "SELECT 1337 as b");

        await registerBuffer(conn1, 'arrow_buffer', arrow_buffer, true);

        reader = await conn1.runAndReadAll('SELECT * FROM arrow_buffer;');
        assert.deepEqual(getRowsJson(reader), [{ 'b': 1337 }]);

        await unregisterBuffer(conn1, 'arrow_buffer');

        reader = await conn1.runAndReadAll('SELECT * FROM arrow_buffer;');
        assert.deepEqual(getRowsJson(reader), [{ 'a': 7 }]);

        await conn1.run('DROP TABLE arrow_buffer;');
    });

    it('Registering buffers should only be visible within current connection', async () => {
        const arrow_buffer1 = await arrow_ipc_materialized(conn1, "SELECT 1337 as a");
        const arrow_buffer2 = await arrow_ipc_materialized(conn2, "SELECT 42 as b");

        await registerBuffer(conn1, 'arrow_buffer', arrow_buffer1, true);
        await registerBuffer(conn2, 'arrow_buffer', arrow_buffer2, true);

        let reader1 = await conn1.runAndReadAll('SELECT * FROM arrow_buffer;');
        assert.deepEqual(getRowsJson(reader1), [{ 'a': 1337 }]);

        let reader2 = await conn2.runAndReadAll('SELECT * FROM arrow_buffer;');
        assert.deepEqual(getRowsJson(reader2), [{ 'b': 42 }]);

        conn1 = null;

        reader2 = await conn2.runAndReadAll('SELECT * FROM arrow_buffer;');
        assert.deepEqual(getRowsJson(reader2), [{ 'b': 42 }]);

        await unregisterBuffer(conn2, 'arrow_buffer');

        await assert.rejects(
            async () => {
                await conn2.runAndReadAll('SELECT * FROM arrow_buffer;');
            },
            (err) => {
                assert(err.message.includes('Catalog Error') || err.message.includes('does not exist'));
                return true;
            }
        );
    });
});

describe(`Single Value IPC`, () => {
    let db;
    let conn;

    before(async () => {
        db = await getDatabase();
        conn = await getConnection(db);
    });

    it('Try to read from query returtning one value', async () => {
        const sql = "select now() as t";
        const result = await streamResults(conn, sql);
        assert.strictEqual(result.length, 1, "Expected exactly one row");
        assert.strictEqual(Object.keys(result[0]).length, 1, "Expected exactly one field");
    });
});

describe('[Benchmark] Arrow IPC TPC-H lineitem.parquet', () => {
    const sql = "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= CAST('1994-01-01' AS date) AND l_shipdate < CAST('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24";
    const answer = [{ revenue: 1193053.2253 }];

    let db;
    let conn;

    before(async () => {
        db = await getDatabase();
        conn = await getConnection(db);
    });

    it('Parquet -> DuckDB Streaming-> Arrow IPC -> DuckDB Query', async () => {
        const ipc_buffers = await arrow_ipc_stream(conn, 'SELECT * FROM "' + parquet_file_path + '"');

        const query = sql.replace("lineitem", "my_arrow_ipc_stream");
        await registerBuffer(conn, "my_arrow_ipc_stream", ipc_buffers, true);

        const reader = await conn.runAndReadAll(query);
        assert.deepEqual(getRowsJson(reader), answer);
    });

    it('Parquet -> DuckDB Materialized -> Arrow IPC -> DuckDB', async () => {
        const ipc_buffers = await arrow_ipc_materialized(conn, 'SELECT * FROM "' + parquet_file_path + '"');

        const query = sql.replace("lineitem", "my_arrow_ipc_stream_2");
        await registerBuffer(conn, "my_arrow_ipc_stream_2", ipc_buffers, true);

        const reader = await conn.runAndReadAll(query);
        assert.deepEqual(getRowsJson(reader), answer);
    });

    it('Parquet -> DuckDB', async () => {
        await conn.run('CREATE OR REPLACE TABLE load_parquet_directly AS SELECT * FROM "' + parquet_file_path + '";');

        const query = sql.replace("lineitem", "load_parquet_directly");
        const reader = await conn.runAndReadAll(query);

        assert.deepEqual(getRowsJson(reader), answer);
    });
});

for (const [name, fun] of Object.entries(to_ipc_functions)) {
    describe(`Arrow IPC TPC-H lineitem SF0.01 (${name})`, () => {
        const queries = [
            "select count(*) from table_name LIMIT 10",
            "select sum(l_orderkey) as sum_orderkey FROM table_name",
            "select * from table_name",
            "select l_orderkey from table_name WHERE l_orderkey=2 LIMIT 2",
            "select l_extendedprice from table_name",
            "select l_extendedprice from table_name WHERE l_extendedprice > 53468 and l_extendedprice < 53469  LIMIT 2",
            "select count(l_orderkey) from table_name where l_commitdate > '1996-10-28'",
            "SELECT sum(l_extendedprice * l_discount) AS revenue FROM table_name WHERE l_shipdate >= CAST('1994-01-01' AS date) AND l_shipdate < CAST('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"
        ];

        let db;
        let conn;
        before(async () => {
            db = await getDatabase();
            conn = await getConnection(db);
        });

        for (const query of queries) {
            it(` ${query}`, async () => {
                const readerExpected = await conn.runAndReadAll(query.replace("table_name", `'${parquet_file_path}'`));
                const expected_value = getRowsJson(readerExpected);

                const ipc_buffers = await fun(conn, 'SELECT * FROM "' + parquet_file_path + '"');

                await registerBuffer(conn, "table_name", ipc_buffers, true);

                const readerActual = await conn.runAndReadAll(query);
                const actual_value = getRowsJson(readerActual);

                assert.deepEqual(actual_value, expected_value, `Query failed: ${query}`);
            });
        }
    });
}
