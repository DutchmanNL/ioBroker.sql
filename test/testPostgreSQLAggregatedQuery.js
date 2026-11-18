// Aggregation in PostgreSQL (#516): the rows of getAggregatedHistory() must give the same answer as the rows of the
// buffered getHistory() read, for every aggregate the database prepares. Runs the queries directly against PostgreSQL
// and calculates both answers with the real @iobroker/aggregate.
const assert = require('node:assert');
const { Client } = require('pg');
const { initAggregate, aggregation, finishAggregation } = require('@iobroker/aggregate');
const PostgreSQL = require('../build/lib/postgresql');

const DB_NAME = 'iob_aggregated_query';
const SUPER_USER = process.env.SQL_USER || 'postgres';
const SUPER_PASS = process.env.SQL_PASS || '';

const START = 1_700_000_000_000;
const ROWS = 20000;
const STEP = 37_000;
const END = START + ROWS * STEP;

const AGGREGATES = ['min', 'max', 'minmax', 'average', 'total'];
const ROUNDS = [undefined, 10000, 100];
const LAYOUTS = {
    'count 50': { count: 50 },
    'count 37 (uneven step)': { count: 37 },
    'step 1234567': { step: 1234567 },
    'interval borders': {
        intervalBorders: [START, START + 1e6, START + 2e8, START + 2.1e8, START + 5e8, START + 6.9e8, END],
    },
};

function client(database) {
    return new Client({ host: '127.0.0.1', port: 5432, user: SUPER_USER, password: SUPER_PASS, database });
}

// What #normalizeRows() in main.ts does to the numbers of a read
function normalize(rows, round) {
    for (const row of rows) {
        if (typeof row.ts === 'string') {
            row.ts = parseInt(row.ts, 10);
        }
        if (typeof row.val === 'number' && isFinite(row.val) && round) {
            row.val = Math.round(row.val * round) / round;
        }
    }
}

function optionsFor(aggregate, round, layout) {
    const options = { start: START, end: END, aggregate, round, limit: 2000, ...LAYOUTS[layout] };
    if (options.intervalBorders) {
        options.start = options.intervalBorders[0];
        options.end = options.intervalBorders[options.intervalBorders.length - 1];
    }
    return options;
}

// The answer the adapter sends, as JSON turns it into
function finish(aggOptions, data) {
    aggregation(aggOptions, data);
    finishAggregation(aggOptions);
    return JSON.parse(JSON.stringify(aggOptions.result));
}

describe(`Test ${__filename}`, function () {
    let db;

    // The query of the buffered read and its answer
    async function buffered(options) {
        const rows = (await db.query(PostgreSQL.getHistory(DB_NAME, 'ts_number', { ...options, index: 1 }))).rows;
        normalize(rows, options.round);
        return finish(initAggregate({ ...options }, 'sql.0.test'), rows);
    }

    // The query of the read aggregated in the database, the way #getAggregatedDataInDatabase() in main.ts uses it
    function queryFor(options) {
        const aggOptions = initAggregate({ ...options }, 'sql.0.test');
        const sql = PostgreSQL.getAggregatedHistory(DB_NAME, 'ts_number', {
            index: 1,
            start: aggOptions.start,
            end: aggOptions.end,
            step: aggOptions.step,
            aggregate: options.aggregate,
            round: options.round,
            intervalBorders: aggOptions.timeIntervals ? aggOptions.intervalBorders : undefined,
        });
        return { aggOptions, sql };
    }

    async function inDatabase(options, sql) {
        const query = queryFor(options);
        const rows = (await db.query(sql || query.sql)).rows;
        const { data, stored } = PostgreSQL.toAggregationInput(rows, options.aggregate);
        normalize(stored, options.round);
        // at most the first, the last, the smallest and the largest value per interval, plus the two border rows
        const maxRows = 4 * (query.aggOptions.maxIndex + 1) + 2;
        return { result: finish(query.aggOptions, data), rows: rows.length, maxRows };
    }

    before(async function () {
        this.timeout(120000);
        const admin = client('postgres');
        await admin.connect();
        await admin.query(`DROP DATABASE IF EXISTS ${DB_NAME}`);
        await admin.query(`CREATE DATABASE ${DB_NAME}`);
        await admin.end();

        db = client(DB_NAME);
        await db.connect();
        // Without statistics on purpose: a wrong estimate of the rows must not make the plan explode
        await db.query(
            'CREATE TABLE ts_number (id INTEGER NOT NULL, ts BIGINT, val REAL, ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts)) WITH (autovacuum_enabled = false)',
        );
        await db.query("SET statement_timeout = '10s'");

        // Fixed pseudo-random values. First half: a wide range with long decimals. Second half: few distinct values,
        // so many rows share the smallest and the largest value, also when they differ only after rounding.
        // Plus gaps, NaN, ±Infinity, a stretch without rows, a stretch of only nulls and two far border rows.
        let seed = 516;
        const random = () => (seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648;
        const values = [[START - 5 * 86_400_000, 12.5]];
        for (let i = 0; i < ROWS; i++) {
            if (i >= 3000 && i < 3400) {
                continue;
            }
            const ts = START + i * STEP + Math.floor(random() * 1000);
            let val;
            if (i >= 7000 && i < 7300) {
                val = null;
            } else if (random() < 0.03) {
                val = null;
            } else if (random() < 0.002) {
                val = 'NaN';
            } else if (random() < 0.001) {
                val = random() < 0.5 ? 'Infinity' : '-Infinity';
            } else if (i < ROWS / 2) {
                val = ((random() - 0.3) * 1000) / 7;
            } else if (random() < 0.5) {
                val = 20 + Math.floor(random() * 5) / 10 + Math.floor(random() * 3) * 0.00002;
            } else {
                val = 20 + random() * 0.4;
            }
            values.push([ts, val]);
        }
        values.push([END + 3 * 86_400_000, -4.25]);

        for (let i = 0; i < values.length; i += 1000) {
            const params = [];
            const tuples = values.slice(i, i + 1000).map(([ts, val], k) => {
                params.push(ts, val);
                return `(1, $${2 * k + 1}, $${2 * k + 2}::real, true, 0, 0)`;
            });
            await db.query(`INSERT INTO ts_number (id, ts, val, ack, _from, q) VALUES ${tuples.join(',')}`, params);
        }
    });

    it(`Test ${__filename}: the table has no statistics`, async function () {
        const stats = (
            await db.query("SELECT last_analyze, last_autoanalyze FROM pg_stat_user_tables WHERE relname = 'ts_number'")
        ).rows[0];
        assert.ok(stats, 'no statistics row for ts_number');
        assert.ok(!stats.last_analyze && !stats.last_autoanalyze, 'ts_number was analyzed');
    });

    for (const aggregate of AGGREGATES) {
        for (const round of ROUNDS) {
            for (const layout of Object.keys(LAYOUTS)) {
                it(`Test ${__filename}: ${aggregate}, round ${round}, ${layout}`, async function () {
                    this.timeout(30000);
                    const options = optionsFor(aggregate, round, layout);
                    const expected = await buffered(options);
                    const { result, rows, maxRows } = await inDatabase(options);
                    assert.ok(expected.length > 5, `only ${expected.length} values`);
                    assert.ok(rows <= maxRows && rows < ROWS / 5, `the database sent ${rows} rows`);
                    assert.deepStrictEqual(result, expected);
                });
            }
        }
    }

    // The comparisons above must be able to see the two things the query has to get exactly right
    it(`Test ${__filename}: control - the latest instead of the earliest largest value is noticed`, async function () {
        const options = optionsFor('minmax', 100, 'count 50');
        const { sql } = queryFor(options);
        const wrong = sql
            .replace('max(ARRAY[v, -(ts::float8), raw])', 'max(ARRAY[v, ts::float8, raw])')
            .replace('-(mx[2])', 'mx[2]');
        assert.notStrictEqual(wrong, sql, 'the control did not change the query');
        const { result } = await inDatabase(options, wrong);
        assert.notDeepStrictEqual(result, await buffered(options));
    });

    it(`Test ${__filename}: control - adding up in another order is noticed`, async function () {
        const options = optionsFor('total', undefined, 'count 50');
        const { sql } = queryFor(options);
        const wrong = sql.replace('sum(v ORDER BY ts)', 'sum(v ORDER BY ts DESC)');
        assert.notStrictEqual(wrong, sql, 'the control did not change the query');
        const { result } = await inDatabase(options, wrong);
        assert.notDeepStrictEqual(result, await buffered(options));
    });

    after(async function () {
        this.timeout(30000);
        await db?.end();
        const admin = client('postgres');
        await admin.connect();
        await admin.query(`DROP DATABASE IF EXISTS ${DB_NAME} WITH (FORCE)`);
        await admin.end();
    });
});
