/* jshint -W097 */
/*jslint node: true */
/*jshint expr: true*/
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const { Client } = require('pg');
const { initAggregate, aggregation, finishAggregation } = require('@iobroker/aggregate');
const setup = require('./lib/setup');

let objects = null;
let states = null;
let onStateChanged = null;
let sendToID = 1;

const adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.') + 1);

// A database of its own, so this file neither reads nor leaves rows in the "iobroker" database that
// testPostgreSQL.js asserts on when the glob runs them in one process.
const DB_NAME = 'iob_aggregated';
const DP = `${adapterShortName}.0.aggregatedValue`;

// 50 intervals of 100 rows. More rows than two batches of STREAM_BATCH_SIZE (2000), so the control read of
// `count`, which stays with the adapter, is streamed in three batches
const ROWS = 5000;
const COUNT = 50;
const STEP = 60_000;
const START = Math.floor((Date.now() - 7 * 86_400_000) / 60_000) * 60_000;
const END = START + ROWS * STEP;
const ROUND = 4;

const SUPER_USER = process.env.SQL_USER || 'postgres';
const SUPER_PASS = process.env.SQL_PASS || '';

async function asSuperuser(statements) {
    const client = new Client({
        host: '127.0.0.1',
        port: 5432,
        user: SUPER_USER,
        password: SUPER_PASS,
        database: 'postgres',
    });
    await client.connect();
    try {
        for (const sql of statements) {
            await client.query(sql);
        }
    } finally {
        await client.end();
    }
}

function checkConnectionOfAdapter(cb, counter) {
    counter ||= 0;
    if (counter > 240) {
        cb?.('Cannot check connection');
        return;
    }

    states.getState(`system.adapter.${adapterShortName}.0.alive`, (err, state) => {
        if (err) {
            console.error(`${adapterShortName}:${err}`);
        }
        if (state && state.val) {
            cb?.();
        } else {
            setTimeout(() => checkConnectionOfAdapter(cb, counter + 1), 250);
        }
    });
}

function sendTo(target, command, message, callback) {
    onStateChanged = function (id, state) {
        if (id === 'messagebox.system.adapter.test.0') {
            callback(state.message);
        }
    };

    states.pushMessage(`system.adapter.${target}`, {
        command: command,
        message: message,
        from: 'system.adapter.test.0',
        callback: {
            message: message,
            id: sendToID++,
            ack: false,
            time: new Date().getTime(),
        },
    });
}

const sendToAsync = (command, message) => new Promise(resolve => sendTo('sql.0', command, message, resolve));

// The adapter runs as a child process and writes its log to the log file of the test controller. Its
// "Send after aggregation (in the database)" line is the evidence that PostgreSQL prepared a read and that
// it did not fall back to the read in the adapter.
const LOG_FILE = path.join(__dirname, '..', 'tmp', 'log', 'iobroker.current.log');

function logSize() {
    return fs.existsSync(LOG_FILE) ? fs.statSync(LOG_FILE).size : 0;
}

function logSince(offset) {
    const size = logSize();
    if (size <= offset) {
        return '';
    }
    const buffer = Buffer.alloc(size - offset);
    const fd = fs.openSync(LOG_FILE, 'r');
    try {
        fs.readSync(fd, buffer, 0, buffer.length, offset);
    } finally {
        fs.closeSync(fd);
    }
    return buffer.toString('utf8');
}

// Run a request and return its answer with the log the adapter wrote for it, once `until` shows up there
async function withLog(fn, until) {
    const offset = logSize();
    const result = await fn();
    let log = '';
    for (let i = 0; i < 50 && !until.test(log); i++) {
        await new Promise(resolve => setTimeout(resolve, 100));
        log = logSince(offset);
    }
    return { result, log };
}

function streamedBatches(log) {
    const match = log.match(/streamed in (\d+) batches/);
    return match ? parseInt(match[1], 10) : 0;
}

// Number of rows PostgreSQL sent for a read it prepared, or -1
function rowsFromDatabase(log) {
    const match = log.match(/Send after aggregation \(in the database\): \d+ of: (\d+) rows/);
    return match ? parseInt(match[1], 10) : -1;
}

// What the buffered read answers: the same rows the query returns (the range, the last row before the
// start and the first row after the end), handed to @iobroker/aggregate as one array
function expectedFor(rawRows, aggregate) {
    const inRange = rawRows.filter(r => r.ts >= START && r.ts < END);
    const before = rawRows.filter(r => r.ts < START).pop();
    const after = rawRows.find(r => r.ts >= END);
    const rows = [before, ...inRange, after].filter(Boolean).map(r => ({
        ts: r.ts,
        val: typeof r.val === 'number' ? Math.round(r.val * 10 ** ROUND) / 10 ** ROUND : r.val,
    }));

    const options = initAggregate(
        {
            start: START,
            end: END,
            count: COUNT,
            limit: COUNT,
            aggregate,
            ignoreNull: false,
            from: false,
            q: false,
            ack: false,
            addId: false,
            returnNewestEntries: false,
            removeBorderValues: false,
            round: 10 ** ROUND,
        },
        DP,
    );
    aggregation(options, rows);
    finishAggregation(options);
    // The answer reaches the test as JSON, which turns NaN (e.g. at the borders of `count`) into null
    return JSON.parse(JSON.stringify(options.result));
}

describe(`Test ${__filename}`, function () {
    let rawRows;

    before(`Test ${__filename} Start js-controller`, function (_done) {
        this.timeout(600000); // because of first install from npm
        setup.adapterStarted = false;

        asSuperuser([`DROP DATABASE IF EXISTS ${DB_NAME}`])
            .then(() => {
                setup.setupController(async function () {
                    const config = await setup.getAdapterConfig();
                    config.common.enabled = true;
                    config.common.loglevel = 'debug';

                    config.native.enableDebugLogs = true;
                    config.native.host = '127.0.0.1';
                    config.native.port = 5432;
                    config.native.dbtype = 'postgresql';
                    config.native.user = SUPER_USER;
                    config.native.password = SUPER_PASS;
                    config.native.dbname = DB_NAME;
                    config.native.round = ROUND;
                    config.native.pgAggregateInDatabase = true;

                    await setup.setAdapterConfig(config.common, config.native);

                    setup.startController(
                        true,
                        () => {},
                        function (id, state) {
                            if (onStateChanged) onStateChanged(id, state);
                        },
                        async (_objects, _states) => {
                            objects = _objects;
                            states = _states;

                            await objects.setObjectAsync('system.adapter.test.0', {
                                common: {},
                                type: 'instance',
                            });
                            states.subscribeMessage('system.adapter.test.0');

                            await objects.setObjectAsync(DP, {
                                common: { type: 'number', role: 'state', read: true, write: true },
                                type: 'state',
                                native: {},
                            });

                            _done();
                        },
                    );
                });
            })
            .catch(_done);
    });

    it(`Test ${__filename}: adapter connects`, function (done) {
        this.timeout(120000);
        checkConnectionOfAdapter(function (error) {
            assert.ok(!error, error);
            let tries = 0;
            const poll = () => {
                states.getState(`${adapterShortName}.0.info.connection`, (err, state) => {
                    assert.ok(!err, err);
                    if (state && state.val === true) {
                        return done();
                    }
                    if (++tries > 200) {
                        return done(new Error('adapter never reported a connection'));
                    }
                    setTimeout(poll, 250);
                });
            };
            poll();
        });
    });

    it(`Test ${__filename}: stores ${ROWS} values and the border values`, async function () {
        this.timeout(300000);
        const result = await sendToAsync('enableHistory', {
            id: DP,
            options: { changesOnly: false, debounce: 0, retention: 0, maxLength: 0, storageType: 'Number' },
        });
        assert.strictEqual(result.error, undefined);
        assert.strictEqual(result.success, true);
        await new Promise(resolve => setTimeout(resolve, 3000));

        // Fixed values with gaps, plus one value two days before the start and one a day after the end. Every
        // other value comes from a few that differ only after the fourth decimal, so many rows of an interval
        // share its smallest and its largest value after rounding.
        const values = [{ ts: START - 2 * 86_400_000, val: 21.5, ack: true }];
        for (let i = 0; i < ROWS; i++) {
            let val;
            if (i % 97 === 0) {
                val = null;
            } else if (i % 2) {
                val = 20 + ((i * 7) % 5) / 10 + (i % 3) * 0.00002;
            } else {
                val = 20 + ((i * 37) % 1000) / 2500 + ((i * 13) % 7) / 1000;
            }
            values.push({ ts: START + i * STEP + 1000, val, ack: true });
        }
        values.push({ ts: END + 86_400_000, val: 3.5, ack: true });

        const stored = await sendToAsync('storeState', { id: DP, state: values });
        assert.ok(!stored.error, `storeState: ${stored.error}`);

        // read back what the database holds, to compute the expected answers
        let raw;
        for (let i = 0; i < 60; i++) {
            raw = await sendToAsync(
                'query',
                `SELECT ts, val FROM ts_number WHERE id = (SELECT id FROM datapoints WHERE name = '${DP}') ORDER BY ts`,
            );
            if (!raw.error && raw.result.length >= ROWS + 2) {
                break;
            }
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
        assert.ok(!raw.error, `query: ${raw.error}`);
        rawRows = raw.result.map(r => ({ ts: parseInt(r.ts, 10), val: r.val }));
        assert.ok(rawRows.length >= ROWS + 2, `only ${rawRows.length} rows stored`);
    });

    for (const aggregate of ['average', 'minmax', 'min', 'max', 'total']) {
        it(`Test ${__filename}: ${aggregate} is prepared by PostgreSQL and equals the buffered answer`, async function () {
            this.timeout(60000);
            const { result, log } = await withLog(
                () =>
                    sendToAsync('getHistory', {
                        id: DP,
                        options: { start: START, end: END, count: COUNT, aggregate, round: ROUND },
                    }),
                /Send after aggregation/,
            );
            assert.ok(!result.error, `getHistory: ${result.error}`);

            const rows = rowsFromDatabase(log);
            console.log(`In the database ${aggregate}: ${result.result.length} values from ${rows} rows`);
            // at most the first, the last, the smallest and the largest value per interval, plus the two border rows
            assert.ok(rows > 0 && rows <= 4 * COUNT + 2, `not prepared by PostgreSQL (rows: ${rows})`);
            assert.strictEqual(streamedBatches(log), 0);
            assert.strictEqual(result.step, 0);
            assert.ok(result.result.length > 10, `only ${result.result.length} values`);
            assert.deepStrictEqual(result.result, expectedFor(rawRows, aggregate));
        });
    }

    it(`Test ${__filename}: control - count stays with the adapter and is streamed`, async function () {
        this.timeout(60000);
        const { result, log } = await withLog(
            () =>
                sendToAsync('getHistory', {
                    id: DP,
                    options: { start: START, end: END, count: COUNT, aggregate: 'count', round: ROUND },
                }),
            /Send after aggregation/,
        );
        assert.ok(!result.error, `getHistory: ${result.error}`);
        // the answer line of this very read must be in the log, or "not in the database" proves nothing
        assert.strictEqual(streamedBatches(log), 3, `not streamed (log: ${log.length} characters)`);
        assert.strictEqual(rowsFromDatabase(log), -1);
        assert.deepStrictEqual(result.result, expectedFor(rawRows, 'count'));
    });

    after(`Test ${__filename} Stop js-controller`, function (done) {
        this.timeout(60000);
        setup.stopController(function (normalTerminated) {
            console.log(`Aggregated: Adapter normal terminated: ${normalTerminated}`);
            asSuperuser([`DROP DATABASE IF EXISTS ${DB_NAME} WITH (FORCE)`])
                .then(() => setTimeout(done, 2000))
                .catch(() => setTimeout(done, 2000));
        });
    });
});
