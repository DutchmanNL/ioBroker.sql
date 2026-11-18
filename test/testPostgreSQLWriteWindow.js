/* jshint -W097 */
/*jslint node: true */
/*jshint expr: true*/
// Write window (#516): with "pgWriteWindow" set, the values of all datapoints are collected and written in one
// transaction per window. Reads see the collected values, and a normal stop writes them.
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const { Client } = require('pg');
const setup = require('./lib/setup');

let objects = null;
let states = null;
let onStateChanged = null;
let sendToID = 1;
let stopped = false;

const adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.') + 1);

// A database of its own, so this file neither reads nor leaves rows in the "iobroker" database that
// testPostgreSQL.js asserts on when the glob runs them in one process.
const DB_NAME = 'iob_write_window';
const WINDOW = 4000;
const DPS = ['windowA', 'windowB', 'windowC'].map(name => `${adapterShortName}.0.${name}`);
const VALUES_PER_DP = 10;
// A text datapoint: its table is taken away to make one transaction fail
const TEXT_DP = `${adapterShortName}.0.windowText`;

const SUPER_USER = process.env.SQL_USER || 'postgres';
const SUPER_PASS = process.env.SQL_PASS || '';

function client(database) {
    return new Client({ host: '127.0.0.1', port: 5432, user: SUPER_USER, password: SUPER_PASS, database });
}

async function asSuperuser(statements) {
    const admin = client('postgres');
    await admin.connect();
    try {
        for (const sql of statements) {
            await admin.query(sql);
        }
    } finally {
        await admin.end();
    }
}

// Read-only queries straight against the database of the adapter
async function query(sql, params) {
    const db = client(DB_NAME);
    await db.connect();
    try {
        return (await db.query(sql, params)).rows;
    } finally {
        await db.end();
    }
}

// The next transaction id of the server. Only transactions that write get one, so the difference counts the
// transactions that wrote in between.
async function nextXid() {
    const rows = await query('SELECT pg_snapshot_xmax(pg_current_snapshot())::text AS xid');
    return parseInt(rows[0].xid, 10);
}

// Rows stored for these timestamps, by datapoint name
async function storedRows(timestamps) {
    return query(
        'SELECT d.name, t.ts, t.val FROM ts_number t JOIN datapoints d ON d.id = t.id WHERE t.ts = ANY($1::bigint[]) ORDER BY t.ts',
        [timestamps],
    );
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
const setStateAsync = (id, state) =>
    new Promise((resolve, reject) => states.setState(id, state, err => (err ? reject(err) : resolve())));
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));

// The adapter runs as a child process and writes its log to the log file of the test controller
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

// Every "Wrote N values of M datapoints in one transaction" line of the log
function windowWrites(log) {
    return [...log.matchAll(/Wrote (\d+) values of (\d+) datapoints in one transaction/g)].map(match => ({
        values: parseInt(match[1], 10),
        datapoints: parseInt(match[2], 10),
    }));
}

// Wait until the rows of these timestamps are stored
async function waitForRows(timestamps) {
    let rows = [];
    for (let i = 0; i < 40 && rows.length < timestamps.length; i++) {
        await wait(250);
        rows = await storedRows(timestamps);
    }
    return rows;
}

describe(`Test ${__filename}`, function () {
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
                    config.native.pgWriteWindow = WINDOW;

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

                            for (const id of DPS) {
                                await objects.setObjectAsync(id, {
                                    common: { type: 'number', role: 'state', read: true, write: true },
                                    type: 'state',
                                    native: {},
                                });
                            }
                            await objects.setObjectAsync(TEXT_DP, {
                                common: { type: 'string', role: 'text', read: true, write: true },
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

    it(`Test ${__filename}: logs ${DPS.length} datapoints`, async function () {
        this.timeout(60000);
        for (const id of DPS) {
            const result = await sendToAsync('enableHistory', {
                id,
                options: { changesOnly: false, debounce: 0, retention: 0, maxLength: 0, storageType: 'Number' },
            });
            assert.strictEqual(result.error, undefined);
            assert.strictEqual(result.success, true);
        }
        const text = await sendToAsync('enableHistory', {
            id: TEXT_DP,
            options: { changesOnly: false, debounce: 0, retention: 0, maxLength: 0, storageType: 'String' },
        });
        assert.strictEqual(text.success, true);
        await wait(3000);

        // One value per datapoint first: it creates the datapoint in the database, which writes too
        const now = Date.now();
        const timestamps = DPS.map((_id, i) => now + i);
        for (let i = 0; i < DPS.length; i++) {
            await setStateAsync(DPS[i], { val: i, ack: true, ts: timestamps[i] });
        }
        const rows = await waitForRows(timestamps);
        assert.strictEqual(rows.length, DPS.length, `only ${rows.length} of the first values stored`);

        const textTs = now + 100;
        await setStateAsync(TEXT_DP, { val: 'first', ack: true, ts: textTs });
        let textRows = [];
        for (let i = 0; i < 40 && !textRows.length; i++) {
            await wait(250);
            textRows = await query('SELECT val FROM ts_string WHERE ts = $1', [textTs]);
        }
        assert.deepStrictEqual(textRows, [{ val: 'first' }]);
    });

    it(`Test ${__filename}: the values of all datapoints are written in one transaction`, async function () {
        this.timeout(60000);
        const offset = logSize();
        const xidBefore = await nextXid();

        const base = Date.now() + 1000;
        const expected = [];
        for (let k = 0; k < VALUES_PER_DP; k++) {
            for (let i = 0; i < DPS.length; i++) {
                const ts = base + k * 10 + i;
                const val = k * 10 + i + 0.5;
                expected.push({ name: DPS[i], ts, val });
                await setStateAsync(DPS[i], { val, ack: true, ts });
            }
        }

        // reads see the values while they are collected
        const history = await sendToAsync('getHistory', {
            id: DPS[0],
            options: { start: base - 1, end: base + 1000, aggregate: 'none', count: 100 },
        });
        assert.ok(!history.error, `getHistory: ${history.error}`);
        // without the value before the start, which a read without aggregation may add as border
        assert.deepStrictEqual(
            history.result.filter(r => r.ts >= base).map(r => r.val),
            expected.filter(e => e.name === DPS[0]).map(e => e.val),
        );

        const rows = await waitForRows(expected.map(e => e.ts));
        assert.deepStrictEqual(
            rows.map(r => ({ name: r.name, ts: parseInt(r.ts, 10), val: r.val })),
            expected,
        );

        const transactions = (await nextXid()) - xidBefore;
        const writes = windowWrites(logSince(offset));
        console.log(
            `Write window: ${expected.length} values in ${transactions} transactions, log: ${JSON.stringify(writes)}`,
        );
        // the values may fall into two windows
        assert.ok(writes.length >= 1 && writes.length <= 2, `${writes.length} window writes in the log`);
        assert.strictEqual(
            writes.reduce((sum, w) => sum + w.values, 0),
            expected.length,
        );
        assert.ok(
            writes.some(w => w.datapoints === DPS.length),
            'no transaction with all datapoints',
        );
        assert.ok(transactions <= 3, `${transactions} transactions wrote`);
    });

    it(`Test ${__filename}: control - storeState writes each value in its own transaction`, async function () {
        this.timeout(60000);
        // without this, a measure that cannot see transactions would pass the test above
        const xidBefore = await nextXid();
        const base = Date.now() + 5000;
        for (let k = 0; k < 20; k++) {
            const result = await sendToAsync('storeState', { id: DPS[0], state: { val: k, ack: true, ts: base + k } });
            assert.ok(!result.error, `storeState: ${result.error}`);
        }
        const transactions = (await nextXid()) - xidBefore;
        console.log(`storeState: 20 values in ${transactions} transactions`);
        assert.ok(transactions >= 20, `only ${transactions} transactions for 20 values`);
    });

    it(`Test ${__filename}: when the transaction fails, every datapoint is written on its own`, async function () {
        this.timeout(60000);
        const offset = logSize();
        // the INSERT of the text datapoint fails, and with it the transaction of the whole window
        await query('ALTER TABLE ts_string RENAME TO ts_string_away');
        const base = Date.now() + 8000;
        const timestamps = DPS.map((_id, i) => base + i);
        try {
            for (let i = 0; i < DPS.length; i++) {
                await setStateAsync(DPS[i], { val: 50 + i, ack: true, ts: timestamps[i] });
            }
            await setStateAsync(TEXT_DP, { val: 'lost', ack: true, ts: base + 100 });

            const rows = await waitForRows(timestamps);
            assert.deepStrictEqual(
                rows.map(r => ({ ts: parseInt(r.ts, 10), val: r.val })),
                timestamps.map((ts, i) => ({ ts, val: 50 + i })),
            );
            const log = logSince(offset);
            assert.ok(
                /The transaction of the write window failed, every datapoint is written on its own/.test(log),
                `no failed transaction in the log (${log.length} characters)`,
            );
            // the values of the number datapoints were in the failed transaction, not in a window of their own
            assert.deepStrictEqual(windowWrites(log), []);
        } finally {
            await query('ALTER TABLE ts_string_away RENAME TO ts_string');
        }
    });

    it(`Test ${__filename}: a normal stop writes the collected values`, async function () {
        this.timeout(60000);
        const base = Date.now() + 10000;
        const timestamps = DPS.map((_id, i) => base + i);
        for (let i = 0; i < DPS.length; i++) {
            await setStateAsync(DPS[i], { val: 100 + i, ack: true, ts: timestamps[i] });
        }
        // give the adapter the state changes, but stop well before the window ends
        await wait(300);
        await new Promise(resolve =>
            setup.stopController(normalTerminated => {
                console.log(`Write window: Adapter normal terminated: ${normalTerminated}`);
                resolve();
            }),
        );
        stopped = true;

        const rows = await storedRows(timestamps);
        assert.deepStrictEqual(
            rows.map(r => ({ ts: parseInt(r.ts, 10), val: r.val })),
            timestamps.map((ts, i) => ({ ts, val: 100 + i })),
        );
    });

    after(`Test ${__filename} Stop js-controller`, function (done) {
        this.timeout(60000);
        const dropDatabase = () =>
            asSuperuser([`DROP DATABASE IF EXISTS ${DB_NAME} WITH (FORCE)`])
                .then(() => setTimeout(done, 2000))
                .catch(() => setTimeout(done, 2000));
        if (stopped) {
            dropDatabase();
        } else {
            setup.stopController(() => dropDatabase());
        }
    });
});
