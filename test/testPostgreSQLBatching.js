/* jshint -W097 */ // jshint strict:false
/*jslint node: true */
/*jshint expr: true*/
const assert = require('node:assert');
const { Client } = require('pg');
const setup = require('./lib/setup');

// Standalone PostgreSQL integration test for the opt-in time-window write batching path (PR-C).
// It runs its OWN js-controller, so it MUST live in its own file and get its own mocha invocation
// (never globbed together with test/testPostgreSQL.js - one js-controller per mocha process).
//
// The adapter is configured with writeInterval = WRITE_INTERVAL ms: state changes are buffered in RAM
// and written together in one transaction every window instead of one commit per value.

let objects = null;
let states = null;
let onStateChanged = null;
let sendToID = 1;

const adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.') + 1);

// Wide enough that a slow CI runner comfortably fits a write plus a DB round trip inside one window;
// CI round trips have been observed to exceed 500 ms. The shutdown-flush test does not rely on this
// margin alone - it syncs to the flush timer first - but the burst test still reads "inside the
// window" by wall clock. All waits in this file scale off this constant.
const WRITE_INTERVAL = 5000;
const dp1 = 'sql.0.batchTest1';
const dp2 = 'sql.0.batchTest2';

function checkConnectionOfAdapter(cb, counter) {
    counter ||= 0;
    if (counter > 20) {
        cb?.('Cannot check connection');
        return;
    }

    states.getState(`system.adapter.${adapterShortName}.0.alive`, (err, state) => {
        if (err) {
            console.error(`PostgreSQL-batching: ${err}`);
        }
        if (state?.val) {
            cb?.();
        } else {
            setTimeout(() => checkConnectionOfAdapter(cb, counter + 1), 1000);
        }
    });
}

// Read one datapoint's numeric values straight from PostgreSQL, bypassing the adapter entirely.
// Used to prove what actually reached the DB (the adapter is stopped at that point).
async function readValuesFromDb(dpName, ts) {
    const client = new Client({
        host: '127.0.0.1',
        port: 5432,
        user: process.env.SQL_USER || 'postgres',
        password: process.env.SQL_PASS || '',
        database: 'iobroker',
    });
    await client.connect();
    try {
        const res = await client.query(
            `SELECT val FROM ts_number WHERE id IN (SELECT id FROM datapoints WHERE name = $1) AND ts = $2`,
            [dpName, ts],
        );
        return res.rows.map(r => r.val);
    } finally {
        await client.end();
    }
}

// Poll the DB until `val` shows up at `ts`. Used to observe a flush actually happening, which is the
// only way a test can tell where it currently sits inside the adapter's fixed flush cadence.
async function waitForValueInDb(dpName, ts, val, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
        const vals = await readValuesFromDb(dpName, ts);
        if (vals.includes(val)) {
            return;
        }
        if (Date.now() >= deadline) {
            throw new Error(
                `value ${val} never reached the DB within ${timeoutMs} ms - the flush timer does not appear to be running`,
            );
        }
        await new Promise(resolve => setTimeout(resolve, 100));
    }
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

describe(`Test ${__filename}`, function () {
    before(`Test ${__filename} Start js-controller`, function (_done) {
        this.timeout(600000); // because of first install from npm
        setup.adapterStarted = false;

        setup.setupController(async function () {
            const config = await setup.getAdapterConfig();
            // enable adapter
            config.common.enabled = true;
            config.common.loglevel = 'debug';

            config.native.enableDebugLogs = true;
            config.native.host = '127.0.0.1';
            config.native.dbtype = 'postgresql';
            config.native.user = process.env.SQL_USER || 'postgres';
            config.native.password = process.env.SQL_PASS || '';
            // The feature under test: buffer values and flush them together once per window.
            config.native.writeInterval = WRITE_INTERVAL;

            await setup.setAdapterConfig(config.common, config.native);

            setup.startController(
                true,
                function (id, obj) {},
                function (id, state) {
                    if (onStateChanged) onStateChanged(id, state);
                },
                async (_objects, _states) => {
                    objects = _objects;
                    states = _states;

                    // This standalone file does not call tests.preInit, so it must set up the message
                    // routing itself: create the test.0 instance and subscribe to its message box, or
                    // sendTo responses never route back and every message test times out.
                    await objects.setObjectAsync('system.adapter.test.0', {
                        common: {},
                        type: 'instance',
                    });
                    states.subscribeMessage('system.adapter.test.0');

                    // Create the two number datapoints that will be logged.
                    const numberObj = () => ({
                        common: { type: 'number', role: 'state', read: true, write: true },
                        type: 'state',
                        native: {},
                    });
                    await objects.setObjectAsync(dp1, numberObj());
                    await objects.setObjectAsync(dp2, numberObj());

                    _done();
                },
            );
        });
    });

    it(`Test ${__filename}: Check if adapter started and enable history`, function (done) {
        this.timeout(60000);
        checkConnectionOfAdapter(function (err) {
            assert.ok(!err, err);
            const options = {
                changesOnly: false,
                debounce: 0,
                retention: 31536000,
                maxLength: 0,
                storageType: 'Number',
            };
            sendTo('sql.0', 'enableHistory', { id: dp1, options }, function (result1) {
                assert.strictEqual(result1.error, undefined);
                assert.strictEqual(result1.success, true);
                sendTo('sql.0', 'enableHistory', { id: dp2, options }, function (result2) {
                    assert.strictEqual(result2.error, undefined);
                    assert.strictEqual(result2.success, true);
                    // let the adapter receive and apply the new settings (subscribe to both datapoints)
                    setTimeout(done, 10000);
                });
            });
        });
    });

    it(`Test ${__filename}: burst is visible immediately (RAM cache) and flushed by the timer`, function (done) {
        this.timeout(30000);

        const testStart = Date.now();
        // Distinct values on two datapoints, written in a quick burst well inside one buffer window.
        const vals1 = [11, 12, 13];
        const vals2 = [21, 22, 23];

        let pending = vals1.length + vals2.length;
        const onSet = () => {
            if (!--pending) {
                afterBurst();
            }
        };

        vals1.forEach((val, i) => states.setState(dp1, { val, ts: testStart + i, ack: true }, onSet));
        vals2.forEach((val, i) => states.setState(dp2, { val, ts: testStart + 100 + i, ack: true }, onSet));

        function afterBurst() {
            // Give the adapter a moment to process the stateChange events, but stay inside the write
            // window so the values are still only in the RAM cache (or an in-flight batch) - both of
            // which getHistory merges into its result. This proves reads are complete during the window.
            setTimeout(() => {
                sendTo(
                    'sql.0',
                    'getHistory',
                    { id: dp1, options: { start: testStart, end: testStart + 60000, aggregate: 'none', count: 100 } },
                    function (res1) {
                        assert.ok(!res1.error, `getHistory dp1 error: ${res1.error}`);
                        const got1 = res1.result.map(r => r.val).filter(v => v !== null);
                        console.log(`PostgreSQL-batching immediate dp1: ${JSON.stringify(got1)}`);
                        vals1.forEach(v => assert.ok(got1.includes(v), `dp1 value ${v} missing from cache-merged read`));

                        sendTo(
                            'sql.0',
                            'getHistory',
                            {
                                id: dp2,
                                options: { start: testStart, end: testStart + 60000, aggregate: 'none', count: 100 },
                            },
                            function (res2) {
                                assert.ok(!res2.error, `getHistory dp2 error: ${res2.error}`);
                                const got2 = res2.result.map(r => r.val).filter(v => v !== null);
                                console.log(`PostgreSQL-batching immediate dp2: ${JSON.stringify(got2)}`);
                                vals2.forEach(v =>
                                    assert.ok(got2.includes(v), `dp2 value ${v} missing from cache-merged read`),
                                );

                                // Now wait well past the window so at least one timer tick has flushed the
                                // buffer, then read the rows straight from the DB (bypassing the RAM cache)
                                // to prove the timer actually committed one batch.
                                setTimeout(assertInDb, WRITE_INTERVAL * 3);
                            },
                        );
                    },
                );
            }, 250);
        }

        function assertInDb() {
            const query =
                `SELECT val FROM ts_number WHERE id IN (SELECT id FROM datapoints WHERE name = '${dp1}') ` +
                `AND ts >= ${testStart} AND ts < ${testStart + 60000} ORDER BY ts ASC`;
            sendTo('sql.0', 'query', query, function (dbRes) {
                assert.ok(!dbRes.error, `raw query error: ${dbRes.error}`);
                const dbVals = dbRes.result.map(r => r.val).filter(v => v !== null);
                console.log(`PostgreSQL-batching in-DB dp1: ${JSON.stringify(dbVals)}`);
                vals1.forEach(v => assert.ok(dbVals.includes(v), `dp1 value ${v} was not flushed to the DB`));
                done();
            });
        }
    });

    it(`Test ${__filename}: buffered value is flushed to the DB on adapter stop`, async function () {
        this.timeout(60000);

        const shutdownTs = Date.now() + 3600000; // 1h in the future: unique, cannot collide with earlier rows
        const shutdownVal = 4242;

        const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
        const setState = (id, val, ts) =>
            new Promise((resolve, reject) => {
                states.setState(id, { val, ts, ack: true }, err => (err ? reject(err) : resolve()));
            });

        // The flush timer is a setInterval started when the adapter booted, so its ticks fall on a fixed
        // cadence that has nothing to do with when this test writes. Writing the value blind would leave a
        // ~100 ms window in which the next tick lands between the write and the pre-stop check, flushing the
        // value early and failing the precondition below for a reason that says nothing about shutdown.
        //
        // So first find out where we are in that cadence: write a sentinel and wait for it to reach the DB.
        // Its arrival means a tick has just fired, which leaves the real value - written immediately after -
        // very nearly a full WRITE_INTERVAL before the next one.
        const sentinelTs = shutdownTs - 1000; // distinct row, same 1h-in-the-future region
        const sentinelVal = 4241;
        await setState(dp1, sentinelVal, sentinelTs);
        await waitForValueInDb(dp1, sentinelTs, sentinelVal, WRITE_INTERVAL * 3);
        console.log(`PostgreSQL-batching synced to flush timer via sentinel at ts=${sentinelTs}`);

        await setState(dp1, shutdownVal, shutdownTs);

        // Wait only 100 ms - a whole window away from the next tick after the sync above - so the value is
        // still only in RAM and has NOT been written by the timer yet. A clean stop must flush it.
        await delay(100);

        // Prove the value is NOT in the DB yet (still buffered in RAM at this point).
        const before = await readValuesFromDb(dp1, shutdownTs);
        console.log(`PostgreSQL-batching pre-stop DB rows for ts=${shutdownTs}: ${JSON.stringify(before)}`);
        assert.ok(
            !before.includes(shutdownVal),
            `value ${shutdownVal} was already in the DB before stop, despite being written right after a ` +
                `flush - it should have stayed buffered for a full ${WRITE_INTERVAL} ms window`,
        );

        const reply = await new Promise(resolve => sendTo('sql.0', 'stopInstance', {}, resolve));
        console.log(`PostgreSQL-batching stopInstance reply: ${JSON.stringify(reply)}`);
        assert.strictEqual(reply, 'stopped');

        // The 'stopped' reply is sent from finish()'s callback, i.e. AFTER every datapoint's buffer has
        // been flushed and the pool closed. The adapter is now stopped, so a direct DB read can only see
        // the value if finish() actually wrote it - proving flush-on-stop.
        await delay(1500);
        const after = await readValuesFromDb(dp1, shutdownTs);
        console.log(`PostgreSQL-batching post-stop DB rows: ${JSON.stringify(after)}`);
        assert.ok(after.includes(shutdownVal), `shutdown value ${shutdownVal} was not flushed to the DB on stop`);
    });

    after(`Test ${__filename} Stop js-controller`, function (done) {
        this.timeout(16000);

        setup.stopController(function (normalTerminated) {
            console.log(`PostgreSQL-batching: Adapter normal terminated: ${normalTerminated}`);
            setTimeout(done, 2000);
        });
    });
});
