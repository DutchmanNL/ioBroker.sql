/* jshint -W097 */ // jshint strict:false
/*jslint node: true */
/*jshint expr: true*/
const assert = require('node:assert');
const setup = require('./lib/setup');

// Standalone PostgreSQL integration test for the streamed aggregated getHistory path (PR-B).
// It runs its OWN js-controller, so it MUST live in its own file and get its own mocha invocation
// (never globbed together with test/testPostgreSQL.js - one js-controller per mocha process).

let objects = null;
let states = null;
let onStateChanged = null;
let sendToID = 1;

const adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.') + 1);

// Dedicated synthetic datapoint: in CI all Postgres test steps share one database, so a shared live
// state (like memHeapTotal, which earlier steps enable and write to) would leak old rows and
// writeNulls markers into this test's time window. A datapoint only this file uses is deterministic.
const testDp = 'sql.0.streamTestValue';

function checkConnectionOfAdapter(cb, counter) {
    counter ||= 0;
    if (counter > 20) {
        cb?.('Cannot check connection');
        return;
    }

    states.getState(`system.adapter.${adapterShortName}.0.alive`, (err, state) => {
        if (err) {
            console.error(`PostgreSQL-streamed: ${err}`);
        }
        if (state?.val) {
            cb?.();
        } else {
            setTimeout(() => checkConnectionOfAdapter(cb, counter + 1), 1000);
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

                    _done();
                },
            );
        });
    });

    it(`Test ${__filename}: Check if adapter started and enable history`, function (done) {
        this.timeout(60000);
        checkConnectionOfAdapter(function () {
            objects.setObject(testDp, { common: { type: 'number', role: 'state', custom: {} }, type: 'state' }, () =>
                sendTo(
                    'sql.0',
                    'enableHistory',
                    {
                        id: testDp,
                        options: {
                            changesOnly: false,
                            debounce: 0,
                            retention: 31536000,
                            maxLength: 0,
                            storageType: 'Number',
                        },
                    },
                    function (result) {
                        assert.strictEqual(result.error, undefined);
                        assert.strictEqual(result.success, true);
                        // wait till adapter receives the new settings
                        setTimeout(function () {
                            done();
                        }, 10000);
                    },
                ),
            );
        });
    });

    it(`Test ${__filename}: Streamed aggregated getHistory (average)`, function (done) {
        this.timeout(30000);

        // All timestamps are in the past so they are stored verbatim.
        const base = Date.now() - 200000;
        const start = base;
        const end = base + 40000; // count 2 -> step 20000 -> two buckets: [start, start+20000), [start+20000, end)

        // storeState is write-through (it flushes to the DB before the callback returns), so by read
        // time the RAM cache for this datapoint is empty and the PostgreSQL streaming path is used.
        const values = [
            { ts: base - 5000, val: 5 }, // one value before start (pre-border)
            { ts: base + 1000, val: 10 }, // bucket 1
            { ts: base + 5000, val: 20 }, // bucket 1 -> avg 15
            { ts: base + 21000, val: 30 }, // bucket 2
            { ts: base + 25000, val: 40 }, // bucket 2 -> avg 35
            { ts: base + 45000, val: 50 }, // one value after end (post-border)
        ];

        sendTo('sql.0', 'storeState', { id: testDp, state: values }, function (storeResult) {
            console.log(`PostgreSQL-streamed store: ${JSON.stringify(storeResult)}`);
            assert.ok(!storeResult.error, `storeState error: ${storeResult.error}`);

            // small settle so the write-through is definitely visible to a fresh read
            setTimeout(function () {
                sendTo(
                    'sql.0',
                    'getHistory',
                    {
                        id: testDp,
                        options: {
                            start,
                            end,
                            count: 2,
                            aggregate: 'average',
                            ignoreNull: false,
                        },
                    },
                    function (result) {
                        console.log(`PostgreSQL-streamed getHistory: ${JSON.stringify(result.result, null, 2)}`);
                        assert.ok(!result.error, `getHistory error: ${result.error}`);
                        assert.ok(Array.isArray(result.result), 'result is an array');
                        assert.strictEqual(result.step, 0);

                        // Pull the two bucket averages out of the returned series. Beautify adds
                        // interpolated border points at start/end, so we assert on the bucket midpoints.
                        const byVal = result.result.map(r => r.val);
                        console.log(`PostgreSQL-streamed values: ${JSON.stringify(byVal)}`);
                        // Deterministic: all timestamps are fixed offsets from `base`, so the
                        // interpolated start/end border points and the two bucket averages are stable.
                        // 15 and 35 are the two bucket averages; 10 and 42.5 are the interpolated borders.
                        assert.deepStrictEqual(byVal, [10, 15, 35, 42.5]);
                        done();
                    },
                );
            }, 1500);
        });
    });

    after(`Test ${__filename} Stop js-controller`, function (done) {
        this.timeout(16000);

        setup.stopController(function (normalTerminated) {
            console.log(`PostgreSQL-streamed: Adapter normal terminated: ${normalTerminated}`);
            setTimeout(done, 2000);
        });
    });
});
