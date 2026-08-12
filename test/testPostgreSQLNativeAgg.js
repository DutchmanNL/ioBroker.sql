/* jshint -W097 */ // jshint strict:false
/*jslint node: true */
/*jshint expr: true*/
const assert = require('node:assert');
const setup = require('./lib/setup');

// Standalone PostgreSQL integration test for the server-side (native) bucket aggregation path (PR-D).
// It runs its OWN js-controller, so it MUST live in its own file and get its own mocha invocation
// (never globbed together with test/testPostgreSQL.js - one js-controller per mocha process).
//
// The oracle is the Node aggregation path itself: every case calls getHistory twice with identical
// options - once normally (which takes the SQL path) and once with options.preferNodeAggregation
// set (which forces the in-memory Node path) - and asserts the two results are deepStrictEqual.
// Nothing is pinned to a hand-computed value; the Node path IS the reference implementation.

let objects = null;
let states = null;
let onStateChanged = null;
let sendToID = 1;

const adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.') + 1);

// Two distinct numeric datapoints so the null-in-range case cannot pollute the null-free cases.
const testDp = 'system.adapter.sql.0.memRss';
const nullDp = 'system.adapter.sql.0.memHeapUsed';

function checkConnectionOfAdapter(cb, counter) {
    counter ||= 0;
    if (counter > 20) {
        cb?.('Cannot check connection');
        return;
    }

    states.getState(`system.adapter.${adapterShortName}.0.alive`, (err, state) => {
        if (err) {
            console.error(`PostgreSQL-nativeAgg: ${err}`);
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

/**
 * A/B parity: run getHistory once on the SQL path and once forced onto the Node path, and assert
 * the two responses match exactly (result + step). The Node path is the oracle.
 */
function assertParity(id, baseOptions, done) {
    sendTo('sql.0', 'getHistory', { id, options: { ...baseOptions } }, function (sqlRes) {
        assert.ok(!sqlRes.error, `SQL-path getHistory error: ${sqlRes.error}`);
        assert.ok(Array.isArray(sqlRes.result), 'SQL-path result is an array');

        sendTo(
            'sql.0',
            'getHistory',
            { id, options: { ...baseOptions, preferNodeAggregation: true } },
            function (nodeRes) {
                assert.ok(!nodeRes.error, `Node-path getHistory error: ${nodeRes.error}`);
                assert.ok(Array.isArray(nodeRes.result), 'Node-path result is an array');

                console.log(
                    `nativeAgg parity [${baseOptions.aggregate}${baseOptions.round !== undefined ? ` round=${baseOptions.round}` : ''}] ` +
                        `SQL=${JSON.stringify(sqlRes.result)} NODE=${JSON.stringify(nodeRes.result)}`,
                );

                assert.deepStrictEqual(sqlRes.result, nodeRes.result);
                assert.strictEqual(sqlRes.step, nodeRes.step);
                done();
            },
        );
    });
}

describe(`Test ${__filename}`, function () {
    // All timestamps are fixed offsets in the past so they are stored verbatim and the RAM cache is
    // empty by read time (a required precondition for the SQL path to trigger).
    const base = Date.now() - 500000;
    const start = base;
    const end = base + 40000;

    // memRss: no nulls anywhere in [start, end]. count 4 -> step 10000 -> four buckets:
    //   [base, base+10000): 10.456, 22.789, 7.111
    //   [base+10000, base+20000): (empty bucket -> dropped, exercises bucket parity)
    //   [base+20000, base+30000): 30.246, 44.802
    //   [base+30000, base+40000): 17.333
    // plus one pre-border (base-5000) and one post-border (base+55000).
    const values = [
        { ts: base - 5000, val: 5.123 },
        { ts: base + 1000, val: 10.456 },
        { ts: base + 5000, val: 22.789 },
        { ts: base + 9000, val: 7.111 },
        { ts: base + 21000, val: 30.246 },
        { ts: base + 25000, val: 44.802 },
        { ts: base + 33000, val: 17.333 },
        { ts: base + 55000, val: 50.5 },
    ];

    // memHeapUsed: contains an explicit null INSIDE the queried range, which forces the SQL path to
    // fall back to the Node path (null ordering semantics are not reproduced in SQL).
    const nb = base;
    const nullStart = nb;
    const nullEnd = nb + 20000;
    const nullValues = [
        { ts: nb - 3000, val: 2.5 },
        { ts: nb + 1000, val: 3.5 },
        { ts: nb + 4000, val: null }, // null in range -> fallback
        { ts: nb + 8000, val: 9.5 },
        { ts: nb + 12000, val: 12.25 },
        { ts: nb + 25000, val: 20.0 },
    ];

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
            // nativeAggregation defaults to true; keep it explicit so the intent is clear.
            config.native.nativeAggregation = true;

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

    it(`Test ${__filename}: Check if adapter started, enable history and store values`, function (done) {
        this.timeout(90000);
        checkConnectionOfAdapter(function (err) {
            assert.ok(!err, err);
            sendTo(
                'sql.0',
                'enableHistory',
                {
                    id: testDp,
                    options: { changesOnly: false, debounce: 0, retention: 31536000, maxLength: 0, storageType: 'Number' },
                },
                function (r1) {
                    assert.strictEqual(r1.error, undefined);
                    assert.strictEqual(r1.success, true);
                    sendTo(
                        'sql.0',
                        'enableHistory',
                        {
                            id: nullDp,
                            options: {
                                changesOnly: false,
                                debounce: 0,
                                retention: 31536000,
                                maxLength: 0,
                                storageType: 'Number',
                            },
                        },
                        function (r2) {
                            assert.strictEqual(r2.error, undefined);
                            assert.strictEqual(r2.success, true);
                            // let the adapter settle so the message/response path and settings are up
                            setTimeout(function () {
                                // storeState is write-through (flushes to the DB before the callback
                                // returns), so by read time the RAM cache is empty and the SQL path runs.
                                sendTo('sql.0', 'storeState', { id: testDp, state: values }, function (s1) {
                                    assert.ok(!s1.error, `storeState error: ${s1.error}`);
                                    sendTo('sql.0', 'storeState', { id: nullDp, state: nullValues }, function (s2) {
                                        assert.ok(!s2.error, `storeState error: ${s2.error}`);
                                        setTimeout(done, 2000);
                                    });
                                });
                            }, 10000);
                        },
                    );
                },
            );
        });
    });

    const modes = ['average', 'min', 'max', 'total', 'count'];
    for (const aggregate of modes) {
        it(`Test ${__filename}: SQL == Node for aggregate '${aggregate}'`, function (done) {
            this.timeout(20000);
            assertParity(testDp, { start, end, count: 4, aggregate, ignoreNull: false }, done);
        });
    }

    it(`Test ${__filename}: SQL == Node for 'average' with round option`, function (done) {
        this.timeout(20000);
        // round: 1 -> round each value to 1 decimal before aggregating (multiplier 10).
        assertParity(testDp, { start, end, count: 4, aggregate: 'average', ignoreNull: false, round: 1 }, done);
    });

    it(`Test ${__filename}: SQL == Node for 'count' with more buckets than points (dropped buckets)`, function (done) {
        this.timeout(20000);
        // count 12 over the same 40s range -> step ~3333ms -> many empty buckets that must be dropped
        // identically on both paths.
        assertParity(testDp, { start, end, count: 12, aggregate: 'count', ignoreNull: false }, done);
    });

    it(`Test ${__filename}: SQL == Node for 'min' with more buckets than points (dropped buckets)`, function (done) {
        this.timeout(20000);
        assertParity(testDp, { start, end, count: 12, aggregate: 'min', ignoreNull: false }, done);
    });

    it(`Test ${__filename}: SQL falls back yet equals Node when a null is in range (average)`, function (done) {
        this.timeout(20000);
        // The queried window contains an explicit null value, so the SQL path detects cnt_null > 0 and
        // falls back to the Node path. The result must still equal the forced-Node reference.
        assertParity(nullDp, { start: nullStart, end: nullEnd, count: 2, aggregate: 'average', ignoreNull: false }, done);
    });

    it(`Test ${__filename}: SQL falls back yet equals Node when a null is in range (total)`, function (done) {
        this.timeout(20000);
        assertParity(nullDp, { start: nullStart, end: nullEnd, count: 2, aggregate: 'total', ignoreNull: false }, done);
    });

    after(`Test ${__filename} Stop js-controller`, function (done) {
        this.timeout(16000);

        setup.stopController(function (normalTerminated) {
            console.log(`PostgreSQL-nativeAgg: Adapter normal terminated: ${normalTerminated}`);
            setTimeout(done, 2000);
        });
    });
});
