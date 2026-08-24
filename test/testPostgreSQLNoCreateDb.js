/* jshint -W097 */ // jshint strict:false
/*jslint node: true */
/*jshint expr: true*/
const assert = require('node:assert');
const { Client } = require('pg');
const setup = require('./lib/setup');

// Standalone PostgreSQL integration test for the "Do not create database" connect path (#404, #285):
// with the option enabled the adapter must connect DIRECTLY to the configured database and never
// require access to the maintenance database "postgres" (restricted roles / managed PostgreSQL).
// It runs its OWN js-controller, so it MUST live in its own file and get its own mocha invocation
// (never globbed together with test/testPostgreSQL.js - one js-controller per mocha process).

let objects = null;
let states = null;
let onStateChanged = null;
let sendToID = 1;

const adapterShortName = setup.adapterName.substring(setup.adapterName.indexOf('.') + 1);

const testDp = 'sql.0.noCreateDbTestValue';

function checkConnectionOfAdapter(cb, counter) {
    counter ||= 0;
    if (counter > 20) {
        cb?.('Cannot check connection');
        return;
    }

    states.getState(`system.adapter.${adapterShortName}.0.alive`, (err, state) => {
        if (err) {
            console.error(`PostgreSQL-noCreateDb: ${err}`);
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

// The scenario needs an EXISTING database (the option promises "already created"). Create it with the
// superuser if missing - in CI the preceding Postgres test steps have already created it anyway.
async function ensureDatabaseExists() {
    const client = new Client({
        host: '127.0.0.1',
        port: 5432,
        user: process.env.SQL_USER || 'postgres',
        password: process.env.SQL_PASS || '',
        database: 'postgres',
    });
    await client.connect();
    try {
        const res = await client.query(`SELECT 1 FROM pg_database WHERE datname = 'iobroker'`);
        if (!res.rows.length) {
            await client.query('CREATE DATABASE iobroker');
        }
    } finally {
        await client.end();
    }
}

describe(`Test ${__filename}`, function () {
    before(`Test ${__filename} Start js-controller`, function (_done) {
        this.timeout(600000); // because of first install from npm
        setup.adapterStarted = false;

        ensureDatabaseExists().then(() =>
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
                // The option under test: connect straight to the existing database, never to "postgres".
                config.native.doNotCreateDatabase = true;

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

                        await objects.setObjectAsync(testDp, {
                            common: { type: 'number', role: 'state', custom: {} },
                            type: 'state',
                        });

                        _done();
                    },
                );
            }),
        );
    });

    it(`Test ${__filename}: adapter connects directly to the existing database`, function (done) {
        this.timeout(60000);
        checkConnectionOfAdapter(function (err) {
            assert.ok(!err, err);
            // info.connection must be true - previously the adapter looped forever on the
            // maintenance-DB connect when the role had no access to "postgres".
            states.getState(`system.adapter.${adapterShortName}.0.info.connection`, (err2, state) => {
                assert.ok(!err2, err2);
                // some js-controller versions route info.connection under the adapter namespace instead
                if (state) {
                    assert.strictEqual(state.val, true);
                    done();
                } else {
                    states.getState(`${adapterShortName}.0.info.connection`, (err3, state2) => {
                        assert.ok(!err3, err3);
                        assert.strictEqual(state2 && state2.val, true);
                        done();
                    });
                }
            });
        });
    });

    it(`Test ${__filename}: write and read a value end-to-end`, function (done) {
        this.timeout(45000);
        const base = Date.now();

        sendTo(
            'sql.0',
            'enableHistory',
            {
                id: testDp,
                options: { changesOnly: false, debounce: 0, retention: 31536000, maxLength: 0, storageType: 'Number' },
            },
            function (result) {
                assert.strictEqual(result.error, undefined);
                assert.strictEqual(result.success, true);
                setTimeout(function () {
                    states.setState(testDp, { val: 42.5, ts: base - 60000, ack: true }, function () {
                        setTimeout(function () {
                            sendTo(
                                'sql.0',
                                'getHistory',
                                {
                                    id: testDp,
                                    options: { start: base - 120000, end: base, aggregate: 'none' },
                                },
                                function (result2) {
                                    console.log(
                                        `PostgreSQL-noCreateDb getHistory: ${JSON.stringify(result2.result)}`,
                                    );
                                    assert.ok(!result2.error, `getHistory error: ${result2.error}`);
                                    assert.ok(
                                        result2.result.some(e => e.val === 42.5),
                                        'written value not found in history',
                                    );
                                    done();
                                },
                            );
                        }, 3000);
                    });
                }, 10000);
            },
        );
    });

    after(`Test ${__filename} Stop js-controller`, function (done) {
        this.timeout(16000);

        setup.stopController(function (normalTerminated) {
            console.log(`PostgreSQL-noCreateDb: Adapter normal terminated: ${normalTerminated}`);
            setTimeout(done, 2000);
        });
    });
});
