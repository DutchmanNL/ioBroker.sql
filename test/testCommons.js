const assert = require('node:assert');
const commons = require('../build/lib/aggregate');

describe('Test Common functions', function () {
    const log = {
        info: t => console.log(t),
        debug: t => console.log(t),
        error: t => console.error(t),
        warn: t => console.warn(t),
    };

    it('Test Common functions: counter 1', function (done) {
        const timeSeries = [
            { ts: 0, val: 100 },
            { ts: 10, val: 200 },
            { ts: 40, val: 500 },
            { ts: 50, val: 0 },
            { ts: 90, val: 400 },
            { ts: 100, val: 0 },
            { ts: 110, val: 100 },
        ];

        const adapter = {
            sendTo: function (from, command, result, callback) {
                assert.strictEqual(result.result, 700);
                done();
            },
            log,
        };

        commons.sendResponseCounter(adapter, {}, { start: 10, end: 100 }, timeSeries);
    });

    it('Test Common functions: counter 2', function (done) {
        const timeSeries = [
            { ts: 10, val: 200 },
            { ts: 40, val: 500 },
        ];

        const adapter = {
            sendTo: function (from, command, result, callback) {
                assert.strictEqual(result.result, 300);
                done();
            },
            log,
        };

        commons.sendResponseCounter(adapter, {}, { start: 10, end: 40 }, timeSeries);
    });

    it('Test Common functions: counter 3', function (done) {
        const timeSeries = [
            { ts: 0, val: 100 },
            { ts: 10, val: 200 },
            { ts: 40, val: 500 },
            { ts: 50, val: 0 },
            { ts: 90, val: 400 },
        ];

        const adapter = {
            sendTo: function (from, command, result, callback) {
                assert.strictEqual(result.result, 550);
                done();
            },
            log,
        };

        commons.sendResponseCounter(adapter, {}, { start: 5, end: 70 }, timeSeries);
    });

    it('Test Common functions: counter 4', function (done) {
        const timeSeries = [
            { ts: 0, val: 100 },
            { ts: 10, val: 200 },
            { ts: 40, val: 500 },
            { ts: 50, val: 0 },
            { ts: 90, val: 400 },
            { ts: 100, val: 0 },
            { ts: 110, val: 100 },
        ];

        const adapter = {
            sendTo: function (from, command, result, callback) {
                assert.strictEqual(result.result, 800);
                done();
            },
            log,
        };

        commons.sendResponseCounter(adapter, {}, { start: 5, end: 105 }, timeSeries);
    });

    it('Test Common functions: counter 5', function (done) {
        const timeSeries = [
            { ts: 0, val: 100 },
            { ts: 10, val: 200 },
            { ts: 40, val: 500 },
            { ts: 50, val: 0 },
            { ts: 90, val: 400 },
            { ts: 100, val: 0 },
        ];

        const adapter = {
            sendTo: function (from, command, result, callback) {
                assert.strictEqual(result.result, 750);
                done();
            },
            log,
        };

        commons.sendResponseCounter(adapter, {}, { start: 5, end: 95 }, timeSeries);
    });

    it('Test Common functions: counter 6', function (done) {
        const timeSeries = [
            { ts: 0, val: 100 },
            { ts: 10, val: 0 },
            { ts: 20, val: 100 },
            { ts: 40, val: 500 },
            { ts: 50, val: 0 },
            { ts: 90, val: 400 },
            { ts: 100, val: 0 },
        ];

        const adapter = {
            sendTo: function (from, command, result, callback) {
                assert.strictEqual(result.result, 900);
                done();
            },
            log,
        };

        commons.sendResponseCounter(adapter, {}, { start: 5, end: 95 }, timeSeries);
    });

    // "onchange" ("raw" in the e-charts UI) must be passed through untouched. aggregationLogic() has no
    // branch for it, so running it through the bucket aggregation returns one entry per interval with
    // val === null - i.e. an empty chart (issue #522).
    it('Test Common functions: aggregate onchange returns the raw values', function (done) {
        const start = 1000;
        const end = 2000;
        const timeSeries = [
            { ts: 1000, val: 1 },
            { ts: 1200, val: 2 },
            { ts: 1500, val: 3 },
            { ts: 1800, val: 4 },
            { ts: 2000, val: 5 },
        ];

        const adapter = {
            sendTo: function (from, command, result) {
                assert.deepStrictEqual(result.result, [
                    { ts: 1000, val: 1 },
                    { ts: 1200, val: 2 },
                    { ts: 1500, val: 3 },
                    { ts: 1800, val: 4 },
                    { ts: 2000, val: 5 },
                ]);
                assert.strictEqual(result.step, 0);
                done();
            },
            log,
        };

        commons.sendResponse(
            adapter,
            { from: 'system.adapter.test.0', command: 'getHistory' },
            'test.0.value',
            { start, end, aggregate: 'onchange', count: 500, limit: 2000, ignoreNull: false },
            timeSeries,
            Date.now(),
        );
    });

    it('Test Common functions: aggregate onchange draws steps to the borders', function (done) {
        const start = 1000;
        const end = 2000;
        // the value before start and the value after end are delivered by the UNION sub-queries of getHistory
        const timeSeries = [
            { ts: 800, val: 1 },
            { ts: 1200, val: 2 },
            { ts: 1800, val: 3 },
            { ts: 2200, val: 4 },
        ];

        const adapter = {
            sendTo: function (from, command, result) {
                // 800 is cut off and becomes the step value at start, 2200 becomes the step value at end
                assert.deepStrictEqual(result.result, [
                    { ts: 1000, val: 1 },
                    { ts: 1200, val: 2 },
                    { ts: 1800, val: 3 },
                    { ts: 2000, val: 4 },
                ]);
                done();
            },
            log,
        };

        commons.sendResponse(
            adapter,
            { from: 'system.adapter.test.0', command: 'getHistory' },
            'test.0.value',
            { start, end, aggregate: 'onchange', count: 500, limit: 2000, ignoreNull: false },
            timeSeries,
            Date.now(),
        );
    });

    // ---------------------------------------------------------------------------------------------
    // PR-B streaming invariant guard (no database).
    //
    // The streamed PostgreSQL aggregated getHistory path folds the ts-ordered DB stream into the
    // aggregator chunk-by-chunk (one initAggregate + N aggregation(chunk) + finishAggregation)
    // instead of buffering the whole range and calling aggregation(allRows) once. These tests prove
    // that, for the exact query shape getHistory() builds (one pre-start border row, several in-range
    // rows, one at/after end border row - all in a single global ORDER BY ts), chunking the stream at
    // ANY boundary yields byte-identical output to the whole-array call. All cross-row state lives on
    // `options`, and there is at most one border row on each side, so the per-chunk border injection
    // in aggregation() cannot misfire.
    // ---------------------------------------------------------------------------------------------

    // Aggregate modes for which the streamed path is enabled (mirrors the trigger in main.ts).
    const STREAMED_MODES = ['average', 'min', 'max', 'total', 'count', 'minmax', 'percentile', 'quantile', 'integral'];

    const START = 1000;
    const END = 2000;
    // step = (END - START) / count = 250 -> in-range buckets are [1000,1250),[1250,1500),...
    const inRange = [
        { ts: 1100, val: 10 },
        { ts: 1300, val: 22 },
        { ts: 1500, val: 14 },
        { ts: 1700, val: 26 },
        { ts: 1900, val: 31 },
    ];
    // Exactly one pre-start border row (last value before start) + in-range + exactly one at/after end.
    // "norm": pre-border sits one step before start (preIndex === -1, injected inline).
    const rowsNorm = [{ ts: 900, val: 5 }].concat(inRange).concat([{ ts: 2000, val: 40 }]);
    // "early": pre-border sits MORE than one step before start (preIndex < -1) -> exercises the
    // too-early injection that happens at the END of the aggregation() call containing that row.
    const rowsEarly = [{ ts: 500, val: 5 }].concat(inRange).concat([{ ts: 2000, val: 40 }]);

    function makeOptions(mode) {
        const options = { start: START, end: END, count: 4, limit: 2000, aggregate: mode, ignoreNull: false };
        if (mode === 'percentile') {
            options.percentile = 50;
        }
        if (mode === 'quantile') {
            options.quantile = 0.5;
        }
        if (mode === 'integral') {
            options.integralUnit = 60;
            options.integralInterpolation = 'none';
        }
        return options;
    }

    // Fresh row objects per run: aggregation()/finishAggregation() may mutate rows/buckets in place,
    // so whole and chunked runs must never share references.
    const cloneRows = rows => rows.map(r => ({ ...r }));

    function runWhole(mode, rows) {
        const options = makeOptions(mode);
        commons.initAggregate(options, 'test.0.value', undefined, undefined);
        commons.aggregation(options, cloneRows(rows));
        commons.finishAggregation(options);
        return options.result;
    }

    function runChunked(mode, rows, sizes) {
        const options = makeOptions(mode);
        commons.initAggregate(options, 'test.0.value', undefined, undefined);
        const data = cloneRows(rows);
        let i = 0;
        for (const size of sizes) {
            commons.aggregation(options, data.slice(i, i + size));
            i += size;
        }
        if (i < data.length) {
            commons.aggregation(options, data.slice(i));
        }
        commons.finishAggregation(options);
        return options.result;
    }

    // Split strategies: whole, all-singletons, every single 2-way boundary (isolates each border row
    // in turn), and a split that isolates BOTH border rows on their own.
    function splitsFor(n) {
        const splits = [[n], new Array(n).fill(1), [1, n - 2, 1]];
        for (let k = 1; k < n; k++) {
            splits.push([k, n - k]);
        }
        return splits;
    }

    for (const mode of STREAMED_MODES) {
        for (const variant of [
            { name: 'pre-border one step before start', rows: rowsNorm },
            { name: 'pre-border more than one step before start', rows: rowsEarly },
        ]) {
            it(`Streaming invariant: ${mode} chunked === whole (${variant.name})`, function () {
                const whole = runWhole(mode, variant.rows);
                for (const sizes of splitsFor(variant.rows.length)) {
                    const chunked = runChunked(mode, variant.rows, sizes);
                    assert.deepStrictEqual(
                        chunked,
                        whole,
                        `mode ${mode} diverged for split ${JSON.stringify(sizes)}: ${JSON.stringify(
                            chunked,
                        )} !== ${JSON.stringify(whole)}`,
                    );
                }
            });
        }
    }

    // Documents WHY 'integralTotal' is excluded from the streamed trigger: its finisher folds a single
    // global series in insertion order without re-sorting, so a too-early pre-border row (injected at
    // the end of its aggregation() call) lands in a different position when the stream is chunked.
    it("Streaming invariant: 'integralTotal' is chunk-invariant only when the border is one step before start", function () {
        // normal border -> still equivalent (injected inline, in ts order)
        const wholeNorm = runWhole('integralTotal', rowsNorm);
        for (const sizes of splitsFor(rowsNorm.length)) {
            assert.deepStrictEqual(runChunked('integralTotal', rowsNorm, sizes), wholeNorm);
        }
        // too-early border -> diverges, which is exactly why the streamed trigger excludes integralTotal
        const wholeEarly = runWhole('integralTotal', rowsEarly);
        const chunkedEarly = runChunked('integralTotal', rowsEarly, [1, 2, 3]);
        assert.notDeepStrictEqual(
            chunkedEarly,
            wholeEarly,
            'integralTotal was expected to diverge on the too-early border; if this no longer holds, it may be eligible for streaming',
        );
    });
});
