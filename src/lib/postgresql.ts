import { boolOrNull, numberOrNull, quotePgString } from './sql-string';
import type { RawEntriesOptions, TableName } from '../types';

export function init(_dbName: string, _doNotCreateDatabase?: boolean): string[] {
    return [
        'CREATE TABLE sources    (id SERIAL NOT NULL PRIMARY KEY, name TEXT);',
        'CREATE TABLE datapoints (id SERIAL NOT NULL PRIMARY KEY, name TEXT, type INTEGER);',
        'CREATE TABLE ts_number  (id INTEGER NOT NULL, ts BIGINT, val REAL,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));',
        'CREATE TABLE ts_string  (id INTEGER NOT NULL, ts BIGINT, val TEXT,    ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));',
        'CREATE TABLE ts_bool    (id INTEGER NOT NULL, ts BIGINT, val BOOLEAN, ack BOOLEAN, _from INTEGER, q INTEGER, PRIMARY KEY(id, ts));',
        'CREATE TABLE ts_counter (id INTEGER NOT NULL, ts BIGINT, val REAL);',
    ];
}

export function destroy(_dbName: string, _doNotCreateDatabase?: boolean): string[] {
    return [
        'DROP TABLE ts_counter;',
        'DROP TABLE ts_number;',
        'DROP TABLE ts_string;',
        'DROP TABLE ts_bool;',
        'DROP TABLE sources;',
        'DROP TABLE datapoints;',
    ];
}

export function getFirstTs(_dbName: string, table: TableName): string {
    return `SELECT id, MIN(ts) AS ts FROM ${table} GROUP BY id;`;
}

/**
 * Count the rows and the covered time range per datapoint index.
 *
 * One query per table instead of one per datapoint: a database that has collected data for years
 * holds thousands of datapoints, and `GROUP BY id` lets the engine do the work in a single pass.
 *
 * @param _dbName unused, PostgreSQL and SQLite connect to the target database directly name of the database
 * @param table the time series table to summarize
 */
export function getIdCounts(_dbName: string, table: TableName): string {
    return `SELECT id, COUNT(*) AS cnt, MIN(ts) AS first_ts, MAX(ts) AS last_ts FROM ${table} GROUP BY id;`;
}

/**
 * Average bytes per row and total bytes of one time series table.
 *
 * There is no portable way to ask for the size of the rows belonging to a single datapoint, so the
 * statistics multiply this average by the row count. The result is an estimate and has to be
 * presented as one.
 *
 * @param _dbName unused, PostgreSQL and SQLite connect to the target database directly name of the database
 * @param table the time series table to measure
 */
export function getTableSize(_dbName: string, table: TableName): string {
    // pg_total_relation_size covers table plus indexes and TOAST; reltuples is the planner's
    // row estimate, which is enough to derive an average width.
    return `SELECT CASE WHEN c.reltuples > 0 THEN (pg_total_relation_size(c.oid) / c.reltuples)::bigint ELSE 0 END AS avg_row_length, pg_total_relation_size(c.oid) AS total_bytes FROM pg_class c WHERE c.relname='${table}';`;
}

export function insert(
    _dbName: string,
    index: number,
    values: {
        table: TableName;
        state: { val: any; ts: number; ack?: boolean; q?: number };
        from?: number;
    }[],
): string[] {
    const insertValues: { [table: string]: string[] } = {};
    values.forEach(original => {
        // work on a copy: the caller keeps `state` as the reference value for "record changes only" (#295),
        // so writing the SQL literal back into it would make the next equal value look like a change
        const value = { ...original, state: { ...original.state } };
        // state, from, table
        insertValues[value.table] = insertValues[value.table] || [];

        if (!value.state || value.state.val === null || value.state.val === undefined) {
            value.state.val = 'NULL';
        } else if (value.table === 'ts_string') {
            value.state.val = quotePgString(value.state.val);
        } else if (value.table === 'ts_number' || value.table === 'ts_counter') {
            value.state.val = numberOrNull(value.state.val);
        } else if (value.table === 'ts_bool') {
            value.state.val = boolOrNull(value.state.val, false);
        }

        if (value.table === 'ts_counter') {
            insertValues[value.table].push(`(${index}, ${value.state.ts}, ${value.state.val})`);
        } else {
            insertValues[value.table].push(
                `(${index}, ${value.state.ts}, ${value.state.val}, ${!!value.state.ack}, ${value.from || 0}, ${value.state.q || 0})`,
            );
        }
    });

    const query: string[] = [];
    for (const table in insertValues) {
        if (table === 'ts_counter') {
            // no ON CONFLICT here: ts_counter has no primary key in PostgreSQL (unlike SQLite),
            // so there is no uniqueness conflict to suppress
            while (insertValues[table].length) {
                query.push(
                    `INSERT INTO ts_counter (id, ts, val) VALUES ${insertValues[table].splice(0, 500).join(',')};`,
                );
            }
        } else {
            while (insertValues[table].length) {
                // ts_number/ts_string/ts_bool have PRIMARY KEY(id, ts). Importing history writes rows
                // that may already exist, and a single duplicate would otherwise abort the whole batch.
                // DO NOTHING only applies to uniqueness conflicts, so conversion errors still surface.
                query.push(
                    `INSERT INTO ${table} (id, ts, val, ack, _from, q) VALUES ${insertValues[table].splice(0, 500).join(',')} ON CONFLICT DO NOTHING;`,
                );
            }
        }
    }

    return query;
}

export function retention(_dbName: string, index: number, table: TableName, retention: number): string {
    const d = new Date();
    d.setSeconds(-retention);
    let query = `DELETE FROM ${table} WHERE`;
    query += ` id=${index}`;
    query += ` AND ts < ${d.getTime()}`;
    query += ';';

    return query;
}

export function getIdSelect(_dbName: string, name?: string): string {
    if (!name) {
        return 'SELECT id, type, name FROM datapoints;';
    }
    return `SELECT id, type, name FROM datapoints WHERE name=${quotePgString(name)};`;
}

export function getIdInsert(_dbName: string, name: string, type: 0 | 1 | 2): string {
    return `INSERT INTO datapoints (name, type) VALUES(${quotePgString(name)}, ${type});`;
}

export function getIdUpdate(_dbName: string, id: number, type: 0 | 1 | 2): string {
    return `UPDATE datapoints SET type = ${type} WHERE id = ${id};`;
}

export function getFromSelect(_dbName: string, name?: string): string {
    if (name) {
        return `SELECT id, name FROM sources WHERE name=${quotePgString(name)};`;
    }
    return 'SELECT id, name FROM sources;';
}

export function getFromInsert(dbName: string, values: string): string {
    return `INSERT INTO sources (name) VALUES(${quotePgString(values)});`;
}

export function getCounterDiff(
    _dbName: string,
    options: {
        index: number;
        start: number;
        end: number;
    },
): string {
    // Take first real value after start
    const subQueryStart = `SELECT ts, val FROM ts_number  WHERE id=${options.index} AND ts>=${options.start} AND ts<${options.end} AND val IS NOT NULL ORDER BY ts ASC LIMIT 1`;
    // Take last real value before the end
    const subQueryEnd = `SELECT ts, val FROM ts_number  WHERE id=${options.index} AND ts>=${options.start} AND ts<${options.end} AND val IS NOT NULL ORDER BY ts DESC LIMIT 1`;
    // Take last value before start
    const subQueryFirst = `SELECT ts, val FROM ts_number  WHERE id=${options.index} AND ts< ${options.start} ORDER BY ts DESC LIMIT 1`;
    // Take next value after end
    const subQueryLast = `SELECT ts, val FROM ts_number  WHERE id=${options.index} AND ts>= ${options.end} ORDER BY ts ASC  LIMIT 1`;
    // get values from counters where counter changed from up to down (e.g. counter changed).
    // No ORDER BY here: the outer ORDER BY sorts the combined result anyway.
    const subQueryCounterChanges = `SELECT ts, val FROM ts_counter WHERE id=${options.index} AND ts>${options.start} AND ts<${options.end} AND val IS NOT NULL`;

    // The ORDER BY belongs in the OUTER query, not inside the derived table: sendResponseCounter
    // consumes the rows positionally, and a derived table's ordering is not guaranteed to survive
    // the SELECT DISTINCT above it - PostgreSQL may hash-aggregate instead of sort/unique.
    return (
        `SELECT DISTINCT a.ts, a.val FROM ((${subQueryFirst})\n` +
        `UNION ALL (${subQueryStart})\n` +
        `UNION ALL (${subQueryEnd})\n` +
        `UNION ALL (${subQueryLast})\n` +
        `UNION ALL (${subQueryCounterChanges})) a ORDER BY a.ts;`
    );
}

export function getHistory(
    _dbName: string,
    table: string,
    options: ioBroker.GetHistoryOptions & { index: number | null },
): string {
    let query = `SELECT ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${
        options.from ? ', sources.name as from' : ''
    }${options.q ? ', q' : ''} FROM ${table}`;

    // LEFT JOIN: a value stored without `from` has `_from` = 0, which has no row in `sources`.
    // With an INNER JOIN, such values were missing from every query that asked for `from`.
    if (options.from) {
        query += ` LEFT JOIN sources ON sources.id=${table}._from`;
    }

    let where = '';

    if (options.index !== null) {
        where += ` ${table}.id=${options.index}`;
    }
    if (options.end) {
        where += `${where ? ' AND' : ''} ${table}.ts < ${options.end}`;
    }
    if (options.start) {
        where += `${where ? ' AND' : ''} ${table}.ts >= ${options.start}`;

        //add last value before start
        let subQuery;
        let subWhere;
        subQuery = ` SELECT ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${
            options.from ? ', sources.name as from' : ''
        }${options.q ? ', q' : ''} FROM ${table}`;
        if (options.from) {
            subQuery += ` LEFT JOIN sources ON sources.id=${table}._from`;
        }
        subWhere = '';
        if (options.index !== null) {
            subWhere += ` ${table}.id=${options.index}`;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? " AND" : '') + " val <> NULL";
        }
        subWhere += `${subWhere ? ' AND' : ''} ${table}.ts < ${options.start}`;
        if (subWhere) {
            subQuery += ` WHERE ${subWhere}`;
        }
        subQuery += ` ORDER BY ${table}.ts DESC LIMIT 1`;
        where += ` UNION ALL (${subQuery})`;

        //add next value after end
        subQuery = ` SELECT ts, val${options.index === null ? `, ${table}.id as id` : ''}${options.ack ? ', ack' : ''}${
            options.from ? ', sources.name as from' : ''
        }${options.q ? ', q' : ''} FROM ${table}`;
        if (options.from) {
            subQuery += ` LEFT JOIN sources ON sources.id=${table}._from`;
        }
        subWhere = '';
        if (options.index !== null) {
            subWhere += ` ${table}.id=${options.index}`;
        }
        if (options.ignoreNull) {
            //subWhere += (subWhere ? " AND" : '') + " val <> NULL";
        }
        subWhere += `${subWhere ? ' AND' : ''} ${table}.ts >= ${options.end}`;
        if (subWhere) {
            subQuery += ` WHERE ${subWhere}`;
        }
        subQuery += ` ORDER BY ${table}.ts ASC LIMIT 1`;
        where += ` UNION ALL(${subQuery})`;
    }

    if (where) {
        query += ` WHERE ${where}`;
    }

    query += ' ORDER BY ts';

    if (
        (!options.start && options.count) ||
        (options.aggregate === 'none' && options.count && options.returnNewestEntries)
    ) {
        query += ' DESC';
    } else {
        query += ' ASC';
    }

    if ((!options.start && options.count) || (options.aggregate === 'none' && options.count)) {
        query += ` LIMIT ${options.count + 2}`;
    }

    query += ';';
    return query;
}

/** The aggregates that getAggregatedHistory() can prepare in the database */
export type DbAggregate = 'min' | 'max' | 'minmax' | 'average' | 'total';

/**
 * Aggregated history read of one numeric datapoint, with the heavy part done by PostgreSQL (#516). The
 * adapter still calculates the answer with `aggregation()` and `finishAggregation()` of `@iobroker/aggregate`;
 * the database only sends what decides each interval instead of every row.
 *
 * - `min`, `max`, `minmax`: the rows that decide an interval: its first and its last row, and the earliest
 *   row with the smallest and with the largest value. `aggregation()` reaches the same state of the interval
 *   for these rows as for all of its rows.
 * - `average`, `total`: one row per interval with the timestamp of its first row, the sum of its values `s`
 *   and their number `c`. The values are added in the order of `ts`, one after the other like the loop in
 *   `aggregation()`, so the sum is the same double.
 *
 * Both also return the last row before the start and the first row at or after the end, like getHistory().
 * Each is one pass over the range without a join, so a wrong row estimate cannot make the plan explode.
 *
 * The intervals are the ones of `@iobroker/aggregate`: `floor((ts - start) / step)` in float8 is the same IEEE
 * calculation as `Math.floor()` in JavaScript, and `String()` writes `step` so that PostgreSQL reads back the
 * same double. With `intervalBorders`, `width_bucket()` finds the interval with `start <= ts < end`.
 *
 * Values are read as the text of the REAL value turned into float8: that is the number the driver's
 * `parseFloat()` makes of a REAL (widening the REAL directly gives a different one). They are compared and
 * added like the adapter does: NaN and ±Infinity are gaps like in `toNumber()` of `@iobroker/aggregate`, and
 * with `round` they are rounded exactly like `Math.round(v * round) / round` (ties towards +Infinity; `round()`
 * of PostgreSQL rounds them away from zero).
 *
 * @param _dbName name of the database (not used by PostgreSQL)
 * @param table table of the datapoint, `ts_number`
 * @param options the read, with `start`, `end` and `step` as `initAggregate()` set them
 * @param options.index id of the datapoint in the database
 * @param options.start start of the first interval
 * @param options.end end of the last interval
 * @param options.step length of an interval
 * @param options.aggregate one of `DbAggregate`
 * @param options.round `Math.round(v * round) / round` for every value, if set
 * @param options.intervalBorders the borders of the intervals from the client, if any
 */
export function getAggregatedHistory(
    _dbName: string,
    table: string,
    options: {
        index: number;
        start: number;
        end: number;
        step: number;
        aggregate: DbAggregate;
        round?: number;
        intervalBorders?: number[];
    },
): string {
    const { index, start, end } = options;
    const range = `id=${index} AND ts >= ${start} AND ts < ${end}`;
    const interval = options.intervalBorders
        ? `width_bucket(ts::float8, ARRAY[${options.intervalBorders.map(b => String(b)).join(',')}]::float8[])`
        : `floor((ts - ${start})::float8 / ${String(options.step)}::float8)`;
    const raw = '(val::text)::float8';
    const value = `CASE WHEN val IN ('NaN'::real, 'Infinity'::real, '-Infinity'::real) THEN NULL ELSE ${raw} END`;

    // one row per stored value: timestamp, interval `b`, the value as the driver reads it `raw`, and `v` to calculate with
    let values: string;
    if (options.round) {
        const round = `${String(options.round)}::float8`;
        values =
            `SELECT ts, b, raw, (CASE WHEN y - floor(y) >= 0.5 THEN ceil(y) ELSE floor(y) END) / ${round} AS v ` +
            `FROM (SELECT ts, ${interval} AS b, ${raw} AS raw, ${value} * ${round} AS y FROM ${table} WHERE ${range}) r`;
    } else {
        values = `SELECT ts, ${interval} AS b, ${raw} AS raw, ${value} AS v FROM ${table} WHERE ${range}`;
    }

    const before = `SELECT ts, ${raw} AS val FROM ${table} WHERE id=${index} AND ts < ${start} ORDER BY ts DESC LIMIT 1`;
    const after = `SELECT ts, ${raw} AS val FROM ${table} WHERE id=${index} AND ts >= ${end} ORDER BY ts ASC LIMIT 1`;

    if (options.aggregate === 'average' || options.aggregate === 'total') {
        return (
            `SELECT ts, val, NULL::float8 AS s, NULL::bigint AS c FROM ((${before}) UNION ALL (${after})) borders` +
            ` UNION ALL SELECT min(ts), NULL::float8, sum(v ORDER BY ts), count(v) FROM (${values}) x GROUP BY b` +
            ' ORDER BY ts ASC;'
        );
    }

    // Per interval [ts, raw] of the first and the last row, [v, ts, raw] of the earliest smallest value and
    // [v, -ts, raw] of the earliest largest one. A float8 holds every ts exactly.
    const decisive =
        'SELECT min(ARRAY[ts::float8, raw]) AS f, max(ARRAY[ts::float8, raw]) AS l,' +
        ' min(ARRAY[v, ts::float8, raw]) FILTER (WHERE v IS NOT NULL) AS mn,' +
        ' max(ARRAY[v, -(ts::float8), raw]) FILTER (WHERE v IS NOT NULL) AS mx' +
        ` FROM (${values}) x GROUP BY b`;
    return (
        'SELECT DISTINCT ts, val FROM (' +
        'SELECT unnest(ARRAY[f[1], l[1], mn[2], -(mx[2])])::bigint AS ts, unnest(ARRAY[f[2], l[2], mn[3], mx[3]]) AS val' +
        ` FROM (${decisive}) g) d WHERE ts IS NOT NULL` +
        ` UNION ALL (${before}) UNION ALL (${after}) ORDER BY ts ASC;`
    );
}

/** A row of getAggregatedHistory(): a stored value, or with `c` set the sum and the number of the values of an interval */
export type AggregatedHistoryRow = { ts: number | string; val: number | null; s?: number | null; c?: string | null };

/**
 * The input for `aggregation()` from the rows of getAggregatedHistory(), in the order of `ts`.
 *
 * @param rows rows of getAggregatedHistory()
 * @param aggregate the aggregate of the read
 * @returns `data` for `aggregation()`, and `stored`, the entries of `data` that are stored values: they still
 * have to be prepared like every row of a read (ts as number, rounding). The other entries carry the average
 * or the total of their interval, calculated from values that were already rounded.
 */
export function toAggregationInput(
    rows: AggregatedHistoryRow[],
    aggregate: DbAggregate,
): { data: { ts: number | string; val: number | null }[]; stored: { ts: number | string; val: number | null }[] } {
    const data: { ts: number | string; val: number | null }[] = [];
    const stored: { ts: number | string; val: number | null }[] = [];
    for (const row of rows) {
        if (row.c === undefined || row.c === null) {
            const entry = { ts: row.ts, val: row.val };
            stored.push(entry);
            data.push(entry);
        } else {
            const count = parseInt(row.c, 10);
            let val: number | null = null;
            if (count) {
                val = aggregate === 'average' ? row.s! / count : row.s!;
            }
            data.push({ ts: parseInt(row.ts as string, 10), val });
        }
    }
    return { data, stored };
}

export function deleteFromTable(
    _dbName: string,
    table: TableName,
    index: number,
    start?: number,
    end?: number,
): string {
    let query = `DELETE FROM ${table} WHERE`;
    query += ` id=${index}`;

    if (start && end) {
        query += ` AND ts>=${start} AND ts <= ${end}`;
    } else if (start) {
        query += ` AND ts=${start}`;
    }

    query += ';';

    return query;
}

/**
 * Remove one row from the `datapoints` lookup table.
 *
 * Used by the cleanup: deleting only the values would leave the ID behind, so it would keep
 * showing up in the statistics with zero rows.
 *
 * @param _dbName unused, PostgreSQL and SQLite connect to the target database directly name of the database
 * @param index the integer key of the datapoint
 */
export function deleteDatapoint(_dbName: string, index: number): string {
    return `DELETE FROM datapoints WHERE id=${index};`;
}

export function update(
    _dbName: string,
    index: number,
    originalState: { val: number | string | boolean | null | undefined; ts: number; q?: number; ack?: boolean },
    from: number | undefined,
    table: 'ts_bool' | 'ts_number' | 'ts_string' | 'ts_counter',
): string {
    // copy, so the caller's state object is not changed (see insert())
    const state = { ...originalState };
    if (!state || state.val === null || state.val === undefined) {
        state.val = 'NULL';
    } else if (table === 'ts_string') {
        state.val = quotePgString(state.val);
    } else if (table === 'ts_number' || table === 'ts_counter') {
        state.val = numberOrNull(state.val);
    } else if (table === 'ts_bool') {
        state.val = boolOrNull(state.val, false);
    }

    let query = `UPDATE ${table} SET `;
    const vals = [];
    if (state.val !== undefined) {
        vals.push(`val=${state.val}`);
    }
    if (state.q !== undefined) {
        vals.push(`q=${state.q}`);
    }
    if (from !== undefined) {
        vals.push(`_from=${from}`);
    }
    if (state.ack !== undefined) {
        vals.push(`ack=${!!state.ack}`);
    }
    query += vals.join(', ');
    query += ' WHERE ';
    query += ` id=${index}`;
    query += ` AND ts=${state.ts}`;
    query += ';';

    return query;
}

function rawEntriesWhere(table: TableName, index: number, options: { start?: number; end?: number }): string {
    let where = `${table}.id=${index}`;
    if (options.start) {
        where += ` AND ${table}.ts>=${options.start}`;
    }
    if (options.end) {
        where += ` AND ${table}.ts<=${options.end}`;
    }
    return where;
}

export function getRawEntries(_dbName: string, table: TableName, index: number, options: RawEntriesOptions): string {
    return (
        `SELECT ${table}.ts, ${table}.val, ${table}.ack, ${table}.q, sources.name AS "from" FROM ${table}` +
        ` LEFT JOIN sources ON sources.id=${table}._from` +
        ` WHERE ${rawEntriesWhere(table, index, options)}` +
        ` ORDER BY ${table}.ts ${options.sort === 'asc' ? 'ASC' : 'DESC'}` +
        ` LIMIT ${options.limit} OFFSET ${options.offset};`
    );
}

export function getRawEntriesCount(
    _dbName: string,
    table: TableName,
    index: number,
    options: { start?: number; end?: number },
): string {
    return `SELECT COUNT(*) AS total FROM ${table} WHERE ${rawEntriesWhere(table, index, options)};`;
}
