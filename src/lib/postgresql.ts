import type { TableName } from '../types';

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

export function destroy(_dbName: string): string[] {
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
    values.forEach(value => {
        // state, from, table
        insertValues[value.table] = insertValues[value.table] || [];

        if (!value.state || value.state.val === null || value.state.val === undefined) {
            value.state.val = 'NULL';
        } else if (value.table === 'ts_string') {
            value.state.val = `'${value.state.val.toString().replace(/'/g, '')}'`;
        } else if (value.table === 'ts_number') {
            if (isNaN(value.state.val)) {
                value.state.val = 'NULL';
            }
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
    return `SELECT id, type, name FROM datapoints WHERE name='${name}';`;
}

export function getIdInsert(_dbName: string, name: string, type: 0 | 1 | 2): string {
    return `INSERT INTO datapoints (name, type) VALUES('${name}', ${type});`;
}

export function getIdUpdate(_dbName: string, id: number, type: 0 | 1 | 2): string {
    return `UPDATE datapoints SET type = ${type} WHERE id = ${id};`;
}

export function getFromSelect(_dbName: string, name?: string): string {
    if (name) {
        return `SELECT id FROM sources WHERE name='${name}';`;
    }
    return 'SELECT id, name FROM sources;';
}

export function getFromInsert(dbName: string, values: string): string {
    return `INSERT INTO sources (name) VALUES('${values}');`;
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
    // get values from counters where counter changed from up to down (e.g. counter changed)
    const subQueryCounterChanges = `SELECT ts, val FROM ts_counter WHERE id=${options.index} AND ts>${options.start} AND ts<${options.end} AND val IS NOT NULL ORDER BY ts ASC`;

    return (
        `SELECT DISTINCT(a.ts), a.val from ((${subQueryFirst})\n` +
        `UNION ALL \n(${subQueryStart})\n` +
        `UNION ALL \n(${subQueryEnd})\n` +
        `UNION ALL \n(${subQueryLast})\n` +
        `UNION ALL \n(${subQueryCounterChanges})\n` +
        `ORDER BY ts) a;`
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

    if (options.from) {
        query += ` INNER JOIN sources ON sources.id=${table}._from`;
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
            subQuery += ` INNER JOIN sources ON sources.id=${table}._from`;
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
            subQuery += ` INNER JOIN sources ON sources.id=${table}._from`;
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

/**
 * PostgreSQL-only server-side bucket aggregation for getHistory.
 *
 * Returns ONE statement that yields, for a single numeric datapoint:
 *   - k=0 rows: one row per non-empty bucket, grouped by the bucket index
 *     `floor((ts - start) / step)`, carrying the per-bucket accumulators the Node
 *     aggregator would otherwise compute in memory (count, null-count, ordered sum,
 *     min, max).
 *   - k=1 row: the last raw row strictly before `start` (pre-border), if any.
 *   - k=2 row: the first raw row at/after `end` (post-border), if any.
 * These are exactly the two border rows today's getHistory() returns; Node feeds
 * them through the real aggregation()/finishAggregation() so border placement,
 * interpolation and beautify stay byte-identical.
 *
 * Float-parity rules (verified on live PostgreSQL; DO NOT change):
 *   - `(val::text)::float8` (NOT `val::float8`): `val` is REAL (float4); the text
 *     round-trip yields the same double Node's parseFloat produces, direct widening
 *     does not.
 *   - `sum(... ORDER BY ts)` forces the same float association order as the Node loop.
 *   - `floor(x + 0.5)` replicates JS Math.round (PG round() is half-to-even).
 *   - `step` is serialized with JS String() so PG parses back the identical double.
 *
 * All interpolated values are adapter-produced numbers (never user strings).
 */
export function getHistoryAggregate(
    _dbName: string,
    table: string,
    options: ioBroker.GetHistoryOptions & { index: number; start: number; end: number },
): string {
    const start = options.start;
    const end = options.end;
    const index = options.index;
    // Shortest round-trippable float literal so PG parses back the identical double.
    const step = String(options.step);

    // Per-value expression, matching what #normalizeRows does in the Node path:
    // when options.round (a power-of-10 multiplier) is set, each raw value is rounded
    // to that resolution BEFORE it is summed / compared.
    const vexpr = options.round
        ? `floor((val::text)::float8 * ${options.round} + 0.5) / ${options.round}`
        : `(val::text)::float8`;

    return (
        `SELECT k, slot, cnt, cnt_null, s, mn, mx, bts, bval FROM (\n` +
        `  SELECT 0 AS k,\n` +
        `         floor((ts - ${start})::float8 / ${step})::int AS slot,\n` +
        `         count(*)              AS cnt,\n` +
        `         count(*) - count(val) AS cnt_null,\n` +
        `         sum(${vexpr} ORDER BY ts) AS s,\n` +
        `         min(${vexpr})          AS mn,\n` +
        `         max(${vexpr})          AS mx,\n` +
        `         NULL::bigint AS bts, NULL::real AS bval\n` +
        `    FROM ${table}\n` +
        `   WHERE id=${index} AND ts >= ${start} AND ts < ${end}\n` +
        `   GROUP BY 2\n` +
        `  UNION ALL\n` +
        `  SELECT 1, NULL,NULL,NULL,NULL,NULL,NULL, ts, val\n` +
        `    FROM (SELECT ts, val FROM ${table} WHERE id=${index} AND ts < ${start}  ORDER BY ts DESC LIMIT 1) pre\n` +
        `  UNION ALL\n` +
        `  SELECT 2, NULL,NULL,NULL,NULL,NULL,NULL, ts, val\n` +
        `    FROM (SELECT ts, val FROM ${table} WHERE id=${index} AND ts >= ${end} ORDER BY ts ASC LIMIT 1) post\n` +
        `) u ORDER BY k, slot;`
    );
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

export function update(
    _dbName: string,
    index: number,
    state: { val: number | string | boolean | null | undefined; ts: number; q?: number; ack?: boolean },
    from: number,
    table: 'ts_bool' | 'ts_number' | 'ts_string' | 'ts_counter',
): string {
    if (!state || state.val === null || state.val === undefined) {
        state.val = 'NULL';
    } else if (table === 'ts_string') {
        state.val = `'${state.val.toString().replace(/'/g, '')}'`;
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
