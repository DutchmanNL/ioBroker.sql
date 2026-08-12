import { ConnectionFactory } from './connection-factory';
import SQLClient from './sql-client';
import { SQLClientPool, type PoolConfig } from './sql-client-pool';

import type { Client, QueryResult, ClientConfig as PostgreSQLOptions } from 'pg';

export type { PostgreSQLOptions };

// PostgreSQLConnectionFactory does not use any of node-pg's built-in pooling.
class PostgreSQLConnectionFactory extends ConnectionFactory {
    private Client: typeof Client | undefined;
    // true when the native (pg-native) client was chosen. pg-native does not support pg-cursor,
    // so streaming has to fall back to the buffered path for it.
    private isNative = false;
    // Lazily imported pg-cursor constructor (default export). `any` keeps check:ts green without
    // depending on the exact shape of @types/pg-cursor.
    private Cursor: any;
    // Remember a missing optional pg-cursor dependency so we do not retry the import on every call.
    private cursorUnavailable = false;

    openConnection(connectString: PostgreSQLOptions, callback: (err: Error | null, connection?: Client) => void): void {
        if (!this.Client) {
            void import('pg').then(
                pg => {
                    const nativeClient = pg.default.native?.Client;
                    this.Client = nativeClient || pg.default.Client;
                    this.isNative = !!nativeClient;
                    this.openConnection(connectString, callback);
                },
                // pg is an optional dependency, so report a missing driver instead of
                // letting the rejection escape as an unhandled promise rejection
                e => callback(new Error(`Node.js DB driver "pg" could not be loaded: ${e}`)),
            );
            return;
        }
        const connection = new this.Client(connectString);
        connection.connect((err: Error) => callback(err, connection));
    }

    closeConnection(connection: Client | null | undefined, callback: (error?: Error | null) => void): void {
        if (connection) {
            connection.end(callback);
        } else {
            callback?.(null);
        }
    }

    execute<T>(
        connection: Client,
        sql: string,
        callback: (err: Error | null | undefined, results?: Array<T>) => void,
    ): void {
        connection.query(sql, (err: Error, results: QueryResult): void => {
            if (err) {
                return callback(err);
            }
            return callback(null, results?.rows);
        });
    }

    executeStreamed<T>(
        connection: Client,
        sql: string,
        batchSize: number,
        onRows: (rows: Array<T>) => void,
        callback: (err: Error | null | undefined, streamed: boolean) => void,
    ): void {
        // pg-native does not support pg-cursor, so it cannot stream.
        if (this.isNative) {
            callback(null, false);
            return;
        }
        // Optional dependency pg-cursor is not installed -> never retry, just fall back.
        if (this.cursorUnavailable) {
            callback(null, false);
            return;
        }
        if (!this.Cursor) {
            void import('pg-cursor').then(
                mod => {
                    this.Cursor = (mod as any).default || mod;
                    this.executeStreamed(connection, sql, batchSize, onRows, callback);
                },
                // pg-cursor is optional; remember it is missing and fall back to the buffered path
                () => {
                    this.cursorUnavailable = true;
                    callback(null, false);
                },
            );
            return;
        }

        const Cursor = this.Cursor;
        const cursor = connection.query(new Cursor(sql));

        // Guard against a double callback if error and close ever overlap.
        let done = false;
        const finish = (err: Error | null | undefined): void => {
            if (done) {
                return;
            }
            done = true;
            callback(err, true);
        };

        const read = (): void => {
            cursor.read(batchSize, (err: Error, rows: Array<T>): void => {
                if (err) {
                    cursor.close(() => finish(err));
                    return;
                }
                if (!rows.length) {
                    cursor.close((closeErr: Error) => finish(closeErr ?? null));
                    return;
                }
                onRows(rows);
                read();
            });
        };
        read();
    }
}

export class PostgreSQLClient extends SQLClient {
    constructor(connectString: PostgreSQLOptions) {
        super(connectString, new PostgreSQLConnectionFactory());
    }
}

export class PostgreSQLClientPool extends SQLClientPool {
    constructor(poolOptions: PoolConfig, connectString: PostgreSQLOptions) {
        super(poolOptions, connectString, new PostgreSQLConnectionFactory());
    }
}
