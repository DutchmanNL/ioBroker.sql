"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.PostgreSQLClientPool = exports.PostgreSQLClient = void 0;
const connection_factory_1 = require("./connection-factory");
const sql_client_1 = __importDefault(require("./sql-client"));
const sql_client_pool_1 = require("./sql-client-pool");
// PostgreSQLConnectionFactory does not use any of node-pg's built-in pooling.
class PostgreSQLConnectionFactory extends connection_factory_1.ConnectionFactory {
    Client;
    // true when the native (pg-native) client was chosen. pg-native does not support pg-cursor,
    // so streaming has to fall back to the buffered path for it.
    isNative = false;
    // Lazily imported pg-cursor constructor (default export). `any` keeps check:ts green without
    // depending on the exact shape of @types/pg-cursor.
    Cursor;
    // Remember a missing optional pg-cursor dependency so we do not retry the import on every call.
    cursorUnavailable = false;
    openConnection(connectString, callback) {
        if (!this.Client) {
            void import('pg').then(pg => {
                const nativeClient = pg.default.native?.Client;
                this.Client = nativeClient || pg.default.Client;
                this.isNative = !!nativeClient;
                this.openConnection(connectString, callback);
            }, 
            // pg is an optional dependency, so report a missing driver instead of
            // letting the rejection escape as an unhandled promise rejection
            e => callback(new Error(`Node.js DB driver "pg" could not be loaded: ${e}`)));
            return;
        }
        const connection = new this.Client(connectString);
        connection.connect((err) => callback(err, connection));
    }
    closeConnection(connection, callback) {
        if (connection) {
            connection.end(callback);
        }
        else {
            callback?.(null);
        }
    }
    execute(connection, sql, callback) {
        connection.query(sql, (err, results) => {
            if (err) {
                return callback(err);
            }
            return callback(null, results?.rows);
        });
    }
    executeStreamed(connection, sql, batchSize, onRows, callback) {
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
            void import('pg-cursor').then(mod => {
                this.Cursor = mod.default || mod;
                this.executeStreamed(connection, sql, batchSize, onRows, callback);
            }, 
            // pg-cursor is optional; remember it is missing and fall back to the buffered path
            () => {
                this.cursorUnavailable = true;
                callback(null, false);
            });
            return;
        }
        const Cursor = this.Cursor;
        const cursor = connection.query(new Cursor(sql));
        // Guard against a double callback if error and close ever overlap.
        let done = false;
        const finish = (err) => {
            if (done) {
                return;
            }
            done = true;
            callback(err, true);
        };
        const read = () => {
            cursor.read(batchSize, (err, rows) => {
                if (err) {
                    cursor.close(() => finish(err));
                    return;
                }
                if (!rows.length) {
                    cursor.close((closeErr) => finish(closeErr ?? null));
                    return;
                }
                onRows(rows);
                read();
            });
        };
        read();
    }
}
class PostgreSQLClient extends sql_client_1.default {
    constructor(connectString) {
        super(connectString, new PostgreSQLConnectionFactory());
    }
}
exports.PostgreSQLClient = PostgreSQLClient;
class PostgreSQLClientPool extends sql_client_pool_1.SQLClientPool {
    constructor(poolOptions, connectString) {
        super(poolOptions, connectString, new PostgreSQLConnectionFactory());
    }
}
exports.PostgreSQLClientPool = PostgreSQLClientPool;
//# sourceMappingURL=postgresql-client.js.map