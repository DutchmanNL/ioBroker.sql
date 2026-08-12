export type SQLConnection = any;

export abstract class ConnectionFactory {
    abstract openConnection(options: any, callback: (err: Error | null, connection?: SQLConnection) => void): void;
    abstract closeConnection(connection: SQLConnection, callback?: (err?: Error | null) => void): void;
    abstract execute<T>(
        connection: SQLConnection,
        sql: string,
        callback: (err: Error | null | undefined, results?: Array<T>) => void,
    ): void;

    /**
     * Stream the rows of a query in batches instead of buffering them all in RAM.
     *
     * The `streamed` boolean handed to `callback` tells the caller whether streaming actually
     * happened. `false` means the driver does not support streaming for this call and the caller
     * must fall back to the buffered `execute`. `onRows` is only ever invoked when streaming is
     * really taking place, so callers can safely defer any per-request setup to the first chunk.
     *
     * The default implementation reports "not supported" so only drivers that override it stream.
     */
    executeStreamed<T>(
        _connection: SQLConnection,
        _sql: string,
        _batchSize: number,
        _onRows: (rows: Array<T>) => void,
        callback: (err: Error | null | undefined, streamed: boolean) => void,
    ): void {
        // default: streaming not supported by this driver
        callback(null, false);
    }
}
