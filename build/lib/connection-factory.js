"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConnectionFactory = void 0;
class ConnectionFactory {
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
    executeStreamed(_connection, _sql, _batchSize, _onRows, callback) {
        // default: streaming not supported by this driver
        callback(null, false);
    }
}
exports.ConnectionFactory = ConnectionFactory;
//# sourceMappingURL=connection-factory.js.map