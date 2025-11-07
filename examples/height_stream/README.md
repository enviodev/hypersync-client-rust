# Height Stream Example

This example demonstrates using the `stream_height()` API to receive real-time height updates from a HyperSync server.

## Running the Example

```bash
# With connection events visible (no env_logger needed)
cargo run -p height_stream

# With debug logs (shows keepalive pings, etc)
RUST_LOG=debug cargo run -p height_stream

# With trace logs (shows all SSE bytes received)
RUST_LOG=trace cargo run -p height_stream
```

## API Design

The stream emits `HeightStreamEvent` enum variants:

- **`Connected`**: Emitted when successfully connected/reconnected to the server
- **`Height(u64)`**: Emitted for each height update received
- **`Reconnecting { delay }`**: Emitted when connection is lost, before waiting to reconnect
