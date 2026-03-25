# hypersync-client

[![CI](https://github.com/enviodev/hypersync-client-rust/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/enviodev/hypersync-client-rust/actions/workflows/ci.yaml) [![Crates.io](https://img.shields.io/crates/v/hypersync-client.svg?style=flat-square)](https://crates.io/crates/hypersync-client) [![docs.rs](https://img.shields.io/docsrs/hypersync-client)](https://docs.rs/hypersync-client/latest/hypersync_client/) [![Discord](https://img.shields.io/badge/Discord-Join%20Chat-7289da?logo=discord&logoColor=white)](https://discord.gg/Q9qt8gZ2fX)

Rust crate for [Envio's](https://envio.dev) HyperSync client. The most performant way to access HyperSync, providing direct access to the underlying Rust implementation with no FFI overhead.

## What is HyperSync?

[HyperSync](https://docs.envio.dev/docs/HyperSync/overview) is Envio's high-performance blockchain data retrieval layer. It is a purpose-built alternative to JSON-RPC endpoints, offering up to 2000x faster data access across 70+ EVM-compatible networks and Fuel.

HyperSync lets you query logs, transactions, blocks, and traces with flexible filtering and field selection, returning only the data you need in binary formats for maximum throughput.

## Features

- **Maximum performance**: Direct Rust implementation with no FFI overhead
- **Arrow format support**: Stream blockchain data as Apache Arrow record batches for in-memory analytics
- **Binary transport**: Uses CapnProto serialization to minimize bandwidth and maximize throughput
- **Flexible queries**: Filter logs, transactions, blocks, and traces with granular control
- **Field selection**: Choose exactly which fields to return, reducing unnecessary data transfer
- **Automatic pagination**: Handles large datasets with built-in pagination
- **Event decoding**: Decode ABI-encoded event data directly in the stream
- **Async/await**: Built on Tokio for fully asynchronous operation
- **70+ networks**: Access any [HyperSync-supported network](https://docs.envio.dev/docs/HyperSync/hypersync-supported-networks)

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
hypersync-client = "1"
tokio = { version = "1", features = ["full"] }
```

## API Token

An API token is required to use HyperSync. [Get your token here](https://docs.envio.dev/docs/HyperSync/api-tokens), then set it as an environment variable:

```bash
export ENVIO_API_TOKEN="your-token-here"
```

## Quick Start

Stream all ERC-20 Transfer events from Ethereum mainnet:

```rust
use hypersync_client::{
    net_types::{LogField, LogFilter, Query},
    Client, SerializationFormat, StreamConfig,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::builder()
        .chain_id(1) // Ethereum mainnet
        .api_token(std::env::var("ENVIO_API_TOKEN")?)
        .serialization_format(SerializationFormat::CapnProto {
            should_cache_queries: true,
        })
        .build()?;

    let query = Query::new()
        .from_block(0)
        .where_logs(
            LogFilter::all().and_topic0([
                // ERC-20 Transfer event signature
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            ])?,
        )
        .select_log_fields([
            LogField::Data,
            LogField::Topic0,
            LogField::Topic1,
            LogField::Topic2,
        ]);

    let mut receiver = client.stream_arrow(query, StreamConfig::default()).await?;

    while let Some(batch) = receiver.recv().await {
        let batch = batch?;
        println!("Received {} logs", batch.data.logs.len());
    }

    Ok(())
}
```

See the [examples directory](./examples) for more usage patterns including wallet transactions, block streaming, and decoded event output.

## Connecting to Different Networks

Change the `chain_id` (or use `url`) to connect to any supported network:

```rust
// Arbitrum
Client::builder().chain_id(42161).api_token(...).build()?;

// Base
Client::builder().chain_id(8453).api_token(...).build()?;

// Or use the URL directly
Client::builder().url("https://eth.hypersync.xyz").api_token(...).build()?;
```

See the full list of [supported networks and URLs](https://docs.envio.dev/docs/HyperSync/hypersync-supported-networks).

## Documentation

- [API Reference (docs.rs)](https://docs.rs/hypersync-client/latest/hypersync_client/)
- [HyperSync Documentation](https://docs.envio.dev/docs/HyperSync/overview)
- [Query Reference](https://docs.envio.dev/docs/HyperSync/hypersync-query)
- [All Client Libraries](https://docs.envio.dev/docs/HyperSync/hypersync-clients) (Node.js, Python, Go)

## FAQ

**How does this compare to using JSON-RPC?**
HyperSync retrieves data up to 2000x faster than traditional JSON-RPC. For example, scanning the entire Arbitrum chain for sparse log data takes seconds instead of hours.

**Do I need an API token?**
Yes, an API token is required. [Get one here](https://docs.envio.dev/docs/HyperSync/api-tokens).

**Which networks are supported?**
70+ EVM-compatible networks and Fuel. See the [full list](https://docs.envio.dev/docs/HyperSync/hypersync-supported-networks).

**What serialization formats are supported?**
CapnProto (recommended for performance) and JSON. Both are available via `SerializationFormat`.

**Is there an Arrow output format?**
Yes. Use `stream_arrow` to receive data as Apache Arrow record batches, which integrates directly with analytics and DataFrame libraries.

**What is the difference between this and the other HyperSync clients?**
This is the native Rust implementation. The [Python](https://github.com/enviodev/hypersync-client-python) and [Node.js](https://github.com/enviodev/hypersync-client-node) clients are built on top of this crate via FFI bindings.

## Support

- [Discord community](https://discord.gg/Q9qt8gZ2fX)
- [GitHub Issues](https://github.com/enviodev/hypersync-client-rust/issues)
- [Documentation](https://docs.envio.dev/docs/HyperSync/overview)
