# hypersync-client

[![CI](https://github.com/enviodev/hypersync-client-rust/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/enviodev/hypersync-client-rust/actions/workflows/ci.yaml) [![Crates.io](https://img.shields.io/crates/v/hypersync-client.svg?style=flat-square)](https://crates.io/crates/hypersync-client) [![docs.rs](https://img.shields.io/docsrs/hypersync-client)](https://docs.rs/hypersync-client/latest/hypersync_client/) [![Discord](https://img.shields.io/badge/Discord-Join%20Chat-7289da?logo=discord&logoColor=white)](https://discord.com/invite/envio)

Rust crate for [Envio's](https://envio.dev) HyperSync client. The most performant way to access HyperSync, with direct access to the underlying Rust implementation and no FFI overhead.

## What is HyperSync?

[HyperSync](https://docs.envio.dev/docs/HyperSync/overview) is Envio's high-performance blockchain data retrieval layer. It is a purpose-built alternative to JSON-RPC endpoints, offering up to 2000x faster data access across 70+ EVM-compatible networks and Fuel.

HyperSync lets you query logs, transactions, blocks, and traces with flexible filtering and field selection, returning only the data you need in binary formats for maximum throughput.

If you need a full indexing framework on top of HyperSync with GraphQL APIs and schema management, see [HyperIndex](https://github.com/enviodev/hyperindex).

## Features

- **Maximum performance**: Direct Rust implementation with no FFI overhead
- **Arrow and Parquet format support**: Stream blockchain data as Apache Arrow record batches for in-memory analytics, or write directly to Parquet files
- **Binary transport**: Uses CapnProto serialization to minimize bandwidth and maximize throughput
- **Flexible queries**: Filter logs, transactions, blocks, and traces with granular control
- **Field selection**: Choose exactly which fields to return, reducing unnecessary data transfer
- **Automatic pagination**: Handles large datasets with built-in pagination
- **Event decoding**: Decode ABI-encoded event data directly in the stream
- **Real-time updates**: Live height streaming via Server-Sent Events
- **Production ready**: Built-in rate limiting, automatic retries, and error handling
- **Async/await**: Built on Tokio for fully asynchronous operation
- **70+ networks**: Access any [HyperSync-supported network](https://docs.envio.dev/docs/HyperSync/hypersync-supported-networks)

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
hypersync-client = "1"
tokio = { version = "1", features = ["full"] }
anyhow = "1"
```

## API Token

An API token is required to use HyperSync. [Get your token here](https://docs.envio.dev/docs/HyperSync/api-tokens), then set it as an environment variable:

```bash
export ENVIO_API_TOKEN="your-token-here"
```

## Quick Start

Query ERC-20 Transfer events from USDC on Ethereum mainnet:

```rust
use hypersync_client::{Client, net_types::{Query, LogFilter, LogField}, StreamConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create a client for Ethereum mainnet
    let client = Client::builder()
        .chain_id(1)
        .api_token(std::env::var("ENVIO_API_TOKEN")?)
        .build()?;

    // Query ERC-20 Transfer events from USDC contract
    let query = Query::new()
        .from_block(19000000)
        .to_block_excl(19001000)
        .where_logs(
            LogFilter::all()
                // USDC contract address
                .and_address(["0xA0b86a33E6411b87Fd9D3DF822C8698FC06BBe4c"])?
                // ERC-20 Transfer event signature
                .and_topic0(["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"])?
        )
        .select_log_fields([
            LogField::Address,
            LogField::Topic1,
            LogField::Topic2,
            LogField::Data,
        ]);

    // Get all data in one response
    let response = client.get(&query).await?;
    println!("Retrieved {} blocks", response.data.blocks.len());

    // Or stream data for large ranges
    let mut receiver = client.stream(query, StreamConfig::default()).await?;
    while let Some(response) = receiver.recv().await {
        let response = response?;
        println!("Streaming: got blocks up to {}", response.next_block);
    }

    Ok(())
}
```

See the [examples directory](./examples) for more usage patterns including wallet transactions, block streaming, and decoded event output.

## Main Types

- [`Client`](https://docs.rs/hypersync-client/latest/hypersync_client/struct.Client.html) - Main client for interacting with HyperSync servers
- [`net_types::Query`](https://docs.rs/hypersync-client/latest/hypersync_net_types/struct.Query.html) - Query builder for specifying what data to fetch
- [`StreamConfig`](https://docs.rs/hypersync-client/latest/hypersync_client/struct.StreamConfig.html) - Configuration for streaming operations
- [`QueryResponse`](https://docs.rs/hypersync-client/latest/hypersync_client/struct.QueryResponse.html) - Response containing blocks, transactions, logs, and traces
- [`ArrowResponse`](https://docs.rs/hypersync-client/latest/hypersync_client/struct.ArrowResponse.html) - Response in Apache Arrow format for high-performance processing

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

## What you can build

The Rust client is well suited for performance-critical applications that need direct, low-latency access to blockchain data:

- **Blockchain indexers**: Build custom data pipelines without the overhead of JSON-RPC
- **Data analytics**: Scan entire chain histories in seconds, not hours
- **Block explorers**: Power responsive, real-time interfaces with comprehensive data access
- **ETL pipelines**: Extract and transform on-chain data at scale using Apache Arrow output
- **Monitoring tools**: Track wallet activity, token transfers, and contract events in near real-time
- **Security tooling**: Scan token approvals and transaction history across 70+ chains. See [Snubb](https://www.npmjs.com/package/snubb), a CLI tool built with HyperSync that scans outstanding token approvals across 70 chains simultaneously

## Documentation

- [API Reference (docs.rs)](https://docs.rs/hypersync-client/latest/hypersync_client/)
- [HyperSync Documentation](https://docs.envio.dev/docs/HyperSync/overview)
- [Query Reference](https://docs.envio.dev/docs/HyperSync/hypersync-query)
- [All Client Libraries](https://docs.envio.dev/docs/HyperSync/hypersync-clients) (Node.js, Python, Go)

## FAQ

**How does this compare to using JSON-RPC?**
HyperSync retrieves data up to 2000x faster than traditional JSON-RPC. Scanning the entire Arbitrum chain for sparse log data takes seconds instead of hours.

**Do I need an API token?**
Yes. [Get one here](https://docs.envio.dev/docs/HyperSync/api-tokens).

**Which networks are supported?**
70+ EVM-compatible networks and Fuel. See the [full list](https://docs.envio.dev/docs/HyperSync/hypersync-supported-networks).

**What serialization formats are supported?**
CapnProto (recommended for performance) and JSON. Both are available via `SerializationFormat`.

**Is there an Arrow output format?**
Yes. Use `stream_arrow` to receive data as Apache Arrow record batches, which integrates directly with analytics and DataFrame libraries.

**What is the difference between this and the other HyperSync clients?**
This is the native Rust implementation. The [Python](https://github.com/enviodev/hypersync-client-python) and [Node.js](https://github.com/enviodev/hypersync-client-node) clients are built on top of this crate via FFI bindings. If you don't need Rust specifically, those clients give you the same HyperSync performance in your preferred language.

**What is the difference between HyperSync and HyperIndex?**
HyperSync is the raw data access layer. Use it when you need direct, low-level access to blockchain data in your own pipeline. [HyperIndex](https://github.com/enviodev/hyperindex) is the full indexing framework built on top of HyperSync, with schema management, event handlers, and a GraphQL API.

## Support

- [Discord community](https://discord.com/invite/envio)
- [GitHub Issues](https://github.com/enviodev/hypersync-client-rust/issues)
- [Documentation](https://docs.envio.dev/docs/HyperSync/overview)
