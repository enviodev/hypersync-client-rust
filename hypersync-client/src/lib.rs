#![deny(missing_docs)]
//! # HyperSync Client
//!
//! A high-performance Rust client for the HyperSync protocol, enabling efficient retrieval
//! of blockchain data including blocks, transactions, logs, and traces.
//!
//! ## Features
//!
//! - **High-performance streaming**: Parallel data fetching with automatic retries
//! - **Flexible querying**: Rich query builder API for precise data selection
//! - **Multiple data formats**: Support for Arrow, Parquet, and simple Rust types
//! - **Event decoding**: Automatic ABI decoding for smart contract events
//! - **Real-time updates**: Live height streaming via Server-Sent Events
//! - **Production ready**: Built-in rate limiting, retries, and error handling
//!
//! ## Quick Start
//!
//! ```no_run
//! use hypersync_client::{Client, net_types::{Query, LogFilter, LogField}, StreamConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Create a client for Ethereum mainnet
//!     let client = Client::builder()
//!         .chain_id(1)
//!         .api_token(std::env::var("ENVIO_API_TOKEN")?)
//!         .build()?;
//!
//!     // Query ERC20 transfer events from USDC contract
//!     let query = Query::new()
//!         .from_block(19000000)
//!         .to_block_excl(19001000)
//!         .where_logs(
//!             LogFilter::all()
//!                 .and_address(["0xA0b86a33E6411b87Fd9D3DF822C8698FC06BBe4c"])?
//!                 .and_topic0(["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"])?
//!         )
//!         .select_log_fields([LogField::Address, LogField::Topic1, LogField::Topic2, LogField::Data]);
//!
//!     // Get all data in one response
//!     let response = client.get(&query).await?;
//!     println!("Retrieved {} blocks", response.data.blocks.len());
//!
//!     // Or stream data for large ranges
//!     let mut receiver = client.stream(query, StreamConfig::default()).await?;
//!     while let Some(response) = receiver.recv().await {
//!         let response = response?;
//!         println!("Streaming: got blocks up to {}", response.next_block);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Main Types
//!
//! - [`Client`] - Main client for interacting with HyperSync servers
//! - [`net_types::Query`] - Query builder for specifying what data to fetch
//! - [`StreamConfig`] - Configuration for streaming operations
//! - [`QueryResponse`] - Response containing blocks, transactions, logs, and traces
//! - [`ArrowResponse`] - Response in Apache Arrow format for high-performance processing
//!
//! ## Authentication
//!
//! You'll need a HyperSync API token to access the service. Get one from
//! [https://envio.dev/app/api-tokens](https://envio.dev/app/api-tokens).
//!
//! ## Examples
//!
//! See the `examples/` directory for more detailed usage patterns including:
//! - ERC20 token transfers
//! - Wallet transaction history  
//! - Event decoding and filtering
//! - Real-time data streaming
use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use hypersync_net_types::{hypersync_net_types_capnp, ArchiveHeight, ChainId, Query};
use polars_arrow::{array::Array, record_batch::RecordBatchT as Chunk};
use reqwest::{header, Method};
use reqwest_eventsource::retry::ExponentialBackoff;
use reqwest_eventsource::{Event, EventSource};

mod column_mapping;
mod config;
mod decode;
mod decode_call;
mod from_arrow;
mod parquet_out;
mod parse_response;
pub mod preset_query;
mod rayon_async;
pub mod simple_types;
mod stream;
#[cfg(feature = "ethers")]
pub mod to_ethers;
mod types;
mod util;

pub use from_arrow::FromArrow;
pub use hypersync_format as format;
pub use hypersync_net_types as net_types;
pub use hypersync_schema as schema;

use parse_response::parse_query_response;
use tokio::sync::mpsc;
use types::{EventResponse, ResponseData};
use url::Url;

pub use column_mapping::{ColumnMapping, DataType};
pub use config::HexOutput;
pub use config::{ClientConfig, SerializationFormat, StreamConfig};
pub use decode::Decoder;
pub use decode_call::CallDecoder;
pub use types::{ArrowBatch, ArrowResponse, ArrowResponseData, QueryResponse};

use crate::parse_response::read_query_response;
use crate::simple_types::InternalEventJoinStrategy;

type ArrowChunk = Chunk<Box<dyn Array>>;

/// Internal client state to handle http requests and retries.
#[derive(Debug)]
struct ClientInner {
    /// Initialized reqwest instance for client url.
    http_client: reqwest::Client,
    /// HyperSync server URL.
    url: Url,
    /// HyperSync server api token.
    api_token: String,
    /// Number of retries to attempt before returning error.
    max_num_retries: usize,
    /// Milliseconds that would be used for retry backoff increasing.
    retry_backoff_ms: u64,
    /// Initial wait time for request backoff.
    retry_base_ms: u64,
    /// Ceiling time for request backoff.
    retry_ceiling_ms: u64,
    /// Query serialization format to use for HTTP requests.
    serialization_format: SerializationFormat,
}

/// Client to handle http requests and retries.
#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    /// Creates a new client with the given configuration.
    ///
    /// Configuration must include the `url` and `api_token` fields.
    /// # Example
    /// ```
    /// use hypersync_client::{Client, ClientConfig};
    ///
    /// let config = ClientConfig {
    ///     url: "https://eth.hypersync.xyz".to_string(),
    ///     api_token: std::env::var("ENVIO_API_TOKEN")?,
    ///     ..Default::default()
    /// };
    /// let client = Client::new(config)?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    ///
    /// # Errors
    /// This method fails if the config is invalid.
    pub fn new(cfg: ClientConfig) -> Result<Self> {
        // hscr stands for hypersync client rust
        cfg.validate().context("invalid ClientConfig")?;
        let user_agent = format!("hscr/{}", env!("CARGO_PKG_VERSION"));
        Self::new_internal(cfg, user_agent)
    }

    /// Creates a new client builder.
    ///
    /// # Example
    /// ```
    /// use hypersync_client::Client;
    ///
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    #[doc(hidden)]
    pub fn new_with_agent(cfg: ClientConfig, user_agent: impl Into<String>) -> Result<Self> {
        // Creates a new client with the given configuration and custom user agent.
        // This is intended for use by language bindings (Python, Node.js) and HyperIndex.
        Self::new_internal(cfg, user_agent.into())
    }

    /// Internal constructor that takes both config and user agent.
    fn new_internal(cfg: ClientConfig, user_agent: String) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .no_gzip()
            .timeout(Duration::from_millis(cfg.http_req_timeout_millis))
            .user_agent(user_agent)
            .build()
            .unwrap();

        let url = Url::parse(&cfg.url).context("url is malformed")?;

        Ok(Self {
            inner: Arc::new(ClientInner {
                http_client,
                url,
                api_token: cfg.api_token,
                max_num_retries: cfg.max_num_retries,
                retry_backoff_ms: cfg.retry_backoff_ms,
                retry_base_ms: cfg.retry_base_ms,
                retry_ceiling_ms: cfg.retry_ceiling_ms,
                serialization_format: cfg.serialization_format,
            }),
        })
    }

    /// Retrieves blocks, transactions, traces, and logs through a stream using the provided
    /// query and stream configuration.
    ///
    /// ### Implementation
    /// Runs multiple queries simultaneously based on config.concurrency.
    ///
    /// Each query runs until it reaches query.to, server height, any max_num_* query param,
    /// or execution timed out by server.
    ///
    /// # ⚠️ Important Warning
    ///
    /// This method will continue executing until the query has run to completion from beginning
    /// to the end of the block range defined in the query. For heavy queries with large block
    /// ranges or high data volumes, consider:
    ///
    /// - Use [`stream()`](Self::stream) to interact with each streamed chunk individually
    /// - Use [`get()`](Self::get) which returns a `next_block` that can be paginated for the next query
    /// - Break large queries into smaller block ranges
    ///
    /// # Example
    /// ```no_run
    /// use hypersync_client::{Client, net_types::{Query, LogFilter, LogField}, StreamConfig};
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN")?)
    ///     .build()?;
    ///
    /// // Query ERC20 transfer events
    /// let query = Query::new()
    ///     .from_block(19000000)
    ///     .to_block_excl(19000010)
    ///     .where_logs(
    ///         LogFilter::all()
    ///             .and_topic0(["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"])?
    ///     )
    ///     .select_log_fields([LogField::Address, LogField::Data]);
    /// let response = client.collect(query, StreamConfig::default()).await?;
    ///
    /// println!("Collected {} events", response.data.logs.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn collect(&self, query: Query, config: StreamConfig) -> Result<QueryResponse> {
        check_simple_stream_params(&config)?;

        let mut recv = stream::stream_arrow(self, query, config)
            .await
            .context("start stream")?;

        let mut data = ResponseData::default();
        let mut archive_height = None;
        let mut next_block = 0;
        let mut total_execution_time = 0;

        while let Some(res) = recv.recv().await {
            let res = res.context("get response")?;
            let res: QueryResponse = QueryResponse::from(&res);

            for batch in res.data.blocks {
                data.blocks.push(batch);
            }
            for batch in res.data.transactions {
                data.transactions.push(batch);
            }
            for batch in res.data.logs {
                data.logs.push(batch);
            }
            for batch in res.data.traces {
                data.traces.push(batch);
            }

            archive_height = res.archive_height;
            next_block = res.next_block;
            total_execution_time += res.total_execution_time
        }

        Ok(QueryResponse {
            archive_height,
            next_block,
            total_execution_time,
            data,
            rollback_guard: None,
        })
    }

    /// Retrieves events through a stream using the provided query and stream configuration.
    ///
    /// # ⚠️ Important Warning
    ///
    /// This method will continue executing until the query has run to completion from beginning
    /// to the end of the block range defined in the query. For heavy queries with large block
    /// ranges or high data volumes, consider:
    ///
    /// - Use [`stream_events()`](Self::stream_events) to interact with each streamed chunk individually
    /// - Use [`get_events()`](Self::get_events) which returns a `next_block` that can be paginated for the next query
    /// - Break large queries into smaller block ranges
    ///
    /// # Example
    /// ```no_run
    /// use hypersync_client::{Client, net_types::{Query, TransactionFilter, TransactionField}, StreamConfig};
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN")?)
    ///     .build()?;
    ///
    /// // Query transactions to a specific address
    /// let query = Query::new()
    ///     .from_block(19000000)
    ///     .to_block_excl(19000100)
    ///     .where_transactions(
    ///         TransactionFilter::all()
    ///             .and_to(["0xA0b86a33E6411b87Fd9D3DF822C8698FC06BBe4c"])?
    ///     )
    ///     .select_transaction_fields([TransactionField::Hash, TransactionField::From, TransactionField::Value]);
    /// let response = client.collect_events(query, StreamConfig::default()).await?;
    ///
    /// println!("Collected {} events", response.data.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn collect_events(
        &self,
        mut query: Query,
        config: StreamConfig,
    ) -> Result<EventResponse> {
        check_simple_stream_params(&config)?;

        let event_join_strategy = InternalEventJoinStrategy::from(&query.field_selection);
        event_join_strategy.add_join_fields_to_selection(&mut query.field_selection);

        let mut recv = stream::stream_arrow(self, query, config)
            .await
            .context("start stream")?;

        let mut data = Vec::new();
        let mut archive_height = None;
        let mut next_block = 0;
        let mut total_execution_time = 0;

        while let Some(res) = recv.recv().await {
            let res = res.context("get response")?;
            let res: QueryResponse = QueryResponse::from(&res);
            let events = event_join_strategy.join_from_response_data(res.data);

            data.extend(events);

            archive_height = res.archive_height;
            next_block = res.next_block;
            total_execution_time += res.total_execution_time
        }

        Ok(EventResponse {
            archive_height,
            next_block,
            total_execution_time,
            data,
            rollback_guard: None,
        })
    }

    /// Retrieves blocks, transactions, traces, and logs in Arrow format through a stream using
    /// the provided query and stream configuration.
    ///
    /// Returns data in Apache Arrow format for high-performance columnar processing.
    /// Useful for analytics workloads or when working with Arrow-compatible tools.
    ///
    /// # ⚠️ Important Warning
    ///
    /// This method will continue executing until the query has run to completion from beginning
    /// to the end of the block range defined in the query. For heavy queries with large block
    /// ranges or high data volumes, consider:
    ///
    /// - Use [`stream_arrow()`](Self::stream_arrow) to interact with each streamed chunk individually
    /// - Use [`get_arrow()`](Self::get_arrow) which returns a `next_block` that can be paginated for the next query
    /// - Break large queries into smaller block ranges
    ///
    /// # Example
    /// ```no_run
    /// use hypersync_client::{Client, net_types::{Query, BlockFilter, BlockField}, StreamConfig};
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN")?)
    ///     .build()?;
    ///
    /// // Get block data in Arrow format for analytics
    /// let query = Query::new()
    ///     .from_block(19000000)
    ///     .to_block_excl(19000100)
    ///     .include_all_blocks()
    ///     .select_block_fields([BlockField::Number, BlockField::Timestamp, BlockField::GasUsed]);
    /// let response = client.collect_arrow(query, StreamConfig::default()).await?;
    ///
    /// println!("Retrieved {} Arrow batches for blocks", response.data.blocks.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn collect_arrow(&self, query: Query, config: StreamConfig) -> Result<ArrowResponse> {
        let mut recv = stream::stream_arrow(self, query, config)
            .await
            .context("start stream")?;

        let mut data = ArrowResponseData::default();
        let mut archive_height = None;
        let mut next_block = 0;
        let mut total_execution_time = 0;

        while let Some(res) = recv.recv().await {
            let res = res.context("get response")?;

            for batch in res.data.blocks {
                data.blocks.push(batch);
            }
            for batch in res.data.transactions {
                data.transactions.push(batch);
            }
            for batch in res.data.logs {
                data.logs.push(batch);
            }
            for batch in res.data.traces {
                data.traces.push(batch);
            }
            for batch in res.data.decoded_logs {
                data.decoded_logs.push(batch);
            }

            archive_height = res.archive_height;
            next_block = res.next_block;
            total_execution_time += res.total_execution_time
        }

        Ok(ArrowResponse {
            archive_height,
            next_block,
            total_execution_time,
            data,
            rollback_guard: None,
        })
    }

    /// Writes parquet file getting data through a stream using the provided path, query,
    /// and stream configuration.
    ///
    /// Streams data directly to a Parquet file for efficient storage and later analysis.
    /// Perfect for data exports or ETL pipelines.
    ///
    /// # ⚠️ Important Warning
    ///
    /// This method will continue executing until the query has run to completion from beginning
    /// to the end of the block range defined in the query. For heavy queries with large block
    /// ranges or high data volumes, consider:
    ///
    /// - Use [`stream_arrow()`](Self::stream_arrow) and write to Parquet incrementally
    /// - Use [`get_arrow()`](Self::get_arrow) with pagination and append to Parquet files
    /// - Break large queries into smaller block ranges
    ///
    /// # Example
    /// ```no_run
    /// use hypersync_client::{Client, net_types::{Query, LogFilter, LogField}, StreamConfig};
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN")?)
    ///     .build()?;
    ///
    /// // Export all DEX trades to Parquet for analysis
    /// let query = Query::new()
    ///     .from_block(19000000)
    ///     .to_block_excl(19010000)
    ///     .where_logs(
    ///         LogFilter::all()
    ///             .and_topic0(["0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"])?
    ///     )
    ///     .select_log_fields([LogField::Address, LogField::Data, LogField::BlockNumber]);
    /// client.collect_parquet("./trades.parquet", query, StreamConfig::default()).await?;
    ///
    /// println!("Trade data exported to trades.parquet");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn collect_parquet(
        &self,
        path: &str,
        query: Query,
        config: StreamConfig,
    ) -> Result<()> {
        parquet_out::collect_parquet(self, path, query, config).await
    }

    /// Internal implementation of getting chain_id from server
    async fn get_chain_id_impl(&self) -> Result<u64> {
        let mut url = self.inner.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        segments.push("chain_id");
        std::mem::drop(segments);
        let req = self
            .inner
            .http_client
            .request(Method::GET, url)
            .bearer_auth(&self.inner.api_token);

        let res = req.send().await.context("execute http req")?;

        let status = res.status();
        if !status.is_success() {
            return Err(anyhow!("http response status code {}", status));
        }

        let chain_id: ChainId = res.json().await.context("read response body json")?;

        Ok(chain_id.chain_id)
    }

    /// Internal implementation of getting height from server
    async fn get_height_impl(&self, http_timeout_override: Option<Duration>) -> Result<u64> {
        let mut url = self.inner.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        segments.push("height");
        std::mem::drop(segments);
        let mut req = self
            .inner
            .http_client
            .request(Method::GET, url)
            .bearer_auth(&self.inner.api_token);

        if let Some(http_timeout_override) = http_timeout_override {
            req = req.timeout(http_timeout_override);
        }

        let res = req.send().await.context("execute http req")?;

        let status = res.status();
        if !status.is_success() {
            return Err(anyhow!("http response status code {}", status));
        }

        let height: ArchiveHeight = res.json().await.context("read response body json")?;

        Ok(height.height.unwrap_or(0))
    }

    /// Get the chain_id from the server with retries.
    ///
    /// # Example
    /// ```no_run
    /// use hypersync_client::Client;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN")?)
    ///     .build()?;
    ///
    /// let chain_id = client.get_chain_id().await?;
    /// println!("Connected to chain ID: {}", chain_id);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_chain_id(&self) -> Result<u64> {
        let mut base = self.inner.retry_base_ms;

        let mut err = anyhow!("");

        for _ in 0..self.inner.max_num_retries + 1 {
            match self.get_chain_id_impl().await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    log::error!(
                        "failed to get chain_id from server, retrying... The error was: {e:?}"
                    );
                    err = err.context(format!("{e:?}"));
                }
            }

            let base_ms = Duration::from_millis(base);
            let jitter = Duration::from_millis(fastrange_rs::fastrange_64(
                rand::random(),
                self.inner.retry_backoff_ms,
            ));

            tokio::time::sleep(base_ms + jitter).await;

            base = std::cmp::min(
                base + self.inner.retry_backoff_ms,
                self.inner.retry_ceiling_ms,
            );
        }

        Err(err)
    }

    /// Get the height of from server with retries.
    ///
    /// # Example
    /// ```no_run
    /// use hypersync_client::Client;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN")?)
    ///     .build()?;
    ///
    /// let height = client.get_height().await?;
    /// println!("Current block height: {}", height);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_height(&self) -> Result<u64> {
        let mut base = self.inner.retry_base_ms;

        let mut err = anyhow!("");

        for _ in 0..self.inner.max_num_retries + 1 {
            match self.get_height_impl(None).await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    log::error!(
                        "failed to get height from server, retrying... The error was: {e:?}"
                    );
                    err = err.context(format!("{e:?}"));
                }
            }

            let base_ms = Duration::from_millis(base);
            let jitter = Duration::from_millis(fastrange_rs::fastrange_64(
                rand::random(),
                self.inner.retry_backoff_ms,
            ));

            tokio::time::sleep(base_ms + jitter).await;

            base = std::cmp::min(
                base + self.inner.retry_backoff_ms,
                self.inner.retry_ceiling_ms,
            );
        }

        Err(err)
    }

    /// Get the height of the Client instance for health checks.
    ///
    /// Doesn't do any retries and the `http_req_timeout` parameter will override the http timeout config set when creating the client.
    ///
    /// # Example
    /// ```no_run
    /// use hypersync_client::Client;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN")?)
    ///     .build()?;
    ///
    /// // Quick health check with 5 second timeout
    /// let height = client.health_check(Some(Duration::from_secs(5))).await?;
    /// println!("Server is healthy at block: {}", height);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check(&self, http_req_timeout: Option<Duration>) -> Result<u64> {
        self.get_height_impl(http_req_timeout).await
    }

    /// Executes query with retries and returns the response.
    ///
    /// # Example
    /// ```no_run
    /// use hypersync_client::{Client, net_types::{Query, BlockFilter, BlockField}};
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN")?)
    ///     .build()?;
    ///
    /// // Query all blocks from a specific range
    /// let query = Query::new()
    ///     .from_block(19000000)
    ///     .to_block_excl(19000010)
    ///     .include_all_blocks()
    ///     .select_block_fields([BlockField::Number, BlockField::Hash, BlockField::Timestamp]);
    /// let response = client.get(&query).await?;
    ///
    /// println!("Retrieved {} blocks", response.data.blocks.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get(&self, query: &Query) -> Result<QueryResponse> {
        let arrow_response = self.get_arrow(query).await.context("get data")?;
        Ok(QueryResponse::from(&arrow_response))
    }

    /// Add block, transaction and log fields selection to the query, executes it with retries
    /// and returns the response.
    ///
    /// This method automatically joins blocks, transactions, and logs into unified events,
    /// making it easier to work with related blockchain data.
    ///
    /// # Example
    /// ```no_run
    /// use hypersync_client::{Client, net_types::{Query, LogFilter, LogField, TransactionField}};
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN")?)
    ///     .build()?;
    ///
    /// // Query ERC20 transfers with transaction context
    /// let query = Query::new()
    ///     .from_block(19000000)
    ///     .to_block_excl(19000010)
    ///     .where_logs(
    ///         LogFilter::all()
    ///             .and_topic0(["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"])?
    ///     )
    ///     .select_log_fields([LogField::Address, LogField::Data])
    ///     .select_transaction_fields([TransactionField::Hash, TransactionField::From]);
    /// let response = client.get_events(query).await?;
    ///
    /// println!("Retrieved {} joined events", response.data.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_events(&self, mut query: Query) -> Result<EventResponse> {
        let event_join_strategy = InternalEventJoinStrategy::from(&query.field_selection);
        event_join_strategy.add_join_fields_to_selection(&mut query.field_selection);
        let arrow_response = self.get_arrow(&query).await.context("get data")?;
        Ok(EventResponse::from_arrow_response(
            &arrow_response,
            &event_join_strategy,
        ))
    }

    /// Executes query once and returns the result in (Arrow, size) format using JSON serialization.
    async fn get_arrow_impl_json(&self, query: &Query) -> Result<(ArrowResponse, u64)> {
        let mut url = self.inner.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        segments.push("query");
        segments.push("arrow-ipc");
        std::mem::drop(segments);
        let req = self
            .inner
            .http_client
            .request(Method::POST, url)
            .bearer_auth(&self.inner.api_token);

        let res = req.json(&query).send().await.context("execute http req")?;

        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.context("read text to see error")?;

            return Err(anyhow!(
                "http response status code {}, err body: {}",
                status,
                text
            ));
        }

        let bytes = res.bytes().await.context("read response body bytes")?;

        let res = tokio::task::block_in_place(|| {
            parse_query_response(&bytes).context("parse query response")
        })?;

        Ok((res, bytes.len().try_into().unwrap()))
    }

    fn should_cache_queries(&self) -> bool {
        matches!(
            self.inner.serialization_format,
            SerializationFormat::CapnProto {
                should_cache_queries: true
            }
        )
    }

    /// Executes query once and returns the result in (Arrow, size) format using Cap'n Proto serialization.
    async fn get_arrow_impl_capnp(&self, query: &Query) -> Result<(ArrowResponse, u64)> {
        let mut url = self.inner.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        segments.push("query");
        segments.push("arrow-ipc");
        segments.push("capnp");
        std::mem::drop(segments);

        let should_cache = self.should_cache_queries();

        if should_cache {
            let query_with_id = {
                let mut message = capnp::message::Builder::new_default();
                let mut request_builder =
                    message.init_root::<hypersync_net_types_capnp::request::Builder>();

                request_builder.build_query_id_from_query(query)?;
                let mut query_with_id = Vec::new();
                capnp::serialize_packed::write_message(&mut query_with_id, &message)?;
                query_with_id
            };

            let mut req = self
                .inner
                .http_client
                .request(Method::POST, url.clone())
                .bearer_auth(&self.inner.api_token);
            req = req.header("content-type", "application/x-capnp");

            let res = req
                .body(query_with_id)
                .send()
                .await
                .context("execute http req")?;

            let status = res.status();
            if status.is_success() {
                let bytes = res.bytes().await.context("read response body bytes")?;

                let mut opts = capnp::message::ReaderOptions::new();
                opts.nesting_limit(i32::MAX).traversal_limit_in_words(None);
                let message_reader = capnp::serialize_packed::read_message(bytes.as_ref(), opts)
                    .context("create message reader")?;
                let query_response = message_reader
                    .get_root::<hypersync_net_types_capnp::cached_query_response::Reader>()
                    .context("get cached_query_response root")?;
                match query_response.get_either().which()? {
                hypersync_net_types_capnp::cached_query_response::either::Which::QueryResponse(
                    query_response,
                ) => {
                    let res = tokio::task::block_in_place(|| {
                        let res = query_response?;
                        read_query_response(&res).context("parse query response cached")
                    })?;
                    return Ok((res, bytes.len().try_into().unwrap()));
                }
                hypersync_net_types_capnp::cached_query_response::either::Which::NotCached(()) => {
                    log::trace!("query was not cached, retrying with full query");
                }
            }
            } else {
                let text = res.text().await.context("read text to see error")?;
                log::warn!(
                    "Failed cache query, will retry full query. {}, err body: {}",
                    status,
                    text
                );
            }
        };

        let full_query_bytes = {
            let mut message = capnp::message::Builder::new_default();
            let mut query_builder =
                message.init_root::<hypersync_net_types_capnp::request::Builder>();

            query_builder.build_full_query_from_query(query, should_cache)?;
            let mut bytes = Vec::new();
            capnp::serialize_packed::write_message(&mut bytes, &message)?;
            bytes
        };

        let mut req = self
            .inner
            .http_client
            .request(Method::POST, url)
            .bearer_auth(&self.inner.api_token);
        req = req.header("content-type", "application/x-capnp");

        let res = req
            .header("content-type", "application/x-capnp")
            .body(full_query_bytes)
            .send()
            .await
            .context("execute http req")?;

        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.context("read text to see error")?;

            return Err(anyhow!(
                "http response status code {}, err body: {}",
                status,
                text
            ));
        }

        let bytes = res.bytes().await.context("read response body bytes")?;

        let res = tokio::task::block_in_place(|| {
            parse_query_response(&bytes).context("parse query response")
        })?;

        Ok((res, bytes.len().try_into().unwrap()))
    }

    /// Executes query once and returns the result in (Arrow, size) format.
    async fn get_arrow_impl(&self, query: &Query) -> Result<(ArrowResponse, u64)> {
        match self.inner.serialization_format {
            SerializationFormat::Json => self.get_arrow_impl_json(query).await,
            SerializationFormat::CapnProto { .. } => self.get_arrow_impl_capnp(query).await,
        }
    }

    /// Executes query with retries and returns the response in Arrow format.
    pub async fn get_arrow(&self, query: &Query) -> Result<ArrowResponse> {
        self.get_arrow_with_size(query).await.map(|res| res.0)
    }

    /// Internal implementation for get_arrow.
    async fn get_arrow_with_size(&self, query: &Query) -> Result<(ArrowResponse, u64)> {
        let mut base = self.inner.retry_base_ms;

        let mut err = anyhow!("");

        for _ in 0..self.inner.max_num_retries + 1 {
            match self.get_arrow_impl(query).await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    log::error!(
                        "failed to get arrow data from server, retrying... The error was: {e:?}"
                    );
                    err = err.context(format!("{e:?}"));
                }
            }

            let base_ms = Duration::from_millis(base);
            let jitter = Duration::from_millis(fastrange_rs::fastrange_64(
                rand::random(),
                self.inner.retry_backoff_ms,
            ));

            tokio::time::sleep(base_ms + jitter).await;

            base = std::cmp::min(
                base + self.inner.retry_backoff_ms,
                self.inner.retry_ceiling_ms,
            );
        }

        Err(err)
    }

    /// Spawns task to execute query and return data via a channel.
    ///
    /// # Example
    /// ```no_run
    /// use hypersync_client::{Client, net_types::{Query, LogFilter, LogField}, StreamConfig};
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN")?)
    ///     .build()?;
    ///
    /// // Stream all ERC20 transfer events
    /// let query = Query::new()
    ///     .from_block(19000000)
    ///     .where_logs(
    ///         LogFilter::all()
    ///             .and_topic0(["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"])?
    ///     )
    ///     .select_log_fields([LogField::Address, LogField::Topic1, LogField::Topic2, LogField::Data]);
    /// let mut receiver = client.stream(query, StreamConfig::default()).await?;
    ///
    /// while let Some(response) = receiver.recv().await {
    ///     let response = response?;
    ///     println!("Got {} events up to block: {}", response.data.logs.len(), response.next_block);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stream(
        &self,
        query: Query,
        config: StreamConfig,
    ) -> Result<mpsc::Receiver<Result<QueryResponse>>> {
        check_simple_stream_params(&config)?;

        let (tx, rx): (_, mpsc::Receiver<Result<QueryResponse>>) =
            mpsc::channel(config.concurrency);

        let mut inner_rx = self
            .stream_arrow(query, config)
            .await
            .context("start inner stream")?;

        tokio::spawn(async move {
            while let Some(resp) = inner_rx.recv().await {
                let is_err = resp.is_err();
                if tx
                    .send(resp.map(|r| QueryResponse::from(&r)))
                    .await
                    .is_err()
                    || is_err
                {
                    return;
                }
            }
        });

        Ok(rx)
    }

    /// Add block, transaction and log fields selection to the query and spawns task to execute it,
    /// returning data via a channel.
    ///
    /// This method automatically joins blocks, transactions, and logs into unified events,
    /// then streams them via a channel for real-time processing.
    ///
    /// # Example
    /// ```no_run
    /// use hypersync_client::{Client, net_types::{Query, LogFilter, LogField, TransactionField}, StreamConfig};
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN")?)
    ///     .build()?;
    ///
    /// // Stream NFT transfer events with transaction context
    /// let query = Query::new()
    ///     .from_block(19000000)
    ///     .where_logs(
    ///         LogFilter::all()
    ///             .and_topic0(["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"])?
    ///     )
    ///     .select_log_fields([LogField::Address, LogField::Topic1, LogField::Topic2])
    ///     .select_transaction_fields([TransactionField::Hash, TransactionField::From]);
    /// let mut receiver = client.stream_events(query, StreamConfig::default()).await?;
    ///
    /// while let Some(response) = receiver.recv().await {
    ///     let response = response?;
    ///     println!("Got {} joined events up to block: {}", response.data.len(), response.next_block);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stream_events(
        &self,
        mut query: Query,
        config: StreamConfig,
    ) -> Result<mpsc::Receiver<Result<EventResponse>>> {
        check_simple_stream_params(&config)?;

        let event_join_strategy = InternalEventJoinStrategy::from(&query.field_selection);

        event_join_strategy.add_join_fields_to_selection(&mut query.field_selection);

        let (tx, rx): (_, mpsc::Receiver<Result<EventResponse>>) =
            mpsc::channel(config.concurrency);

        let mut inner_rx = self
            .stream_arrow(query, config)
            .await
            .context("start inner stream")?;

        tokio::spawn(async move {
            while let Some(resp) = inner_rx.recv().await {
                let is_err = resp.is_err();
                if tx
                    .send(
                        resp.map(|r| EventResponse::from_arrow_response(&r, &event_join_strategy)),
                    )
                    .await
                    .is_err()
                    || is_err
                {
                    return;
                }
            }
        });

        Ok(rx)
    }

    /// Spawns task to execute query and return data via a channel in Arrow format.
    ///
    /// Returns raw Apache Arrow data via a channel for high-performance processing.
    /// Ideal for applications that need to work directly with columnar data.
    ///
    /// # Example
    /// ```no_run
    /// use hypersync_client::{Client, net_types::{Query, TransactionFilter, TransactionField}, StreamConfig};
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN")?)
    ///     .build()?;
    ///
    /// // Stream transaction data in Arrow format for analytics
    /// let query = Query::new()
    ///     .from_block(19000000)
    ///     .to_block_excl(19000100)
    ///     .where_transactions(
    ///         TransactionFilter::all()
    ///             .and_contract_address(["0xA0b86a33E6411b87Fd9D3DF822C8698FC06BBe4c"])?
    ///     )
    ///     .select_transaction_fields([TransactionField::Hash, TransactionField::From, TransactionField::Value]);
    /// let mut receiver = client.stream_arrow(query, StreamConfig::default()).await?;
    ///
    /// while let Some(response) = receiver.recv().await {
    ///     let response = response?;
    ///     println!("Got {} Arrow batches for transactions", response.data.transactions.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stream_arrow(
        &self,
        query: Query,
        config: StreamConfig,
    ) -> Result<mpsc::Receiver<Result<ArrowResponse>>> {
        stream::stream_arrow(self, query, config).await
    }

    /// Getter for url field.
    ///
    /// # Example
    /// ```
    /// use hypersync_client::Client;
    ///
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
    ///     .build()
    ///     .unwrap();
    ///
    /// println!("Client URL: {}", client.url());
    /// ```
    pub fn url(&self) -> &Url {
        &self.inner.url
    }
}

/// Builder for creating a hypersync client with configuration options.
///
/// This builder provides a fluent API for configuring client settings like URL,
/// authentication, timeouts, and retry behavior.
///
/// # Example
/// ```
/// use hypersync_client::{Client, SerializationFormat};
///
/// let client = Client::builder()
///     .chain_id(1)
///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
///     .http_req_timeout_millis(30000)
///     .max_num_retries(3)
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone, Default)]
pub struct ClientBuilder(ClientConfig);

impl ClientBuilder {
    /// Creates a new ClientBuilder with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the chain ID and automatically configures the URL for the hypersync endpoint.
    ///
    /// This is a convenience method that sets the URL to `https://{chain_id}.hypersync.xyz`.
    ///
    /// # Arguments
    /// * `chain_id` - The blockchain chain ID (e.g., 1 for Ethereum mainnet)
    ///
    /// # Example
    /// ```
    /// use hypersync_client::Client;
    ///
    /// let client = Client::builder()
    ///     .chain_id(1) // Ethereum mainnet
    ///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn chain_id(mut self, chain_id: u64) -> Self {
        self.0.url = format!("https://{chain_id}.hypersync.xyz");
        self
    }

    /// Sets a custom URL for the hypersync server.
    ///
    /// Use this method when you need to connect to a custom hypersync endpoint
    /// instead of the default public endpoints.
    ///
    /// # Arguments
    /// * `url` - The hypersync server URL
    ///
    /// # Example
    /// ```
    /// use hypersync_client::Client;
    ///
    /// let client = Client::builder()
    ///     .url("https://my-custom-hypersync.example.com")
    ///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn url<S: ToString>(mut self, url: S) -> Self {
        self.0.url = url.to_string();
        self
    }

    /// Sets the api token for authentication.
    ///
    /// Required for accessing authenticated hypersync endpoints.
    ///
    /// # Arguments
    /// * `api_token` - The authentication token
    ///
    /// # Example
    /// ```
    /// use hypersync_client::Client;
    ///
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn api_token<S: ToString>(mut self, api_token: S) -> Self {
        self.0.api_token = api_token.to_string();
        self
    }

    /// Sets the HTTP request timeout in milliseconds.
    ///
    /// # Arguments
    /// * `http_req_timeout_millis` - Timeout in milliseconds (default: 30000)
    ///
    /// # Example
    /// ```
    /// use hypersync_client::Client;
    ///
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
    ///     .http_req_timeout_millis(60000) // 60 second timeout
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn http_req_timeout_millis(mut self, http_req_timeout_millis: u64) -> Self {
        self.0.http_req_timeout_millis = http_req_timeout_millis;
        self
    }

    /// Sets the maximum number of retries for failed requests.
    ///
    /// # Arguments
    /// * `max_num_retries` - Maximum number of retries (default: 10)
    ///
    /// # Example
    /// ```
    /// use hypersync_client::Client;
    ///
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
    ///     .max_num_retries(5)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn max_num_retries(mut self, max_num_retries: usize) -> Self {
        self.0.max_num_retries = max_num_retries;
        self
    }

    /// Sets the backoff increment for retry delays.
    ///
    /// This value is added to the base delay on each retry attempt.
    ///
    /// # Arguments
    /// * `retry_backoff_ms` - Backoff increment in milliseconds (default: 500)
    ///
    /// # Example
    /// ```
    /// use hypersync_client::Client;
    ///
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
    ///     .retry_backoff_ms(1000) // 1 second backoff increment
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn retry_backoff_ms(mut self, retry_backoff_ms: u64) -> Self {
        self.0.retry_backoff_ms = retry_backoff_ms;
        self
    }

    /// Sets the initial delay for retry attempts.
    ///
    /// # Arguments
    /// * `retry_base_ms` - Initial retry delay in milliseconds (default: 500)
    ///
    /// # Example
    /// ```
    /// use hypersync_client::Client;
    ///
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
    ///     .retry_base_ms(1000) // Start with 1 second delay
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn retry_base_ms(mut self, retry_base_ms: u64) -> Self {
        self.0.retry_base_ms = retry_base_ms;
        self
    }

    /// Sets the maximum delay for retry attempts.
    ///
    /// The retry delay will not exceed this value, even with backoff increments.
    ///
    /// # Arguments
    /// * `retry_ceiling_ms` - Maximum retry delay in milliseconds (default: 10000)
    ///
    /// # Example
    /// ```
    /// use hypersync_client::Client;
    ///
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
    ///     .retry_ceiling_ms(30000) // Cap at 30 seconds
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn retry_ceiling_ms(mut self, retry_ceiling_ms: u64) -> Self {
        self.0.retry_ceiling_ms = retry_ceiling_ms;
        self
    }

    /// Sets the serialization format for client-server communication.
    ///
    /// # Arguments
    /// * `serialization_format` - The format to use (JSON or CapnProto)
    ///
    /// # Example
    /// ```
    /// use hypersync_client::{Client, SerializationFormat};
    ///
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
    ///     .serialization_format(SerializationFormat::Json)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn serialization_format(mut self, serialization_format: SerializationFormat) -> Self {
        self.0.serialization_format = serialization_format;
        self
    }

    /// Builds the client with the configured settings.
    ///
    /// # Returns
    /// * `Result<Client>` - The configured client or an error if configuration is invalid
    ///
    /// # Errors
    /// Returns an error if:
    /// * The URL is malformed
    /// * Required configuration is missing
    ///
    /// # Example
    /// ```
    /// use hypersync_client::Client;
    ///
    /// let client = Client::builder()
    ///     .chain_id(1)
    ///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn build(self) -> Result<Client> {
        if self.0.url.is_empty() {
            anyhow::bail!(
                "endpoint needs to be set, try using builder.chain_id(1) or\
                builder.url(\"https://eth.hypersync.xyz\") to set the endpoint"
            )
        }
        Client::new(self.0)
    }
}

/// 200ms
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(200);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
/// Timeout for detecting dead connections. Server sends keepalive pings every 5s,
/// so we timeout after 15s (3x the ping interval).
const READ_TIMEOUT: Duration = Duration::from_secs(15);

/// Events emitted by the height stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeightStreamEvent {
    /// Successfully connected or reconnected to the SSE stream.
    Connected,
    /// Received a height update from the server.
    Height(u64),
    /// Connection lost, will attempt to reconnect after the specified delay.
    Reconnecting {
        /// Duration to wait before attempting reconnection.
        delay: Duration,
        /// The error that caused the reconnection.
        error_msg: String,
    },
}

enum InternalStreamEvent {
    Publish(HeightStreamEvent),
    Ping,
    Unknown(String),
}

impl Client {
    fn get_es_stream(&self) -> Result<EventSource> {
        // Build the GET /height/sse request
        let mut url = self.inner.url.clone();
        url.path_segments_mut()
            .ok()
            .context("invalid base URL")?
            .extend(&["height", "sse"]);

        let req = self
            .inner
            .http_client
            .get(url)
            .header(header::ACCEPT, "text/event-stream")
            .bearer_auth(&self.inner.api_token);

        // Configure exponential backoff for library-level retries
        let retry_policy = ExponentialBackoff::new(
            INITIAL_RECONNECT_DELAY,
            2.0,
            Some(MAX_RECONNECT_DELAY),
            None, // unlimited retries
        );

        // Turn the request into an EventSource stream with retries
        let mut es = reqwest_eventsource::EventSource::new(req)
            .context("unexpected error creating EventSource")?;
        es.set_retry_policy(Box::new(retry_policy));
        Ok(es)
    }

    async fn next_height(event_source: &mut EventSource) -> Result<Option<InternalStreamEvent>> {
        let Some(res) = tokio::time::timeout(READ_TIMEOUT, event_source.next())
            .await
            .map_err(|d| anyhow::anyhow!("stream timed out after {d}"))?
        else {
            return Ok(None);
        };

        let e = match res.context("failed response")? {
            Event::Open => InternalStreamEvent::Publish(HeightStreamEvent::Connected),
            Event::Message(event) => match event.event.as_str() {
                "height" => {
                    let height = event
                        .data
                        .trim()
                        .parse::<u64>()
                        .context("parsing height from event data")?;
                    InternalStreamEvent::Publish(HeightStreamEvent::Height(height))
                }
                "ping" => InternalStreamEvent::Ping,
                _ => InternalStreamEvent::Unknown(format!("unknown event: {:?}", event)),
            },
        };

        Ok(Some(e))
    }

    async fn stream_height_events(
        es: &mut EventSource,
        tx: &mpsc::Sender<HeightStreamEvent>,
    ) -> Result<bool> {
        let mut received_an_event = false;
        while let Some(event) = Self::next_height(es).await.context("failed next height")? {
            match event {
                InternalStreamEvent::Publish(event) => {
                    received_an_event = true;
                    if tx.send(event).await.is_err() {
                        return Ok(received_an_event); // Receiver dropped, exit task
                    }
                }
                InternalStreamEvent::Ping => (), // ignore pings
                InternalStreamEvent::Unknown(_event) => (), // ignore unknown events
            }
        }
        Ok(received_an_event)
    }

    fn get_delay(consecutive_failures: u32) -> Duration {
        if consecutive_failures > 0 {
            /// helper function to calculate 2^x
            /// optimization using bit shifting
            const fn two_to_pow(x: u32) -> u32 {
                1 << x
            }
            // Exponential backoff: 200ms, 400ms, 800ms, ... up to 30s
            INITIAL_RECONNECT_DELAY
                .saturating_mul(two_to_pow(consecutive_failures - 1))
                .min(MAX_RECONNECT_DELAY)
        } else {
            // On zero consecutive failures, 0 delay
            Duration::from_millis(0)
        }
    }

    async fn stream_height_events_with_retry(
        &self,
        tx: &mpsc::Sender<HeightStreamEvent>,
    ) -> Result<()> {
        let mut consecutive_failures = 0u32;

        loop {
            // should always be able to creat a new es stream
            // something is wrong with the req builder otherwise
            let mut es = self.get_es_stream().context("get es stream")?;

            let mut error = anyhow!("");

            match Self::stream_height_events(&mut es, tx).await {
                Ok(received_an_event) => {
                    if received_an_event {
                        consecutive_failures = 0; // Reset after successful connection that then failed
                    }
                    log::trace!("Stream height exited");
                }
                Err(e) => {
                    log::trace!("Stream height failed: {e:?}");
                    error = e;
                }
            }

            es.close();

            // If the receiver is closed, exit the task
            if tx.is_closed() {
                break;
            }

            let delay = Self::get_delay(consecutive_failures);
            log::trace!("Reconnecting in {:?}...", delay);

            let error_msg = format!("{error:?}");

            if tx
                .send(HeightStreamEvent::Reconnecting { delay, error_msg })
                .await
                .is_err()
            {
                return Ok(()); // Receiver dropped, exit task
            }
            tokio::time::sleep(delay).await;

            // increment consecutive failures so that on the next try
            // it will start using back offs
            consecutive_failures += 1;
        }

        Ok(())
    }

    /// Streams archive height updates from the server via Server-Sent Events.
    ///
    /// Establishes a long-lived SSE connection to `/height/sse` that automatically reconnects
    /// on disconnection with exponential backoff (200ms → 400ms → ... → max 30s).
    ///
    /// The stream emits [`HeightStreamEvent`] to notify consumers of connection state changes
    /// and height updates. This allows applications to display connection status to users.
    ///
    /// # Returns
    /// Channel receiver yielding [`HeightStreamEvent`]s. The background task handles connection
    /// lifecycle and sends events through this channel.
    ///
    /// # Example
    /// ```
    /// # use hypersync_client::{Client, HeightStreamEvent};
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::builder()
    ///     .url("https://eth.hypersync.xyz")
    ///     .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
    ///     .build()?;
    ///
    /// let mut rx = client.stream_height();
    ///
    /// while let Some(event) = rx.recv().await {
    ///     match event {
    ///         HeightStreamEvent::Connected => println!("Connected to stream"),
    ///         HeightStreamEvent::Height(h) => println!("Height: {}", h),
    ///         HeightStreamEvent::Reconnecting { delay } => {
    ///             println!("Reconnecting in {:?}...", delay)
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn stream_height(&self) -> mpsc::Receiver<HeightStreamEvent> {
        let (tx, rx) = mpsc::channel(16);
        let client = self.clone();

        tokio::spawn(async move {
            if let Err(e) = client.stream_height_events_with_retry(&tx).await {
                log::error!("Stream height failed unexpectedly: {e:?}");
            }
        });

        rx
    }
}

fn check_simple_stream_params(config: &StreamConfig) -> Result<()> {
    if config.event_signature.is_some() {
        return Err(anyhow!(
            "config.event_signature can't be passed to simple type function. User is expected to \
             decode the logs using Decoder."
        ));
    }
    if config.column_mapping.is_some() {
        return Err(anyhow!(
            "config.column_mapping can't be passed to single type function. User is expected to \
             map values manually."
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_get_delay() {
        assert_eq!(
            Client::get_delay(0),
            Duration::from_millis(0),
            "starts with 0 delay"
        );
        // powers of 2 backoff
        assert_eq!(Client::get_delay(1), Duration::from_millis(200));
        assert_eq!(Client::get_delay(2), Duration::from_millis(400));
        assert_eq!(Client::get_delay(3), Duration::from_millis(800));
        // maxes out at 30s
        assert_eq!(
            Client::get_delay(9),
            Duration::from_secs(30),
            "max delay is 30s"
        );
        assert_eq!(
            Client::get_delay(10),
            Duration::from_secs(30),
            "max delay is 30s"
        );
    }

    #[tokio::test]
    #[ignore = "integration test with live hs server for height stream"]
    async fn test_stream_height_events() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(16);
        let handle = tokio::spawn(async move {
            let client = Client::builder()
                .url("https://monad-testnet.hypersync.xyz")
                .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
                .build()?;
            let mut es = client.get_es_stream().context("get es stream")?;
            Client::stream_height_events(&mut es, &tx).await
        });

        let val = rx.recv().await;
        assert!(val.is_some());
        assert_eq!(val.unwrap(), HeightStreamEvent::Connected);
        let Some(HeightStreamEvent::Height(height)) = rx.recv().await else {
            panic!("should have received height")
        };
        let Some(HeightStreamEvent::Height(height2)) = rx.recv().await else {
            panic!("should have received height")
        };
        assert!(height2 > height);
        drop(rx);

        let res = handle.await.expect("should have joined");
        let received_an_event =
            res.expect("should have ended the stream gracefully after dropping rx");
        assert!(received_an_event, "should have received an event");

        Ok(())
    }
}
