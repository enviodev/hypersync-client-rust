#![deny(missing_docs)]
//! Hypersync client library for interacting with hypersync server.
use std::{num::NonZeroU64, sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use hypersync_net_types::{ArchiveHeight, ChainId, Query};
use polars_arrow::{array::Array, record_batch::RecordBatchT as Chunk};
use reqwest::header;
use reqwest::Method;
use serde::Deserialize;

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
pub use config::{ClientConfig, StreamConfig};
pub use decode::Decoder;
pub use decode_call::CallDecoder;
pub use types::{ArrowBatch, ArrowResponse, ArrowResponseData, QueryResponse};

use crate::simple_types::InternalEventJoinStrategy;

type ArrowChunk = Chunk<Box<dyn Array>>;

/// Internal client to handle http requests and retries.
#[derive(Clone, Debug)]
pub struct Client {
    /// Initialized reqwest instance for client url.
    http_client: reqwest::Client,
    /// HyperSync server URL.
    url: Url,
    /// HyperSync server bearer token.
    bearer_token: Option<String>,
    /// Number of retries to attempt before returning error.
    max_num_retries: usize,
    /// Milliseconds that would be used for retry backoff increasing.
    retry_backoff_ms: u64,
    /// Initial wait time for request backoff.
    retry_base_ms: u64,
    /// Ceiling time for request backoff.
    retry_ceiling_ms: u64,
}

impl Client {
    /// Creates a new client with the given configuration.
    pub fn new(cfg: ClientConfig) -> Result<Self> {
        let timeout = cfg
            .http_req_timeout_millis
            .unwrap_or(NonZeroU64::new(30_000).unwrap());

        let http_client = reqwest::Client::builder()
            .no_gzip()
            .timeout(Duration::from_millis(timeout.get()))
            .build()
            .unwrap();

        Ok(Self {
            http_client,
            url: cfg
                .url
                .unwrap_or("https://eth.hypersync.xyz".parse().context("parse url")?),
            bearer_token: cfg.bearer_token,
            max_num_retries: cfg.max_num_retries.unwrap_or(12),
            retry_backoff_ms: cfg.retry_backoff_ms.unwrap_or(500),
            retry_base_ms: cfg.retry_base_ms.unwrap_or(200),
            retry_ceiling_ms: cfg.retry_ceiling_ms.unwrap_or(5_000),
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
    pub async fn collect(
        self: Arc<Self>,
        query: Query,
        config: StreamConfig,
    ) -> Result<QueryResponse> {
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
    pub async fn collect_events(
        self: Arc<Self>,
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
    pub async fn collect_arrow(
        self: Arc<Self>,
        query: Query,
        config: StreamConfig,
    ) -> Result<ArrowResponse> {
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
    pub async fn collect_parquet(
        self: Arc<Self>,
        path: &str,
        query: Query,
        config: StreamConfig,
    ) -> Result<()> {
        parquet_out::collect_parquet(self, path, query, config).await
    }

    /// Internal implementation of getting chain_id from server
    async fn get_chain_id_impl(&self) -> Result<u64> {
        let mut url = self.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        segments.push("chain_id");
        std::mem::drop(segments);
        let mut req = self.http_client.request(Method::GET, url);

        if let Some(bearer_token) = &self.bearer_token {
            req = req.bearer_auth(bearer_token);
        }

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
        let mut url = self.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        segments.push("height");
        std::mem::drop(segments);
        let mut req = self.http_client.request(Method::GET, url);

        if let Some(bearer_token) = &self.bearer_token {
            req = req.bearer_auth(bearer_token);
        }

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
    pub async fn get_chain_id(&self) -> Result<u64> {
        let mut base = self.retry_base_ms;

        let mut err = anyhow!("");

        for _ in 0..self.max_num_retries + 1 {
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
                self.retry_backoff_ms,
            ));

            tokio::time::sleep(base_ms + jitter).await;

            base = std::cmp::min(base + self.retry_backoff_ms, self.retry_ceiling_ms);
        }

        Err(err)
    }

    /// Get the height of from server with retries.
    pub async fn get_height(&self) -> Result<u64> {
        let mut base = self.retry_base_ms;

        let mut err = anyhow!("");

        for _ in 0..self.max_num_retries + 1 {
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
                self.retry_backoff_ms,
            ));

            tokio::time::sleep(base_ms + jitter).await;

            base = std::cmp::min(base + self.retry_backoff_ms, self.retry_ceiling_ms);
        }

        Err(err)
    }

    /// Get the height of the Client instance for health checks.
    /// Doesn't do any retries and the `http_req_timeout` parameter will override the http timeout config set when creating the client.
    pub async fn health_check(&self, http_req_timeout: Option<Duration>) -> Result<u64> {
        self.get_height_impl(http_req_timeout).await
    }

    /// Executes query with retries and returns the response.
    pub async fn get(&self, query: &Query) -> Result<QueryResponse> {
        let arrow_response = self.get_arrow(query).await.context("get data")?;
        Ok(QueryResponse::from(&arrow_response))
    }

    /// Add block, transaction and log fields selection to the query, executes it with retries
    /// and returns the response.
    pub async fn get_events(&self, mut query: Query) -> Result<EventResponse> {
        let event_join_strategy = InternalEventJoinStrategy::from(&query.field_selection);
        event_join_strategy.add_join_fields_to_selection(&mut query.field_selection);
        let arrow_response = self.get_arrow(&query).await.context("get data")?;
        Ok(EventResponse::from_arrow_response(
            &arrow_response,
            &event_join_strategy,
        ))
    }

    /// Executes query once and returns the result in (Arrow, size) format.
    async fn get_arrow_impl(&self, query: &Query) -> Result<(ArrowResponse, u64)> {
        let mut url = self.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        segments.push("query");
        segments.push("arrow-ipc");
        std::mem::drop(segments);
        let mut req = self.http_client.request(Method::POST, url);

        if let Some(bearer_token) = &self.bearer_token {
            req = req.bearer_auth(bearer_token);
        }

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

    /// Executes query with retries and returns the response in Arrow format.
    pub async fn get_arrow(&self, query: &Query) -> Result<ArrowResponse> {
        self.get_arrow_with_size(query).await.map(|res| res.0)
    }

    /// Internal implementation for get_arrow.
    async fn get_arrow_with_size(&self, query: &Query) -> Result<(ArrowResponse, u64)> {
        let mut base = self.retry_base_ms;

        let mut err = anyhow!("");

        for _ in 0..self.max_num_retries + 1 {
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
                self.retry_backoff_ms,
            ));

            tokio::time::sleep(base_ms + jitter).await;

            base = std::cmp::min(base + self.retry_backoff_ms, self.retry_ceiling_ms);
        }

        Err(err)
    }

    /// Spawns task to execute query and return data via a channel.
    pub async fn stream(
        self: Arc<Self>,
        query: Query,
        config: StreamConfig,
    ) -> Result<mpsc::Receiver<Result<QueryResponse>>> {
        check_simple_stream_params(&config)?;

        let (tx, rx): (_, mpsc::Receiver<Result<QueryResponse>>) =
            mpsc::channel(config.concurrency.unwrap_or(10));

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
    pub async fn stream_events(
        self: Arc<Self>,
        mut query: Query,
        config: StreamConfig,
    ) -> Result<mpsc::Receiver<Result<EventResponse>>> {
        check_simple_stream_params(&config)?;

        let event_join_strategy = InternalEventJoinStrategy::from(&query.field_selection);

        event_join_strategy.add_join_fields_to_selection(&mut query.field_selection);

        let (tx, rx): (_, mpsc::Receiver<Result<EventResponse>>) =
            mpsc::channel(config.concurrency.unwrap_or(10));

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
    pub async fn stream_arrow(
        self: Arc<Self>,
        query: Query,
        config: StreamConfig,
    ) -> Result<mpsc::Receiver<Result<ArrowResponse>>> {
        stream::stream_arrow(self, query, config).await
    }

    /// Getter for url field.
    pub fn url(&self) -> &Url {
        &self.url
    }
}

#[derive(Debug, Deserialize)]
struct HeightSsePayloadJson {
    height: Option<u64>,
}

const INITIAL_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_millis(200);
const MAX_RECONNECT_DELAY: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_CONNECTION_AGE: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);
/// Timeout for detecting dead connections. Server sends keepalive pings every 5s (when idle),
/// so we timeout after 15s (3x the ping interval) if no data is received.
/// This allows the client to detect server crashes that don't trigger graceful shutdown.
/// Note: The server uses smart keepalive - it only pings when no height updates occur,
/// so active chains won't generate unnecessary pings.
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

impl Client {
    /// Streams latest archive height updates from the server using the `/height/sse` SSE endpoint.
    ///
    /// # Overview
    /// This function establishes a long-lived Server-Sent Events (SSE) connection to continuously
    /// receive height updates from the hypersync server. The connection is resilient and will
    /// automatically reconnect if it drops due to network issues, server restarts, or shutdowns.
    ///
    /// # Returns
    /// Returns a channel receiver that yields `Result<u64>` heights. A background task manages
    /// the connection lifecycle and sends height updates through this channel.
    ///
    /// # Connection Management
    /// - **Automatic Reconnection**: If the connection drops, the client automatically attempts
    ///   to reconnect with exponential backoff (1s → 2s → 4s → ... → max 30s)
    /// - **Graceful Shutdown**: When the server closes the stream (e.g., during restart), the
    ///   client detects it and reconnects immediately
    /// - **Error Handling**: Connection errors are logged and don't terminate the stream
    ///
    /// # SSE Protocol Details
    /// The function parses SSE messages according to the W3C EventSource spec:
    /// - Messages are separated by blank lines (`\n\n`)
    /// - Each message can have `event:` and `data:` fields
    /// - Keep-alive comments (`:ping`) are ignored
    /// - Only `event:height` messages are processed
    ///
    /// # Example
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use hypersync_client::{Client, ClientConfig};
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Arc::new(Client::new(ClientConfig {
    ///     url: Some("https://eth.hypersync.xyz".parse()?),
    ///     ..Default::default()
    /// })?);
    ///
    /// let mut rx = client.stream_height().await?;
    ///
    /// while let Some(result) = rx.recv().await {
    ///     match result {
    ///         Ok(height) => println!("Height: {}", height),
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stream_height(self: Arc<Self>) -> Result<mpsc::Receiver<Result<u64>>> {
        // Create a channel for sending height updates from the background task to the caller.
        // Buffer size of 16 allows for some burst handling without blocking the sender.
        let (tx, rx) = mpsc::channel(16);
        let client = self.clone();

        // Spawn a background task that manages the SSE connection lifecycle.
        // This task runs indefinitely, handling reconnections automatically.
        tokio::spawn(async move {
            // Reconnection delay starts at 1 second and doubles on each failure (exponential backoff).
            // This prevents hammering the server when it's down or restarting.
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY;

            // Main reconnection loop - runs forever, attempting to maintain a connection.
            loop {
                // === STEP 1: Build the SSE endpoint URL ===
                // Construct the full URL path: <base_url>/height/sse
                let mut url = client.url.clone();
                let mut segments = url.path_segments_mut().ok().unwrap();
                segments.push("height");
                segments.push("sse");
                std::mem::drop(segments); // Release the mutable borrow on url

                // === STEP 2: Prepare the HTTP GET request ===
                let mut req = client.http_client.request(Method::GET, url);

                // Add bearer token authentication if configured
                if let Some(bearer_token) = &client.bearer_token {
                    req = req.bearer_auth(bearer_token);
                }

                // Configure request headers and timeout.
                // SSE connections are long-lived, so we use a 24-hour timeout to prevent
                // the HTTP client from terminating the connection prematurely.
                req = req
                    .header(header::ACCEPT, "text/event-stream")
                    .timeout(MAX_CONNECTION_AGE);

                // === STEP 3: Attempt to establish the SSE connection ===
                match req.send().await {
                    Ok(res) => {
                        let status = res.status();

                        // Check for HTTP errors (non-2xx status codes)
                        if !status.is_success() {
                            log::warn!("❌ HTTP error: status code {}", status);

                            // Wait before retrying with exponential backoff
                            tokio::time::sleep(reconnect_delay).await;
                            reconnect_delay =
                                std::cmp::min(reconnect_delay * 2, MAX_RECONNECT_DELAY);
                            continue; // Retry the connection
                        }

                        // Successfully connected!
                        log::info!("✅ Connected to height SSE stream");

                        // Reset reconnection delay after successful connection
                        reconnect_delay = INITIAL_RECONNECT_DELAY;

                        // === STEP 4: Process the SSE byte stream ===
                        // Get the response body as a stream of bytes
                        let mut byte_stream = res.bytes_stream();

                        // Buffer for accumulating incomplete SSE messages.
                        // SSE messages are text-based and separated by blank lines (\n\n).
                        let mut buf = String::new();

                        // Flag to track if the connection is still active
                        let mut connection_active = true;

                        // Main message processing loop - runs until the connection drops
                        while connection_active {
                            // Wait for next chunk with timeout to detect dead connections.
                            // Server sends keepalive pings every 20s, so if we don't receive
                            // ANY data (including pings) within 60s, the connection is dead.
                            let next_chunk =
                                tokio::time::timeout(READ_TIMEOUT, byte_stream.next()).await;

                            match next_chunk {
                                // === Timeout: no data received within READ_TIMEOUT ===
                                Err(_) => {
                                    log::warn!(
                                        "⏱️  No data received for {:?}, connection appears dead",
                                        READ_TIMEOUT
                                    );
                                    connection_active = false;
                                }

                                // === Successfully received bytes from the stream ===
                                Ok(Some(Ok(bytes))) => {
                                    log::trace!(
                                        "📦 Received {} bytes from SSE stream",
                                        bytes.len()
                                    );

                                    use std::fmt::Write as _;

                                    // Convert bytes to UTF-8 string
                                    let chunk_str = match std::str::from_utf8(&bytes) {
                                        Ok(s) => s,
                                        Err(_) => {
                                            // Handle invalid UTF-8 by doing lossy conversion.
                                            // This is rare but can happen with network corruption.
                                            let mut tmp = String::with_capacity(bytes.len() * 2);
                                            for &b in bytes.as_ref() {
                                                let _ = write!(&mut tmp, "{}", char::from(b));
                                            }
                                            buf.push_str(&tmp);
                                            continue;
                                        }
                                    };

                                    // Append the new chunk to our buffer
                                    buf.push_str(chunk_str);

                                    // === STEP 5: Parse complete SSE messages ===
                                    // SSE messages are separated by blank lines (\n\n).
                                    // Process all complete messages currently in the buffer.
                                    loop {
                                        if let Some(idx) = buf.find("\n\n") {
                                            // Extract one complete SSE message
                                            let event_block = buf[..idx].to_string();
                                            buf.drain(..idx + 2); // Remove message + blank line from buffer

                                            // Parse the SSE event fields according to the W3C spec
                                            let mut event_name: Option<&str> = None;
                                            let mut data_lines: Vec<&str> = Vec::new();

                                            // Process each line in the event block
                                            for line in event_block.lines() {
                                                if line.is_empty() {
                                                    continue;
                                                }
                                                if line.starts_with(':') {
                                                    // Comment line (used for keep-alive pings).
                                                    // Format: ": ping" or ": <comment text>"
                                                    continue;
                                                }
                                                if let Some(rest) = line.strip_prefix("event:") {
                                                    // Event type field.
                                                    // Format: "event: height"
                                                    event_name = Some(rest.trim());
                                                    continue;
                                                }
                                                if let Some(rest) = line.strip_prefix("data:") {
                                                    // Data field (can be multiple per event).
                                                    // Format: "data: 12345"
                                                    data_lines.push(rest.trim());
                                                    continue;
                                                }
                                                // Ignore other SSE fields like "id:" and "retry:"
                                            }

                                            // === STEP 6: Process height events ===
                                            let name = event_name.unwrap_or("");
                                            if name == "height" {
                                                // Combine multiple data lines (though typically just one)
                                                let data = data_lines.join("\n");

                                                // Try parsing as plain integer (preferred format).
                                                // Server sends: event:height\ndata:12345\n\n
                                                if let Ok(h) = data.trim().parse::<u64>() {
                                                    log::debug!("📈 Height update: {}", h);

                                                    // Send the height through the channel.
                                                    // If the receiver is dropped, exit the task gracefully.
                                                    if tx.send(Ok(h)).await.is_err() {
                                                        log::info!(
                                                            "Receiver dropped, exiting stream task"
                                                        );
                                                        return;
                                                    }
                                                } else {
                                                    log::warn!(
                                                        "❌ Failed to parse height: {}",
                                                        data
                                                    );
                                                    connection_active = false;
                                                    continue;
                                                }
                                            }
                                        } else {
                                            // No complete message in buffer yet, wait for more data
                                            break;
                                        }
                                    }
                                }

                                // === Stream error occurred (network issue, timeout, etc.) ===
                                Ok(Some(Err(e))) => {
                                    log::warn!("⚠️  SSE stream error: {:?}", e);
                                    connection_active = false; // Exit loop and reconnect
                                }

                                // === Stream ended (server closed the connection) ===
                                // This happens during server restarts, shutdowns, or SIGTERM simulation
                                Ok(None) => {
                                    log::info!("🔌 SSE stream closed by server, will reconnect");
                                    connection_active = false; // Exit loop and reconnect
                                }
                            }
                        }
                    }

                    // === Failed to establish HTTP connection ===
                    Err(e) => {
                        log::warn!("❌ Failed to connect to height stream: {:?}", e);
                    }
                }

                // === STEP 7: Wait before reconnecting ===
                // After any disconnection (graceful or error), wait before attempting to reconnect.
                // This implements exponential backoff to avoid overwhelming the server.
                log::info!("⏳ Reconnecting in {:?}...", reconnect_delay);
                tokio::time::sleep(reconnect_delay).await;

                // Double the delay for the next attempt, up to the maximum.
                // Pattern: 0.5s → 1s → 2s → 4s → 8s → 16s → 30s (max) → 30s → ...
                reconnect_delay = std::cmp::min(reconnect_delay * 2, MAX_RECONNECT_DELAY);
            }
        });

        Ok(rx)
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
