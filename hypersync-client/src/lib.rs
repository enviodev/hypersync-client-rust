#![deny(missing_docs)]
//! Hypersync client library for interacting with hypersync server.
use std::{num::NonZeroU64, sync::Arc, time::Duration};

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
    /// Query serialization format to use for HTTP requests.
    serialization_format: SerializationFormat,
}

impl Client {
    /// Creates a new client with the given configuration.
    pub fn new(cfg: ClientConfig) -> Result<Self> {
        // hscr stands for hypersync client rust
        let user_agent = format!("hscr/{}", env!("CARGO_PKG_VERSION"));
        Self::new_internal(cfg, user_agent)
    }

    #[doc(hidden)]
    pub fn new_with_agent(cfg: ClientConfig, user_agent: impl Into<String>) -> Result<Self> {
        // Creates a new client with the given configuration and custom user agent.
        // This is intended for use by language bindings (Python, Node.js) and HyperIndex.
        Self::new_internal(cfg, user_agent.into())
    }

    /// Internal constructor that takes both config and user agent.
    fn new_internal(cfg: ClientConfig, user_agent: String) -> Result<Self> {
        let timeout = cfg
            .http_req_timeout_millis
            .unwrap_or(NonZeroU64::new(30_000).unwrap());

        let http_client = reqwest::Client::builder()
            .no_gzip()
            .timeout(Duration::from_millis(timeout.get()))
            .user_agent(user_agent)
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
            serialization_format: cfg.serialization_format,
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

    /// Executes query once and returns the result in (Arrow, size) format using JSON serialization.
    async fn get_arrow_impl_json(&self, query: &Query) -> Result<(ArrowResponse, u64)> {
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

    fn should_cache_queries(&self) -> bool {
        matches!(
            self.serialization_format,
            SerializationFormat::CapnProto {
                should_cache_queries: true
            }
        )
    }

    /// Executes query once and returns the result in (Arrow, size) format using Cap'n Proto serialization.
    async fn get_arrow_impl_capnp(&self, query: &Query) -> Result<(ArrowResponse, u64)> {
        let mut url = self.url.clone();
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

            let mut req = self.http_client.request(Method::POST, url.clone());
            req = req.header("content-type", "application/x-capnp");
            if let Some(bearer_token) = &self.bearer_token {
                req = req.bearer_auth(bearer_token);
            }

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
                log::error!(
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

        let mut req = self.http_client.request(Method::POST, url);
        req = req.header("content-type", "application/x-capnp");
        if let Some(bearer_token) = &self.bearer_token {
            req = req.bearer_auth(bearer_token);
        }

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
        match self.serialization_format {
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
    },
}

enum InternalStreamEvent {
    Publish(HeightStreamEvent),
    Ping,
    Unknown(String),
}

impl Client {
    fn get_es_stream(self: Arc<Self>) -> Result<EventSource> {
        // Build the GET /height/sse request
        let mut url = self.url.clone();
        url.path_segments_mut()
            .ok()
            .context("invalid base URL")?
            .extend(&["height", "sse"]);

        let mut req = self
            .http_client
            .get(url)
            .header(header::ACCEPT, "text/event-stream");

        if let Some(bearer) = &self.bearer_token {
            req = req.bearer_auth(bearer);
        }

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
        self: Arc<Self>,
        tx: &mpsc::Sender<HeightStreamEvent>,
    ) -> Result<()> {
        let mut consecutive_failures = 0u32;

        loop {
            // should always be able to creat a new es stream
            // something is wrong with the req builder otherwise
            let mut es = self.clone().get_es_stream().context("get es stream")?;

            match Self::stream_height_events(&mut es, tx).await {
                Ok(received_an_event) => {
                    if received_an_event {
                        consecutive_failures = 0; // Reset after successful connection that then failed
                    }
                    log::trace!("Stream height exited");
                }
                Err(e) => {
                    log::trace!("Stream height failed: {e:?}");
                }
            }

            es.close();

            // If the receiver is closed, exit the task
            if tx.is_closed() {
                break;
            }

            let delay = Self::get_delay(consecutive_failures);
            log::trace!("Reconnecting in {:?}...", delay);

            if tx
                .send(HeightStreamEvent::Reconnecting { delay })
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
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use hypersync_client::{Client, ClientConfig, HeightStreamEvent};
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Arc::new(Client::new(ClientConfig {
    ///     url: Some("https://eth.hypersync.xyz".parse()?),
    ///     ..Default::default()
    /// })?);
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
    pub fn stream_height(self: Arc<Self>) -> mpsc::Receiver<HeightStreamEvent> {
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            if let Err(e) = self.stream_height_events_with_retry(&tx).await {
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
            let client = Arc::new(Client::new(ClientConfig {
                url: Some("https://monad-testnet.hypersync.xyz".parse()?),
                ..Default::default()
            })?);
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
