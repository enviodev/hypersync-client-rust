#![deny(missing_docs)]
//! Hypersync client library for interacting with hypersync server.
use std::{num::NonZeroU64, sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use hypersync_net_types::{ArchiveHeight, ChainId, Query};
use polars_arrow::{array::Array, record_batch::RecordBatchT as Chunk};
use reqwest::Method;

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
