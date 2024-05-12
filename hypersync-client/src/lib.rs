use std::{num::NonZeroU64, sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use hypersync_net_types::{ArchiveHeight, Query};
use polars_arrow::{array::Array, record_batch::RecordBatch as Chunk};
use reqwest::Method;

mod column_mapping;
mod config;
mod decode;
mod parquet_out;
mod parse_response;
pub mod preset_query;
mod rayon_async;
mod stream;
mod types;
mod util;

pub use hypersync_format as format;
pub use hypersync_net_types as net_types;
pub use hypersync_schema as schema;

pub use column_mapping::{ColumnMapping, DataType};
pub use config::{ClientConfig, StreamConfig};
pub use decode::Decoder;
pub use parse_response::parse_query_response;
use tokio::sync::mpsc;
use types::ArrowResponse;
pub use types::{ArrowBatch, ArrowResponseData, QueryResponse};
use url::Url;

pub type ArrowChunk = Chunk<Box<dyn Array>>;

#[derive(Clone)]
pub struct Client {
    http_client: reqwest::Client,
    url: Url,
    bearer_token: Option<String>,
    max_num_retries: usize,
    retry_backoff_ms: u64,
    retry_base_ms: u64,
    retry_ceiling_ms: u64,
}

impl Client {
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
                .unwrap_or("https://eth.hypersync.xyz".parse().unwrap()),
            bearer_token: cfg.bearer_token,
            max_num_retries: cfg.max_num_retries.unwrap_or(12),
            retry_backoff_ms: cfg.retry_backoff_ms.unwrap_or(500),
            retry_base_ms: cfg.retry_base_ms.unwrap_or(200),
            retry_ceiling_ms: cfg.retry_ceiling_ms.unwrap_or(5_000),
        })
    }

    // pub async fn collect(&self, query: Query, config: StreamConfig) -> Result<()> {
    //     todo!()
    // }

    // pub async fn collect_events(&self, query: Query, config: StreamConfig) -> Result<()> {
    //     todo!()
    // }

    pub async fn collect_arrow(
        self: Arc<Self>,
        query: Query,
        config: StreamConfig,
    ) -> Result<Vec<ArrowResponse>> {
        let mut recv = stream::stream_arrow(self, query, config)
            .await
            .context("start stream")?;

        let mut resps = Vec::new();

        while let Some(res) = recv.recv().await {
            resps.push(res.context("get response")?);
        }

        Ok(resps)
    }

    pub async fn collect_parquet(
        &self,
        path: &str,
        query: Query,
        config: StreamConfig,
    ) -> Result<()> {
        todo!()
    }

    async fn get_height_impl(&self) -> Result<u64> {
        let mut url = self.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        segments.push("height");
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

        let height: ArchiveHeight = res.json().await.context("read response body json")?;

        Ok(height.height.unwrap_or(0))
    }

    pub async fn get_height(&self) -> Result<u64> {
        let mut base = self.retry_base_ms;

        let mut err = anyhow!("");

        for _ in 0..self.max_num_retries {
            match self.get_height_impl().await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    log::error!(
                        "failed to get height from server, retrying... The error was: {:?}",
                        e
                    );
                    err = err.context(e);
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

    // pub async fn get(&self, query: &Query) -> Result<QueryResponse> {
    //     todo!()
    // }

    // pub async fn get_events(&self, query: &Query) -> Result<QueryResponse> {
    //     todo!()
    // }

    async fn get_arrow_impl(&self, query: &Query) -> Result<ArrowResponse> {
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

        Ok(res)
    }

    pub async fn get_arrow(&self, query: &Query) -> Result<ArrowResponse> {
        let mut base = self.retry_base_ms;

        let mut err = anyhow!("");

        for _ in 0..self.max_num_retries {
            match self.get_arrow_impl(query).await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    log::error!(
                        "failed to get height from server, retrying... The error was: {:?}",
                        e
                    );
                    err = err.context(e);
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

    // pub async fn stream(&self, query: Query, config: StreamConfig) -> Result<Stream> {
    //     todo!()
    // }

    // pub async fn stream_events(&self, query: Query, config: StreamConfig) -> Result<EventStream> {
    //     todo!()
    // }

    pub async fn stream_arrow(
        self: Arc<Self>,
        query: Query,
        config: StreamConfig,
    ) -> Result<mpsc::Receiver<Result<ArrowResponse>>> {
        stream::stream_arrow(self, query, config).await
    }
}
