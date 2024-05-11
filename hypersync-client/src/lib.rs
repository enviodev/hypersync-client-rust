use std::{cmp, time::Duration};

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use hypersync_net_types::{ArchiveHeight, Query};
use polars_arrow::{array::Array, record_batch::RecordBatch as Chunk};
use reqwest::Method;
use tokio::sync::mpsc;

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
pub use types::{ArrowBatch, QueryResponse, QueryResponseData};

pub type ArrowChunk = Chunk<Box<dyn Array>>;

#[derive(Clone)]
pub struct Client {
    http_client: reqwest::Client,
    cfg: ClientConfig,
}

impl Client {
    /// Create a new client with given config
    pub fn new(cfg: ClientConfig) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .no_gzip()
            .timeout(Duration::from_millis(cfg.http_req_timeout_millis.get()))
            .build()
            .unwrap();

        Ok(Self { http_client, cfg })
    }

    // pub async fn collect(&self, query: Query, config: CollectConfig) -> Result<()> {
    //     todo!()
    // }

    // pub async fn collect_events(&self, query: Query, config: CollectConfig) -> Result<()> {
    //     todo!()
    // }

    // pub async fn collect_arrow(&self, query: Query, config: CollectConfig) -> Result<()> {
    //     todo!()
    // }

    // pub async fn collect_parquet(&self, path: &str, query: Query, config: CollectConfig) -> Result<()> {
    //     todo!()
    // }

    // pub async fn get_height(&self, config: &RequestConfig) -> Result<u64> {
    //     todo!()
    // }

    // pub async fn get(&self, query: &Query, config: &RequestConfig) -> Result<Response> {
    //     todo!()
    // }

    // pub async fn get_events(&self, query: &Query, config: &RequestConfig) -> Result<EventResponse> {
    //     todo!()
    // }

    // pub async fn get_arrow(&self, query: &Query, config: &RequestConfig) -> Result<ArrowResponse> {
    //     todo!()
    // }

    // pub async fn stream(&self, query: Query, config: StreamConfig) -> Result<Stream> {
    //     todo!()
    // }

    // pub async fn stream_events(&self, query: Query, config: StreamConfig) -> Result<EventStream> {
    //     todo!()
    // }

    // pub async fn stream_arrow(&self, query: Query, config: StreamConfig) -> Result<ArrowStream> {
    //     todo!()
    // }

    /// Create a parquet file by executing a query.
    ///
    /// Path should point to a folder that will contain the parquet files in the end.
    pub async fn create_parquet_folder(&self, query: Query, config: ParquetConfig) -> Result<()> {
        parquet_out::create_parquet_folder(self, query, config).await
    }

    /// Get the height of the source hypersync instance
    pub async fn get_height(&self) -> Result<u64> {
        let mut url = self.cfg.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        segments.push("height");
        std::mem::drop(segments);
        let mut req = self.http_client.request(Method::GET, url);

        if let Some(bearer_token) = &self.cfg.bearer_token {
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

    /// Get the height of the source hypersync instance
    /// Internally calls get_height.
    /// On an error from the source hypersync instance, sleeps for
    /// 1 second (increasing by 1 each failure up to max of 5 seconds)
    /// and retries query until success.
    pub async fn get_height_with_retry(&self) -> Result<u64> {
        let mut base = 1;

        loop {
            match self.get_height().await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    log::error!("failed to send request to hypersync server: {:?}", e);
                }
            }

            let secs = Duration::from_secs(base);
            let millis = Duration::from_millis(fastrange_rs::fastrange_64(rand::random(), 1000));

            tokio::time::sleep(secs + millis).await;

            base = std::cmp::min(base + 1, 5);
        }
    }

    pub async fn stream<Format: TransportFormat>(
        &self,
        query: Query,
        config: StreamConfig,
    ) -> Result<mpsc::Receiver<Result<QueryResponse>>> {
        let (tx, rx) = mpsc::channel(config.concurrency);

        let to_block = match query.to_block {
            Some(to_block) => to_block,
            None => {
                if config.retry {
                    self.get_height_with_retry().await.context("get height")?
                } else {
                    self.get_height().await.context("get height")?
                }
            }
        };

        let client = self.clone();
        let step = usize::try_from(config.batch_size).unwrap();
        tokio::spawn(async move {
            let mut query = query;
            let initial_res = if config.retry {
                client
                    .send_with_retry::<crate::ArrowIpc>(&query)
                    .await
                    .context("run initial query")
            } else {
                client
                    .send::<crate::ArrowIpc>(&query)
                    .await
                    .context("run initial query")
            };
            match initial_res {
                Ok(res) => {
                    query.from_block = res.next_block;
                    if tx.send(Ok(res)).await.is_err() {
                        return;
                    }
                }
                Err(e) => {
                    tx.send(Err(e)).await.ok();
                    return;
                }
            }

            let futs = (query.from_block..to_block)
                .step_by(step)
                .map(move |start| {
                    let end = cmp::min(start + config.batch_size, to_block);
                    let mut query = query.clone();
                    query.from_block = start;
                    query.to_block = Some(end);

                    Self::run_query_to_end(client.clone(), query, config.retry)
                });

            let mut stream = futures::stream::iter(futs).buffered(config.concurrency);

            while let Some(resps) = stream.next().await {
                let resps = match resps {
                    Ok(resps) => resps,
                    Err(e) => {
                        tx.send(Err(e)).await.ok();
                        return;
                    }
                };

                for resp in resps {
                    if tx.send(Ok(resp)).await.is_err() {
                        return;
                    }
                }
            }
        });

        Ok(rx)
    }

    async fn run_query_to_end(self, query: Query, retry: bool) -> Result<Vec<QueryResponse>> {
        let mut resps = Vec::new();

        let to_block = query.to_block.unwrap();

        let mut query = query;

        loop {
            let resp = if retry {
                self.send_with_retry::<crate::ArrowIpc>(&query)
                    .await
                    .context("send query")?
            } else {
                self.send::<crate::ArrowIpc>(&query)
                    .await
                    .context("send query")?
            };

            let next_block = resp.next_block;

            resps.push(resp);

            if next_block >= to_block {
                break;
            } else {
                query.from_block = next_block;
            }
        }

        Ok(resps)
    }

    /// Send a query request to the source hypersync instance.
    ///
    /// Returns a query response which contains block, tx and log data.
    /// Format can be ArrowIpc or Parquet.
    pub async fn send<Format: TransportFormat>(&self, query: &Query) -> Result<QueryResponse> {
        let mut url = self.cfg.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        segments.push("query");
        segments.push(Format::path());
        std::mem::drop(segments);
        let mut req = self.http_client.request(Method::POST, url);

        if let Some(bearer_token) = &self.cfg.bearer_token {
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
            Self::parse_query_response::<Format>(&bytes).context("parse query response")
        })?;

        Ok(res)
    }

    /// Send a query request to the source hypersync instance.
    /// Internally calls send.
    /// On an error from the source hypersync instance, sleeps for
    /// 1 second (increasing by 1 each failure up to max of 5 seconds)
    /// and retries query until success.
    ///
    /// Returns a query response which contains block, tx and log data.
    /// Format can be ArrowIpc or Parquet.
    pub async fn send_with_retry<Format: TransportFormat>(
        &self,
        query: &Query,
    ) -> Result<QueryResponse> {
        let mut base = 1;

        loop {
            match self.send::<Format>(query).await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    log::error!("failed to send request to hypersync server: {:?}", e);
                }
            }

            let secs = Duration::from_secs(base);
            let millis = Duration::from_millis(fastrange_rs::fastrange_64(rand::random(), 1000));

            tokio::time::sleep(secs + millis).await;

            base = std::cmp::min(base + 1, 5);
        }
    }
}
