use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::{cmp, collections::BTreeSet, error::Error, time::Duration};

use anyhow::{anyhow, Context, Result};
use arrayvec::ArrayVec;
use format::{Address, LogArgument};
use futures::StreamExt;
use hypersync_net_types::{
    hypersync_net_types_capnp, ArchiveHeight, FieldSelection, LogSelection, Query, RollbackGuard,
    TransactionSelection,
};
use polars_arrow::{array::Array, record_batch::RecordBatch as Chunk};
use reqwest::Method;
mod block_iterator;
mod column_mapping;
pub mod config;
mod decode;
mod parquet_out;
mod rayon_async;
mod transport_format;
mod types;

use crate::block_iterator::{
    BlockIterator, BLOCK_COEFFICIENT, LOG_COEFFICIENT, TARGET_SIZE,
    TRACE_COEFFICIENT, TX_COEFFICIENT,
};
pub use column_mapping::{ColumnMapping, DataType};
pub use config::Config;
pub use decode::Decoder;
pub use hypersync_format as format;
pub use hypersync_net_types as net_types;
pub use hypersync_schema as schema;
pub use parquet_out::map_batch_to_binary_view;
use tokio::sync::mpsc;
pub use transport_format::{ArrowIpc, TransportFormat};
pub use types::{ArrowBatch, ParquetConfig, QueryResponse, QueryResponseData, StreamConfig};

pub type ArrowChunk = Chunk<Box<dyn Array>>;

#[derive(Clone)]
pub struct Client {
    http_client: reqwest::Client,
    cfg: Config,
}

impl Client {
    /// Create a new client with given config
    pub fn new(cfg: Config) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .no_gzip()
            .timeout(Duration::from_millis(cfg.http_req_timeout_millis.get()))
            .build()
            .unwrap();

        Ok(Self { http_client, cfg })
    }

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
        let step = Arc::new(AtomicU64::new(config.batch_size));
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

            let it = BlockIterator::build(query.from_block, to_block, step.clone());
            let futs = it.map(move |(start, end)| {
                let mut query = query.clone();
                query.from_block = start;
                query.to_block = Some(end);
                Self::run_query_to_end(client.clone(), query, config.retry)
            });

            let mut stream = futures::stream::iter(futs).buffered(config.concurrency);

            // responces are unordered so old request could affect step size for fresher requests
            while let Some(resps) = stream.next().await {
                let resps = match resps {
                    Ok(resps) => resps,
                    Err(e) => {
                        tx.send(Err(e)).await.ok();
                        return;
                    }
                };
                for resp in resps {
                    let transaction: u64 = resp.data.transactions.iter().map(|x| x.chunk.len() as u64).sum();
                    let logs: u64 = resp.data.logs.iter().map(|x| x.chunk.len() as u64).sum();
                    let traces: u64 = resp.data.traces.iter().map(|x| x.chunk.len() as u64).sum();
                    let blocks: u64 = resp.data.blocks.iter().map(|x| x.chunk.len() as u64).sum();
                    let sum = transaction * TX_COEFFICIENT as u64
                        + logs * LOG_COEFFICIENT as u64
                        + traces * TRACE_COEFFICIENT as u64
                        + blocks * BLOCK_COEFFICIENT as u64;
                    if sum < TARGET_SIZE {
                        _ = step.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| Some(cmp::min(x * 2, 200_000)));
                    } else {
                        _ = step.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| Some(cmp::max(x / 2, 100)));
                    }
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

        // we could return len of bytes in here for statistics
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

    fn parse_query_response<Format: TransportFormat>(bytes: &[u8]) -> Result<QueryResponse> {
        let mut opts = capnp::message::ReaderOptions::new();
        opts.nesting_limit(i32::MAX).traversal_limit_in_words(None);
        let message_reader =
            capnp::serialize_packed::read_message(bytes, opts).context("create message reader")?;

        let query_response = message_reader
            .get_root::<hypersync_net_types_capnp::query_response::Reader>()
            .context("get root")?;

        let archive_height = match query_response.get_archive_height() {
            -1 => None,
            h => Some(
                h.try_into()
                    .context("invalid archive height returned from server")?,
            ),
        };

        let rollback_guard = if query_response.has_rollback_guard() {
            let rg = query_response
                .get_rollback_guard()
                .context("get rollback guard")?;

            Some(RollbackGuard {
                block_number: rg.get_block_number(),
                timestamp: rg.get_timestamp(),
                hash: rg
                    .get_hash()
                    .context("get rollback guard hash")?
                    .try_into()
                    .context("hash size")?,
                first_block_number: rg.get_first_block_number(),
                first_parent_hash: rg
                    .get_first_parent_hash()
                    .context("get rollback guard first parent hash")?
                    .try_into()
                    .context("hash size")?,
            })
        } else {
            None
        };

        let data = query_response.get_data().context("read data")?;

        let blocks = Format::read_chunks(data.get_blocks().context("get data")?)
            .context("parse block data")?;
        let transactions = Format::read_chunks(data.get_transactions().context("get data")?)
            .context("parse tx data")?;
        let logs =
            Format::read_chunks(data.get_logs().context("get data")?).context("parse log data")?;
        let traces = if data.has_traces() {
            Format::read_chunks(data.get_traces().context("get data")?)
                .context("parse traces data")?
        } else {
            Vec::new()
        };

        Ok(QueryResponse {
            archive_height,
            next_block: query_response.get_next_block(),
            total_execution_time: query_response.get_total_execution_time(),
            data: QueryResponseData {
                blocks,
                transactions,
                logs,
                traces,
            },
            rollback_guard,
        })
    }

    /// Returns a query for all Blocks and Transactions within the block range (from_block, to_block]
    /// If to_block is None then query runs to the head of the chain.
    /// Note: this is only for quickstart purposes.  For the best performance, create a custom query
    /// that only includes the fields you'll use in `field_selection`.
    pub fn preset_query_blocks_and_transactions(from_block: u64, to_block: Option<u64>) -> Query {
        let all_block_fields: BTreeSet<String> = hypersync_schema::block_header()
            .fields
            .iter()
            .map(|x| x.name.clone())
            .collect();

        let all_tx_fields: BTreeSet<String> = hypersync_schema::transaction()
            .fields
            .iter()
            .map(|x| x.name.clone())
            .collect();

        Query {
            from_block,
            to_block,
            include_all_blocks: true,
            transactions: vec![TransactionSelection::default()],
            field_selection: FieldSelection {
                block: all_block_fields,
                transaction: all_tx_fields,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Returns a query object for all Blocks and hashes of the Transactions within the block range
    /// (from_block, to_block].  Also returns the block_hash and block_number fields on each Transaction
    /// so it can be mapped to a block.  If to_block is None then query runs to the head of the chain.
    /// Note: this is only for quickstart purposes.  For the best performance, create a custom query
    /// that only includes the fields you'll use in `field_selection`.
    pub fn preset_query_blocks_and_transaction_hashes(
        from_block: u64,
        to_block: Option<u64>,
    ) -> Query {
        let mut tx_field_selection = BTreeSet::new();
        tx_field_selection.insert("block_hash".to_owned());
        tx_field_selection.insert("block_number".to_owned());
        tx_field_selection.insert("hash".to_owned());

        let all_block_fields: BTreeSet<String> = hypersync_schema::block_header()
            .fields
            .iter()
            .map(|x| x.name.clone())
            .collect();

        Query {
            from_block,
            to_block,
            include_all_blocks: true,
            transactions: vec![TransactionSelection::default()],
            field_selection: FieldSelection {
                block: all_block_fields,
                transaction: tx_field_selection,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Returns a query object for all Logs within the block range (from_block, to_block] from
    /// the given address.  If to_block is None then query runs to the head of the chain.
    /// Note: this is only for quickstart purposes.  For the best performance, create a custom query
    /// that only includes the fields you'll use in `field_selection`.
    pub fn preset_query_logs<A>(from_block: u64, to_block: Option<u64>, address: A) -> Result<Query>
    where
        A: TryInto<Address>,
        <A as TryInto<Address>>::Error: Error + Send + Sync + 'static,
    {
        let address = address.try_into().context("convert Address type")?;

        let all_log_fields: BTreeSet<String> = hypersync_schema::log()
            .fields
            .iter()
            .map(|x| x.name.clone())
            .collect();

        Ok(Query {
            from_block,
            to_block,
            logs: vec![LogSelection {
                address: vec![address],
                ..Default::default()
            }],
            field_selection: FieldSelection {
                log: all_log_fields,
                ..Default::default()
            },
            ..Default::default()
        })
    }

    /// Returns a query for all Logs within the block range (from_block, to_block] from the
    /// given address with a matching topic0 event signature.  Topic0 is the keccak256 hash
    /// of the event signature.  If to_block is None then query runs to the head of the chain.
    /// Note: this is only for quickstart purposes.  For the best performance, create a custom query
    /// that only includes the fields you'll use in `field_selection`.
    pub fn preset_query_logs_of_event<A, T>(
        from_block: u64,
        to_block: Option<u64>,
        topic0: T,
        address: A,
    ) -> Result<Query>
    where
        A: TryInto<Address>,
        <A as TryInto<Address>>::Error: Error + Send + Sync + 'static,
        T: TryInto<LogArgument>,
        <T as TryInto<LogArgument>>::Error: Error + Send + Sync + 'static,
    {
        let topic0 = topic0.try_into().context("convert Topic0 type")?;
        let mut topics = ArrayVec::<Vec<LogArgument>, 4>::new();
        topics.insert(0, vec![topic0]);

        let address = address.try_into().context("convert Address type")?;

        let all_log_fields: BTreeSet<String> = hypersync_schema::log()
            .fields
            .iter()
            .map(|x| x.name.clone())
            .collect();

        Ok(Query {
            from_block,
            to_block,
            logs: vec![LogSelection {
                address: vec![address],
                topics,
            }],
            field_selection: FieldSelection {
                log: all_log_fields,
                ..Default::default()
            },
            ..Default::default()
        })
    }

    /// Returns a query object for all transactions within the block range (from_block, to_block].
    /// If to_block is None then query runs to the head of the chain.
    /// Note: this is only for quickstart purposes.  For the best performance, create a custom query
    /// that only includes the fields you'll use in `field_selection`.
    pub fn preset_query_transactions(from_block: u64, to_block: Option<u64>) -> Query {
        let all_txn_fields: BTreeSet<String> = hypersync_schema::transaction()
            .fields
            .iter()
            .map(|x| x.name.clone())
            .collect();

        Query {
            from_block,
            to_block,
            transactions: vec![TransactionSelection::default()],
            field_selection: FieldSelection {
                transaction: all_txn_fields,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Returns a query object for all transactions from an address within the block range
    /// (from_block, to_block].  If to_block is None then query runs to the head of the chain.
    /// Note: this is only for quickstart purposes.  For the best performance, create a custom query
    /// that only includes the fields you'll use in `field_selection`.
    pub fn preset_query_transactions_from_address<A>(
        from_block: u64,
        to_block: Option<u64>,
        address: A,
    ) -> Result<Query>
    where
        A: TryInto<Address>,
        <A as TryInto<Address>>::Error: Error + Send + Sync + 'static,
    {
        let address = address.try_into().context("convert Address type")?;

        let all_txn_fields: BTreeSet<String> = hypersync_schema::transaction()
            .fields
            .iter()
            .map(|x| x.name.clone())
            .collect();

        Ok(Query {
            from_block,
            to_block,
            transactions: vec![TransactionSelection {
                from: vec![address],
                ..Default::default()
            }],
            field_selection: FieldSelection {
                transaction: all_txn_fields,
                ..Default::default()
            },
            ..Default::default()
        })
    }
}
