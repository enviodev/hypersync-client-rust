use std::{
    cmp,
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, Context, Result};
use futures::{Stream, StreamExt};
use hypersync_net_types::Query;
use pin_project::pin_project;
use polars_arrow::{
    array::{Array, BinaryArray, BooleanArray, UInt64Array, UInt8Array, Utf8Array},
    datatypes::ArrowDataType,
    record_batch::RecordBatch,
};
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::{
    config::HexOutput,
    rayon_async,
    types::ArrowResponse,
    util::{decode_logs_batch, hex_encode_batch, hex_encode_prefixed},
    ArrowBatch, ArrowResponseData, StreamConfig,
};

#[pin_project]
pub struct ArrowResponseStream {
    #[pin]
    client: Arc<crate::Client>,
    query: Query,
    config: StreamConfig,
    current_block: u64,
    to_block: u64,
    step: Arc<AtomicU64>,
    reverse: bool,
    pending_responses: VecDeque<ArrowResponse>,
    current_batch: Option<JoinHandle<Result<(Vec<ArrowResponse>, u64)>>>,
    next_generation: u32,
    stats: StreamStats,
}

struct StreamStats {
    num_blocks: usize,
    num_transactions: usize,
    num_logs: usize,
    num_traces: usize,
}

impl ArrowResponseStream {
    pub async fn new(
        client: Arc<crate::Client>,
        query: Query,
        config: StreamConfig,
    ) -> Result<Self> {
        let to_block = match query.to_block {
            Some(to_block) => to_block,
            None => client.get_height().await.context("get height")?,
        };

        let batch_size = config.batch_size.unwrap_or(1000);
        let reverse = config.reverse.unwrap_or_default();
        let step = Arc::new(AtomicU64::new(batch_size));

        let current_block = if reverse { to_block } else { query.from_block };

        Ok(Self {
            client,
            query,
            config,
            current_block,
            to_block,
            step,
            reverse,
            pending_responses: VecDeque::new(),
            current_batch: None,
            next_generation: 0,
            stats: StreamStats {
                num_blocks: 0,
                num_transactions: 0,
                num_logs: 0,
                num_traces: 0,
            },
        })
    }

    fn spawn_next_batch(&mut self) -> Result<()> {
        let step_val = self.step.load(Ordering::SeqCst);
        let batch_size = step_val as u32;

        let (start, end) = if self.reverse {
            let end = self.current_block;
            let start = end.saturating_sub(u64::from(batch_size));
            (start, end)
        } else {
            let start = self.current_block;
            let end = std::cmp::min(start + u64::from(batch_size), self.to_block);
            (start, end)
        };

        let mut query = self.query.clone();
        query.from_block = start;
        query.to_block = Some(end);

        let client = self.client.clone();
        self.current_batch = Some(tokio::spawn(async move {
            run_query_to_end(client, query).await
        }));

        Ok(())
    }

    async fn process_batch_response(
        &mut self,
        responses: Vec<ArrowResponse>,
        response_size: u64,
    ) -> Result<()> {
        // Update current block position
        if !self.pending_responses.is_empty() {
            let last_resp = self.pending_responses.back().unwrap();
            self.current_block = last_resp.next_block;
        }

        // Adjust batch size based on response size
        if response_size > self.config.response_bytes_ceiling.unwrap_or(500_000) {
            self.adjust_batch_size(response_size, true);
        } else if response_size < self.config.response_bytes_floor.unwrap_or(250_000) {
            self.adjust_batch_size(response_size, false);
        }

        // Map and store responses
        let responses = map_responses(self.config.clone(), responses, self.reverse).await?;
        self.pending_responses.extend(responses);

        Ok(())
    }

    fn adjust_batch_size(&self, response_size: u64, decrease: bool) {
        let ceiling = self.config.response_bytes_ceiling.unwrap_or(500_000) as f64;
        let floor = self.config.response_bytes_floor.unwrap_or(250_000) as f64;
        let target = if decrease { ceiling } else { floor };

        let ratio = target / response_size as f64;

        self.step
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                let batch_size = x as u32;
                let new_size = if decrease {
                    cmp::max(
                        (batch_size as f64 * ratio) as u64,
                        self.config.min_batch_size.unwrap_or(200),
                    )
                } else {
                    cmp::min(
                        (batch_size as f64 * ratio) as u64,
                        self.config.max_batch_size.unwrap_or(200_000),
                    )
                };

                Some(new_size | u64::from(self.next_generation) << 32)
            })
            .ok();
    }
}

impl Stream for ArrowResponseStream {
    type Item = Result<ArrowResponse>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // Return pending response if available
        if let Some(response) = this.pending_responses.pop_front() {
            // Update statistics
            this.stats.num_blocks += count_rows(&response.data.blocks);
            this.stats.num_transactions += count_rows(&response.data.transactions);
            this.stats.num_logs += count_rows(&response.data.logs);
            this.stats.num_traces += count_rows(&response.data.traces);

            // Check limits
            if check_entity_limit(*this.stats.num_blocks, this.config.max_num_blocks)
                || check_entity_limit(
                    *this.stats.num_transactions,
                    this.config.max_num_transactions,
                )
                || check_entity_limit(*this.stats.num_logs, this.config.max_num_logs)
                || check_entity_limit(*this.stats.num_traces, this.config.max_num_traces)
            {
                return Poll::Ready(None);
            }

            return Poll::Ready(Some(Ok(response)));
        }

        // Check if we're done
        if *this.current_block == *this.to_block {
            return Poll::Ready(None);
        }

        // Start new batch if needed
        if this.current_batch.is_none() {
            if let Err(e) = this.spawn_next_batch() {
                return Poll::Ready(Some(Err(e)));
            }
        }

        // Check current batch
        if let Some(batch) = this.current_batch.as_mut() {
            match batch.poll_unpin(cx) {
                Poll::Ready(Ok(Ok((responses, size)))) => {
                    *this.current_batch = None;
                    match this.process_batch_response(responses, size).await {
                        Ok(()) => this.poll_next(cx),
                        Err(e) => Poll::Ready(Some(Err(e))),
                    }
                }
                Poll::Ready(Ok(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e.into()))),
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Pending
        }
    }
}

pub async fn stream_arrow(
    client: Arc<crate::Client>,
    query: Query,
    config: StreamConfig,
) -> Result<impl Stream<Item = Result<ArrowResponse>>> {
    ArrowResponseStream::new(client, query, config).await
}

fn count_rows(batches: &[ArrowBatch]) -> usize {
    batches.iter().map(|b| b.chunk.len()).sum()
}

fn check_entity_limit(val: usize, limit: Option<usize>) -> bool {
    if let Some(limit) = limit {
        val >= limit
    } else {
        false
    }
}

async fn map_responses(
    cfg: StreamConfig,
    mut responses: Vec<ArrowResponse>,
    reverse: bool,
) -> Result<Vec<ArrowResponse>> {
    if reverse {
        responses.reverse();
        for resp in responses.iter_mut() {
            resp.data.blocks.reverse();
            resp.data.transactions.reverse();
            resp.data.logs.reverse();
            resp.data.traces.reverse();
        }
    }

    rayon_async::spawn(move || {
        responses
            .into_iter()
            .map(|resp| {
                Ok(ArrowResponse {
                    data: ArrowResponseData {
                        decoded_logs: match cfg.event_signature.as_ref() {
                            Some(sig) => resp
                                .data
                                .logs
                                .iter()
                                .map(|batch| {
                                    let batch =
                                        decode_logs_batch(sig, batch).context("decode logs")?;
                                    map_batch(
                                        cfg.column_mapping.as_ref().map(|cm| &cm.decoded_log),
                                        cfg.hex_output,
                                        batch,
                                        reverse,
                                    )
                                    .context("map batch")
                                })
                                .collect::<Result<Vec<_>>>()?,
                            None => Vec::new(),
                        },
                        blocks: resp
                            .data
                            .blocks
                            .into_iter()
                            .map(|batch| {
                                map_batch(
                                    cfg.column_mapping.as_ref().map(|cm| &cm.block),
                                    cfg.hex_output,
                                    batch,
                                    reverse,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?,
                        transactions: resp
                            .data
                            .transactions
                            .into_iter()
                            .map(|batch| {
                                map_batch(
                                    cfg.column_mapping.as_ref().map(|cm| &cm.transaction),
                                    cfg.hex_output,
                                    batch,
                                    reverse,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?,
                        logs: resp
                            .data
                            .logs
                            .into_iter()
                            .map(|batch| {
                                map_batch(
                                    cfg.column_mapping.as_ref().map(|cm| &cm.log),
                                    cfg.hex_output,
                                    batch,
                                    reverse,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?,
                        traces: resp
                            .data
                            .traces
                            .into_iter()
                            .map(|batch| {
                                map_batch(
                                    cfg.column_mapping.as_ref().map(|cm| &cm.trace),
                                    cfg.hex_output,
                                    batch,
                                    reverse,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?,
                    },
                    ..resp
                })
            })
            .collect()
    })
    .await
    .unwrap()
}

fn map_batch(
    column_mapping: Option<&BTreeMap<String, crate::DataType>>,
    hex_output: HexOutput,
    mut batch: ArrowBatch,
    reverse: bool,
) -> Result<ArrowBatch> {
    if reverse {
        let cols = batch
            .chunk
            .columns()
            .iter()
            .map(|a| reverse_array(a.as_ref()))
            .collect::<Result<Vec<_>>>()
            .context("reverse the arrays")?;
        let chunk = Arc::new(RecordBatch::new(cols));
        batch = ArrowBatch {
            chunk,
            schema: batch.schema,
        };
    }

    if let Some(map) = column_mapping {
        batch =
            crate::column_mapping::apply_to_batch(&batch, map).context("apply column mapping")?;
    }

    match hex_output {
        HexOutput::NonPrefixed => batch = hex_encode_batch(&batch, faster_hex::hex_string),
        HexOutput::Prefixed => batch = hex_encode_batch(&batch, hex_encode_prefixed),
        HexOutput::NoEncode => (),
    }

    Ok(batch)
}

fn reverse_array(array: &dyn Array) -> Result<Box<dyn Array>> {
    match array.data_type() {
        ArrowDataType::Binary => Ok(BinaryArray::<i32>::from_iter(
            array
                .as_any()
                .downcast_ref::<BinaryArray<i32>>()
                .unwrap()
                .iter()
                .rev(),
        )
        .boxed()),
        ArrowDataType::Utf8 => Ok(Utf8Array::<i32>::from_iter(
            array
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .unwrap()
                .iter()
                .rev(),
        )
        .boxed()),
        ArrowDataType::Boolean => Ok(BooleanArray::from_iter(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .rev(),
        )
        .boxed()),
        ArrowDataType::UInt64 => Ok(UInt64Array::from_iter(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .iter()
                .map(|opt| opt.copied())
                .rev(),
        )
        .boxed()),
        ArrowDataType::UInt8 => Ok(UInt8Array::from_iter(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .iter()
                .map(|opt| opt.copied())
                .rev(),
        )
        .boxed()),
        dt => Err(anyhow!(
            "reversing an array of datatype {:?} is not supported",
            dt
        )),
    }
}

async fn run_query_to_end(
    client: Arc<crate::Client>,
    query: Query,
) -> Result<(Vec<ArrowResponse>, u64)> {
    let mut resps = Vec::new();

    let to_block = query.to_block.unwrap();

    let mut size = 0;

    let mut query = query;

    loop {
        let (resp, resp_size) = client
            .get_arrow_with_size(&query)
            .await
            .context("get data")?;
        size += resp_size;

        let next_block = resp.next_block;

        resps.push(resp);

        if next_block >= to_block {
            break;
        } else {
            query.from_block = next_block;
        }
    }

    Ok((resps, size))
}
