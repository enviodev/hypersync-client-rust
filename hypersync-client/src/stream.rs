use std::{
    cmp,
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, Context, Result};
use hypersync_net_types::Query;
use polars_arrow::{
    array::{Array, BinaryArray, BooleanArray, UInt64Array, UInt8Array, Utf8Array},
    datatypes::ArrowDataType,
    record_batch::RecordBatch,
};
use tokio::task::JoinSet;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    config::HexOutput,
    rayon_async,
    types::ArrowResponse,
    util::{decode_logs_batch, hex_encode_batch, hex_encode_prefixed},
    ArrowBatch, ArrowResponseData, StreamConfig,
};

pub struct ArrowStream {
    // Used to cancel the stream
    cancel_token: CancellationToken,
    // Join handle for waiting for the stream to finish
    handle: JoinHandle<()>,
    // Receiver for the stream
    rx: mpsc::Receiver<Result<ArrowResponse>>,

    /// The highest block number that any currently in-flight request is processing.
    /// Only meaningful for non-reverse streams.
    pub end_block: Arc<AtomicU64>,
}

/// A handle which holds the end_block and can be awaited to drain all leftover items.
pub struct StreamDrainer {
    /// The highest block that any in-flight request was processing when drain started
    pub stream_end_block: Option<u64>,

    cancel_token: CancellationToken,
    rx: mpsc::Receiver<Result<ArrowResponse>>,
}

impl StreamDrainer {
    /// Actually drain the remaining items in the channel, returning them in a vector.
    pub async fn drain(mut self) -> Vec<Result<ArrowResponse>> {
        let mut drained = Vec::new();

        // Then collect all leftover items
        while let Some(item) = self.rx.recv().await {
            drained.push(item);
        }

        drained
    }
}

impl ArrowStream {
    /// Returns the highest end_block (if known) and a future handle to drain leftover data.
    pub fn drain_and_stop(self) -> StreamDrainer {
        // Signal all tasks to stop
        self.cancel_token.cancel();

        StreamDrainer {
            stream_end_block: if self.end_block.load(Ordering::SeqCst) > 0 {
                Some(self.end_block.load(Ordering::SeqCst))
            } else {
                None
            },
            cancel_token: self.cancel_token,
            rx: self.rx,
        }
    }
}

impl ArrowStream {
    pub async fn recv(&mut self) -> Option<Result<ArrowResponse>> {
        self.rx.recv().await
    }
}

pub async fn stream_arrow(
    client: Arc<crate::Client>,
    query: Query,
    config: StreamConfig,
) -> Result<ArrowStream> {
    let concurrency = config.concurrency.unwrap_or(10);
    let batch_size = config.batch_size.unwrap_or(1000);
    let max_batch_size = config.max_batch_size.unwrap_or(200_000);
    let min_batch_size = config.min_batch_size.unwrap_or(200);
    let response_size_ceiling = config.response_bytes_ceiling.unwrap_or(500_000);
    let response_size_floor = config.response_bytes_floor.unwrap_or(250_000);
    let reverse = config.reverse.unwrap_or_default();

    let step = Arc::new(AtomicU64::new(batch_size));

    let (tx, rx) = mpsc::channel(concurrency * 2);

    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    let to_block = match query.to_block {
        Some(to_block) => to_block,
        None => client.get_height().await.context("get height")?,
    };

    let end_block = Arc::new(AtomicU64::new(0));
    let end_block_clone = end_block.clone();

    let handle = tokio::spawn(async move {
        if cancel_token.is_cancelled() {
            return;
        }

        let mut query = query;

        if !reverse {
            let initial_res = client.get_arrow(&query).await.context("get initial data");
            match initial_res {
                Ok(res) => {
                    let res = match map_responses(config.clone(), vec![res], reverse).await {
                        Ok(mut resps) => resps.remove(0),
                        Err(e) => {
                            tx.send(Err(e)).await.ok();
                            return;
                        }
                    };

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
        }

        let range_iter = BlockRangeIterator::new(query.from_block, to_block, step.clone(), reverse);

        let mut futs = range_iter
            .enumerate()
            .map(move |(req_idx, (start, end, generation))| {
                let mut query = query.clone();
                query.from_block = start;
                query.to_block = Some(end);

                if !reverse {
                    end_block.fetch_max(end, Ordering::SeqCst);
                }

                let client = client.clone();
                async move { (generation, req_idx, run_query_to_end(client, query).await) }
            })
            .peekable();

        // we use unordered parallelization here so need to order the responses later.
        // Using unordered parallelization gives a big boost in performance.
        let (res_tx, mut res_rx) = mpsc::channel(concurrency * 2);

        tokio::spawn(async move {
            let mut set = JoinSet::new();
            let mut queue = BTreeMap::new();
            let mut next_req_idx = 0;

            while futs.peek().is_some() {
                if cancel_token.is_cancelled() {
                    break;
                }

                while let Some(res) = set.try_join_next() {
                    let (generation, req_idx, resps) = res.unwrap();
                    queue.insert(req_idx, (generation, resps));
                }
                while set.len() >= concurrency {
                    let (generation, req_idx, resps) = set.join_next().await.unwrap().unwrap();
                    queue.insert(req_idx, (generation, resps));
                }
                if queue.len() < concurrency * 2 {
                    futs.by_ref().take(concurrency - set.len()).for_each(|fut| {
                        set.spawn(fut);
                    });
                } else {
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
                while let Some(resps) = queue.remove(&next_req_idx) {
                    if res_tx.send(resps).await.is_err() {
                        return;
                    }
                    next_req_idx += 1;
                }
            }

            while let Some(res) = set.join_next().await {
                let (generation, req_idx, resps) = res.unwrap();
                queue.insert(req_idx, (generation, resps));
            }
            while let Some(resps) = queue.remove(&next_req_idx) {
                if res_tx.send(resps).await.is_err() {
                    return;
                }
                next_req_idx += 1;
            }
        });

        let mut num_blocks = 0;
        let mut num_transactions = 0;
        let mut num_logs = 0;
        let mut num_traces = 0;

        // Generation is used so if we change batch_size we only want to change it again
        // based on the new batch size we just set.
        // If we don't check generations then we might apply same change to batch size multiple times.
        let mut next_generation = 0;
        while let Some((generation, resps)) = res_rx.recv().await {
            let resps = match resps {
                Ok(resps) => resps,
                Err(e) => {
                    tx.send(Err(e)).await.ok();
                    return;
                }
            };

            let (resps, resps_size) = resps;
            let resps = match map_responses(config.clone(), resps, reverse).await {
                Ok(resps) => resps,
                Err(e) => {
                    tx.send(Err(e)).await.ok();
                    return;
                }
            };

            if generation == next_generation {
                next_generation += 1;
                if resps_size > response_size_ceiling {
                    let ratio = response_size_ceiling as f64 / resps_size as f64;

                    step.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                        // extract batch_size value
                        let x = x as u32;

                        let batch_size = cmp::max((x as f64 * ratio) as u64, min_batch_size);
                        let step = batch_size | u64::from(next_generation) << 32;
                        Some(step)
                    })
                    .ok();
                } else if resps_size < response_size_floor {
                    let ratio = response_size_floor as f64 / resps_size as f64;

                    step.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                        let x = x as u32;

                        let batch_size = cmp::min((x as f64 * ratio) as u64, max_batch_size);
                        let step = batch_size | u64::from(next_generation) << 32;
                        Some(step)
                    })
                    .ok();
                }
            }

            for resp in resps {
                num_blocks += count_rows(&resp.data.blocks);
                num_transactions += count_rows(&resp.data.transactions);
                num_logs += count_rows(&resp.data.logs);
                num_traces += count_rows(&resp.data.traces);

                if tx.send(Ok(resp)).await.is_err() {
                    return;
                }
            }

            if check_entity_limit(num_blocks, config.max_num_blocks)
                || check_entity_limit(num_transactions, config.max_num_transactions)
                || check_entity_limit(num_logs, config.max_num_logs)
                || check_entity_limit(num_traces, config.max_num_traces)
            {
                return;
            }
        }
    });

    Ok(ArrowStream {
        cancel_token: cancel_token_clone,
        handle,
        rx,
        end_block: end_block_clone,
    })
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

pub struct BlockRangeIterator {
    offset: u64,
    end: u64,
    step: Arc<AtomicU64>,
    reverse: bool,
}

impl BlockRangeIterator {
    pub fn new(start: u64, end: u64, step: Arc<AtomicU64>, reverse: bool) -> Self {
        if reverse {
            Self {
                offset: end,
                end: start,
                step,
                reverse,
            }
        } else {
            Self {
                offset: start,
                end,
                step,
                reverse,
            }
        }
    }
}

impl Iterator for BlockRangeIterator {
    type Item = (u64, u64, u32);

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.end {
            return None;
        }
        let start = self.offset;

        let step = self.step.load(Ordering::SeqCst);

        // we extract two u32 values from u64
        // unsafe casts are intentional
        let generation = (step >> 32) as u32;
        let batch_size = step as u32;

        if self.reverse {
            self.offset = cmp::max(self.offset.saturating_sub(u64::from(batch_size)), self.end);
            Some((self.offset, start, generation))
        } else {
            self.offset = cmp::min(self.offset + u64::from(batch_size), self.end);
            Some((start, self.offset, generation))
        }
    }
}
