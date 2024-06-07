use std::{
    cmp,
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use hypersync_net_types::Query;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::{
    config::HexOutput,
    rayon_async,
    types::ArrowResponse,
    util::{decode_logs_batch, hex_encode_batch, hex_encode_prefixed},
    ArrowBatch, ArrowResponseData, StreamConfig,
};

pub async fn stream_arrow(
    client: Arc<crate::Client>,
    query: Query,
    config: StreamConfig,
) -> Result<mpsc::Receiver<Result<ArrowResponse>>> {
    let concurrency = config.concurrency.unwrap_or(10);
    let batch_size = config.batch_size.unwrap_or(1000);
    let max_batch_size = config.max_batch_size.unwrap_or(200_000);
    let min_batch_size = config.min_batch_size.unwrap_or(200);
    let response_size_ceiling = config.response_bytes_ceiling.unwrap_or(500_000);
    let response_size_floor = config.response_bytes_floor.unwrap_or(250_000);

    let step = Arc::new(AtomicU64::new(batch_size));

    let (tx, rx) = mpsc::channel(concurrency * 2);

    let to_block = match query.to_block {
        Some(to_block) => to_block,
        None => client.get_height().await.context("get height")?,
    };

    tokio::spawn(async move {
        let mut query = query;
        let initial_res = client.get_arrow(&query).await.context("get initial data");
        match initial_res {
            Ok(res) => {
                let res = match map_responses(config.clone(), vec![res]).await {
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

        let range_iter = BlockRangeIterator::new(query.from_block, to_block, step.clone());

        let mut futs = range_iter
            .enumerate()
            .map(move |(req_idx, (start, end, generation))| {
                let mut query = query.clone();
                query.from_block = start;
                query.to_block = Some(end);
                let client = client.clone();
                async move { (generation, req_idx, run_query_to_end(client, query).await) }
            }).peekable();

        // we use unordered parallelization here so need to order the responses later.
        // Using unordered parallelization gives a big boost in performance.
        let (res_tx, mut res_rx) = mpsc::channel(concurrency * 2);

        tokio::spawn( async move {
            let mut set = JoinSet::new();
            let mut queue = BTreeMap::new();
            let mut next_req_idx = 0;

            while futs.peek().is_some() {
                while let Some(res) = set.try_join_next() {
                    let (generation, req_idx, resps) = res.unwrap();
                    queue.insert(req_idx, (generation, resps));
                }
                while set.len() >= concurrency {
                    let (generation, req_idx, resps) = set.join_next().await.unwrap().unwrap();
                    queue.insert(req_idx, (generation, resps));
                }
                futs.by_ref().take(concurrency- set.len()).for_each(|fut| {
                    set.spawn(fut);
                });
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
            let resps = match map_responses(config.clone(), resps).await {
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

    Ok(rx)
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
    responses: Vec<ArrowResponse>,
) -> Result<Vec<ArrowResponse>> {
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
) -> Result<ArrowBatch> {
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
}

impl BlockRangeIterator {
    pub fn new(offset: u64, end: u64, step: Arc<AtomicU64>) -> Self {
        Self { offset, end, step }
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

        self.offset = cmp::min(self.offset + u64::from(batch_size), self.end);
        Some((start, self.offset, generation))
    }
}
