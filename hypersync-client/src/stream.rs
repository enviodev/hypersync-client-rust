use std::{cmp, collections::BTreeMap, sync::Arc};

use anyhow::{Context, Result};
use futures::StreamExt;
use hypersync_net_types::Query;
use tokio::sync::mpsc;

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
    let batch_size = config.batch_size.unwrap_or(100_000);

    let (tx, rx) = mpsc::channel(concurrency);

    let to_block = match query.to_block {
        Some(to_block) => to_block,
        None => client.get_height().await.context("get height")?,
    };

    let step = usize::try_from(batch_size).unwrap();

    tokio::spawn(async move {
        let mut query = query;
        let initial_res = client.get_arrow(&query).await.context("get initial data");
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
                let end = cmp::min(start + batch_size, to_block);
                let mut query = query.clone();
                query.from_block = start;
                query.to_block = Some(end);

                run_query_to_end(client.clone(), query)
            });

        let mut stream = futures::stream::iter(futs).buffered(concurrency);

        let mut num_blocks = 0;
        let mut num_transactions = 0;
        let mut num_logs = 0;
        let mut num_traces = 0;

        while let Some(resps) = stream.next().await {
            let resps = match resps {
                Ok(resps) => resps,
                Err(e) => {
                    tx.send(Err(e)).await.ok();
                    return;
                }
            };

            let resps = match map_responses(config.clone(), resps).await {
                Ok(resps) => resps,
                Err(e) => {
                    tx.send(Err(e)).await.ok();
                    return;
                }
            };

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
                                        cfg.column_mapping.as_ref().map(|cm| &cm.log),
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

async fn run_query_to_end(client: Arc<crate::Client>, query: Query) -> Result<Vec<ArrowResponse>> {
    let mut resps = Vec::new();

    let to_block = query.to_block.unwrap();

    let mut query = query;

    loop {
        let resp = client.get_arrow(&query).await.context("get data")?;

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
