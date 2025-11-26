use std::{path::PathBuf, time::Instant};

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use hypersync_net_types::Query;
use parquet::{
    arrow::async_writer::AsyncArrowWriter,
    file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{config::StreamConfig, Client};

pub async fn collect_parquet(
    client: &Client,
    path: &str,
    query: Query,
    config: StreamConfig,
) -> Result<()> {
    let path = PathBuf::from(path);

    tokio::fs::create_dir_all(&path)
        .await
        .context("create parquet dir")?;

    let mut blocks_path = path.clone();
    blocks_path.push("blocks.parquet");
    let (mut blocks_sender, blocks_join) = spawn_writer(blocks_path)?;

    let mut transactions_path = path.clone();
    transactions_path.push("transactions.parquet");
    let (mut transactions_sender, transactions_join) = spawn_writer(transactions_path)?;

    let mut logs_path = path.clone();
    logs_path.push("logs.parquet");
    let (mut logs_sender, logs_join) = spawn_writer(logs_path)?;

    let mut traces_path = path.clone();
    traces_path.push("traces.parquet");
    let (mut traces_sender, traces_join) = spawn_writer(traces_path)?;

    let mut decoded_logs_path = path.clone();
    decoded_logs_path.push("decoded_logs.parquet");
    let (mut decoded_logs_sender, decoded_logs_join) = spawn_writer(decoded_logs_path)?;

    let mut rx = client
        .stream_arrow(query, config)
        .await
        .context("start stream")?;

    while let Some(resp) = rx.recv().await {
        let resp = resp.context("get query response")?;

        log::trace!("got data up to block {}", resp.next_block);

        let blocks_fut = async move {
            for batch in resp.data.blocks {
                blocks_sender
                    .send(batch)
                    .await
                    .context("write blocks chunk to parquet")?;
            }

            Ok::<_, anyhow::Error>(blocks_sender)
        };

        let txs_fut = async move {
            for batch in resp.data.transactions {
                transactions_sender
                    .send(batch)
                    .await
                    .context("write transactions chunk to parquet")?;
            }

            Ok::<_, anyhow::Error>(transactions_sender)
        };

        let logs_fut = {
            let data = resp.data.logs.clone();
            async move {
                for batch in data {
                    logs_sender
                        .send(batch)
                        .await
                        .context("write logs chunk to parquet")?;
                }

                Ok::<_, anyhow::Error>(logs_sender)
            }
        };

        let traces_fut = async move {
            for batch in resp.data.traces {
                traces_sender
                    .send(batch)
                    .await
                    .context("write traces chunk to parquet")?;
            }

            Ok::<_, anyhow::Error>(traces_sender)
        };

        let decoded_logs_fut = async move {
            for batch in resp.data.decoded_logs {
                decoded_logs_sender
                    .send(batch)
                    .await
                    .context("write decoded_logs chunk to parquet")?;
            }

            Ok::<_, anyhow::Error>(decoded_logs_sender)
        };

        let start = Instant::now();

        (
            blocks_sender,
            transactions_sender,
            logs_sender,
            traces_sender,
            decoded_logs_sender,
        ) = futures::future::try_join5(blocks_fut, txs_fut, logs_fut, traces_fut, decoded_logs_fut)
            .await
            .context("write to parquet")?;

        log::trace!("wrote to parquet in {} ms", start.elapsed().as_millis());
    }

    std::mem::drop(blocks_sender);
    std::mem::drop(transactions_sender);
    std::mem::drop(logs_sender);
    std::mem::drop(traces_sender);
    std::mem::drop(decoded_logs_sender);

    blocks_join
        .await
        .context("join blocks task")?
        .context("finish blocks file")?;
    transactions_join
        .await
        .context("join transactions task")?
        .context("finish transactions file")?;
    logs_join
        .await
        .context("join logs task")?
        .context("finish logs file")?;
    traces_join
        .await
        .context("join traces task")?
        .context("finish traces file")?;
    decoded_logs_join
        .await
        .context("join decoded_logs task")?
        .context("finish decoded_logs file")?;

    Ok(())
}

fn spawn_writer(path: PathBuf) -> Result<(mpsc::Sender<RecordBatch>, JoinHandle<Result<()>>)> {
    let (tx, rx) = mpsc::channel(64);

    let handle = tokio::task::spawn(async move {
        match run_writer(rx, path).await {
            Ok(v) => Ok(v),
            Err(e) => {
                log::error!("failed to run parquet writer: {e:?}");
                Err(e)
            }
        }
    });

    Ok((tx, handle))
}

async fn run_writer(mut rx: mpsc::Receiver<RecordBatch>, path: PathBuf) -> Result<()> {
    let first_batch = match rx.recv().await {
        Some(batch) => batch,
        None => return Ok(()),
    };

    let file = tokio::io::BufWriter::new(
        tokio::fs::File::create(&path)
            .await
            .context("create parquet file")?,
    );

    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let mut writer = AsyncArrowWriter::try_new(file, first_batch.schema(), Some(props))
        .context("create writer")?;

    writer
        .write(&first_batch)
        .await
        .context("write first batch")?;

    while let Some(batch) = rx.recv().await {
        writer.write(&batch).await.context("write batch")?;
    }

    writer.close().await.context("finish writer")?;

    Ok(())
}
