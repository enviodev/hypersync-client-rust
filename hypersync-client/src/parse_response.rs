use std::{io::Cursor, sync::Arc};

use crate::{types::ArrowResponse, ArrowBatch, ArrowResponseData, QueryResponse};
use anyhow::{Context, Result};
use hypersync_net_types::{hypersync_net_types_capnp, RollbackGuard};
use polars_arrow::io::ipc;

fn read_chunks(bytes: &[u8]) -> Result<Vec<ArrowBatch>> {
    let mut reader = Cursor::new(bytes);

    let metadata = ipc::read::read_file_metadata(&mut reader).context("read metadata")?;

    let schema = metadata.schema.clone();

    let reader = ipc::read::FileReader::new(reader, metadata, None, None);

    let chunks = reader
        .map(|chunk| {
            chunk.context("read chunk").map(|chunk| ArrowBatch {
                chunk: Arc::new(chunk),
                schema: schema.clone(),
            })
        })
        .collect::<Result<Vec<ArrowBatch>>>()?;

    Ok(chunks)
}

pub fn parse_query_response(bytes: &[u8]) -> Result<ArrowResponse> {
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

    let blocks = read_chunks(data.get_blocks().context("get data")?).context("parse block data")?;
    let transactions =
        read_chunks(data.get_transactions().context("get data")?).context("parse tx data")?;
    let logs = read_chunks(data.get_logs().context("get data")?).context("parse log data")?;
    let traces = if data.has_traces() {
        read_chunks(data.get_traces().context("get data")?).context("parse traces data")?
    } else {
        Vec::new()
    };

    Ok(QueryResponse {
        archive_height,
        next_block: query_response.get_next_block(),
        total_execution_time: query_response.get_total_execution_time(),
        data: ArrowResponseData {
            blocks,
            transactions,
            logs,
            traces,
            decoded_logs: Vec::new(),
        },
        rollback_guard,
    })
}
