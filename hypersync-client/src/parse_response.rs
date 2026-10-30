use std::ops::Range;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::{types::ArrowResponse, ArrowResponseData, QueryResponse};
use anyhow::{anyhow, ensure, Context, Result};
use arrow::{
    array::{make_array, ArrayData, RecordBatch, RecordBatchOptions},
    buffer::{BooleanBuffer, Buffer, NullBuffer},
    ipc::{
        convert::fb_to_schema,
        reader::{read_footer_length, FileDecoder},
        root_as_footer, Block,
    },
};
use capnp::{
    message::{Reader, ReaderOptions},
    serialize::OwnedSegments,
};
use hypersync_net_types::{hypersync_net_types_capnp, RollbackGuard};

/// Footer length (i32) followed by the `ARROW1` magic.
const IPC_TRAILER_LEN: usize = 10;

/// An unpacked capnp message whose bytes decoded Arrow arrays can share instead of copying.
pub struct Message {
    segments: Arc<OwnedSegments>,
    bytes: Buffer,
}

impl Message {
    pub fn read_packed(bytes: &[u8]) -> Result<Self> {
        let segments = capnp::serialize_packed::read_message(bytes, reader_options())
            .context("create message reader")?
            .into_segments();
        let segments = Arc::new(segments);
        let all: &[u8] = &segments;
        // SAFETY: `all` is the immutable memory of `segments`, which the Buffer keeps alive.
        let bytes = unsafe {
            Buffer::from_custom_allocation(NonNull::from(all).cast(), all.len(), segments.clone())
        };
        Ok(Self { segments, bytes })
    }

    pub fn reader(&self) -> Reader<&OwnedSegments> {
        Reader::new(&*self.segments, reader_options())
    }

    /// Returns `data`, read from this message, as a `Buffer` sharing its memory.
    fn slice(&self, data: &[u8]) -> Result<Buffer> {
        let offset = (data.as_ptr() as usize)
            .checked_sub(self.bytes.as_ptr() as usize)
            .context("data is outside of the message")?;
        checked_slice(&self.bytes, offset, data.len()).context("data is outside of the message")
    }
}

fn reader_options() -> ReaderOptions {
    let mut opts = ReaderOptions::new();
    opts.nesting_limit(i32::MAX).traversal_limit_in_words(None);
    opts
}

fn checked_slice(buffer: &Buffer, offset: usize, len: usize) -> Option<Buffer> {
    let end = offset.checked_add(len)?;
    (end <= buffer.len()).then(|| buffer.slice_with_length(offset, len))
}

fn block_data(buffer: &Buffer, block: &Block) -> Result<Buffer> {
    let offset = usize::try_from(block.offset()).context("block offset")?;
    let len = usize::try_from(block.metaDataLength()).context("block metadata length")?
        + usize::try_from(block.bodyLength()).context("block body length")?;
    checked_slice(buffer, offset, len).context("block is out of bounds")
}

/// Decodes an Arrow IPC file without copying it out of the message.
///
/// Decompressed buffers are new allocations. Buffers stored uncompressed (uncompressed
/// files, or buffers the writer found incompressible, e.g. hashes, which are stored raw even
/// in compressed files) would point into the message and keep the whole response alive, so
/// those are copied out.
fn read_chunks(message: &Message, data: &[u8]) -> Result<Vec<RecordBatch>> {
    let trailer_start = data
        .len()
        .checked_sub(IPC_TRAILER_LEN)
        .context("ipc file too short")?;
    let buffer = message.slice(data)?;

    let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap())
        .context("read footer length")?;
    let footer_start = trailer_start
        .checked_sub(footer_len)
        .context("footer length out of bounds")?;
    let footer = root_as_footer(&buffer[footer_start..trailer_start])
        .map_err(|e| anyhow!("parse footer: {e}"))?;
    let ipc_schema = footer.schema().context("footer has no schema")?;
    ensure!(
        ipc_schema.endianness().equals_to_target_endianness(),
        "ipc file endianness does not match this platform"
    );
    let mut decoder = FileDecoder::new(Arc::new(fb_to_schema(ipc_schema)), footer.version());

    for block in footer.dictionaries().iter().flatten() {
        decoder
            .read_dictionary(block, &block_data(&buffer, block)?)
            .context("read dictionary")?;
    }

    let message_range = message.bytes.as_ptr_range();
    let mut chunks = Vec::new();
    for block in footer.recordBatches().iter().flatten() {
        let chunk = decoder
            .read_record_batch(block, &block_data(&buffer, block)?)
            .context("read chunk")?;
        if let Some(chunk) = chunk {
            chunks.push(detach(chunk, &message_range).context("copy chunk out of message")?);
        }
    }

    Ok(chunks)
}

/// Copies the buffers of `batch` that point into `message`, leaving the rest shared.
fn detach(batch: RecordBatch, message: &Range<*const u8>) -> Result<RecordBatch> {
    let columns: Vec<ArrayData> = batch.columns().iter().map(|c| c.to_data()).collect();
    if !columns.iter().any(|c| points_into(c, message)) {
        return Ok(batch);
    }
    let columns = columns
        .into_iter()
        .map(|c| Ok(make_array(detach_data(c, message)?)))
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &options,
    )?)
}

/// Whether `b` is a slice of the message. The end is inclusive: an empty slice at the very
/// end of the message still holds a reference to it.
fn in_message(b: &Buffer, message: &Range<*const u8>) -> bool {
    (message.start..=message.end).contains(&b.as_ptr())
}

fn points_into(data: &ArrayData, message: &Range<*const u8>) -> bool {
    data.buffers().iter().any(|b| in_message(b, message))
        || data
            .nulls()
            .is_some_and(|n| in_message(n.buffer(), message))
        || data.child_data().iter().any(|c| points_into(c, message))
}

fn detach_data(data: ArrayData, message: &Range<*const u8>) -> Result<ArrayData> {
    if !points_into(&data, message) {
        return Ok(data);
    }
    let copy = |b: &Buffer| {
        if in_message(b, message) {
            Buffer::from_slice_ref(b.as_slice())
        } else {
            b.clone()
        }
    };
    let buffers = data.buffers().iter().map(copy).collect();
    let nulls = data
        .nulls()
        .map(|n| NullBuffer::new(BooleanBuffer::new(copy(n.buffer()), n.offset(), n.len())));
    let children = data
        .child_data()
        .iter()
        .map(|c| detach_data(c.clone(), message))
        .collect::<Result<_>>()?;
    let builder = data
        .into_builder()
        .buffers(buffers)
        .nulls(nulls)
        .child_data(children);
    // SAFETY: only buffers are replaced, each by a byte-identical copy, so the data is
    // exactly what FileDecoder already validated.
    Ok(unsafe { builder.build_unchecked() })
}

/// `message` must be the message `query_response` was read from.
pub fn read_query_response(
    query_response: &hypersync_net_types_capnp::query_response::Reader,
    message: &Message,
) -> Result<ArrowResponse> {
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

    let read = |data| read_chunks(message, data);
    let blocks = read(data.get_blocks().context("get block data")?).context("parse block data")?;
    let transactions =
        read(data.get_transactions().context("get transaction data")?).context("parse tx data")?;
    let logs = read(data.get_logs().context("get log data")?).context("parse log data")?;
    let traces = if data.has_traces() {
        read(data.get_traces().context("get trace data")?).context("parse traces data")?
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

pub fn parse_query_response(bytes: &[u8]) -> Result<ArrowResponse> {
    let message = Message::read_packed(bytes)?;
    let reader = message.reader();
    let query_response = reader
        .get_root::<hypersync_net_types_capnp::query_response::Reader>()
        .context("get root")?;
    read_query_response(&query_response, &message).context("read query response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
    use arrow::ipc::CompressionType;

    fn batch(rows: u64) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("number", DataType::UInt64, false),
            Field::new("hash", DataType::Binary, true),
        ]);
        let hashes: Vec<Vec<u8>> = (0..rows).map(|i| i.to_le_bytes().repeat(4)).collect();
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from_iter_values(0..rows)),
                Arc::new(BinaryArray::from_iter_values(hashes)),
            ],
        )
        .unwrap()
    }

    fn ipc_file(batches: &[RecordBatch], compression: Option<CompressionType>) -> Vec<u8> {
        let opts = IpcWriteOptions::default()
            .try_with_compression(compression)
            .unwrap();
        let mut writer =
            FileWriter::try_new_with_options(Vec::new(), &batches[0].schema(), opts).unwrap();
        for b in batches {
            writer.write(b).unwrap();
        }
        writer.finish().unwrap();
        writer.into_inner().unwrap()
    }

    fn response_bytes(blocks: &[u8], transactions: &[u8], logs: &[u8]) -> Vec<u8> {
        let mut message = capnp::message::Builder::new_default();
        let mut response =
            message.init_root::<hypersync_net_types_capnp::query_response::Builder>();
        response.set_archive_height(100);
        response.set_next_block(42);
        let mut data = response.init_data();
        data.set_blocks(blocks);
        data.set_transactions(transactions);
        data.set_logs(logs);
        let mut bytes = Vec::new();
        capnp::serialize_packed::write_message(&mut bytes, &message).unwrap();
        bytes
    }

    #[test]
    fn decodes_compressed_and_uncompressed_tables() {
        let blocks = vec![batch(3)];
        let transactions = vec![batch(1000), batch(10)];
        let logs = vec![batch(5000)];
        let bytes = response_bytes(
            &ipc_file(&blocks, None),
            &ipc_file(&transactions, Some(CompressionType::ZSTD)),
            &ipc_file(&logs, Some(CompressionType::LZ4_FRAME)),
        );

        let res = parse_query_response(&bytes).unwrap();

        assert_eq!(
            [
                res.data.blocks,
                res.data.transactions,
                res.data.logs,
                res.data.traces
            ],
            [blocks, transactions, logs, Vec::new()]
        );
    }

    #[test]
    fn malformed_ipc_returns_error() {
        let valid = ipc_file(&[batch(100)], Some(CompressionType::ZSTD));
        let mut bad_footer_len = valid.clone();
        let len = bad_footer_len.len();
        bad_footer_len[len - 10..len - 6].copy_from_slice(&i32::MAX.to_le_bytes());
        let cases = [
            Vec::new(),
            b"ARROW1".to_vec(),
            valid[valid.len() / 2..].to_vec(),
            bad_footer_len,
        ];

        let errors: Vec<String> = cases
            .iter()
            .map(|logs| {
                let err = parse_query_response(&response_bytes(&valid, &valid, logs)).unwrap_err();
                format!("{:#}", err.root_cause())
            })
            .collect();

        assert_eq!(
            errors,
            [
                "ipc file too short",
                "ipc file too short",
                "block is out of bounds",
                "footer length out of bounds",
            ],
        );
    }

    /// Random bytes, which ZSTD can't compress, so the writer stores them raw.
    fn incompressible_batch(rows: usize) -> RecordBatch {
        let mut state = 0x9e3779b97f4a7c15u64;
        let hashes: Vec<Vec<u8>> = (0..rows)
            .map(|_| {
                (0..4)
                    .flat_map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        state.to_le_bytes()
                    })
                    .collect()
            })
            .collect();
        let schema = Schema::new(vec![Field::new("hash", DataType::Binary, false)]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(BinaryArray::from_iter_values(hashes))],
        )
        .unwrap()
    }

    #[test]
    fn decoded_batches_do_not_retain_message() {
        let logs = vec![incompressible_batch(1000)];
        let blocks = vec![batch(100)];
        let empty_values = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "empty",
                DataType::Binary,
                false,
            )])),
            vec![Arc::new(BinaryArray::from_iter_values(vec![b""; 10]))],
        )
        .unwrap();
        let transactions = vec![empty_values];
        let message = Message::read_packed(&response_bytes(
            &ipc_file(&blocks, None),
            &ipc_file(&transactions, None),
            &ipc_file(&logs, Some(CompressionType::ZSTD)),
        ))
        .unwrap();
        let reader = message.reader();
        let data = reader
            .get_root::<hypersync_net_types_capnp::query_response::Reader>()
            .unwrap()
            .get_data()
            .unwrap();
        let message_range = message.bytes.as_ptr_range();

        for (table, expected) in [
            (data.get_logs().unwrap(), &logs),
            (data.get_blocks().unwrap(), &blocks),
            (data.get_transactions().unwrap(), &transactions),
        ] {
            let chunks = read_chunks(&message, table).unwrap();
            assert_eq!(&chunks, expected);
            for chunk in &chunks {
                for col in chunk.columns() {
                    assert!(!points_into(&col.to_data(), &message_range));
                }
            }
        }
    }

    #[test]
    fn uncompressed_tables_are_copied_out_of_message() {
        let logs = vec![batch(1000)];
        let message =
            Message::read_packed(&response_bytes(&[], &[], &ipc_file(&logs, None))).unwrap();
        let reader = message.reader();
        let data = reader
            .get_root::<hypersync_net_types_capnp::query_response::Reader>()
            .unwrap()
            .get_data()
            .unwrap();
        let message_range = message.bytes.as_ptr_range();

        let chunks = read_chunks(&message, data.get_logs().unwrap()).unwrap();
        let values = chunks[0].column(0).to_data().buffers()[0].as_ptr();

        assert_eq!((chunks, message_range.contains(&values)), (logs, false));
    }
}
