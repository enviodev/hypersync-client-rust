use std::{io::Cursor, sync::Arc};

use crate::ArrowBatch;
use anyhow::{Context, Result};
use polars_arrow::io::ipc;

pub trait TransportFormat {
    fn read_chunks(bytes: &[u8]) -> Result<Vec<ArrowBatch>>;
    fn path() -> &'static str;
}

pub struct ArrowIpc;

impl TransportFormat for ArrowIpc {
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

    fn path() -> &'static str {
        "arrow-ipc"
    }
}
