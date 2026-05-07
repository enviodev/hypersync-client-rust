//! Minimal WASM bindings for hypersync.
//!
//! Exposes a single `Client::get_arrow(query)` entry point that POSTs the
//! query and returns the server's Arrow IPC payloads as `Uint8Array`s. JS
//! callers are expected to decode them with `apache-arrow` (or similar).
//!
//! Intentionally omits: streaming, retries, rate-limit handling, parquet
//! output, capnp request encoding, decoded logs.

use std::io::Cursor;

use anyhow::{anyhow, Context, Result};
use arrow::ipc::{reader::FileReader, writer::FileWriter};
use hypersync_net_types::{hypersync_net_types_capnp, Query};
use serde::Serialize;
use wasm_bindgen::prelude::*;

/// Re-encode an Arrow IPC file as uncompressed.
///
/// The hypersync server emits LZ4/ZSTD-compressed batches, which the Rust
/// `arrow` crate decodes transparently but the JS `apache-arrow` reader does
/// not yet support. We round-trip through `FileReader`/`FileWriter` so JS sees
/// plain uncompressed IPC.
fn reencode_uncompressed(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let reader = FileReader::try_new(Cursor::new(bytes), None).context("open ipc reader")?;
    let schema = reader.schema();
    let mut out = Vec::with_capacity(bytes.len());
    {
        let mut writer = FileWriter::try_new(&mut out, &schema).context("open ipc writer")?;
        for batch in reader {
            let batch = batch.context("decode record batch")?;
            writer.write(&batch).context("write record batch")?;
        }
        writer.finish().context("finish ipc file")?;
    }
    Ok(out)
}

#[wasm_bindgen(start)]
pub fn _start() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub struct Client {
    url: String,
    api_token: String,
    http: reqwest::Client,
}

#[wasm_bindgen]
impl Client {
    #[wasm_bindgen(constructor)]
    pub fn new(url: String, api_token: String) -> Result<Client, JsError> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| JsError::new(&format!("build http client: {e}")))?;
        Ok(Client {
            url,
            api_token,
            http,
        })
    }

    /// Run a query and return arrow IPC bytes for each table.
    ///
    /// `query` is a JS object matching the JSON shape of [`hypersync_net_types::Query`].
    #[wasm_bindgen]
    pub async fn get_arrow(&self, query: JsValue) -> Result<ArrowResponse, JsError> {
        let query: Query = serde_wasm_bindgen::from_value(query)
            .map_err(|e| JsError::new(&format!("invalid query: {e}")))?;
        self.get_arrow_inner(query)
            .await
            .map_err(|e| JsError::new(&format!("{e:?}")))
    }
}

impl Client {
    async fn get_arrow_inner(&self, query: Query) -> Result<ArrowResponse> {
        let url = format!("{}/query/arrow-ipc", self.url.trim_end_matches('/'));

        let res = self
            .http
            .post(&url)
            .bearer_auth(&self.api_token)
            .json(&query)
            .send()
            .await
            .context("send request")?;

        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.unwrap_or_default();
            return Err(anyhow!("http status {status}: {text}"));
        }

        let bytes = res.bytes().await.context("read response body")?;

        let mut opts = capnp::message::ReaderOptions::new();
        opts.nesting_limit(i32::MAX).traversal_limit_in_words(None);
        let message_reader = capnp::serialize_packed::read_message(bytes.as_ref(), opts)
            .context("parse capnp envelope")?;
        let qr = message_reader
            .get_root::<hypersync_net_types_capnp::query_response::Reader>()
            .context("get query_response root")?;

        let archive_height = match qr.get_archive_height() {
            -1 => None,
            h => Some(u64::try_from(h).context("invalid archive height returned from server")?),
        };
        let data = qr.get_data().context("get data")?;

        Ok(ArrowResponse {
            archive_height,
            next_block: qr.get_next_block(),
            total_execution_time: qr.get_total_execution_time(),
            blocks: reencode_uncompressed(data.get_blocks().context("blocks")?)
                .context("blocks")?,
            transactions: reencode_uncompressed(data.get_transactions().context("transactions")?)
                .context("transactions")?,
            logs: reencode_uncompressed(data.get_logs().context("logs")?).context("logs")?,
            traces: if data.has_traces() {
                reencode_uncompressed(data.get_traces().context("traces")?).context("traces")?
            } else {
                Vec::new()
            },
        })
    }
}

/// Result of a single `get_arrow` call. Each table is a separate Arrow IPC
/// file that JS can feed into `apache-arrow`'s `tableFromIPC`.
#[wasm_bindgen]
#[derive(Serialize)]
pub struct ArrowResponse {
    archive_height: Option<u64>,
    next_block: u64,
    total_execution_time: u64,
    blocks: Vec<u8>,
    transactions: Vec<u8>,
    logs: Vec<u8>,
    traces: Vec<u8>,
}

#[wasm_bindgen]
impl ArrowResponse {
    #[wasm_bindgen(getter)]
    pub fn archive_height(&self) -> Option<u64> {
        self.archive_height
    }
    #[wasm_bindgen(getter)]
    pub fn next_block(&self) -> u64 {
        self.next_block
    }
    #[wasm_bindgen(getter)]
    pub fn total_execution_time(&self) -> u64 {
        self.total_execution_time
    }
    #[wasm_bindgen(getter)]
    pub fn blocks(&self) -> Vec<u8> {
        self.blocks.clone()
    }
    #[wasm_bindgen(getter)]
    pub fn transactions(&self) -> Vec<u8> {
        self.transactions.clone()
    }
    #[wasm_bindgen(getter)]
    pub fn logs(&self) -> Vec<u8> {
        self.logs.clone()
    }
    #[wasm_bindgen(getter)]
    pub fn traces(&self) -> Vec<u8> {
        self.traces.clone()
    }
}
