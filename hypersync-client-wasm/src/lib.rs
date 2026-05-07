//! WASM bindings for hypersync.
//!
//! This crate is a thin wasm-bindgen wrapper over `hypersync_client::Client`.
//! All types — `Query`, `ClientConfig`, `ArrowResponse` — are reused from the
//! main client. The native `Client` already compiles to wasm32 (streaming,
//! parquet, sse, and rayon parallelism are cfg-gated out), so the wasm
//! variant inherits retries, payload-too-large halving, rate-limit tracking,
//! and the cap'n proto query-cache toggle for free.
//!
//! Currently exposed: `Client::new`, `Client::get_arrow`, `Client::get_height`,
//! `Client::get_chain_id`. Streaming/parquet/sse stay native-only.

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::ipc::writer::FileWriter;
use hypersync_client::{net_types::Query, Client as InnerClient, ClientConfig};
use serde::Serialize;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn _start() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

/// Encode an Arrow IPC file from a sequence of `RecordBatch`es using the
/// default (uncompressed) IPC writer.
///
/// The JS `apache-arrow` package does not yet support reading LZ4/ZSTD
/// compressed batches, so we always re-emit uncompressed bytes for the wasm
/// boundary. The native Rust `arrow` crate decoded the server's compressed
/// payload transparently before we got here.
fn encode_batches(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut out = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut out, &schema).context("open ipc writer")?;
        for batch in batches {
            writer.write(batch).context("write record batch")?;
        }
        writer.finish().context("finish ipc file")?;
    }
    Ok(out)
}

/// Wraps `hypersync_client::Client` for JS.
#[wasm_bindgen]
pub struct Client {
    inner: InnerClient,
}

#[wasm_bindgen]
impl Client {
    /// Construct a new client.
    ///
    /// `url` is the hypersync endpoint (e.g. `https://eth.hypersync.xyz`),
    /// `api_token` is the bearer token. All other options use library
    /// defaults (CapnProto serialization with query caching enabled, 3
    /// retries, 30s http timeout). Use `Client.with_config` to override.
    #[wasm_bindgen(constructor)]
    pub fn new(url: String, api_token: String) -> Result<Client, JsError> {
        let cfg = ClientConfig {
            url,
            api_token,
            ..Default::default()
        };
        Self::with_config_inner(cfg)
    }

    /// Construct a new client from a JS object matching the JSON shape of
    /// [`hypersync_client::ClientConfig`].
    #[wasm_bindgen]
    pub fn with_config(config: JsValue) -> Result<Client, JsError> {
        let cfg: ClientConfig = serde_wasm_bindgen::from_value(config)
            .map_err(|e| JsError::new(&format!("invalid ClientConfig: {e}")))?;
        Self::with_config_inner(cfg)
    }

    /// Run a query and return arrow IPC bytes for each table.
    ///
    /// `query` is a JS object matching the JSON shape of
    /// [`hypersync_client::net_types::Query`].
    #[wasm_bindgen]
    pub async fn get_arrow(&self, query: JsValue) -> Result<ArrowResponse, JsError> {
        let query: Query = serde_wasm_bindgen::from_value(query)
            .map_err(|e| JsError::new(&format!("invalid query: {e}")))?;
        let res = self
            .inner
            .get_arrow(&query)
            .await
            .map_err(|e| JsError::new(&format!("{e:?}")))?;
        ArrowResponse::from_native(res).map_err(|e| JsError::new(&format!("{e:?}")))
    }

    /// Get current archive height of the underlying server.
    #[wasm_bindgen]
    pub async fn get_height(&self) -> Result<u64, JsError> {
        self.inner
            .get_height()
            .await
            .map_err(|e| JsError::new(&format!("{e:?}")))
    }

    /// Get the EVM chain id this server is serving.
    #[wasm_bindgen]
    pub async fn get_chain_id(&self) -> Result<u64, JsError> {
        self.inner
            .get_chain_id()
            .await
            .map_err(|e| JsError::new(&format!("{e:?}")))
    }

    /// Endpoint URL the client is pointed at.
    #[wasm_bindgen(getter)]
    pub fn url(&self) -> String {
        self.inner.url().to_string()
    }
}

impl Client {
    fn with_config_inner(cfg: ClientConfig) -> Result<Client, JsError> {
        let inner =
            InnerClient::new(cfg).map_err(|e| JsError::new(&format!("build client: {e:?}")))?;
        Ok(Client { inner })
    }
}

/// Result of a single `get_arrow` call. Each table is a separate Arrow IPC
/// file (uncompressed) that JS can feed into `apache-arrow`'s `tableFromIPC`.
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
    decoded_logs: Vec<u8>,
}

impl ArrowResponse {
    fn from_native(res: hypersync_client::ArrowResponse) -> Result<Self> {
        Ok(Self {
            archive_height: res.archive_height,
            next_block: res.next_block,
            total_execution_time: res.total_execution_time,
            blocks: encode_batches(&res.data.blocks).context("blocks")?,
            transactions: encode_batches(&res.data.transactions).context("transactions")?,
            logs: encode_batches(&res.data.logs).context("logs")?,
            traces: encode_batches(&res.data.traces).context("traces")?,
            decoded_logs: encode_batches(&res.data.decoded_logs).context("decoded_logs")?,
        })
    }
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
    #[wasm_bindgen(getter)]
    pub fn decoded_logs(&self) -> Vec<u8> {
        self.decoded_logs.clone()
    }
}
