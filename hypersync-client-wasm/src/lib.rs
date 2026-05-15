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

use alloy_dyn_abi::DynSolValue;
use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::ipc::writer::FileWriter;
use hypersync_client::format::{Hex, LogArgument};
use hypersync_client::net_types::{Query, RollbackGuard};
use hypersync_client::simple_types::{Block, Log, Trace, Transaction};
use hypersync_client::{
    ArrowResponse as InnerArrowResponse, Client as InnerClient, ClientConfig,
    Decoder as InnerDecoder, QueryResponse, StreamConfig,
};
use js_sys::{Array, BigInt, Object, Reflect};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use wasm_bindgen::prelude::*;

mod balance;

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

    /// Run a query and return decoded simple types (`blocks`, `transactions`,
    /// `logs`, `traces`) as a plain JS object.
    ///
    /// Use this when you don't want to think about Arrow on the JS side. Use
    /// [`Client::get_arrow`] when you do — Arrow is faster for large result
    /// sets because we avoid materializing per-row JS objects.
    #[wasm_bindgen]
    pub async fn get(&self, query: JsValue) -> Result<JsValue, JsError> {
        let query: Query = serde_wasm_bindgen::from_value(query)
            .map_err(|e| JsError::new(&format!("invalid query: {e}")))?;
        let res = self
            .inner
            .get(&query)
            .await
            .map_err(|e| JsError::new(&format!("{e:?}")))?;
        // `serialize_large_number_types_as_bigints` keeps `u64` block numbers,
        // gas, etc. accurate beyond 2^53. Without this they would lose
        // precision when serialized as JS `number`.
        let serializer =
            serde_wasm_bindgen::Serializer::new().serialize_large_number_types_as_bigints(true);
        SerializableQueryResponse::from(&res)
            .serialize(&serializer)
            .map_err(|e| JsError::new(&format!("serialize response: {e}")))
    }

    /// Stream the result of a query in chunks. Returns an [`ArrowStream`]
    /// whose `next()` method yields one chunk at a time until the stream
    /// ends.
    ///
    /// `query` matches the shape of [`hypersync_client::net_types::Query`].
    /// `config` is optional and matches [`hypersync_client::StreamConfig`].
    /// Pass `undefined`/null to use defaults.
    ///
    /// Example:
    /// ```js
    /// const stream = await client.stream_arrow(query);
    /// let chunk;
    /// while ((chunk = await stream.next())) {
    ///     // chunk.logs, chunk.blocks, ... are Uint8Array of arrow IPC bytes
    /// }
    /// ```
    #[wasm_bindgen]
    pub async fn stream_arrow(
        &self,
        query: JsValue,
        config: JsValue,
    ) -> Result<ArrowStream, JsError> {
        let query: Query = serde_wasm_bindgen::from_value(query)
            .map_err(|e| JsError::new(&format!("invalid query: {e}")))?;
        let config: StreamConfig = if config.is_null() || config.is_undefined() {
            StreamConfig::default()
        } else {
            serde_wasm_bindgen::from_value(config)
                .map_err(|e| JsError::new(&format!("invalid StreamConfig: {e}")))?
        };
        let rx = self
            .inner
            .stream_arrow(query, config)
            .await
            .map_err(|e| JsError::new(&format!("{e:?}")))?;
        Ok(ArrowStream { rx: Mutex::new(rx) })
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

/// Async-iterable handle to an in-flight `stream_arrow` request. The inner
/// `mpsc::Receiver` pulls already-decoded `ArrowResponse` chunks pushed by
/// the unordered concurrent fetcher in the main client.
///
/// JS callers use it like:
///
/// ```js
/// const stream = await client.stream_arrow(query);
/// let chunk;
/// while ((chunk = await stream.next())) {
///     // chunk is an ArrowResponse
/// }
/// ```
#[wasm_bindgen]
pub struct ArrowStream {
    // `Mutex<...>` because wasm-bindgen exposes `&self` to JS; we need
    // interior mutability to call `recv(&mut self)`. `tokio::sync::Mutex`
    // is wasm-clean and async-aware (recv awaits inside the lock).
    rx: Mutex<mpsc::Receiver<Result<InnerArrowResponse>>>,
}

#[wasm_bindgen]
impl ArrowStream {
    /// Pulls the next chunk. Resolves to `undefined` when the stream is
    /// exhausted; throws if the underlying request fails.
    #[wasm_bindgen]
    pub async fn next(&self) -> Result<Option<ArrowResponse>, JsError> {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(Ok(resp)) => Ok(Some(
                ArrowResponse::from_native(resp).map_err(|e| JsError::new(&format!("{e:?}")))?,
            )),
            Some(Err(e)) => Err(JsError::new(&format!("{e:?}"))),
            None => Ok(None),
        }
    }
}

/// Event decoder. Wraps `hypersync_client::Decoder` for JS.
///
/// JS shape (matching `@envio-dev/hypersync-client`):
///
/// ```js
/// const decoder = Decoder.from_signatures([
///     "Transfer(address indexed from, address indexed to, uint256 amount)",
/// ]);
/// const decoded = decoder.decode_logs(logs);
/// ```
///
/// Each input `log` only needs `topics` (array of hex strings or null) and
/// `data` (hex string). Output entries are either `null` (no matching
/// signature / decoder error) or `{ indexed, body }`, where each value is
/// `{ val }` and `val` is `boolean | bigint | string | Array<{val}>`.
#[wasm_bindgen]
pub struct Decoder {
    inner: Arc<InnerDecoder>,
    checksum_addresses: bool,
}

/// Minimal log shape required by `Decoder::decode_logs`.
#[derive(Deserialize)]
struct DecoderLogInput {
    #[serde(default)]
    topics: Vec<Option<String>>,
    #[serde(default)]
    data: Option<String>,
}

#[wasm_bindgen]
impl Decoder {
    /// Construct from a list of event signatures, e.g.
    /// `["Transfer(address indexed from, address indexed to, uint256 amount)"]`.
    #[wasm_bindgen]
    pub fn from_signatures(signatures: Vec<String>) -> Result<Decoder, JsError> {
        let inner = InnerDecoder::from_signatures(&signatures)
            .map_err(|e| JsError::new(&format!("{e:?}")))?;
        Ok(Decoder {
            inner: Arc::new(inner),
            checksum_addresses: false,
        })
    }

    /// Toggle EIP-55 checksumming on decoded `address` values.
    #[wasm_bindgen]
    pub fn enable_checksummed_addresses(&mut self) {
        self.checksum_addresses = true;
    }
    #[wasm_bindgen]
    pub fn disable_checksummed_addresses(&mut self) {
        self.checksum_addresses = false;
    }

    /// Decode a JS array of logs. Each entry resolves to `null` if the
    /// signature wasn't recognized or topics/data couldn't be parsed.
    #[wasm_bindgen]
    pub fn decode_logs(&self, logs: JsValue) -> Result<JsValue, JsError> {
        let logs: Vec<DecoderLogInput> = serde_wasm_bindgen::from_value(logs)
            .map_err(|e| JsError::new(&format!("invalid logs: {e}")))?;
        let out = Array::new_with_length(logs.len() as u32);
        for (i, log) in logs.iter().enumerate() {
            let decoded = self
                .decode_one(log)
                .map_err(|e| JsError::new(&format!("decode log[{i}]: {e:?}")))?;
            out.set(i as u32, decoded);
        }
        Ok(out.into())
    }
}

impl Decoder {
    fn decode_one(&self, log: &DecoderLogInput) -> Result<JsValue> {
        let topics_decoded = log
            .topics
            .iter()
            .map(|t| {
                t.as_ref()
                    .map(|s| LogArgument::decode_hex(s).context("decode topic"))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;

        let topic0 = match topics_decoded.first().and_then(|x| x.as_ref()) {
            Some(t) => t,
            None => return Ok(JsValue::NULL),
        };

        let data = match log.data.as_ref() {
            Some(d) => hypersync_client::format::Data::decode_hex(d).context("decode data")?,
            None => return Ok(JsValue::NULL),
        };

        let decoded = match self
            .inner
            .decode(topic0.as_slice(), &topics_decoded, &data)?
        {
            Some(d) => d,
            None => return Ok(JsValue::NULL),
        };

        let event = Object::new();
        Reflect::set(
            &event,
            &"indexed".into(),
            &dyn_sol_values_to_js(&decoded.indexed, self.checksum_addresses),
        )
        .map_err(|e| anyhow::anyhow!("set indexed: {e:?}"))?;
        Reflect::set(
            &event,
            &"body".into(),
            &dyn_sol_values_to_js(&decoded.body, self.checksum_addresses),
        )
        .map_err(|e| anyhow::anyhow!("set body: {e:?}"))?;
        Ok(event.into())
    }
}

/// Maps a `Vec<DynSolValue>` to a JS array of `{ val }`. Mirrors the shape
/// returned by `@envio-dev/hypersync-client::Decoder`.
fn dyn_sol_values_to_js(values: &[DynSolValue], checksum_addresses: bool) -> JsValue {
    let arr = Array::new_with_length(values.len() as u32);
    for (i, v) in values.iter().enumerate() {
        let wrapper = Object::new();
        let _ = Reflect::set(
            &wrapper,
            &"val".into(),
            &dyn_sol_value_to_js(v, checksum_addresses),
        );
        arr.set(i as u32, wrapper.into());
    }
    arr.into()
}

fn dyn_sol_value_to_js(v: &DynSolValue, checksum_addresses: bool) -> JsValue {
    match v {
        DynSolValue::Bool(b) => JsValue::from_bool(*b),
        DynSolValue::Int(i, _) => BigInt::new(&JsValue::from_str(&i.to_string()))
            .map(JsValue::from)
            .unwrap_or(JsValue::NULL),
        DynSolValue::Uint(u, _) => BigInt::new(&JsValue::from_str(&u.to_string()))
            .map(JsValue::from)
            .unwrap_or(JsValue::NULL),
        DynSolValue::FixedBytes(b, _) => JsValue::from_str(&hex_prefixed(b.as_slice())),
        DynSolValue::Address(a) => {
            if checksum_addresses {
                JsValue::from_str(&a.to_checksum(None))
            } else {
                JsValue::from_str(&hex_prefixed(a.as_slice()))
            }
        }
        DynSolValue::Function(b) => JsValue::from_str(&hex_prefixed(b.as_slice())),
        DynSolValue::Bytes(b) => JsValue::from_str(&hex_prefixed(b)),
        DynSolValue::String(s) => JsValue::from_str(s),
        DynSolValue::Array(vals) | DynSolValue::FixedArray(vals) | DynSolValue::Tuple(vals) => {
            dyn_sol_values_to_js(vals, checksum_addresses)
        }
    }
}

pub(crate) fn hex_prefixed(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "0x".into();
    }
    let mut out = vec![0u8; bytes.len() * 2 + 2];
    out[0] = b'0';
    out[1] = b'x';
    faster_hex::hex_encode(bytes, &mut out[2..]).unwrap();
    String::from_utf8(out).unwrap()
}

/// JSON-shaped view of `hypersync_client::QueryResponse` for the wasm
/// boundary. We can't `derive(Serialize)` on the upstream type without
/// modifying it, so we project the fields we care about by reference.
#[derive(Serialize)]
struct SerializableQueryResponse<'a> {
    archive_height: Option<u64>,
    next_block: u64,
    total_execution_time: u64,
    data: SerializableResponseData<'a>,
    rollback_guard: Option<&'a RollbackGuard>,
}

#[derive(Serialize)]
struct SerializableResponseData<'a> {
    blocks: &'a [Vec<Block>],
    transactions: &'a [Vec<Transaction>],
    logs: &'a [Vec<Log>],
    traces: &'a [Vec<Trace>],
}

impl<'a> From<&'a QueryResponse> for SerializableQueryResponse<'a> {
    fn from(r: &'a QueryResponse) -> Self {
        Self {
            archive_height: r.archive_height,
            next_block: r.next_block,
            total_execution_time: r.total_execution_time,
            data: SerializableResponseData {
                blocks: &r.data.blocks,
                transactions: &r.data.transactions,
                logs: &r.data.logs,
                traces: &r.data.traces,
            },
            rollback_guard: r.rollback_guard.as_ref(),
        }
    }
}

/// Result of a single `get_arrow` call.
///
/// Wraps the original `hypersync_client::ArrowResponse` (which owns the
/// `RecordBatch`es) behind an `Arc`, so that:
///   * the per-table getters (`logs`, `blocks`, ...) can lazy-encode Arrow
///     IPC bytes for JS-side consumers like `apache-arrow.tableFromIPC`, and
///   * Rust-side consumers like [`BalanceTracker`] can iterate the
///     `RecordBatch`es directly via the zero-copy readers in
///     `hypersync_client::arrow_reader` without an IPC round-trip.
#[wasm_bindgen]
pub struct ArrowResponse {
    pub(crate) inner: Arc<hypersync_client::ArrowResponse>,
}

impl ArrowResponse {
    fn from_native(res: hypersync_client::ArrowResponse) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(res),
        })
    }
}

#[wasm_bindgen]
impl ArrowResponse {
    #[wasm_bindgen(getter)]
    pub fn archive_height(&self) -> Option<u64> {
        self.inner.archive_height
    }
    #[wasm_bindgen(getter)]
    pub fn next_block(&self) -> u64 {
        self.inner.next_block
    }
    #[wasm_bindgen(getter)]
    pub fn total_execution_time(&self) -> u64 {
        self.inner.total_execution_time
    }
    #[wasm_bindgen(getter)]
    pub fn blocks(&self) -> Result<Vec<u8>, JsError> {
        encode_batches(&self.inner.data.blocks).map_err(|e| JsError::new(&format!("{e:?}")))
    }
    #[wasm_bindgen(getter)]
    pub fn transactions(&self) -> Result<Vec<u8>, JsError> {
        encode_batches(&self.inner.data.transactions).map_err(|e| JsError::new(&format!("{e:?}")))
    }
    #[wasm_bindgen(getter)]
    pub fn logs(&self) -> Result<Vec<u8>, JsError> {
        encode_batches(&self.inner.data.logs).map_err(|e| JsError::new(&format!("{e:?}")))
    }
    #[wasm_bindgen(getter)]
    pub fn traces(&self) -> Result<Vec<u8>, JsError> {
        encode_batches(&self.inner.data.traces).map_err(|e| JsError::new(&format!("{e:?}")))
    }
    #[wasm_bindgen(getter)]
    pub fn decoded_logs(&self) -> Result<Vec<u8>, JsError> {
        encode_batches(&self.inner.data.decoded_logs).map_err(|e| JsError::new(&format!("{e:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::FileReader;
    use hypersync_client::ArrowResponseData;
    use std::io::Cursor;
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("number", DataType::UInt64, false),
            Field::new("hash", DataType::Utf8, false),
        ]));
        let numbers: Arc<dyn arrow::array::Array> = Arc::new(UInt64Array::from(vec![1, 2, 3]));
        let hashes: Arc<dyn arrow::array::Array> =
            Arc::new(StringArray::from(vec!["0x01", "0x02", "0x03"]));
        RecordBatch::try_new(schema, vec![numbers, hashes]).unwrap()
    }

    /// `encode_batches` returns an empty Vec for an empty input (and crucially
    /// does not panic trying to read `batches[0].schema()`).
    #[test]
    fn encode_batches_empty() {
        assert_eq!(encode_batches(&[]).unwrap(), Vec::<u8>::new());
    }

    /// `encode_batches` produces a self-describing Arrow IPC file that round-
    /// trips back to the same rows via `FileReader`. This is the contract the
    /// JS side relies on (apache-arrow's `tableFromIPC`).
    #[test]
    fn encode_batches_round_trip() {
        let batch = make_batch();
        let bytes = encode_batches(std::slice::from_ref(&batch)).unwrap();
        assert!(!bytes.is_empty());

        let reader = FileReader::try_new(Cursor::new(bytes), None).unwrap();
        let read_back: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();

        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].num_rows(), batch.num_rows());
        assert_eq!(read_back[0].schema(), batch.schema());
    }

    /// Multiple batches concatenate into a single IPC file, preserving order.
    #[test]
    fn encode_batches_multiple() {
        let batches = vec![make_batch(), make_batch()];
        let bytes = encode_batches(&batches).unwrap();
        let reader = FileReader::try_new(Cursor::new(bytes), None).unwrap();
        let read_back: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(read_back.len(), 2);
        assert_eq!(read_back[0].num_rows() + read_back[1].num_rows(), 6);
    }

    /// `ArrowResponse::from_native` faithfully copies scalar header fields and
    /// emits empty IPC payloads for empty tables (matches the JS test's
    /// expectation that `decoded_logs.byteLength === 0` when no signature was
    /// supplied).
    #[test]
    fn from_native_copies_header_and_empty_tables() {
        let native = hypersync_client::ArrowResponse {
            archive_height: Some(123),
            next_block: 100,
            total_execution_time: 42,
            data: ArrowResponseData::default(),
            rollback_guard: None,
        };

        let resp = ArrowResponse::from_native(native).unwrap();

        assert_eq!(resp.archive_height(), Some(123));
        assert_eq!(resp.next_block(), 100);
        assert_eq!(resp.total_execution_time(), 42);
        assert!(resp.blocks().unwrap().is_empty());
        assert!(resp.transactions().unwrap().is_empty());
        assert!(resp.logs().unwrap().is_empty());
        assert!(resp.traces().unwrap().is_empty());
        assert!(resp.decoded_logs().unwrap().is_empty());
    }

    /// One non-empty table produces a non-empty IPC payload while siblings
    /// stay empty. Catches accidental cross-wiring (e.g., logs into traces).
    #[test]
    fn from_native_routes_tables_independently() {
        let native = hypersync_client::ArrowResponse {
            archive_height: None,
            next_block: 0,
            total_execution_time: 0,
            data: ArrowResponseData {
                logs: vec![make_batch()],
                ..ArrowResponseData::default()
            },
            rollback_guard: None,
        };

        let resp = ArrowResponse::from_native(native).unwrap();

        assert!(!resp.logs().unwrap().is_empty());
        assert!(resp.blocks().unwrap().is_empty());
        assert!(resp.transactions().unwrap().is_empty());
        assert!(resp.traces().unwrap().is_empty());
        assert!(resp.decoded_logs().unwrap().is_empty());
    }
}
