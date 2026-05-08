//! Stateful ERC20 transfer accumulator, exposed to JS as `BalanceTracker`.
//!
//! This module is the "if you wanted to write your own wasm-bindgen for your
//! site, here's the shape" example. The browser demo streams Arrow chunks
//! from `Client::stream_arrow` and feeds each one to `BalanceTracker.process`,
//! which:
//!
//!   * walks the Arrow `RecordBatch`es with the zero-copy readers from
//!     `hypersync_client::arrow_reader` (no IPC re-encode, no apache-arrow
//!     pass on the JS side),
//!   * filters to `Transfer(address,address,uint256)` events touching the
//!     tracked wallet,
//!   * keeps running `U256` totals in/out and a bounded ring of the last N
//!     transfers (with block number + timestamp + counterparty + amount),
//!   * returns a serializable delta describing only what changed in this
//!     batch — JS doesn't need to know how to read Arrow at all.
//!
//! All heavy work (column lookups, address comparison, U256 add) stays in
//! Rust. The wasm-bindgen boundary is crossed exactly once per chunk: in,
//! the borrowed `&ArrowResponse`; out, one serialized `BatchDelta` JsValue.
//!
//! ```js
//! const tracker = new BalanceTracker(wallet, 10);
//! const stream = await client.stream_arrow(query);
//! while ((chunk = await stream.next())) {
//!     const delta = tracker.process(chunk);
//!     // delta.in_delta, delta.out_delta, delta.new_entries, delta.last_block, ...
//! }
//! ```
//!
//! The `Transfer` topic0 is the only signature this tracker recognizes.

use std::collections::{HashMap, VecDeque};

use alloy_primitives::U256;
use anyhow::{bail, Context, Result};
use hypersync_client::arrow_reader::{BlockReader, LogReader};
use serde::Serialize;
use wasm_bindgen::prelude::*;

use crate::{hex_prefixed, ArrowResponse};

/// `keccak256("Transfer(address,address,uint256)")`.
const TRANSFER_TOPIC0: [u8; 32] = [
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
];

/// One row appended to the ledger.
///
/// `direction` is `"in"` or `"out"`. `amount` is a decimal string (kept raw —
/// the JS side scales by `decimals` for display). Numbers that fit in u64
/// (block number, log index, timestamp) cross the boundary as JS BigInts via
/// the `serialize_large_number_types_as_bigints(true)` setting.
#[derive(Serialize, Clone)]
struct LedgerEntry {
    direction: &'static str,
    amount: String,
    counterparty: String,
    block_number: u64,
    block_timestamp: Option<u64>,
    transaction_hash: Option<String>,
    log_index: u64,
}

/// Returned from `BalanceTracker.process` — covers only the rows in this batch.
#[derive(Serialize)]
struct BatchDelta {
    /// New `in` amount this batch (decimal string).
    in_delta: String,
    /// New `out` amount this batch.
    out_delta: String,
    /// How many matching rows we saw in this batch.
    matches: u32,
    /// Cumulative balance (in - out) after applying this batch (decimal string,
    /// signed).
    balance: String,
    /// Cumulative running totals.
    in_total: String,
    out_total: String,
    transfer_count: u64,
    /// Highest block number we've now consumed (`response.next_block - 1` if
    /// the chunk advanced, or unchanged if the chunk was empty).
    last_block: Option<u64>,
    /// Wall-clock timestamp of `last_block` if we saw it in this or any
    /// earlier batch's `blocks` table. Unix seconds.
    last_block_timestamp: Option<u64>,
    /// Most recent N entries (`N` set at construction). Newest first.
    ledger: Vec<LedgerEntry>,
    /// The subset of `ledger` that was added by *this* batch, in insertion
    /// order.
    new_entries: Vec<LedgerEntry>,
}

/// Stateful ERC20-Transfer accumulator. Construct once, feed every
/// `ArrowResponse` from a stream into `process`.
#[wasm_bindgen]
pub struct BalanceTracker {
    /// 20-byte target address (lowercase, no 0x).
    wallet: [u8; 20],
    /// Capacity of the ring buffer.
    ledger_capacity: usize,
    in_total: U256,
    out_total: U256,
    transfer_count: u64,
    /// Newest entries are pushed to the front; tail evicted past capacity.
    ledger: VecDeque<LedgerEntry>,
    last_block: Option<u64>,
    last_block_timestamp: Option<u64>,
}

#[wasm_bindgen]
impl BalanceTracker {
    /// Construct a tracker for `wallet_hex` (with or without `0x`), keeping
    /// the most recent `ledger_capacity` matched transfers.
    #[wasm_bindgen(constructor)]
    pub fn new(wallet_hex: &str, ledger_capacity: usize) -> Result<BalanceTracker, JsError> {
        let wallet = parse_address_20(wallet_hex)
            .map_err(|e| JsError::new(&format!("invalid wallet address: {e:?}")))?;
        Ok(Self {
            wallet,
            ledger_capacity,
            in_total: U256::ZERO,
            out_total: U256::ZERO,
            transfer_count: 0,
            ledger: VecDeque::with_capacity(ledger_capacity),
            last_block: None,
            last_block_timestamp: None,
        })
    }

    /// Consume one Arrow chunk. Updates running totals and the ledger,
    /// returns a `BatchDelta` describing this chunk's contribution plus the
    /// new cumulative state. The returned object has bigint fields where
    /// useful.
    pub fn process(&mut self, response: &ArrowResponse) -> Result<JsValue, JsError> {
        let delta = self
            .process_inner(response)
            .map_err(|e| JsError::new(&format!("{e:?}")))?;
        let serializer =
            serde_wasm_bindgen::Serializer::new().serialize_large_number_types_as_bigints(true);
        delta
            .serialize(&serializer)
            .map_err(|e| JsError::new(&format!("serialize delta: {e}")))
    }
}

impl BalanceTracker {
    fn process_inner(&mut self, response: &ArrowResponse) -> Result<BatchDelta> {
        let inner = &response.inner;

        // Build block_number → timestamp map for this chunk. Cheap; one batch
        // typically holds <1000 blocks, often single digits per chunk for an
        // address-narrow query like ours. Timestamps stay as Quantity bytes
        // until we need them.
        let mut timestamps: HashMap<u64, u64> = HashMap::new();
        for batch in &inner.data.blocks {
            for r in BlockReader::iter(batch) {
                let number = match r.number() {
                    Ok(n) => n,
                    Err(_) => continue,
                };
                if let Ok(ts) = r.timestamp() {
                    if let Some(v) = quantity_to_u64(ts.as_ref()) {
                        timestamps.insert(number, v);
                    }
                }
            }
        }

        let mut in_delta = U256::ZERO;
        let mut out_delta = U256::ZERO;
        let mut new_entries = Vec::new();

        for batch in &inner.data.logs {
            for r in LogReader::iter(batch) {
                // Topic0 must be the Transfer signature.
                let Some(t0) = r.topic0().ok().flatten() else {
                    continue;
                };
                if t0.as_ref() != TRANSFER_TOPIC0 {
                    continue;
                }

                let topic1 = r.topic1().ok().flatten();
                let topic2 = r.topic2().ok().flatten();

                // Indexed addresses are 32-byte left-padded; the address lives
                // in the last 20 bytes of the topic.
                let suffix_eq_wallet = |t: &hypersync_client::format::FixedSizeData<32>| {
                    let s = t.as_ref();
                    s.len() == 32 && s[12..32] == self.wallet
                };
                let from_match = topic1.as_ref().map(suffix_eq_wallet).unwrap_or(false);
                let to_match = topic2.as_ref().map(suffix_eq_wallet).unwrap_or(false);
                if !from_match && !to_match {
                    continue;
                }

                let data = r.data().context("read data")?;
                let amount = U256::try_from_be_slice(data.as_ref()).unwrap_or(U256::ZERO);

                let block_number: u64 = r.block_number().context("read block_number")?.into();
                let log_index: u64 = r.log_index().context("read log_index")?.into();
                let block_timestamp = timestamps.get(&block_number).copied();
                let tx_hash = r.transaction_hash().ok().map(|h| hex_prefixed(h.as_ref()));

                if from_match {
                    self.out_total = self.out_total.saturating_add(amount);
                    out_delta = out_delta.saturating_add(amount);
                    self.transfer_count += 1;
                    let counterparty = topic2
                        .as_ref()
                        .map(|t| hex_prefixed(&t.as_ref()[12..32]))
                        .unwrap_or_else(|| "0x".into());
                    let entry = LedgerEntry {
                        direction: "out",
                        amount: amount.to_string(),
                        counterparty,
                        block_number,
                        block_timestamp,
                        transaction_hash: tx_hash.clone(),
                        log_index,
                    };
                    push_front_capped(&mut self.ledger, entry.clone(), self.ledger_capacity);
                    new_entries.push(entry);
                }
                if to_match {
                    self.in_total = self.in_total.saturating_add(amount);
                    in_delta = in_delta.saturating_add(amount);
                    self.transfer_count += 1;
                    let counterparty = topic1
                        .as_ref()
                        .map(|t| hex_prefixed(&t.as_ref()[12..32]))
                        .unwrap_or_else(|| "0x".into());
                    let entry = LedgerEntry {
                        direction: "in",
                        amount: amount.to_string(),
                        counterparty,
                        block_number,
                        block_timestamp,
                        transaction_hash: tx_hash,
                        log_index,
                    };
                    push_front_capped(&mut self.ledger, entry.clone(), self.ledger_capacity);
                    new_entries.push(entry);
                }
            }
        }

        // Advance the high-watermark. `next_block` is the first block we
        // *haven't* served yet, so the last consumed block is one less, but
        // only if we actually moved forward.
        if inner.next_block > 0 {
            let consumed = inner.next_block - 1;
            self.last_block = Some(consumed);
            // Prefer an exact match; otherwise the highest timestamp we saw
            // in this chunk; otherwise leave whatever we had.
            let new_ts = timestamps
                .get(&consumed)
                .copied()
                .or_else(|| timestamps.values().copied().max());
            if let Some(ts) = new_ts {
                self.last_block_timestamp = Some(ts);
            }
        }

        // Signed cumulative balance as a decimal string.
        let balance = signed_diff_dec(self.in_total, self.out_total);

        Ok(BatchDelta {
            in_delta: in_delta.to_string(),
            out_delta: out_delta.to_string(),
            matches: new_entries.len() as u32,
            balance,
            in_total: self.in_total.to_string(),
            out_total: self.out_total.to_string(),
            transfer_count: self.transfer_count,
            last_block: self.last_block,
            last_block_timestamp: self.last_block_timestamp,
            ledger: self.ledger.iter().cloned().collect(),
            new_entries,
        })
    }
}

fn push_front_capped(buf: &mut VecDeque<LedgerEntry>, entry: LedgerEntry, cap: usize) {
    if cap == 0 {
        return;
    }
    if buf.len() == cap {
        buf.pop_back();
    }
    buf.push_front(entry);
}

/// Decimal-string of a signed difference of two `U256`s. Avoids pulling in a
/// signed-256 type for what is just display formatting.
fn signed_diff_dec(a: U256, b: U256) -> String {
    if a >= b {
        (a - b).to_string()
    } else {
        format!("-{}", (b - a))
    }
}

/// Parse a 20-byte address from a `0x`-prefixed (or bare) hex string.
fn parse_address_20(s: &str) -> Result<[u8; 20]> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() != 40 {
        bail!("expected 20-byte hex (40 chars), got {}", s.len());
    }
    let mut out = [0u8; 20];
    faster_hex::hex_decode(s.as_bytes(), &mut out).context("hex decode")?;
    Ok(out)
}

/// Hypersync `Quantity` is a big-endian byte slice. Block timestamps fit
/// comfortably in u64 (in fact in u32 until ~the year 2106).
fn quantity_to_u64(bytes: &[u8]) -> Option<u64> {
    if bytes.len() > 8 {
        // Truncate from the front (most-significant) only if the high bytes
        // are zero — otherwise we'd silently corrupt a large value.
        let extra = &bytes[..bytes.len() - 8];
        if extra.iter().any(|&b| b != 0) {
            return None;
        }
        let tail = &bytes[bytes.len() - 8..];
        return Some(u64::from_be_bytes(tail.try_into().ok()?));
    }
    let mut buf = [0u8; 8];
    buf[8 - bytes.len()..].copy_from_slice(bytes);
    Some(u64::from_be_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantity_round_trip() {
        assert_eq!(quantity_to_u64(&[]), Some(0));
        assert_eq!(quantity_to_u64(&[0x2a]), Some(42));
        assert_eq!(quantity_to_u64(&[0x01, 0x00]), Some(256));
        assert_eq!(
            quantity_to_u64(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
            Some(u64::MAX)
        );
        // Leading zero bytes truncate fine.
        assert_eq!(
            quantity_to_u64(&[0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
            Some(u64::MAX)
        );
        // Real overflow returns None rather than corrupting silently.
        assert_eq!(
            quantity_to_u64(&[0x01, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
            None
        );
    }

    #[test]
    fn signed_diff_dec_basics() {
        assert_eq!(signed_diff_dec(U256::from(5), U256::from(2)), "3");
        assert_eq!(signed_diff_dec(U256::from(2), U256::from(5)), "-3");
        assert_eq!(signed_diff_dec(U256::ZERO, U256::ZERO), "0");
    }

    #[test]
    fn ledger_capacity_is_enforced() {
        let mut buf = VecDeque::new();
        for i in 0..5 {
            push_front_capped(
                &mut buf,
                LedgerEntry {
                    direction: "in",
                    amount: i.to_string(),
                    counterparty: "0x".into(),
                    block_number: i,
                    block_timestamp: None,
                    transaction_hash: None,
                    log_index: 0,
                },
                3,
            );
        }
        // Newest first; capped at 3.
        assert_eq!(buf.len(), 3);
        assert_eq!(buf[0].block_number, 4);
        assert_eq!(buf[2].block_number, 2);
    }

    #[test]
    fn parse_address_with_or_without_prefix() {
        let want = [
            0xa0, 0xb8, 0x69, 0x91, 0xc6, 0x21, 0x8b, 0x36, 0xc1, 0xd1, 0x9d, 0x4a, 0x2e, 0x9e,
            0xb0, 0xce, 0x36, 0x06, 0xeb, 0x48,
        ];
        assert_eq!(
            parse_address_20("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48").unwrap(),
            want
        );
        assert_eq!(
            parse_address_20("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48").unwrap(),
            want
        );
        assert!(parse_address_20("0xnope").is_err());
    }
}
