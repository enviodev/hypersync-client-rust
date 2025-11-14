//! # HyperSync Schema
//!
//! Apache Arrow schemas and data transformation utilities for the HyperSync protocol.
//!
//! This crate provides the Arrow schema definitions and data transformation
//! utilities used by HyperSync for high-performance columnar data processing.
//! It bridges the gap between HyperSync's native data formats and Apache Arrow.
//!
//! ## Features
//!
//! - **Arrow schemas**: Predefined schemas for blocks, transactions, logs, and traces
//! - **Data transformation**: Utilities for converting between formats
//! - **High performance**: Optimized columnar data operations
//! - **Schema projection**: Select only needed columns for memory efficiency
//!
//! ## Key Functions
//!
//! - [`block_schema()`] - Get Arrow schema for block data
//! - [`transaction_schema()`] - Get Arrow schema for transaction data  
//! - [`log_schema()`] - Get Arrow schema for log/event data
//! - [`trace_schema()`] - Get Arrow schema for trace data
//! - [`project_schema()`] - Project schema to subset of columns
//! - [`concat_chunks()`] - Efficiently concatenate Arrow chunks
//!
//! ## Example
//!
//! ```
//! use hypersync_schema::{transaction, log, project_schema};
//! use std::collections::BTreeSet;
//!
//! // Get schema for transaction data
//! let tx_schema = transaction();
//! println!("Transaction schema has {} fields", tx_schema.fields.len());
//!
//! // Get schema for log data  
//! let log_schema = log();
//! println!("Log schema has {} fields", log_schema.fields.len());
//!
//! // Project to subset of fields
//! let fields: BTreeSet<String> = ["hash", "from"].iter().map(|s| s.to_string()).collect();
//! let projected = project_schema(&tx_schema, &fields);
//! println!("Projected schema has {} fields", projected.fields.len());
//! ```

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use polars_arrow::array::{new_empty_array, Array, BinaryArray, Utf8Array};
use polars_arrow::compute;
use polars_arrow::datatypes::{ArrowDataType as DataType, ArrowSchema as Schema, Field, SchemaRef};
use polars_arrow::record_batch::RecordBatchT as Chunk;

mod util;

pub use util::project_schema;

pub type ArrowChunk = Chunk<Box<dyn Array>>;

fn hash_dt() -> DataType {
    DataType::BinaryView
}

fn addr_dt() -> DataType {
    DataType::BinaryView
}

fn quantity_dt() -> DataType {
    DataType::BinaryView
}

const NULLABLE: bool = true;

pub fn block_header() -> SchemaRef {
    Schema::from(vec![
        Field::new("number", DataType::UInt64, !NULLABLE),
        Field::new("hash", hash_dt(), !NULLABLE),
        Field::new("parent_hash", hash_dt(), !NULLABLE),
        Field::new("nonce", DataType::BinaryView, NULLABLE),
        Field::new("sha3_uncles", hash_dt(), !NULLABLE),
        Field::new("logs_bloom", DataType::BinaryView, !NULLABLE),
        Field::new("transactions_root", hash_dt(), !NULLABLE),
        Field::new("state_root", hash_dt(), !NULLABLE),
        Field::new("receipts_root", hash_dt(), !NULLABLE),
        Field::new("miner", addr_dt(), !NULLABLE),
        Field::new("difficulty", quantity_dt(), NULLABLE),
        Field::new("total_difficulty", quantity_dt(), NULLABLE),
        Field::new("extra_data", DataType::BinaryView, !NULLABLE),
        Field::new("size", quantity_dt(), !NULLABLE),
        Field::new("gas_limit", quantity_dt(), !NULLABLE),
        Field::new("gas_used", quantity_dt(), !NULLABLE),
        Field::new("timestamp", quantity_dt(), !NULLABLE),
        Field::new("uncles", DataType::BinaryView, NULLABLE),
        Field::new("base_fee_per_gas", quantity_dt(), NULLABLE),
        Field::new("blob_gas_used", quantity_dt(), NULLABLE),
        Field::new("excess_blob_gas", quantity_dt(), NULLABLE),
        Field::new("parent_beacon_block_root", hash_dt(), NULLABLE),
        Field::new("withdrawals_root", hash_dt(), NULLABLE),
        Field::new("withdrawals", DataType::BinaryView, NULLABLE),
        Field::new("l1_block_number", DataType::UInt64, NULLABLE),
        Field::new("send_count", quantity_dt(), NULLABLE),
        Field::new("send_root", hash_dt(), NULLABLE),
        Field::new("mix_hash", hash_dt(), NULLABLE),
    ])
    .into()
}

pub fn transaction() -> SchemaRef {
    Schema::from(vec![
        Field::new("block_hash", hash_dt(), !NULLABLE),
        Field::new("block_number", DataType::UInt64, !NULLABLE),
        Field::new("from", addr_dt(), NULLABLE),
        Field::new("gas", quantity_dt(), !NULLABLE),
        Field::new("gas_price", quantity_dt(), NULLABLE),
        Field::new("hash", hash_dt(), !NULLABLE),
        Field::new("input", DataType::BinaryView, !NULLABLE),
        Field::new("nonce", quantity_dt(), !NULLABLE),
        Field::new("to", addr_dt(), NULLABLE),
        Field::new("transaction_index", DataType::UInt64, !NULLABLE),
        Field::new("value", quantity_dt(), !NULLABLE),
        Field::new("v", quantity_dt(), NULLABLE),
        Field::new("r", quantity_dt(), NULLABLE),
        Field::new("s", quantity_dt(), NULLABLE),
        Field::new("max_priority_fee_per_gas", quantity_dt(), NULLABLE),
        Field::new("max_fee_per_gas", quantity_dt(), NULLABLE),
        Field::new("chain_id", quantity_dt(), NULLABLE),
        Field::new("cumulative_gas_used", quantity_dt(), !NULLABLE),
        Field::new("effective_gas_price", quantity_dt(), !NULLABLE),
        Field::new("gas_used", quantity_dt(), !NULLABLE),
        Field::new("contract_address", addr_dt(), NULLABLE),
        Field::new("logs_bloom", DataType::BinaryView, !NULLABLE),
        Field::new("type", DataType::UInt8, NULLABLE),
        Field::new("root", hash_dt(), NULLABLE),
        Field::new("status", DataType::UInt8, NULLABLE),
        Field::new("sighash", DataType::BinaryView, NULLABLE),
        Field::new("y_parity", quantity_dt(), NULLABLE),
        Field::new("access_list", DataType::BinaryView, NULLABLE),
        Field::new("authorization_list", DataType::BinaryView, NULLABLE),
        Field::new("l1_fee", quantity_dt(), NULLABLE),
        Field::new("l1_gas_price", quantity_dt(), NULLABLE),
        Field::new("l1_gas_used", quantity_dt(), NULLABLE),
        Field::new("l1_fee_scalar", quantity_dt(), NULLABLE),
        Field::new("gas_used_for_l1", quantity_dt(), NULLABLE),
        Field::new("max_fee_per_blob_gas", quantity_dt(), NULLABLE),
        Field::new("blob_versioned_hashes", DataType::BinaryView, NULLABLE),
        Field::new("deposit_nonce", quantity_dt(), NULLABLE),
        Field::new("blob_gas_price", quantity_dt(), NULLABLE),
        Field::new("deposit_receipt_version", quantity_dt(), NULLABLE),
        Field::new("blob_gas_used", quantity_dt(), NULLABLE),
        Field::new("l1_base_fee_scalar", quantity_dt(), NULLABLE),
        Field::new("l1_blob_base_fee", quantity_dt(), NULLABLE),
        Field::new("l1_blob_base_fee_scalar", quantity_dt(), NULLABLE),
        Field::new("l1_block_number", quantity_dt(), NULLABLE),
        Field::new("mint", quantity_dt(), NULLABLE),
        Field::new("source_hash", hash_dt(), NULLABLE),
    ])
    .into()
}

pub fn log() -> SchemaRef {
    Schema::from(vec![
        Field::new("removed", DataType::Boolean, NULLABLE),
        Field::new("log_index", DataType::UInt64, !NULLABLE),
        Field::new("transaction_index", DataType::UInt64, !NULLABLE),
        Field::new("transaction_hash", hash_dt(), !NULLABLE),
        Field::new("block_hash", hash_dt(), !NULLABLE),
        Field::new("block_number", DataType::UInt64, !NULLABLE),
        Field::new("address", addr_dt(), !NULLABLE),
        Field::new("data", DataType::BinaryView, !NULLABLE),
        Field::new("topic0", DataType::BinaryView, NULLABLE),
        Field::new("topic1", DataType::BinaryView, NULLABLE),
        Field::new("topic2", DataType::BinaryView, NULLABLE),
        Field::new("topic3", DataType::BinaryView, NULLABLE),
    ])
    .into()
}

pub fn trace() -> SchemaRef {
    Schema::from(vec![
        Field::new("from", addr_dt(), NULLABLE),
        Field::new("to", addr_dt(), NULLABLE),
        Field::new("call_type", DataType::Utf8View, NULLABLE),
        Field::new("gas", quantity_dt(), NULLABLE),
        Field::new("input", DataType::BinaryView, NULLABLE),
        Field::new("init", DataType::BinaryView, NULLABLE),
        Field::new("value", quantity_dt(), NULLABLE),
        Field::new("author", addr_dt(), NULLABLE),
        Field::new("reward_type", DataType::Utf8View, NULLABLE),
        Field::new("block_hash", DataType::BinaryView, !NULLABLE),
        Field::new("block_number", DataType::UInt64, !NULLABLE),
        Field::new("address", addr_dt(), NULLABLE),
        Field::new("code", DataType::BinaryView, NULLABLE),
        Field::new("gas_used", quantity_dt(), NULLABLE),
        Field::new("output", DataType::BinaryView, NULLABLE),
        Field::new("subtraces", DataType::UInt64, NULLABLE),
        Field::new("trace_address", DataType::BinaryView, NULLABLE),
        Field::new("transaction_hash", DataType::BinaryView, NULLABLE),
        Field::new("transaction_position", DataType::UInt64, NULLABLE),
        Field::new("type", DataType::Utf8View, NULLABLE),
        Field::new("error", DataType::Utf8View, NULLABLE),
        Field::new("sighash", DataType::BinaryView, NULLABLE),
        Field::new("action_address", addr_dt(), NULLABLE),
        Field::new("balance", quantity_dt(), NULLABLE),
        Field::new("refund_address", addr_dt(), NULLABLE),
    ])
    .into()
}

pub fn concat_chunks(chunks: &[Arc<ArrowChunk>]) -> Result<ArrowChunk> {
    if chunks.is_empty() {
        return Err(anyhow!("can't concat 0 chunks"));
    }

    let num_cols = chunks[0].columns().len();

    let cols = (0..num_cols)
        .map(|col| {
            let mut is_utf8 = false;
            let arrs = chunks
                .iter()
                .map(|chunk| {
                    let col = chunk
                        .columns()
                        .get(col)
                        .map(|col| col.as_ref())
                        .context("get column")?;
                    is_utf8 = col.data_type() == &DataType::Utf8;
                    Ok(col)
                })
                .collect::<Result<Vec<_>>>()?;
            if !is_utf8 {
                compute::concatenate::concatenate(&arrs).context("concat arrays")
            } else {
                let arrs = arrs
                    .into_iter()
                    .map(|a| {
                        a.as_any()
                            .downcast_ref::<Utf8Array<i32>>()
                            .unwrap()
                            .to_binary()
                            .boxed()
                    })
                    .collect::<Vec<_>>();
                let arrs = arrs.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
                let arr =
                    compute::concatenate::concatenate(arrs.as_slice()).context("concat arrays")?;

                Ok(compute::cast::binary_to_utf8(
                    arr.as_any().downcast_ref::<BinaryArray<i32>>().unwrap(),
                    DataType::Utf8,
                )
                .unwrap()
                .boxed())
            }
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ArrowChunk::new(cols))
}

pub fn empty_chunk(schema: &Schema) -> ArrowChunk {
    let mut cols = Vec::new();
    for field in schema.fields.iter() {
        cols.push(new_empty_array(field.data_type().clone()));
    }
    ArrowChunk::new(cols)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_test_schema_constructors() {
        block_header();
        transaction();
        log();
        trace();
    }

    #[test]
    fn test_concat_utf8() {
        let chunks = [
            Arc::new(Chunk::new(vec![Utf8Array::<i32>::from(&[Some(
                "hello".to_owned(),
            )])
            .boxed()])),
            Arc::new(Chunk::new(vec![Utf8Array::<i32>::from(&[Some(
                "world".to_owned(),
            )])
            .boxed()])),
        ];

        let out = concat_chunks(&chunks).unwrap();

        assert_eq!(
            out,
            ArrowChunk::new(vec![Utf8Array::<i32>::from(&[
                Some("hello".to_owned()),
                Some("world".to_owned())
            ])
            .boxed(),])
        )
    }
}
