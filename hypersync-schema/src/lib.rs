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

pub fn block_header() -> SchemaRef {
    Schema::from(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("hash", hash_dt(), false),
        Field::new("parent_hash", hash_dt(), false),
        Field::new("nonce", DataType::BinaryView, true),
        Field::new("sha3_uncles", hash_dt(), false),
        Field::new("logs_bloom", DataType::BinaryView, false),
        Field::new("transactions_root", hash_dt(), false),
        Field::new("state_root", hash_dt(), false),
        Field::new("receipts_root", hash_dt(), false),
        Field::new("miner", addr_dt(), false),
        Field::new("difficulty", quantity_dt(), true),
        Field::new("total_difficulty", quantity_dt(), true),
        Field::new("extra_data", DataType::BinaryView, false),
        Field::new("size", quantity_dt(), false),
        Field::new("gas_limit", quantity_dt(), false),
        Field::new("gas_used", quantity_dt(), false),
        Field::new("timestamp", quantity_dt(), false),
        Field::new("uncles", DataType::BinaryView, true),
        Field::new("base_fee_per_gas", quantity_dt(), true),
        Field::new("blob_gas_used", quantity_dt(), true),
        Field::new("excess_blob_gas", quantity_dt(), true),
        Field::new("parent_beacon_block_root", hash_dt(), true),
        Field::new("withdrawals_root", hash_dt(), true),
        Field::new("withdrawals", DataType::BinaryView, true),
        Field::new("l1_block_number", DataType::UInt64, true),
        Field::new("send_count", quantity_dt(), true),
        Field::new("send_root", hash_dt(), true),
        Field::new("mix_hash", hash_dt(), true),
    ])
    .into()
}

pub fn transaction() -> SchemaRef {
    Schema::from(vec![
        Field::new("block_hash", hash_dt(), false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("from", addr_dt(), true),
        Field::new("gas", quantity_dt(), false),
        Field::new("gas_price", quantity_dt(), true),
        Field::new("hash", hash_dt(), false),
        Field::new("input", DataType::BinaryView, false),
        Field::new("nonce", quantity_dt(), false),
        Field::new("to", addr_dt(), true),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("value", quantity_dt(), false),
        Field::new("v", quantity_dt(), true),
        Field::new("r", quantity_dt(), true),
        Field::new("s", quantity_dt(), true),
        Field::new("max_priority_fee_per_gas", quantity_dt(), true),
        Field::new("max_fee_per_gas", quantity_dt(), true),
        Field::new("chain_id", quantity_dt(), true),
        Field::new("cumulative_gas_used", quantity_dt(), false),
        Field::new("effective_gas_price", quantity_dt(), false),
        Field::new("gas_used", quantity_dt(), false),
        Field::new("contract_address", addr_dt(), true),
        Field::new("logs_bloom", DataType::BinaryView, false),
        Field::new("type", DataType::UInt8, true),
        Field::new("root", hash_dt(), true),
        Field::new("status", DataType::UInt8, true),
        Field::new("sighash", DataType::BinaryView, true),
        Field::new("y_parity", quantity_dt(), true),
        Field::new("access_list", DataType::BinaryView, true),
        Field::new("l1_fee", quantity_dt(), true),
        Field::new("l1_gas_price", quantity_dt(), true),
        Field::new("l1_gas_used", quantity_dt(), true),
        Field::new("l1_fee_scalar", quantity_dt(), true),
        Field::new("gas_used_for_l1", quantity_dt(), true),
        Field::new("max_fee_per_blob_gas", quantity_dt(), true),
        Field::new("blob_versioned_hashes", DataType::BinaryView, true),
        Field::new("blob_gas_price", quantity_dt(), true),
        Field::new("deposit_nonce", quantity_dt(), true),
        Field::new("deposit_receipt_version", quantity_dt(), true),
        Field::new("blob_gas_used", quantity_dt(), true),
        Field::new("l1_base_fee_scalar", quantity_dt(), true),
        Field::new("l1_blob_base_fee", quantity_dt(), true),
        Field::new("l1_blob_base_fee_scalar", quantity_dt(), true),
        Field::new("l1_block_number", quantity_dt(), true),
        Field::new("mint", quantity_dt(), true),
        Field::new("source_hash", hash_dt(), true),
    ])
    .into()
}

pub fn log() -> SchemaRef {
    Schema::from(vec![
        Field::new("removed", DataType::Boolean, true),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("transaction_hash", hash_dt(), false),
        Field::new("block_hash", hash_dt(), false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("address", addr_dt(), false),
        Field::new("data", DataType::BinaryView, false),
        Field::new("topic0", DataType::BinaryView, true),
        Field::new("topic1", DataType::BinaryView, true),
        Field::new("topic2", DataType::BinaryView, true),
        Field::new("topic3", DataType::BinaryView, true),
    ])
    .into()
}

pub fn trace() -> SchemaRef {
    Schema::from(vec![
        Field::new("from", addr_dt(), true),
        Field::new("to", addr_dt(), true),
        Field::new("call_type", DataType::Utf8View, true),
        Field::new("gas", quantity_dt(), true),
        Field::new("input", DataType::BinaryView, true),
        Field::new("init", DataType::BinaryView, true),
        Field::new("value", quantity_dt(), true),
        Field::new("author", addr_dt(), true),
        Field::new("reward_type", DataType::Utf8View, true),
        Field::new("block_hash", DataType::BinaryView, false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("address", addr_dt(), true),
        Field::new("code", DataType::BinaryView, true),
        Field::new("gas_used", quantity_dt(), true),
        Field::new("output", DataType::BinaryView, true),
        Field::new("subtraces", DataType::UInt64, true),
        Field::new("trace_address", DataType::BinaryView, true),
        Field::new("transaction_hash", DataType::BinaryView, true),
        Field::new("transaction_position", DataType::UInt64, true),
        Field::new("type", DataType::Utf8View, true),
        Field::new("error", DataType::Utf8View, true),
        Field::new("sighash", DataType::BinaryView, true),
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
