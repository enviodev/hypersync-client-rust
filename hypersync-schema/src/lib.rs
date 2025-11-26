//! # HyperSync Schema
//!
//! Apache Arrow schemas for the HyperSync protocol.

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

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
    Schema::new(vec![
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
    Schema::new(vec![
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
    Schema::new(vec![
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
    Schema::new(vec![
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
}
