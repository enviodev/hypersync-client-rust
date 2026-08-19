// These tests are here to check that we can deserialize from
// RPC Responses returned from providers.
// These should be expanded in the future as we add support for
// more providers/chains

use hypersync_format::*;

use serde::de::{DeserializeOwned, Error};

pub fn deserialize_with_path<T>(json: &str) -> std::result::Result<T, serde_json::Error>
where
    T: DeserializeOwned,
{
    let deserializer = &mut serde_json::Deserializer::from_str(json);
    match serde_path_to_error::deserialize(deserializer) {
        Ok(value) => Ok(value),
        Err(e) => Err(serde_json::Error::custom(format!(
            "Deserialization failed at path '{}' due to error: {}",
            e.path(),
            e.inner()
        ))),
    }
}

fn read_json_file(name: &str) -> String {
    std::fs::read_to_string(format!("{}/test-data/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap()
}

#[test]
fn test_block_without_tx_deserialize() {
    let file = read_json_file("block_without_tx.json");
    let _: Block<Hash> = serde_json::from_str(&file).unwrap();
}

#[test]
fn test_block_with_tx_deserialize() {
    let file = read_json_file("block_with_tx.json");
    let _: Block<Transaction> = serde_json::from_str(&file).unwrap();
}

#[test]
fn test_transaction_deserialize() {
    let file = read_json_file("transaction.json");
    let _: Transaction = serde_json::from_str(&file).unwrap();
}

#[test]
fn test_eip7702_transaction_deserialize() {
    let file = read_json_file("eip7702_transaction.json");
    let _: Transaction = serde_json::from_str(&file).unwrap();
}

#[test]
fn test_transaction_receipt_deserialize() {
    let file = read_json_file("transaction_receipt.json");
    let _: TransactionReceipt = serde_json::from_str(&file).unwrap();
}

#[test]
fn test_log_deserialize() {
    let file = read_json_file("log.json");
    let _: Log = serde_json::from_str(&file).unwrap();
}

#[test]
fn test_arbitrum_receipt() {
    let file = read_json_file("arbitrum_tx_receipt.json");
    let _: TransactionReceipt = serde_json::from_str(&file).unwrap();
}

#[test]
fn test_arbitrum_receipt2() {
    let file = read_json_file("arbitrum_tx_receipt2.json");
    let _: TransactionReceipt = serde_json::from_str(&file).unwrap();
}

// Stock nitro v3.11.2 marshals the receipt's l1BlockNumber as `L1BlockNumber`
// (wrong case; fixed in v3.11.3). Chainstack's arbitrum head fleet served this
// shape in production (2026-08-19); the field must parse, not read as null.
#[test]
fn test_arbitrum_receipt_nitro_v3_11_2_miscased_l1_block_number() {
    let file = read_json_file("arbitrum_tx_receipt_nitro_v3_11_2_miscased.json");
    let receipt: TransactionReceipt = deserialize_with_path(&file).unwrap();
    // Compare via JSON round-trip so we don't depend on Quantity's internals.
    assert_eq!(
        serde_json::to_value(&receipt.l1_block_number).unwrap(),
        serde_json::json!("0x18987ff")
    );
    assert_eq!(
        serde_json::to_value(&receipt.gas_used_for_l1).unwrap(),
        serde_json::json!("0x8b6")
    );
}

// The correctly-cased key must keep working, and the alias must not affect
// serialization: a round-tripped receipt emits camelCase `l1BlockNumber`.
#[test]
fn test_arbitrum_receipt_l1_block_number_roundtrip() {
    let file = read_json_file("arbitrum_tx_receipt.json");
    let receipt: TransactionReceipt = deserialize_with_path(&file).unwrap();
    assert!(receipt.l1_block_number.is_some());
    let out = serde_json::to_string(&receipt).unwrap();
    assert!(out.contains("\"l1BlockNumber\""));
    assert!(!out.contains("\"L1BlockNumber\""));
}

#[test]
fn test_base_receipt() {
    let file = read_json_file("base_tx_receipt.json");
    let _: TransactionReceipt = serde_json::from_str(&file).unwrap();
}

#[test]
fn test_optimism_receipt() {
    let file = read_json_file("optimism_tx_receipt.json");
    let _: TransactionReceipt = serde_json::from_str(&file).unwrap();
}

#[test]
fn test_tron_block_without_tx_deserialize() {
    let file = read_json_file("tron_block_without_tx.json");
    let _: Block<Hash> = deserialize_with_path(&file).unwrap();
}
