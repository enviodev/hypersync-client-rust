use hypersync_net_types::*;
use pretty_assertions::assert_eq;
use serde_json::Value;

#[test]
fn test_old_log_selection_deserialization() {
    let old_format = r#"{
        "address": ["0x6B175474E89094C44Da98b954EedeAC495271d0F"],
        "topics": [
            ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
        ]
    }"#;

    let log_selection: LogSelection =
        serde_json::from_str(old_format).expect("Should be able to deserialize old format");

    assert_eq!(log_selection.include.address.len(), 1);
    assert_eq!(log_selection.include.topics.len(), 1);
    assert!(log_selection.exclude.is_none());

    // Test that serialized form can be deserialized back correctly
    let serialized_val = serde_json::to_value(&log_selection).unwrap();
    let original_val: Value = serde_json::from_str(old_format).unwrap();
    assert_eq!(
        serialized_val, original_val,
        "Serializing new format without exclude should be the same as old format"
    );
}

#[test]
fn test_old_transaction_selection_deserialization() {
    let old_format = r#"{
        "from": ["0xD1a923D70510814EaE7695A76326201cA06d080F"],
        "to": ["0xc0A101c4E9Bb4463BD2F5d6833c2276C36914Fb6"]
    }"#;

    let tx_selection: TransactionSelection =
        serde_json::from_str(old_format).expect("Should be able to deserialize old format");

    assert_eq!(tx_selection.include.from.len(), 1);
    assert_eq!(tx_selection.include.to.len(), 1);
    assert!(tx_selection.exclude.is_none());

    // Test that serialized form can be deserialized back correctly
    let serialized_val = serde_json::to_value(&tx_selection).unwrap();
    let original_val: Value = serde_json::from_str(old_format).unwrap();
    assert_eq!(
        serialized_val, original_val,
        "Serializing new format without exclude should be the same as old format"
    );
}

#[test]
fn test_old_block_selection_deserialization() {
    let old_format = r#"{
        "miner": ["0x1234567890123456789012345678901234567890"]
    }"#;

    let block_selection: BlockSelection =
        serde_json::from_str(old_format).expect("Should be able to deserialize old format");

    assert_eq!(block_selection.include.miner.len(), 1);
    assert!(block_selection.exclude.is_none());

    // Test that serialized form can be deserialized back correctly
    let serialized_val = serde_json::to_value(&block_selection).unwrap();
    let original_val: Value = serde_json::from_str(old_format).unwrap();
    assert_eq!(
        serialized_val, original_val,
        "Serializing new format without exclude should be the same as old format"
    );
}

#[test]
fn test_old_trace_selection_deserialization() {
    let old_format = r#"{
        "from": ["0x1234567890123456789012345678901234567890"],
        "call_type": ["call"]
    }"#;

    let trace_selection: TraceSelection =
        serde_json::from_str(old_format).expect("Should be able to deserialize old format");

    assert_eq!(trace_selection.include.from.len(), 1);
    assert_eq!(trace_selection.include.call_type.len(), 1);
    assert!(trace_selection.exclude.is_none());

    // Test that serialized form can be deserialized back correctly
    let serialized_val = serde_json::to_value(&trace_selection).unwrap();
    let original_val: Value = serde_json::from_str(old_format).unwrap();
    assert_eq!(
        serialized_val, original_val,
        "Serializing new format without exclude should be the same as old format"
    );
}

#[test]
fn test_old_query_deserialization() {
    let old_format = r#"{
        "from_block": 10123123,
        "logs": [
            {
                "topics": [
                    ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
                ]
            }
        ],
        "transactions": [
            {"from": ["0xD1a923D70510814EaE7695A76326201cA06d080F"]},
            {"to": ["0xc0A101c4E9Bb4463BD2F5d6833c2276C36914Fb6"]}
        ],
        "field_selection": {
            "log": [
                "data",
                "topic0",
                "topic1",
                "topic2",
                "topic3"
            ]
        }
    }"#;

    let query: Query =
        serde_json::from_str(old_format).expect("Should be able to deserialize old format");

    assert_eq!(query.from_block, 10123123);
    assert_eq!(query.logs.len(), 1);
    assert_eq!(query.transactions.len(), 2);

    let log_selection = &query.logs[0];
    assert_eq!(log_selection.include.topics.len(), 1);
    assert!(log_selection.exclude.is_none());

    let tx_selection_1 = &query.transactions[0];
    assert_eq!(tx_selection_1.include.from.len(), 1);
    assert!(tx_selection_1.exclude.is_none());

    let tx_selection_2 = &query.transactions[1];
    assert_eq!(tx_selection_2.include.to.len(), 1);
    assert!(tx_selection_2.exclude.is_none());

    // Test that serialized form can be deserialized back correctly
    let serialized_val = serde_json::to_value(&query).unwrap();
    let original_val: Value = serde_json::from_str(old_format).unwrap();
    assert_eq!(
        serialized_val, original_val,
        "Serializing new format without exclude should be the same as old format"
    );
}

#[test]
fn test_complex_old_query_deserialization() {
    let old_format = r#"{
        "from_block": 0,
        "logs": [
            {
                "topics": [
                    ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
                    [],
                    ["0x000000000000000000000000D1a923D70510814EaE7695A76326201cA06d080F"],
                    []
                ]
            },
            {
                "topics": [
                    ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
                    ["0x000000000000000000000000D1a923D70510814EaE7695A76326201cA06d080F"],
                    [],
                    []
                ]
            }
        ],
        "transactions": [
            {"from": ["0xD1a923D70510814EaE7695A76326201cA06d080F", "0xc0A101c4E9Bb4463BD2F5d6833c2276C36914Fb6"]},
            {"to": ["0xa0FBaEdC4C110f5A0c5E96c3eeAC9B5635b74CE7", "0x32448eb389aBe39b20d5782f04a8d71a2b2e7189"]}
        ]
    }"#;

    let query: Query =
        serde_json::from_str(old_format).expect("Should be able to deserialize old format");

    assert_eq!(query.from_block, 0);
    assert_eq!(query.logs.len(), 2);
    assert_eq!(query.transactions.len(), 2);

    let log_selection_1 = &query.logs[0];
    assert_eq!(log_selection_1.include.topics.len(), 4);
    assert_eq!(log_selection_1.include.topics[2].len(), 1);

    let log_selection_2 = &query.logs[1];
    assert_eq!(log_selection_2.include.topics.len(), 4);
    assert_eq!(log_selection_2.include.topics[1].len(), 1);

    let tx_selection_1 = &query.transactions[0];
    assert_eq!(tx_selection_1.include.from.len(), 2);

    let tx_selection_2 = &query.transactions[1];
    assert_eq!(tx_selection_2.include.to.len(), 2);

    // Test that serialized form can be deserialized back correctly
    let serialized_val = serde_json::to_value(&query).unwrap();
    let original_val: Value = serde_json::from_str(old_format).unwrap();
    assert_eq!(
        serialized_val, original_val,
        "Serializing new format without exclude should be the same as old format"
    );
}
