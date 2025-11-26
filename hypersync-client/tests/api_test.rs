use std::{collections::BTreeSet, env::temp_dir, sync::Arc};

use alloy_json_abi::JsonAbi;
use arrow::{array::AsArray, datatypes::UInt64Type};
use hypersync_client::{
    preset_query, simple_types::Transaction, Client, ColumnMapping, SerializationFormat,
    StreamConfig,
};
use hypersync_format::{Address, FilterWrapper, Hex, LogArgument};
use hypersync_net_types::{
    block::BlockField, log::LogField, transaction::TransactionField, FieldSelection, LogSelection,
    Query, TransactionFilter, TransactionSelection,
};

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_arrow_ipc() {
    let client = Client::builder()
        .url("https://eth.hypersync.xyz")
        .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
        .build()
        .unwrap();

    let mut block_field_selection = BTreeSet::new();
    block_field_selection.insert(BlockField::Number);
    block_field_selection.insert(BlockField::Timestamp);
    block_field_selection.insert(BlockField::Hash);

    let res = client
        .get_arrow(&Query {
            from_block: 14000000,
            to_block: None,
            logs: Vec::new(),
            transactions: Vec::new(),
            include_all_blocks: true,
            field_selection: FieldSelection {
                block: block_field_selection,
                log: Default::default(),
                transaction: Default::default(),
                trace: Default::default(),
            },
            ..Default::default()
        })
        .await
        .unwrap();

    dbg!(res.next_block);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_arrow_ipc_ordering() {
    let client = Client::builder()
        .url("https://eth.hypersync.xyz")
        .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
        .build()
        .unwrap();

    let mut block_field_selection = BTreeSet::new();
    block_field_selection.insert("number".to_owned());

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 13171881,
        "to_block": 18270333,
        "logs": [
            {
                "address": [
                    "0x15b7c0c907e4C6b9AdaAaabC300C08991D6CEA05"
                ],
                "topics": [
                    [
                        "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                    ]
                ]
            }
        ],
        "field_selection": {
            "block": [
                "number"
            ],
            "log": [
                "log_index",
                "block_number"
            ]
        }
    }))
    .unwrap();

    let res = client.get_arrow(&query).await.unwrap();

    assert!(res.next_block > 13223105);

    let mut last = (0, 0);
    for batch in res.data.logs {
        let block_number = batch
            .column_by_name("block_number")
            .unwrap()
            .as_primitive::<UInt64Type>();
        let log_index = batch
            .column_by_name("log_index")
            .unwrap()
            .as_primitive::<UInt64Type>();

        for (block_number, log_index) in block_number.iter().zip(log_index.iter()) {
            let block_number = block_number.unwrap();
            let log_index = log_index.unwrap();
            let number = (block_number, log_index);
            assert!(last < number, "last: {:?};number: {:?};", last, number);
            last = number;
        }
    }
}

fn get_file_path(name: &str) -> String {
    format!("{}/test-data/{name}", env!("CARGO_MANIFEST_DIR"))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_decode_logs() {
    env_logger::try_init().ok();

    const ADDR: &str = "0xc18360217d8f7ab5e7c516566761ea12ce7f9d72";

    let client = Arc::new(
        Client::builder()
            .url("https://eth.hypersync.xyz")
            .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
            .build()
            .unwrap(),
    );

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 18680952,
        "to_block": 18680953,
        "logs": [
            {
                "address": [
                    ADDR
                ]
            }
        ],
        "field_selection": {
            "log": [
                "address",
                "data",
                "topic0",
                "topic1",
                "topic2",
                "topic3"
            ]
        }
    }))
    .unwrap();

    let mut rx = client
        .stream_arrow(
            query,
            StreamConfig {
                event_signature: Some(
                    "Transfer(address indexed from, address indexed to, uint indexed amount)"
                        .into(),
                ),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let res = rx.recv().await.unwrap().unwrap();

    let decoded_logs = res.data.decoded_logs;

    dbg!(res.data.logs);

    assert_eq!(decoded_logs[0].num_rows(), 1);

    println!("{:?}", decoded_logs[0]);
}

#[test]
fn parse_nameless_abi() {
    let path = get_file_path("nameless.abi.json");
    let abi = std::fs::read_to_string(path).unwrap();
    let _abi: JsonAbi = serde_json::from_str(&abi).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_get_events_without_join_fields() {
    env_logger::try_init().ok();

    let client = Client::builder()
        .url("https://base.hypersync.xyz")
        .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
        .build()
        .unwrap();

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 6589327,
        "to_block": 6589328,
        "logs": [{
            "address": ["0xd981ed72b1b3bf866563a9755d41a887d3e4721a"],
            "topics": [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]],
        }],
        "field_selection": {
            "log": ["block_number", "topic0", "topic1", "topic2", "topic3", "data", "address"],
            "transaction": ["value"],
            "block": ["gas_used"],
        }
    }))
    .unwrap();

    let res = client.get_events(query).await.unwrap();

    dbg!(res.data);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_stream_decode_with_invalid_log() {
    env_logger::try_init().ok();

    let client = Client::builder()
        .url("https://base.hypersync.xyz")
        .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
        .build()
        .unwrap();
    let client = Arc::new(client);

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 6589327,
        "to_block": 6589328,
        "logs": [{
            "address": ["0xd981ed72b1b3bf866563a9755d41a887d3e4721a"],
            "topics": [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]],
        }],
        "field_selection": {
            "log": ["block_number", "topic0", "topic1", "topic2", "topic3", "data", "address"],
        }
    }))
    .unwrap();

    let data = client
        .collect_arrow(
            query,
            StreamConfig {
                column_mapping: Some(ColumnMapping {
                    block: maplit::btreemap! {
                        "number".to_owned() => hypersync_client::DataType::Float32,
                    },
                    transaction: maplit::btreemap! {
                        "value".to_owned() => hypersync_client::DataType::Float64,
                    },
                    log: Default::default(),
                    trace: Default::default(),
                    decoded_log: maplit::btreemap! {
                        "amount".to_owned() => hypersync_client::DataType::Float64,
                    },
                }),
                event_signature: Some(
                    "Transfer(address indexed from, address indexed to, uint indexed amount)"
                        .into(),
                ),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    dbg!(data);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_parquet_out() {
    env_logger::try_init().ok();

    let client = Arc::new(
        Client::builder()
            .url("https://eth.hypersync.xyz")
            .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
            .build()
            .unwrap(),
    );

    let path = format!("{}/{}", temp_dir().to_string_lossy(), uuid::Uuid::new_v4());

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 19277345,
        "to_block": 19277346,
        "logs": [{
            "address": ["0xdAC17F958D2ee523a2206206994597C13D831ec7"],
            "topics": [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]],
        }],
        "transactions": [{}],
        "include_all_blocks": true,
        "field_selection": {
            "log": ["block_number", "topic0", "topic1", "topic2", "topic3", "data", "address"],
        }
    }))
    .unwrap();

    client
        .collect_parquet(
            &path,
            query,
            StreamConfig {
                column_mapping: Some(ColumnMapping {
                    block: maplit::btreemap! {
                        "number".to_owned() => hypersync_client::DataType::Float32,
                    },
                    transaction: maplit::btreemap! {
                        "value".to_owned() => hypersync_client::DataType::Float64,
                    },
                    log: Default::default(),
                    trace: Default::default(),
                    decoded_log: maplit::btreemap! {
                        //"amount".to_owned() => hypersync_client::DataType::Float64,
                    },
                }),
                event_signature: Some(
                    "Transfer(address indexed from, address indexed to, uint indexed amount)"
                        .into(),
                ),
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_preset_query_blocks_and_transactions() {
    let client = Arc::new(
        Client::builder()
            .url("https://eth.hypersync.xyz")
            .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
            .build()
            .unwrap(),
    );
    let query = preset_query::blocks_and_transactions(18_000_000, Some(18_000_010));
    let res = client.get_arrow(&query).await.unwrap();

    let num_blocks: usize = res
        .data
        .blocks
        .into_iter()
        .map(|batch| batch.num_rows())
        .sum();
    let num_txs: usize = res
        .data
        .transactions
        .into_iter()
        .map(|batch| batch.num_rows())
        .sum();

    assert_eq!(res.next_block, 18_000_010);
    assert_eq!(num_blocks, 10);
    assert!(num_txs > 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_preset_query_blocks_and_transaction_hashes() {
    let client = Client::builder()
        .url("https://eth.hypersync.xyz")
        .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
        .build()
        .unwrap();
    let query = preset_query::blocks_and_transaction_hashes(18_000_000, Some(18_000_010));
    let res = client.get_arrow(&query).await.unwrap();

    let num_blocks: usize = res
        .data
        .blocks
        .into_iter()
        .map(|batch| batch.num_rows())
        .sum();
    let num_txs: usize = res
        .data
        .transactions
        .into_iter()
        .map(|batch| batch.num_rows())
        .sum();

    assert_eq!(res.next_block, 18_000_010);
    assert_eq!(num_blocks, 10);
    assert!(num_txs > 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_preset_query_logs() {
    let client = Client::builder()
        .url("https://eth.hypersync.xyz")
        .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
        .build()
        .unwrap();

    let usdt_addr = Address::decode_hex("0xdAC17F958D2ee523a2206206994597C13D831ec7").unwrap();
    let query = preset_query::logs(18_000_000, Some(18_000_010), usdt_addr);
    let res = client.get_arrow(&query).await.unwrap();

    let num_logs: usize = res
        .data
        .logs
        .into_iter()
        .map(|batch| batch.num_rows())
        .sum();

    assert_eq!(res.next_block, 18_000_010);
    assert!(num_logs > 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_preset_query_logs_of_event() {
    let client = Client::builder()
        .url("https://eth.hypersync.xyz")
        .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
        .build()
        .unwrap();

    let usdt_addr = Address::decode_hex("0xdAC17F958D2ee523a2206206994597C13D831ec7").unwrap();
    let transfer_topic0 = LogArgument::decode_hex(
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    )
    .unwrap();
    let query =
        preset_query::logs_of_event(18_000_000, Some(18_000_010), transfer_topic0, usdt_addr);

    let res = client.get_arrow(&query).await.unwrap();

    let num_logs: usize = res
        .data
        .logs
        .into_iter()
        .map(|batch| batch.num_rows())
        .sum();

    assert_eq!(res.next_block, 18_000_010);
    assert!(num_logs > 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_preset_query_transactions() {
    let client = Client::builder()
        .url("https://eth.hypersync.xyz")
        .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
        .build()
        .unwrap();
    let query = preset_query::transactions(18_000_000, Some(18_000_010));
    let res = client.get_arrow(&query).await.unwrap();

    let num_txs: usize = res
        .data
        .transactions
        .into_iter()
        .map(|batch| batch.num_rows())
        .sum();

    assert_eq!(res.next_block, 18_000_010);
    assert!(num_txs > 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_preset_query_transactions_from_address() {
    let client = Client::builder()
        .url("https://eth.hypersync.xyz")
        .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
        .build()
        .unwrap();

    let vitalik_eth_addr =
        Address::decode_hex("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045").unwrap();
    let query =
        preset_query::transactions_from_address(19_000_000, Some(19_300_000), vitalik_eth_addr);
    let res = client.get_arrow(&query).await.unwrap();

    let num_txs: usize = res
        .data
        .transactions
        .into_iter()
        .map(|batch| batch.num_rows())
        .sum();

    assert!(res.next_block == 19_300_000);
    assert!(num_txs == 21);
}

// same query as above (test_api_preset_query_transactions_from_address) except it uses a bloom filter instead of a
// vector of addresses to target the specified address
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_small_bloom_filter_query() {
    let client = Arc::new(
        Client::builder()
            .url("https://eth.hypersync.xyz")
            .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
            .build()
            .unwrap(),
    );

    let vitalik_eth_addr =
        Address::decode_hex("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045").unwrap();

    let mut txn_field_selection = BTreeSet::new();
    txn_field_selection.insert(TransactionField::BlockNumber);
    txn_field_selection.insert(TransactionField::From);
    txn_field_selection.insert(TransactionField::Hash);

    let addrs = [vitalik_eth_addr.clone()];
    let from_address_filter =
        FilterWrapper::from_keys(addrs.iter().map(|d| d.as_ref()), None).unwrap();

    let query = Query {
        from_block: 19_000_000,
        to_block: Some(19_300_000),
        logs: Vec::new(),
        transactions: vec![TransactionSelection::from(TransactionFilter {
            from_filter: Some(from_address_filter),
            ..Default::default()
        })],
        field_selection: FieldSelection {
            block: Default::default(),
            log: Default::default(),
            transaction: txn_field_selection,
            trace: Default::default(),
        },
        ..Default::default()
    };

    let stream_config = StreamConfig::default();

    let res = client.collect(query, stream_config).await.unwrap();

    let txns: Vec<Transaction> = res.data.transactions.into_iter().flatten().collect();
    let num_txns = txns.len();

    for txn in txns {
        if txn.from.as_ref() != Some(&vitalik_eth_addr) {
            panic!("returned an address not in the bloom filter")
        }
    }

    assert_eq!(res.next_block, 19_300_000);
    assert_eq!(num_txns, 21);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_decode_string_param_into_arrow() {
    let client = Arc::new(
        Client::builder()
            .url("https://mev-commit.hypersync.xyz")
            .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
            .build()
            .unwrap(),
    );

    let query: Query = serde_json::from_value(serde_json::json!({
        "from_block": 0,
        "logs": [{
            "address": ["0xCAC68D97a56b19204Dd3dbDC103CB24D47A825A3"],
            "topics": [["0xe44dd4d002deb2c79cf08ce285a9d80c69753f31ca65c8e49f0a60d27ed9fea3"]],
        }],
        "field_selection": {
            "log": ["block_number", "topic0", "topic1", "topic2", "topic3", "data", "address"],
        }
    }))
    .unwrap();

    let conf = StreamConfig {
        event_signature: Some(
            "CommitmentStored(bytes32 indexed commitmentIndex, address bidder, address commiter, \
             uint256 bid, uint64 blockNumber, bytes32 bidHash, uint64 decayStartTimeStamp, uint64 \
             decayEndTimeStamp, string txnHash, string revertingTxHashes, bytes32 commitmentHash, \
             bytes bidSignature, bytes commitmentSignature, uint64 dispatchTimestamp, bytes \
             sharedSecretKey)"
                .into(),
        ),
        ..Default::default()
    };

    let data = client.collect_arrow(query, conf).await.unwrap();

    dbg!(data.data.decoded_logs);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_capnp_client() {
    let client = Arc::new(
        Client::builder()
            .chain_id(1)
            .api_token(std::env::var("ENVIO_API_TOKEN").unwrap())
            .serialization_format(SerializationFormat::CapnProto {
                should_cache_queries: true,
            })
            .build()
            .unwrap(),
    );

    let field_selection = FieldSelection {
        block: BlockField::all(),
        log: LogField::all(),
        transaction: TransactionField::all(),
        trace: Default::default(),
    };
    let query = Query {
        from_block: 0,
        logs: vec![LogSelection::default()],
        transactions: Vec::new(),
        include_all_blocks: true,
        field_selection,
        ..Default::default()
    };
    println!("starting stream, query {:?}", &query);

    let mut res = client.stream(query, StreamConfig::default()).await.unwrap();

    let mut iters = 0;

    while let Some(res) = res.recv().await {
        let res = res.unwrap();
        dbg!(res);
        iters += 1;
        if iters > 5 {
            break;
        }
    }
}
