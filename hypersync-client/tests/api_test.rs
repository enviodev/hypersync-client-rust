use std::{collections::BTreeSet, env::temp_dir};

use alloy_dyn_abi::DynSolValue;
use alloy_json_abi::JsonAbi;
use hypersync_client::{ArrowIpc, Client, ColumnMapping, Config, Decoder, ParquetConfig};
use hypersync_format::{Address, Hex, LogArgument};
use hypersync_net_types::{FieldSelection, Query};
use polars_arrow::array::UInt64Array;

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_arrow_ipc() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

    let mut block_field_selection = BTreeSet::new();
    block_field_selection.insert("number".to_owned());
    block_field_selection.insert("timestamp".to_owned());
    block_field_selection.insert("hash".to_owned());

    let res = client
        .send::<ArrowIpc>(&Query {
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
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
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

    let res = client.send::<ArrowIpc>(&query).await.unwrap();

    assert!(res.next_block > 13223105);

    let mut last = (0, 0);
    for batch in res.data.logs {
        let block_number = batch.column::<UInt64Array>("block_number").unwrap();
        let log_index = batch.column::<UInt64Array>("log_index").unwrap();

        for (&block_number, &log_index) in block_number.values_iter().zip(log_index.values_iter()) {
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
    const ADDR: &str = "0xc18360217d8f7ab5e7c516566761ea12ce7f9d72";
    let address = Address::decode_hex(ADDR).unwrap();

    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

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

    let res = client.send::<ArrowIpc>(&query).await.unwrap();

    let path = get_file_path("ens_token_abi.json");
    let abi = tokio::fs::read_to_string(path).await.unwrap();
    let abi: JsonAbi = serde_json::from_str(&abi).unwrap();

    let decoder = Decoder::new(&[(address, abi)]).unwrap();

    let decoded_logs = decoder.decode_logs(&res.data.logs).unwrap().unwrap();

    assert_eq!(decoded_logs.len(), 1);

    println!("{:?}", decoded_logs[0]);
}

const URL: &str = "https://eth.hypersync.xyz";

#[test]
fn decode_zero_erc20_transfer() {
    const ADDR: &str = "0xc18360217d8f7ab5e7c516566761ea12ce7f9d72";
    let address = Address::decode_hex(ADDR).unwrap();

    let path = get_file_path("erc20.abi.json");
    let abi = std::fs::read_to_string(path).unwrap();
    let abi: JsonAbi = serde_json::from_str(&abi).unwrap();

    let decoder = Decoder::new(&[(address.clone(), abi)]).unwrap();

    let topics = [
        Some(
            LogArgument::decode_hex(
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            )
            .unwrap(),
        ),
        Some(
            LogArgument::decode_hex(
                "0x000000000000000000000000327339b55b16345a4b206bfb09c3fa27ab4689ec",
            )
            .unwrap(),
        ),
        Some(
            LogArgument::decode_hex(
                "0x0000000000000000000000001e037f97d730cc881e77f01e409d828b0bb14de0",
            )
            .unwrap(),
        ),
        None,
    ];

    let topics = topics
        .iter()
        .map(|t| t.as_ref().map(|t| t.as_slice()))
        .collect::<Vec<_>>();

    let event = decoder
        .decode(
            address.as_slice(),
            topics[0].unwrap(),
            topics.as_slice(),
            &[],
        )
        .unwrap()
        .unwrap();

    assert_eq!(event.body[0], DynSolValue::Uint(0.try_into().unwrap(), 256));
}

#[test]
fn parse_nameless_abi() {
    let path = get_file_path("nameless.abi.json");
    let abi = std::fs::read_to_string(path).unwrap();
    let _abi: JsonAbi = serde_json::from_str(&abi).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_parquet_out_decode_with_invalid_log() {
    env_logger::try_init().ok();

    let client = Client::new(Config {
        url: "https://base.hypersync.xyz".parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

    let path = format!("{}/{}", temp_dir().to_string_lossy(), uuid::Uuid::new_v4());

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

    client
        .create_parquet_folder(
            query,
            ParquetConfig {
                path,
                hex_output: true,
                batch_size: 100,
                concurrency: 10,
                retry: false,
                column_mapping: ColumnMapping {
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
                },
                event_signature: Some(
                    "Transfer(address indexed from, address indexed to, uint indexed amount)"
                        .into(),
                ),
            },
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_parquet_out() {
    env_logger::try_init().ok();

    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

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
        .create_parquet_folder(
            query,
            ParquetConfig {
                path,
                hex_output: true,
                batch_size: 100,
                concurrency: 10,
                retry: false,
                column_mapping: ColumnMapping {
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
                },
                event_signature: Some(
                    "Transfer(address indexed from, address indexed to, uint indexed amount)"
                        .into(),
                ),
            },
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_preset_query_blocks_and_transactions() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();
    let query = Client::preset_query_blocks_and_transactions(18_000_000, Some(18_000_100));
    let res = client.send::<ArrowIpc>(&query).await.unwrap();

    let num_blocks: usize = res
        .data
        .blocks
        .into_iter()
        .map(|batch| batch.chunk.len())
        .sum();
    let num_txs: usize = res
        .data
        .transactions
        .into_iter()
        .map(|batch| batch.chunk.len())
        .sum();

    assert!(res.next_block == 18_000_100);
    assert!(num_blocks == 100);
    assert!(num_txs > 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_preset_query_blocks_and_transaction_hashes() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();
    let query = Client::preset_query_blocks_and_transaction_hashes(18_000_000, Some(18_000_100));
    let res = client.send::<ArrowIpc>(&query).await.unwrap();

    let num_blocks: usize = res
        .data
        .blocks
        .into_iter()
        .map(|batch| batch.chunk.len())
        .sum();
    let num_txs: usize = res
        .data
        .transactions
        .into_iter()
        .map(|batch| batch.chunk.len())
        .sum();

    assert!(res.next_block == 18_000_100);
    assert!(num_blocks == 100);
    assert!(num_txs > 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_preset_query_logs() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

    let usdt_addr = hex_literal::hex!("dAC17F958D2ee523a2206206994597C13D831ec7");
    let query = Client::preset_query_logs(18_000_000, Some(18_001_000), usdt_addr).unwrap();
    let res = client.send::<ArrowIpc>(&query).await.unwrap();

    let num_logs: usize = res
        .data
        .logs
        .into_iter()
        .map(|batch| batch.chunk.len())
        .sum();

    assert!(res.next_block == 18_001_000);
    assert!(num_logs > 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_preset_query_logs_of_event() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

    let usdt_addr = hex_literal::hex!("dAC17F958D2ee523a2206206994597C13D831ec7");
    let transfer_topic0 =
        hex_literal::hex!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
    let query = Client::preset_query_logs_of_event(
        18_000_000,
        Some(18_001_000),
        transfer_topic0,
        usdt_addr,
    )
    .unwrap();

    let res = client.send::<ArrowIpc>(&query).await.unwrap();

    let num_logs: usize = res
        .data
        .logs
        .into_iter()
        .map(|batch| batch.chunk.len())
        .sum();

    assert!(res.next_block == 18_001_000);
    assert!(num_logs > 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_preset_query_transactions() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();
    let query = Client::preset_query_transactions(18_000_000, Some(18_000_100));
    let res = client.send::<ArrowIpc>(&query).await.unwrap();

    let num_txs: usize = res
        .data
        .transactions
        .into_iter()
        .map(|batch| batch.chunk.len())
        .sum();

    assert!(res.next_block == 18_000_100);
    assert!(num_txs > 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api_preset_query_transactions_from_address() {
    let client = Client::new(Config {
        url: URL.parse().unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    })
    .unwrap();

    let vitalik_eth_addr = hex_literal::hex!("d8dA6BF26964aF9D7eEd9e03E53415D37aA96045");
    let query = Client::preset_query_transactions_from_address(
        19_000_000,
        Some(19_300_000),
        vitalik_eth_addr,
    )
    .unwrap();
    let res = client.send::<ArrowIpc>(&query).await.unwrap();

    let num_txs: usize = res
        .data
        .transactions
        .into_iter()
        .map(|batch| batch.chunk.len())
        .sum();

    assert!(res.next_block == 18_000_100);
    assert!(num_txs > 1);
}
