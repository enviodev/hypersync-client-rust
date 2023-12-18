# hypersync-client-rust
Rust crate for Envio's HyperSync client

### Can find more examples in `examples/`

### Example usage
```rust
use alloy_dyn_abi::DecodedEvent;
use alloy_json_abi::JsonAbi;
use arrow2::array::BinaryArray;
use hypersync_client::{
    client,
    format::Address,
    net_types::{FieldSelection, LogSelection, Query},
};
use std::{collections::HashSet, num::NonZeroU64};
use url::Url;

#[tokio::main]
async fn main() {
    // create hypersync client using the mainnet hypersync endpoint
    let client_config = client::Config {
        url: Url::parse("https://eth.hypersync.xyz").unwrap(),
        bearer_token: None,
        http_req_timeout_millis: NonZeroU64::new(30000).unwrap(),
    };
    let client = client::Client::new(client_config).unwrap();

    let height = client.get_height().await.unwrap();
    println!("Height: {height}");

    // The address we want to get all ERC20 transfers and transactions for
    let addr = "1e037f97d730Cc881e77F01E409D828b0bb14de0";

    // The query to run
    let query: Query = serde_json::from_value(serde_json::json!( {
        // start from block 0 and go to the end of the chain (we don't specify a toBlock).
        "from_block": 0,
        // The logs we want. We will also automatically get transactions and blocks relating to these logs (the query implicitly joins them)
        "logs": [
            {
                "topics": [
                    // We want ERC20 transfers
                    ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
                    [],
                    // We want the transfers that go to this address.
                    // appending zeroes because topic is 32 bytes but address is 20 bytes
                    [format!("0x000000000000000000000000{}", addr)],
                ],
            },
            {
                "topics": [
                    // We want ERC20 transfers
                    ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
                    // We want the transfers that go to this address.
                    // appending zeroes because topic is 32 bytes but address is 20 bytes
                    [format!("0x000000000000000000000000{}", addr)],
                    [],
                ],
            },
        ],
        "transactions": [
            // We want all the transactions that come from this address
            {"from": [format!("0x{}", addr)]},
            // We want all the transactions that went to this address
            {"to": [format!("0x{}", addr)]},
        ],
        // Select the fields we are interested in
        "field_selection": {
            "block": ["number", "timestamp", "hash"],
            "log": [
                "block_number",
                "log_index",
                "transaction_index",
                "data",
                "address",
                "topic0",
                "topic1",
                "topic2",
                "topic3",
            ],
            "transaction": [
                "block_number",
                "transaction_index",
                "hash",
                "from",
                "to",
                "value",
                "input",
            ],
        },
    }))
    .unwrap();

    println!("Query: {:?}", query);

    // run the query once
    let res = client.send::<client::ArrowIpc>(&query).await.unwrap();
    println!("Res: {:?}", res);

    // read json abi file for erc20
    let path = "./erc20.abi.json";
    let abi = tokio::fs::read_to_string(path).await.unwrap();
    let abi: JsonAbi = serde_json::from_str(&abi).unwrap();

    // set of (address -> abi)
    let mut abis = HashSet::new();

    // every log we get should be decodable by this abi but we don't know
    // the specific contract addresses since we are indexing all erc20 transfers.
    for log in &res.data.logs {
        // returned data is in arrow format so we have to convert to Address
        let col = log.column::<BinaryArray<i32>>("address").unwrap();
        for val in col.into_iter().flatten() {
            let address: Address = val.try_into().unwrap();
            abis.insert((address, abi.clone()));
        }
    }

    // convert hash set into a vector for decoder argument
    let abis: Vec<(Address, JsonAbi)> = abis.into_iter().collect();

    // Create a decoder with our mapping
    let decoder = client::Decoder::new(abis.as_slice()).unwrap();

    // Decode the logs
    let decoded_logs = decoder
        .decode_logs(&res.data.logs)
        .unwrap()
        .unwrap_or_default();

    // filter out None
    let decoded_logs: Vec<DecodedEvent> = decoded_logs.into_iter().flatten().collect();

    println!("decoded logs:");
    for decoded_log in decoded_logs {
        println!("{:?}", decoded_log.indexed);
        println!("{:?}", decoded_log.body);
    }

    // alternatively can save data to parquet format
    // Create a parquet folder by running the query and writing the contents to disk
    client
        .create_parquet_folder(query, "data".into())
        .await
        .unwrap();
    println!("Finished writing parquet folder");
}


```