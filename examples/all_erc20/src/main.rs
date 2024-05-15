use alloy_dyn_abi::{DecodedEvent, DynSolValue};
use alloy_json_abi::JsonAbi;
use alloy_primitives::Uint;
use hypersync_client::{config::Config, format::Address, ArrowIpc, Client, Decoder, StreamConfig};
use polars_arrow::array::BinaryArray;
use std::any::Any;
use std::{collections::HashSet, num::NonZeroU64};
use url::Url;

#[tokio::main]
async fn main() {
    // create hypersync client using the mainnet hypersync endpoint
    let client_config = Config {
        url: Url::parse("http://167.235.0.227:2104/").unwrap(),
        bearer_token: None,
        http_req_timeout_millis: NonZeroU64::new(30000).unwrap(),
    };
    let client = Client::new(client_config).unwrap();

    // The query to run
    let query = serde_json::from_value(serde_json::json!( {
        // start from block 0 and go to the end of the chain (we don't specify a toBlock).
        "from_block": 0,
        // The logs we want. We will also automatically get transactions and blocks relating to these logs (the query implicitly joins them).
        "logs": [
            {
            "address": [
                "0x85F17Cf997934a597031b2E18a9aB6ebD4B9f6a4",
            ]
        }],
        // Select the fields we are interested in, notice topics are selected as topic0,1,2,3
        "field_selection": {
            "block": [
                "hash",
                "number",
                "timestamp",
            ],
            "log": [
                "block_number",
                "transaction_index",
                "log_index",
                "transaction_hash",
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
            ]
        }
    }))
    .unwrap();

    println!("Running the query...");

    // Run the query once, the query is automatically paginated so it will return when it reaches some limit (time, response size etc.)
    // there is a next_block field on the response object so we can set the from_block of our query to this value and continue our query until
    // res.next_block is equal to res.archive_height or query.to_block in case we specified an end block.
    // let res = client.send::<ArrowIpc>(&query).await.unwrap();
    let scfg = StreamConfig {
        batch_size: 10000,
        concurrency: 20,
        retry: true,
    };
    let mut channel = client.stream::<ArrowIpc>(query, scfg).await.unwrap();

    let mut prev = 0;
    while let Some(res) = channel.recv().await {
        let res = res.unwrap();
        if res.next_block - prev != 10000 {
            println!(
                "Ran the query once.  Next block to query is {}, range: {}, time {}",
                res.next_block,
                res.next_block - prev,
                res.total_execution_time,
            );
        }
        prev = res.next_block;
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
        let decoder = Decoder::new(abis.as_slice()).unwrap();

        // Decode the logs
        let decoded_logs = decoder
            .decode_logs(&res.data.logs)
            .unwrap()
            .unwrap_or_default();

        // filter out None
        let decoded_logs: Vec<DecodedEvent> = decoded_logs.into_iter().flatten().collect();

        let mut total_volume: usize = decoded_logs.len();

        let total_blocks = res.next_block;

        // println!("total volume was {total_volume} in {total_blocks} blocks");
    }
}
