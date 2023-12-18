use alloy_dyn_abi::{DecodedEvent, DynSolValue};
use alloy_json_abi::JsonAbi;
use alloy_primitives::Uint;
use arrow2::array::BinaryArray;
use hypersync_client::{client, format::Address};
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

    // The query to run
    let query = serde_json::from_value(serde_json::json!( {
        // start from block 0 and go to the end of the chain (we don't specify a toBlock).
        "from_block": 0,
        // The logs we want. We will also automatically get transactions and blocks relating to these logs (the query implicitly joins them).
        "logs": [
            {
                // We want All ERC20 transfers so no address filter and only a filter for the first topic
            "topics": [
                ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
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
    let res = client.send::<client::ArrowIpc>(&query).await.unwrap();

    println!(
        "Ran the query once.  Next block to query is {}",
        res.next_block
    );

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

    // lets count total volume, it is meaningless because of currency differences but good as an example
    // This needs to be larger than 256, otherwise it will overflow
    let mut total_volume: Uint<512, 8> = Uint::from(0);

    for log in decoded_logs {
        let maybe_volume = log.body.get(0).unwrap();
        match maybe_volume {
            DynSolValue::Uint(volume, _) => {
                let volume: Uint<512, 8> = Uint::from(*volume);
                total_volume += volume;
            }
            _ => panic!("not a Uint"),
        }
    }

    let total_blocks = res.next_block - query.from_block;

    println!("total volume was {total_volume} in {total_blocks} blocks");
}
