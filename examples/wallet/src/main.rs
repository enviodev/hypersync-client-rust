use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU64,
    ops::Add,
    str::FromStr,
};

use alloy_dyn_abi::{DecodedEvent, DynSolValue};
use alloy_json_abi::JsonAbi;
use alloy_primitives::{serde_hex, Uint};
use arrayvec::ArrayVec;
use arrow2::array::BinaryArray;
use hypersync_client::{
    self, client,
    format::Hash,
    net_types::{FieldSelection, LogSelection, Query, TransactionSelection},
};
use url::Url;

// Convert address (20 bytes) to hash (32 bytes) so it can be used as a topic filter.
// Pads the address with zeroes.
fn address_to_topic(address: &str) -> String {
    format!("0x000000000000000000000000{}", &address[2..])
}

#[tokio::main]
async fn main() {
    let addresses = vec![
        "0xD1a923D70510814EaE7695A76326201cA06d080F",
        "0xc0A101c4E9Bb4463BD2F5d6833c2276C36914Fb6",
        "0xa0FBaEdC4C110f5A0c5E96c3eeAC9B5635b74CE7",
        "0x32448eb389aBe39b20d5782f04a8d71a2b2e7189",
    ];

    // pad our addresses so we can use them as a topic filter
    let address_topic_filter: Vec<String> = addresses.iter().map(|a| address_to_topic(a)).collect();

    // create hypersync client using the mainnet hypersync endpoint
    let client_config = client::Config {
        url: Url::parse("https://eth.hypersync.xyz").unwrap(),
        bearer_token: None,
        http_req_timeout_millis: NonZeroU64::new(30000).unwrap(),
    };
    let client = client::Client::new(client_config).unwrap();

    // The query to run
    let query: Query = serde_json::from_value(serde_json::json!( {
        // start from block 0 and go to the end of the chain (we don't specify a toBlock).
        "from_block": 0,
        // The logs we want. We will also automatically get transactions and blocks relating to these logs (the query implicitly joins them).
        "logs": [
            {
                // We want All ERC20 transfers coming to any of our addresses
                "topics":[
                    ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
                    [],
                    // in the "to" position here
                    address_topic_filter,
                    [],
                ]
            },
            {
                // We want All ERC20 transfers coming from any of our addresses
                "topics":[
                    ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
                    // in the "from" position here
                    address_topic_filter,
                    [],
                    [],
                ]
            },
        ],
        "transactions": [
            // get all transactions coming from OR going to any of our addresses
            {"from": addresses},
            {"to": addresses}
        ],
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
        },
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
        // returned data is in arrow format so we have to convert bytes to Address
        let col = log.column::<BinaryArray<i32>>("address").unwrap();
        for val in col.into_iter().flatten() {
            let address: hypersync_client::format::Address = val.try_into().unwrap();
            abis.insert((address, abi.clone()));
        }
    }

    // convert hash set into a vector for decoder argument
    let abis: Vec<(hypersync_client::format::Address, JsonAbi)> = abis.into_iter().collect();

    // Create a decoder with our mapping
    let decoder = client::Decoder::new(abis.as_slice()).unwrap();

    // Decode the logs
    let decoded_logs = decoder
        .decode_logs(&res.data.logs)
        .unwrap()
        .unwrap_or_default();

    // filter out None
    let decoded_logs: Vec<DecodedEvent> = decoded_logs.into_iter().flatten().collect();

    // Let's count total volume for each address, it is meaningless because of currency differences but good as an example.

    // balance needs to be larger than 256, otherwise it will overflow
    let mut total_erc20_volume: HashMap<alloy_primitives::Address, Uint<512, 8>> = HashMap::new();

    for log in decoded_logs {
        let addr1 = match log.indexed.get(0).unwrap() {
            DynSolValue::Address(addr) => addr,
            _ => panic!("not an address"),
        };
        let addr2 = match log.indexed.get(1).unwrap() {
            DynSolValue::Address(addr) => addr,
            _ => panic!("not an address"),
        };
        let volume: Uint<512, 8> = match log.body.get(0).unwrap() {
            DynSolValue::Uint(volume, _) => Uint::from(*volume),
            _ => panic!("not a Uint"),
        };

        // add the volume to each address
        *total_erc20_volume.entry(*addr1).or_default() += volume;
        *total_erc20_volume.entry(*addr2).or_default() += volume;
    }

    for address in &addresses {
        let address: alloy_primitives::Address =
            alloy_primitives::Address::from_str(&address).unwrap();
        let erc20_volume = total_erc20_volume.get(&address).unwrap();
        println!("total erc20 transfer volume for address {address} is {erc20_volume}")
    }

    // do the same for wei, but pulling from res.data.transactions instead of decoded logs
    let mut total_wei_volume: HashMap<alloy_primitives::Address, Uint<512, 8>> = HashMap::new();

    for tx in res.data.transactions {
        // tx is in arrow format so we have to convert
        let from_col = tx.column::<BinaryArray<i32>>("from").unwrap();
        let from_addresses: Vec<alloy_primitives::Address> = from_col
            .iter()
            .flatten()
            .map(|b| b.try_into().unwrap())
            .collect();

        let to_col = tx.column::<BinaryArray<i32>>("to").unwrap();
        let to_addresses: Vec<alloy_primitives::Address> = to_col
            .iter()
            .flatten()
            .map(|b| b.try_into().unwrap())
            .collect();

        let tx_val_col = tx.column::<BinaryArray<i32>>("value").unwrap();
        let tx_vals: Vec<Uint<512, 8>> = tx_val_col
            .iter()
            .flatten()
            .map(|b| Uint::from_be_slice(b))
            .collect();

        for (from, (to, tx_val)) in from_addresses.iter().zip(to_addresses.iter().zip(tx_vals)) {
            // add the volume to each address
            *total_wei_volume.entry(*from).or_default() += tx_val;
            *total_wei_volume.entry(*to).or_default() += tx_val;
        }
    }

    for address in addresses {
        let address: alloy_primitives::Address =
            alloy_primitives::Address::from_str(&address).unwrap();
        let wei_volume = *total_wei_volume.get(&address).unwrap();
        println!("total wei transfer volume for address {address} is {wei_volume}")
    }
}
