use alloy_dyn_abi::DecodedEvent;
use alloy_json_abi::JsonAbi;
use arrayvec::ArrayVec;
use arrow2::array::BinaryArray;
use hypersync_client::{
    client,
    format::Address,
    net_types::{FieldSelection, LogSelection, Query, TransactionSelection},
};
use std::num::NonZeroU64;
use url::Url;

#[tokio::main]
async fn main() {
    // create hypersync client using the mainner hypersync endpoint
    let client_config = client::Config {
        url: Url::parse("https://eth.hypersync.xyz").unwrap(),
        bearer_token: None,
        http_req_timeout_millis: NonZeroU64::new(30000).unwrap(),
    };
    let client = client::Client::new(client_config).unwrap();

    let query = Query {
        include_all_blocks: false,
        from_block: 0,
        to_block: None,
        logs: vec![LogSelection {
            address: vec![],
            topics: ArrayVec::try_from([
                vec![hex_literal::hex!(
                    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                )
                .try_into()
                .unwrap()],
                vec![],
                vec![],
                vec![],
            ])
            .unwrap(),
        }],
        transactions: vec![],
        field_selection: FieldSelection {
            block: [
                "hash".to_owned(),
                "number".to_owned(),
                "timestamp".to_owned(),
            ]
            .into_iter()
            .collect(),
            log: [
                "block_number".to_owned(),
                "transaction_index".to_owned(),
                "log_index".to_owned(),
                "transaction_hash".to_owned(),
                "data".to_owned(),
                "address".to_owned(),
                "topic0".to_owned(),
                "topic1".to_owned(),
                "topic2".to_owned(),
                "topic3".to_owned(),
            ]
            .into_iter()
            .collect(),
            transaction: [
                "block_number".to_owned(),
                "transaction_index".to_owned(),
                "hash".to_owned(),
                "from".to_owned(),
                "to".to_owned(),
                "value".to_owned(),
                "input".to_owned(),
            ]
            .into_iter()
            .collect(),
        },
        max_num_blocks: None,
        max_num_transactions: None,
        max_num_logs: None,
    };

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

    // vector of (contract address -> abi)
    let mut abis = vec![];

    // every log we get should be decodable by this abi but we don't know
    // the specific contract addresses since we are indexing all erc20 transfers.
    for log in &res.data.logs {
        // returned data is in arrow format so we have to convert to string
        let _ = log
            .column::<BinaryArray<i32>>("address")
            .unwrap()
            .iter()
            .map(|val| {
                if let Some(val) = val {
                    // convert bytes to address type
                    let address: Address = val.try_into().unwrap();
                    // add to the association address -> abi
                    abis.push((address, abi.clone()))
                }
            });
    }

    println!("abis len: {}", abis.len());

    // Create a decoder with our mapping
    let decoder = client::Decoder::new(abis.as_slice()).unwrap();

    // Decode the logs
    let decoded_logs = decoder
        .decode_logs(&res.data.logs)
        .unwrap()
        .unwrap_or_else(|| vec![]);

    // filter out None
    let decoded_logs: Vec<DecodedEvent> = decoded_logs.into_iter().filter_map(|v| v).collect();

    // lets count total volume, it is meaningless because of currency differences butgood as an example
    let mut total_volume = 0;
    for log in decoded_logs {
        let volume = log.body.get(0).unwrap();
        println!("volume: {volume:?}")
    }
}
