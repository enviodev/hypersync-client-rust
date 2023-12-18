use alloy_dyn_abi::{DecodedEvent, DynSolValue};
use alloy_json_abi::JsonAbi;
use alloy_primitives::Uint;
use hypersync_client::{
    client,
    format::{self, Address, Hex},
    net_types::Query,
};
use std::num::NonZeroU64;
use tokio::time::{sleep, Duration};
use url::Url;

const DAI_ADDRESS: &str = "0x6B175474E89094C44Da98b954EedeAC495271d0F";

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
    let mut query: Query = serde_json::from_value(serde_json::json!( {
        // start from block 0 and go to the end of the chain (we don't specify a toBlock).
        "from_block": 0,
        // The logs we want. We will also automatically get transactions and blocks relating to these logs (the query implicitly joins them).
        "logs": [
            {
            "address": [DAI_ADDRESS],
            // We want All ERC20 transfers so no address filter and only a filter for the first topic
            "topics": [
                ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
            ]
        }],
        // Select the fields we are interested in, notice topics are selected as topic0,1,2,3
        "field_selection": {
            "log": [
                "data",
                "address",
                "topic0",
                "topic1",
                "topic2",
                "topic3",
            ],
        }
    }))
    .unwrap();

    // read json abi file for erc20
    let path = "./erc20.abi.json";
    let abi = tokio::fs::read_to_string(path).await.unwrap();
    let abi: JsonAbi = serde_json::from_str(&abi).unwrap();

    // parse string address into hypersync_client::Address type
    let address: format::Address = Address::decode_hex(DAI_ADDRESS).unwrap();

    // associate address to the abi that decodes it
    let abis = vec![(address, abi)];

    // Create a decoder with our mapping
    let decoder = client::Decoder::new(abis.as_slice()).unwrap();

    // need to increase size to a 512 because a 256 might overflow
    let mut total_dai_volume: Uint<512, 8> = Uint::from(0);
    loop {
        let res = client.send::<client::ArrowIpc>(&query).await.unwrap();

        if !res.data.logs.is_empty() {
            // Decode the logs
            let decoded_logs = decoder
                .decode_logs(&res.data.logs)
                .unwrap()
                .unwrap_or_default();

            // filter out None
            let decoded_logs: Vec<DecodedEvent> = decoded_logs.into_iter().flatten().collect();

            for log in decoded_logs {
                match log.body.first().unwrap() {
                    DynSolValue::Uint(volume, _) => {
                        let volume: Uint<512, 8> = Uint::from(*volume);
                        total_dai_volume += volume;
                    }
                    _ => panic!("not uint"),
                }
            }
        }

        let next_block = res.next_block;
        let usd_dai_volume = total_dai_volume / Uint::from(1e18);
        println!(
            "scanned up to {next_block} and total DAI transfer volume is {usd_dai_volume} USD"
        );

        if let Some(archive_height) = res.archive_height {
            if archive_height == res.next_block {
                // wait if we are at the head
                sleep(Duration::from_secs(1)).await;
            }
        }

        // continue query from next_block
        query.from_block = res.next_block;
    }
}
