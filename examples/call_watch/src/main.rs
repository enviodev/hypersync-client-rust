// Example of watching for new DAI transfers
// WARNING: This example doesn't account for rollbacks

use hypersync_client::{net_types::Query, CallDecoder, Client, ClientConfig};
use tokio::time::{sleep, Duration};

const DAI_ADDRESS: &str = "0x6B175474E89094C44Da98b954EedeAC495271d0F";

#[tokio::main]
async fn main() {
    env_logger::init().unwrap();

    // create default client, uses eth mainnet
    let client = Client::new(ClientConfig::default()).unwrap();

    // The query to run
    let mut query: Query = serde_json::from_value(serde_json::json!( {
        // start from tip since we only want new transfers
        "from_block": 20519993,
        // The logs we want. We will also automatically get transactions and blocks relating to these logs (the query implicitly joins them).
        "transactions": [
            {
                "from": [DAI_ADDRESS],
            },
            {
                "to": [DAI_ADDRESS],
            },
        ],
        // Select the fields we are interested in, notice topics are selected as topic0,1,2,3
        "field_selection": {
            "transaction": [
                "hash",
                "input",
            ],
        }
    }))
    .unwrap();

    let decoder = CallDecoder::from_signatures(&[
        "transfer(address dst, uint256 wad)",
        "transferFrom(address src, address dst, uint256 wad)",
    ])
    .unwrap();

    loop {
        let res = client.get(&query).await.unwrap();

        for batch in res.data.transactions {
            for tx in batch {
                if let Some(decoded_call) = decoder.decode_input(&tx.input.unwrap()).unwrap() {
                    if decoded_call.len() == 2 {
                        println!(
                            "Found DAU transfer {:?}. to: {}, amount: {:?}",
                            &tx.hash,
                            decoded_call[0].as_address().unwrap(),
                            decoded_call[1].as_uint().unwrap()
                        );
                    } else if decoded_call.len() == 3 {
                        println!(
                            "Found DAU transfer {:?}. from: {}, to: {}, amount: {:?}",
                            &tx.hash,
                            decoded_call[0].as_address().unwrap(),
                            decoded_call[1].as_address().unwrap(),
                            decoded_call[2].as_uint().unwrap()
                        );
                    }
                }
            }
        }

        println!("scanned up to block {}", res.next_block);

        if let Some(archive_height) = res.archive_height {
            if archive_height < res.next_block {
                // wait if we are at the head
                // notice we use explicit get_height in order to not waste data requests.
                // get_height is lighter compared to spamming data requests at the tip.
                while client.get_height().await.unwrap() < res.next_block {
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }

        // continue query from next_block
        query.from_block = res.next_block;
    }
}
