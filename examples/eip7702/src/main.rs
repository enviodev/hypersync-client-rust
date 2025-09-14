// Example of getting all EIP-7702 transactions

use std::sync::Arc;

use hypersync_client::{net_types::Query, Client, ClientConfig};

#[tokio::main]
async fn main() {
    env_logger::init().unwrap();

    let client = Client::new(ClientConfig {
        url: Some("https://eth.hypersync.xyz".parse().unwrap()),
        ..Default::default()
    })
    .unwrap();

    let query: Query = serde_json::from_value(serde_json::json!( {
        // start from block 0 and go to the end of the chain (we don't specify a toBlock).
        "from_block": 22490287,
        "to_block": 22490297,
        "transactions": [
            {"authorization_list": [{/*"chain_id": [1], // chain_id filterring isn't working currently*/ "address": ["0x80296ff8d1ed46f8e3c7992664d13b833504c2bb"]}]}
        ],
        // Select the fields we are interested in, notice topics are selected as topic0,1,2,3
        "field_selection": {
            "transaction": [
                "hash","authorization_list"
            ]
        },
    }))
    .unwrap();

    let client = Arc::new(client);

    println!("Fetching Data");

    let res = client.get(&query).await.unwrap();

    for batch in res.data.transactions {
        for tx in batch {
            println!("Transaction: {:?}", tx.authorization_list);
        }
    }
}
