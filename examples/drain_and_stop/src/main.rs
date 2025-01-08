// Example of using the client to stream data and then draining and stopping the stream
// It has no practical use but it is meant to show how to use the client

use std::sync::Arc;

use hypersync_client::{Client, ClientConfig, ColumnMapping, DataType, StreamConfig};

#[tokio::main]
async fn main() {
    env_logger::init().unwrap();

    // create default client, uses eth mainnet
    let client = Client::new(ClientConfig::default()).unwrap();

    let query = serde_json::from_value(serde_json::json!( {
        // start from block 10123123 and go to the end of the chain (we don't specify a toBlock).
        "from_block": 10123123,
        // The logs we want. We will also automatically get transactions and blocks relating to these logs (the query implicitly joins them).
        "logs": [
            {
                // We want All ERC20 transfers so no address filter and only a filter for the first topic
                "topics": [
                    ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
                ]
            }
        ],
        // Select the fields we are interested in, notice topics are selected as topic0,1,2,3
        "field_selection": {
            "block": [
                "number",
            ],
            "log": [
                "data",
                "topic0",
                "topic1",
                "topic2",
                "topic3",
            ]
        }
    }))
    .unwrap();

    println!("Starting the stream");

    // Put the client inside Arc so we can use it for streaming
    let client = Arc::new(client);

    let mut drained = vec![];

    // Stream arrow data so we can average the erc20 transfer amounts in memory
    //
    // This will parallelize internal requests so we don't have to worry about pipelining/parallelizing make request -> handle response -> handle data loop
    let mut receiver = client
        .stream_arrow(
            query,
            StreamConfig {
                // Pass the event signature for decoding
                event_signature: Some(
                    "Transfer(address indexed from, address indexed to, uint amount)".to_owned(),
                ),
                column_mapping: Some(ColumnMapping {
                    decoded_log: [
                        // Map the amount column to float so we can do aggregation on it
                        ("amount".to_owned(), DataType::Float64),
                    ]
                    .into_iter()
                    .collect(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut count = 0;

    // Receive the data in a loop
    while let Some(res) = receiver.recv().await {
        let res = res.unwrap();
        count += 1;

        println!(
            "scanned up to block: {}, found {} blocks",
            res.next_block,
            res.data.blocks.len()
        );

        if res.next_block > 10129290 {
            drained = receiver.drain_and_stop().await;
            println!("Drained {} responses", drained.len());
            break;
        }
    }

    count += drained.len();

    for data in drained {
        println!("data: {:?}", data.unwrap().next_block);
    }

    println!("response count: {}", count);
}
