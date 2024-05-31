// Example of getting all erc20 transfers from eth mainnet and averaging transfer amount
// It has no practical use but it is meant to show how to use the client

use std::sync::Arc;

use hypersync_client::{Client, ClientConfig, ColumnMapping, DataType, StreamConfig};
use polars_arrow::{
    array::{Array, Float64Array},
    compute,
    scalar::PrimitiveScalar,
};

#[tokio::main]
async fn main() {
    env_logger::init().unwrap();

    // create default client, uses eth mainnet
    let client = Client::new(ClientConfig::default())
    .unwrap();

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
        }],
        // Select the fields we are interested in, notice topics are selected as topic0,1,2,3
        "field_selection": {
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

    let mut num_transfers = 0;
    let mut total_amount = 0f64;
    // Receive the data in a loop
    while let Some(res) = receiver.recv().await {
        let res = res.unwrap();

        for batch in res.data.decoded_logs {
            // get amount array so we can sum/count
            let amount = batch.column::<Float64Array>("amount").unwrap();

            // exclude null rows to avoid counting invalid logs
            num_transfers += amount.len() - amount.null_count();

            total_amount += compute::aggregate::sum(amount)
                .unwrap()
                .as_any()
                .downcast_ref::<PrimitiveScalar<f64>>()
                .unwrap()
                .value()
                .unwrap_or_default();
        }

        println!(
            "scanned up to block: {}, found {} transfers, average amount is: {:.2}",
            res.next_block,
            num_transfers,
            total_amount / num_transfers as f64
        );
    }
}
