// Example of getting all erc20 transfers from eth mainnet and averaging transfer amount
// It has no practical use but it is meant to show how to use the client

use std::time::Instant;

use hypersync_client::{
    net_types::{LogField, LogFilter, Query},
    Client, ColumnMapping, DataType, SerializationFormat, StreamConfig,
};
use polars_arrow::{
    array::{Array, Float64Array},
    compute,
    scalar::PrimitiveScalar,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init().unwrap();

    // create default client, uses eth mainnet
    let client = Client::builder()
        .chain_id(1)
        .api_token(std::env::var("ENVIO_API_TOKEN")?)
        .serialization_format(SerializationFormat::CapnProto {
            should_cache_queries: true,
        })
        .build()
        .unwrap();

    let query = Query::new()
        // start from block 10123123 and go to the end of the chain (we don't specify a toBlock).
        .from_block(10123123)
        // The logs we want. We will also automatically get transactions and blocks relating to these logs (the query implicitly joins them).
        .where_logs(
            LogFilter::all()
                // We want All ERC20 transfers so no address filter and only a filter for the first topic
                .and_topic0([
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                ])?,
        )
        // Select the fields we are interested in, notice topics are selected as topic0,1,2,3
        .select_log_fields([
            LogField::Data,
            LogField::Topic0,
            LogField::Topic1,
            LogField::Topic2,
            LogField::Topic3,
        ]);

    println!("Starting the stream");

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
    let start = Instant::now();
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
            "scanned up to block: {}, found {} transfers, events per second: {}, average amount \
             is: {:.2}",
            res.next_block,
            num_transfers,
            (num_transfers as u64)
                .checked_div(start.elapsed().as_secs())
                .unwrap_or_default(),
            total_amount / num_transfers as f64
        );
    }
    Ok(())
}
