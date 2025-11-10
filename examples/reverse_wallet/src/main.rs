// Example of getting all erc20 transfers from eth mainnet and averaging transfer amount
// It has no practical use but it is meant to show how to use the client

use std::sync::Arc;

use hypersync_client::{
    net_types::{Query, TransactionField, TransactionFilter},
    Client, ClientConfig, StreamConfig,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init().unwrap();

    // create default client, uses eth mainnet
    let client = Client::new(ClientConfig::default()).unwrap();

    let address = "0x5a830d7a5149b2f1a2e72d15cd51b84379ee81e5";

    let query = Query::new()
        .from_block(0)
        .select_transaction_fields([
            TransactionField::BlockNumber,
            TransactionField::Hash,
            TransactionField::From,
            TransactionField::To,
            TransactionField::Value,
        ])
        .where_transactions(
            TransactionFilter::all()
                .and_from_address([address])?
                .and_to_address([address])?,
        );

    println!("Starting the stream");

    // Put the client inside Arc so we can use it for streaming
    let client = Arc::new(client);

    // Stream arrow data in reverse order
    //
    // This will parallelize internal requests so we don't have to worry about pipelining/parallelizing make request -> handle response -> handle data loop
    let mut receiver = client
        .clone()
        .stream(
            query.clone(),
            StreamConfig {
                reverse: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut reversed_order_transactions = Vec::new();

    // Receive the data in a loop
    while let Some(res) = receiver.recv().await {
        let res = res.unwrap();

        for batch in res.data.transactions {
            for tx in batch {
                println!("{}", serde_json::to_string_pretty(&tx).unwrap());
                reversed_order_transactions.push(tx);
            }
        }
    }

    // Stream without reversing
    let mut receiver = client.stream(query, StreamConfig::default()).await.unwrap();

    let mut regular_order_transactions = Vec::new();
    while let Some(res) = receiver.recv().await {
        let res = res.unwrap();

        for batch in res.data.transactions {
            for tx in batch {
                //println!("{}", serde_json::to_string_pretty(&tx).unwrap());
                regular_order_transactions.push(tx);
            }
        }
    }
    // reverse the transactions order after collecting
    regular_order_transactions.reverse();

    pretty_assertions::assert_eq!(reversed_order_transactions, regular_order_transactions);
    Ok(())
}
