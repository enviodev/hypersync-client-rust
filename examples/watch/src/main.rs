// Example of watching for new DAI transfers
// WARNING: This example doesn't account for rollbacks

use hypersync_client::{
    net_types::{LogField, LogFilter, Query},
    Client, ClientConfig, Decoder,
};
use tokio::time::{sleep, Duration};

const DAI_ADDRESS: &str = "0x6B175474E89094C44Da98b954EedeAC495271d0F";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init().unwrap();

    // create default client, uses eth mainnet
    let client = Client::new(ClientConfig::default()).unwrap();

    let height = client.get_height().await.unwrap();

    println!("server height is {height}");

    // The query to run
    let mut query = Query::new()
        // start from tip since we only want new transfers
        .from_block(height)
        // The logs we want. We will also automatically get transactions and blocks relating to these logs (the query implicitly joins them).
        .where_logs([LogFilter::any()
            .and_address([DAI_ADDRESS])?
            // we only want transfer events
            .and_topic0(["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"])?])
        .select_log_fields([
            LogField::Data,
            LogField::Topic0,
            LogField::Topic1,
            LogField::Topic2,
            LogField::Topic3,
        ]);

    let decoder = Decoder::from_signatures(&[
        "Transfer(address indexed from, address indexed to, uint amount)",
    ])
    .unwrap();

    loop {
        let res = client.get(&query).await.unwrap();

        for batch in res.data.logs {
            for log in batch {
                let decoded_log = decoder.decode_log(&log).unwrap().unwrap();
                let amount = decoded_log.body[0].as_uint().unwrap();
                let from = decoded_log.indexed[0].as_address().unwrap();
                let to = decoded_log.indexed[1].as_address().unwrap();

                println!(
                    "found DAI transfer. from: {}, to: {}, amount: {}",
                    from, to, amount.0
                );
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
