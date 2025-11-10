// Example of getting all transactions and erc20 transfers of a multi-address wallet on arbitrum

use std::sync::Arc;

use hypersync_client::{
    format::Hex,
    net_types::{LogField, LogFilter, Query, TransactionField, TransactionFilter},
    Client, ClientConfig, Decoder, StreamConfig,
};

// Convert address (20 bytes) to hash (32 bytes) so it can be used as a topic filter.
// Pads the address with zeroes.
fn address_to_topic(address: &str) -> String {
    format!("0x000000000000000000000000{}", &address[2..])
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init().unwrap();

    let client = Client::new(ClientConfig {
        url: Some("https://eth.hypersync.xyz".parse().unwrap()),
        ..Default::default()
    })
    .unwrap();

    let addresses = vec![
        "0xD1a923D70510814EaE7695A76326201cA06d080F",
        "0xc0A101c4E9Bb4463BD2F5d6833c2276C36914Fb6",
        "0xa0FBaEdC4C110f5A0c5E96c3eeAC9B5635b74CE7",
        "0x32448eb389aBe39b20d5782f04a8d71a2b2e7189",
    ];

    // pad our addresses so we can use them as a topic filter
    let address_topic_filter: Vec<String> = addresses.iter().map(|a| address_to_topic(a)).collect();

    let query = Query::new()
        .from_block(0)
        .where_logs(
            // We want All ERC20 transfers coming to any of our addresses
            LogFilter::all()
                .and_topic0(["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"])?
                // topic2 is the "to" position
                .and_topic2(address_topic_filter.clone())?
                .or(
                    // We want All ERC20 transfers coming from any of our addresses
                    LogFilter::all()
                        .and_topic0([
                            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        ])?
                        // topic1 is the "from" position
                        .and_topic1(address_topic_filter.clone())?,
                ),
        )
        .where_transactions(
            // get all transactions coming from OR going to any of our addresses
            TransactionFilter::any()
                .and_from_address(addresses.clone())?
                .or(TransactionFilter::any().and_to_address(addresses.clone())?),
        )
        // Select the fields we are interested in, notice topics are selected as topic0,1,2,3
        .select_log_fields([
            LogField::Address,
            LogField::Data,
            LogField::Topic0,
            LogField::Topic1,
            LogField::Topic2,
            LogField::Topic3,
        ])
        .select_transaction_fields([
            TransactionField::From,
            TransactionField::To,
            TransactionField::Value,
        ]);

    let client = Arc::new(client);

    println!("Starting the stream");

    // Stream data
    //
    // This will parallelize internal requests so we don't have to worry about pipelining/parallelizing make request -> handle response -> handle data loop
    let mut receiver = client.stream(query, StreamConfig::default()).await.unwrap();

    let decoder = Decoder::from_signatures(&[
        "Transfer(address indexed from, address indexed to, uint amount)",
    ])
    .unwrap();

    while let Some(res) = receiver.recv().await {
        let res = res.unwrap();

        for batch in res.data.logs {
            for log in batch {
                if let Ok(decoded_log) = decoder.decode_log(&log) {
                    let decoded_log = decoded_log.unwrap();
                    let amount = decoded_log.body[0].as_uint().unwrap();
                    let from = decoded_log.indexed[0].as_address().unwrap();
                    let to = decoded_log.indexed[1].as_address().unwrap();

                    println!(
                        "found erc20 transfer. token_address: {}, from: {}, to: {}, amount: {}",
                        log.address.unwrap().encode_hex(),
                        from,
                        to,
                        amount.0
                    );
                }
            }
        }

        for batch in res.data.transactions {
            for tx in batch {
                println!(
                    "found transaction. from: {}, to: {}, value: {}",
                    tx.from.unwrap().encode_hex(),
                    tx.to.unwrap().encode_hex(),
                    ruint::aliases::U256::from_be_slice(&tx.value.unwrap())
                );
            }
        }

        println!("scanned up to block {}", res.next_block);
    }
    Ok(())
}
