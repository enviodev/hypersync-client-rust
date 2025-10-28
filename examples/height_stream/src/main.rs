use std::sync::Arc;

use anyhow::Result;
use hypersync_client::{Client, ClientConfig};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let client = Arc::new(Client::new(ClientConfig {
        url: Some(
            "https://arbitrum-sepolia.zone1.hypersync.xyz"
                .parse()
                .unwrap(),
        ),
        ..Default::default()
    })?);

    let mut rx = client.clone().stream_height().await?;

    println!("listening for height updates... (Ctrl+C to quit)");

    while let Some(msg) = rx.recv().await {
        match msg {
            Ok(height) => println!("height: {}", height),
            Err(e) => {
                eprintln!("stream error - will automatically reconnect: {e:?}");
                break;
            }
        }
    }

    Ok(())
}
