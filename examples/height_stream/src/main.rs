use std::sync::Arc;

use anyhow::Result;
use hypersync_client::{Client, ClientConfig, HeightStreamEvent};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let client = Arc::new(Client::new(ClientConfig {
        url: Some("https://arbitrum-sepolia.hypersync.xyz".parse().unwrap()),
        ..Default::default()
    })?);

    let mut rx = client.clone().stream_height().await?;

    println!("listening for height updates... (Ctrl+C to quit)");

    while let Some(event) = rx.recv().await {
        match event {
            HeightStreamEvent::Connected => println!("✓ Connected to stream"),
            HeightStreamEvent::Height(height) => println!("height: {}", height),
            HeightStreamEvent::Reconnecting { delay } => {
                println!("⟳ Reconnecting in {:?}...", delay)
            }
        }
    }

    Ok(())
}
