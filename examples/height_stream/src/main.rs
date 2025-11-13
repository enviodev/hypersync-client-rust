use std::sync::Arc;

use anyhow::Result;
use hypersync_client::{Client, HeightStreamEvent};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let client = Arc::new(
        Client::builder()
            .url("https://arbitrum-sepolia.hypersync.xyz")
            .bearer_token(std::env::var("HYPERSYNC_API_TOKEN")?)
            .build()?,
    );

    let mut rx = client.clone().stream_height();

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
