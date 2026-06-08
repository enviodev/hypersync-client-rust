//! `tune_stream` — a config-sweep CLI for the HyperSync streaming engine.
//!
//! Give it a query (JSON) and it runs the stream under a grid of `StreamConfig`s,
//! printing a comparison table of the aggregate metrics so you can pick the best
//! one (highest throughput, response sizes near `response_bytes_target`, low
//! truncation). Because it takes a query JSON it is usable by any client
//! regardless of language.
//!
//! Usage:
//!   ENVIO_API_TOKEN=... cargo run -p tune_stream -- <query.json> [--single]
//!
//! Args / env:
//!   <query.json>      Path to a JSON-serialised `Query` (default: `query.json`).
//!                     Set a `fromBlock`/`toBlock` for a bounded benchmark.
//!   --single          Run one config (the default) and print a detailed report
//!                     instead of the sweep table.
//!   CHAIN_ID=<n>      Chain to query (default: 1 / eth mainnet).
//!   HYPERSYNC_URL=... Override the server URL (otherwise derived from CHAIN_ID).
//!   TUNE_DEBUG=1      Emit one `log::debug!` line per request (needs
//!                     `RUST_LOG=debug`).

use std::sync::Arc;

use hypersync_client::{
    net_types::Query, Client, RequestStats, StreamConfig, StreamMetrics, StreamObserver,
    StreamSummary, SIZE_BUCKET_LABELS,
};

/// Observer that aggregates into a [`StreamMetrics`] handle and, when `debug` is
/// set, logs one line per request.
struct TuneObserver {
    metrics: Arc<StreamMetrics>,
    debug: bool,
}

impl StreamObserver for TuneObserver {
    fn on_request(&self, s: &RequestStats) {
        if self.debug {
            log::debug!(
                "req {}..{} -> {} | {} bytes ({:.2}x) | {} blocks | {:?} | trunc={} | {:?}",
                s.from_block,
                s.requested_end,
                s.next_block,
                s.response_bytes,
                s.size_ratio,
                s.actual_blocks,
                s.kind,
                s.truncated,
                s.duration,
            );
        }
        self.metrics.on_request(s);
    }

    fn on_progress(&self, in_flight: u64, buffered_bytes: u64) {
        self.metrics.on_progress(in_flight, buffered_bytes);
    }

    fn on_finish(&self, summary: &StreamSummary) {
        self.metrics.on_finish(summary);
    }
}

/// One config in the sweep grid.
struct Variant {
    label: String,
    config: StreamConfig,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let mut args = std::env::args().skip(1);
    let mut query_path = String::from("query.json");
    let mut single = false;
    for arg in args.by_ref() {
        match arg.as_str() {
            "--single" => single = true,
            other => query_path = other.to_string(),
        }
    }

    let query_json = std::fs::read_to_string(&query_path)
        .map_err(|e| anyhow::anyhow!("failed to read query file '{query_path}': {e}"))?;
    let query: Query = serde_json::from_str(&query_json)
        .map_err(|e| anyhow::anyhow!("failed to parse query JSON: {e}"))?;

    let chain_id: u64 = std::env::var("CHAIN_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let api_token = std::env::var("ENVIO_API_TOKEN")
        .map_err(|_| anyhow::anyhow!("ENVIO_API_TOKEN env var is required"))?;

    let mut builder = Client::builder().chain_id(chain_id).api_token(api_token);
    if let Ok(url) = std::env::var("HYPERSYNC_URL") {
        builder = builder.url(url);
    }
    let client = builder.build()?;

    let debug = std::env::var("TUNE_DEBUG").as_deref() == Ok("1");

    let variants = if single {
        vec![Variant {
            label: "default".to_string(),
            config: StreamConfig::default(),
        }]
    } else {
        build_grid()
    };

    println!(
        "Running {} config(s) against '{query_path}' on chain {chain_id}\n",
        variants.len()
    );

    let mut rows = Vec::new();
    for variant in &variants {
        eprintln!("running: {} ...", variant.label);
        let summary = run_config(&client, query.clone(), variant.config.clone(), debug).await?;
        if single {
            print_report(&variant.label, &variant.config, &summary);
        }
        rows.push((variant.label.clone(), summary));
    }

    if !single {
        print_table(&rows);
    }

    Ok(())
}

/// The default sweep grid: a small cross-product of the dynamic knobs.
fn build_grid() -> Vec<Variant> {
    let targets = [200_000u64, 400_000, 800_000];
    let concurrencies = [5usize, 10, 20];
    let mut variants = Vec::new();
    for &target in &targets {
        for &concurrency in &concurrencies {
            variants.push(Variant {
                label: format!("t={}k c={}", target / 1000, concurrency),
                config: StreamConfig {
                    response_bytes_target: target,
                    concurrency,
                    ..Default::default()
                },
            });
        }
    }
    variants
}

async fn run_config(
    client: &Client,
    query: Query,
    config: StreamConfig,
    debug: bool,
) -> anyhow::Result<StreamSummary> {
    let metrics = Arc::new(StreamMetrics::new());
    let observer: Arc<dyn StreamObserver> = Arc::new(TuneObserver {
        metrics: metrics.clone(),
        debug,
    });
    let mut rx = client
        .stream_arrow_with_observer(query, config, observer)
        .await?;
    while let Some(res) = rx.recv().await {
        // Drain; we only care about the metrics, not the payload.
        let _ = res?;
    }
    Ok(metrics.summary())
}

fn print_table(rows: &[(String, StreamSummary)]) {
    println!(
        "{:<14} {:>6} {:>7} {:>10} {:>9} {:>9} {:>9} {:>9}",
        "config", "reqs", "trunc%", "blocks/s", "MB/s", "ratio", "mblocks", "maxbuf"
    );
    println!("{}", "-".repeat(80));
    for (label, s) in rows {
        println!(
            "{:<14} {:>6} {:>6.1}% {:>10.0} {:>9.2} {:>9.2} {:>9.0} {:>8}M",
            label,
            s.num_requests,
            s.truncation_rate * 100.0,
            s.blocks_per_sec,
            s.bytes_per_sec / 1_000_000.0,
            s.mean_size_ratio,
            s.mean_blocks,
            s.max_buffered_bytes_observed / 1_000_000,
        );
    }
}

fn print_report(label: &str, config: &StreamConfig, s: &StreamSummary) {
    println!("===== {label} =====");
    println!(
        "config: target={} concurrency={} batch_size={} max_batch_size={:?} max_buffered_bytes={:?}",
        config.response_bytes_target,
        config.concurrency,
        config.batch_size,
        config.max_batch_size,
        config.max_buffered_bytes,
    );
    println!("requests:        {}", s.num_requests);
    println!(
        "truncated:       {} ({:.1}%)",
        s.num_truncated,
        s.truncation_rate * 100.0
    );
    println!("total bytes:     {}", s.total_bytes);
    println!("total blocks:    {}", s.total_blocks);
    println!("wall clock:      {:?}", s.wall_clock);
    println!("blocks/s:        {:.0}", s.blocks_per_sec);
    println!("MB/s:            {:.2}", s.bytes_per_sec / 1_000_000.0);
    println!("mean size ratio: {:.3}", s.mean_size_ratio);
    println!(
        "blocks/req:      min={} mean={:.0} max={}",
        s.min_blocks, s.mean_blocks, s.max_blocks
    );
    println!("mean bytes/blk:  {:.1}", s.mean_bytes_per_block);
    println!("mean in-flight:  {:.2}", s.mean_in_flight);
    println!("max buffered:    {} bytes", s.max_buffered_bytes_observed);
    println!("frontier/gap:    {} / {}", s.num_frontier, s.num_gap_fill);
    println!("size-vs-target histogram:");
    for (label, count) in SIZE_BUCKET_LABELS.iter().zip(s.size_histogram.iter()) {
        println!("  {label:>9}x: {count}");
    }
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_query_deserializes() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/query.example.json");
        let json = std::fs::read_to_string(path).unwrap();
        let query: Query = serde_json::from_str(&json).unwrap();
        assert_eq!(query.from_block, 18_000_000);
        assert_eq!(query.to_block, Some(18_100_000));
        assert_eq!(query.logs.len(), 1);
        assert_eq!(query.field_selection.log.len(), 5);
    }

    #[test]
    fn grid_is_non_empty_and_distinct() {
        let grid = build_grid();
        assert_eq!(grid.len(), 9);
        let labels: std::collections::HashSet<_> = grid.iter().map(|v| v.label.clone()).collect();
        assert_eq!(labels.len(), grid.len(), "labels are unique");
    }
}
