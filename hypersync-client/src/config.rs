use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::ColumnMapping;

/// Configuration for the hypersync client.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClientConfig {
    /// HyperSync server URL.
    #[serde(default)]
    pub url: String,
    /// HyperSync server api token.
    #[serde(default)]
    pub api_token: String,
    /// Milliseconds to wait for a response before timing out.
    #[serde(default = "ClientConfig::default_http_req_timeout_millis")]
    pub http_req_timeout_millis: u64,
    /// Number of retries to attempt before returning error.
    #[serde(default = "ClientConfig::default_max_num_retries")]
    pub max_num_retries: usize,
    /// Milliseconds that would be used for retry backoff increasing.
    #[serde(default = "ClientConfig::default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    /// Initial wait time for request backoff.
    #[serde(default = "ClientConfig::default_retry_base_ms")]
    pub retry_base_ms: u64,
    /// Ceiling time for request backoff.
    #[serde(default = "ClientConfig::default_retry_ceiling_ms")]
    pub retry_ceiling_ms: u64,
    /// Query serialization format to use for HTTP requests.
    #[serde(default)]
    pub serialization_format: SerializationFormat,
    /// Whether to proactively sleep when the rate limit is exhausted instead of
    /// sending requests that will be rejected with 429.
    ///
    /// Enabled by default. Set to `false` to opt out and handle rate limits yourself.
    #[serde(default = "ClientConfig::default_proactive_rate_limit_sleep")]
    pub proactive_rate_limit_sleep: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: String::default(),
            api_token: String::default(),
            http_req_timeout_millis: Self::default_http_req_timeout_millis(),
            max_num_retries: Self::default_max_num_retries(),
            retry_backoff_ms: Self::default_retry_backoff_ms(),
            retry_base_ms: Self::default_retry_base_ms(),
            retry_ceiling_ms: Self::default_retry_ceiling_ms(),
            serialization_format: SerializationFormat::default(),
            proactive_rate_limit_sleep: Self::default_proactive_rate_limit_sleep(),
        }
    }
}

impl ClientConfig {
    /// Default HTTP request timeout in milliseconds
    pub const fn default_http_req_timeout_millis() -> u64 {
        30_000
    }

    /// Default maximum number of retries
    pub const fn default_max_num_retries() -> usize {
        12
    }

    /// Default retry backoff in milliseconds
    pub const fn default_retry_backoff_ms() -> u64 {
        500
    }

    /// Default retry base time in milliseconds
    pub const fn default_retry_base_ms() -> u64 {
        200
    }

    /// Default retry ceiling time in milliseconds
    pub const fn default_retry_ceiling_ms() -> u64 {
        5_000
    }

    /// Default proactive rate limit sleep setting
    pub const fn default_proactive_rate_limit_sleep() -> bool {
        true
    }
    /// Validates the config
    pub fn validate(&self) -> Result<()> {
        if self.url.is_empty() {
            anyhow::bail!("url is required");
        }

        // validate that url is a valid url
        if Url::parse(&self.url).is_err() {
            anyhow::bail!("url is malformed");
        }

        if self.api_token.is_empty() {
            anyhow::bail!("api_token is required - get one from https://envio.dev/app/api-tokens");
        }
        // validate that api token is a uuid
        if uuid::Uuid::parse_str(self.api_token.as_str()).is_err() {
            anyhow::bail!("api_token is malformed - make sure its a token from https://envio.dev/app/api-tokens");
        }

        if self.http_req_timeout_millis == 0 {
            anyhow::bail!("http_req_timeout_millis must be greater than 0");
        }

        Ok(())
    }
}

/// Determines query serialization format for HTTP requests.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum SerializationFormat {
    /// Use JSON serialization
    Json,
    /// Use Cap'n Proto binary serialization (default, with query caching enabled)
    CapnProto {
        /// Whether to use query caching
        should_cache_queries: bool,
    },
}

impl Default for SerializationFormat {
    fn default() -> Self {
        Self::CapnProto {
            should_cache_queries: true,
        }
    }
}

/// Config for hypersync event streaming.
///
/// `column_mapping`, `event_signature` and `hex_output` only affect the Arrow output path
/// (`collect_arrow`, `collect_parquet`, `stream_arrow`). The simple-type functions (`collect`,
/// `collect_events`, `stream`, `stream_events`) always work on raw binary columns: they reject
/// `column_mapping` and `event_signature`, and ignore `hex_output`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StreamConfig {
    /// Column mapping for stream function output.
    /// It lets you map columns you want into the DataTypes you want.
    ///
    /// Arrow output path only. The simple-type functions return an error if this is set.
    pub column_mapping: Option<ColumnMapping>,
    /// Event signature used to populate decode logs. Decode logs would be empty if set to None.
    ///
    /// Arrow output path only. The simple-type functions return an error if this is set.
    pub event_signature: Option<String>,
    /// Determines formatting of binary columns numbers into utf8 hex.
    ///
    /// Arrow output path only. The simple-type functions ignore this and always decode from
    /// binary columns, since their output types hold raw bytes and render hex themselves.
    #[serde(default)]
    pub hex_output: HexOutput,
    /// Initial, deliberately-overestimated batch size, used for the first wave of
    /// requests and as a fallback before any response density has been measured.
    #[serde(default = "StreamConfig::default_batch_size")]
    pub batch_size: u64,
    /// Optional hard upper cap on the number of blocks fetched in a single
    /// request. `None` (the default) means no cap: an over-large request that the
    /// server truncates simply leaves a gap that is backfilled, so overshoot is
    /// self-correcting. Set it to bound blocks-per-chunk explicitly.
    #[serde(default)]
    pub max_batch_size: Option<u64>,
    /// Hard lower clamp on the projected block count, to avoid tiny ranges.
    #[serde(default = "StreamConfig::default_min_batch_size")]
    pub min_batch_size: u64,
    /// Number of async threads that would be spawned to execute different block ranges of queries.
    /// `0` is an error, `1` streams sequentially, `>= 2` uses the projecting scheduler.
    #[serde(default = "StreamConfig::default_concurrency")]
    pub concurrency: usize,
    /// Max number of blocks to fetch in a single request.
    #[serde(default)]
    pub max_num_blocks: Option<usize>,
    /// Max number of transactions to fetch in a single request.
    #[serde(default)]
    pub max_num_transactions: Option<usize>,
    /// Max number of logs to fetch in a single request.
    #[serde(default)]
    pub max_num_logs: Option<usize>,
    /// Max number of traces to fetch in a single request.
    #[serde(default)]
    pub max_num_traces: Option<usize>,
    /// Target response size in bytes. Each request's block span is projected from
    /// the most recently observed byte-density to aim each response at this size.
    #[serde(default = "StreamConfig::default_response_bytes_target")]
    pub response_bytes_target: u64,
    /// Optional cap on the bytes of fetched-but-undelivered chunks held in the
    /// reorder buffer (consumer backpressure). `None` (the default) is
    /// **adaptive**: it starts at `2 * concurrency * response_bytes_target` and
    /// grows to `2 * concurrency * max(response_bytes_target, largest_response)`
    /// so the pipeline stays full even for byte-heavy queries whose responses far
    /// exceed the target (otherwise a single response could exceed the cap and
    /// throttle look-ahead to near-sequential). Set an explicit value to bound
    /// memory; an explicit cap is honoured verbatim and never grown. `Some(0)` is
    /// valid and means "no look-ahead buffer": only the chunk delivery is
    /// currently waiting on is fetched, so the stream runs effectively
    /// sequentially with minimal memory (it still completes — the watermark chunk
    /// is always allowed through).
    #[serde(default)]
    pub max_buffered_bytes: Option<u64>,
    /// Stream data in reverse order
    #[serde(default = "StreamConfig::default_reverse")]
    pub reverse: bool,
}

/// Determines format of Binary column in Arrow output.
///
/// Only used by `collect_arrow`, `collect_parquet` and `stream_arrow`. Has no effect on the
/// simple-type functions.
#[derive(Default, Clone, Copy, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub enum HexOutput {
    /// Binary column won't be formatted as hex
    #[default]
    NoEncode,
    /// Binary column would be formatted as prefixed hex i.e. 0xdeadbeef
    Prefixed,
    /// Binary column would be formatted as non prefixed hex i.e. deadbeef
    NonPrefixed,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            column_mapping: None,
            event_signature: None,
            hex_output: HexOutput::default(),
            batch_size: Self::default_batch_size(),
            max_batch_size: None,
            min_batch_size: Self::default_min_batch_size(),
            concurrency: Self::default_concurrency(),
            max_num_blocks: None,
            max_num_transactions: None,
            max_num_logs: None,
            max_num_traces: None,
            response_bytes_target: Self::default_response_bytes_target(),
            max_buffered_bytes: None,
            reverse: Self::default_reverse(),
        }
    }
}

impl StreamConfig {
    /// Default concurrency for stream processing
    pub const fn default_concurrency() -> usize {
        10
    }

    /// Default initial batch size
    pub const fn default_batch_size() -> u64 {
        1000
    }

    /// Default minimum batch size
    pub const fn default_min_batch_size() -> u64 {
        200
    }

    /// Default target response size in bytes that projection aims each response at
    pub const fn default_response_bytes_target() -> u64 {
        400_000
    }

    /// Default reverse streaming setting
    pub const fn default_reverse() -> bool {
        false
    }

    /// Preset for **dense** workloads: queries that match a lot of data per block
    /// (busy contracts, all-logs, popular ERC-20 transfers).
    ///
    /// Such streams are throughput-bound and scale well with parallelism, so this
    /// raises `concurrency` above the default. The default `response_bytes_target`
    /// (400 KB) is already a good fit — benchmarking showed dense responses
    /// plateau near that size, and pushing the target higher mostly adds
    /// truncation/backfill rather than bigger responses.
    ///
    /// `max_buffered_bytes` is left unset so the adaptive default applies. If you
    /// have plenty of rate-limit headroom you can push `concurrency` higher still.
    pub fn dense() -> Self {
        Self {
            concurrency: 20,
            response_bytes_target: Self::default_response_bytes_target(),
            ..Self::default()
        }
    }

    /// Preset for **sparse** workloads: selective queries over wide block ranges
    /// (rare events, low-volume contracts) where most blocks match nothing.
    ///
    /// Here latency, not bytes, dominates, and benchmarking showed that *high*
    /// concurrency actually hurts: extra workers just fragment a large empty
    /// region into more (smaller) requests. So this keeps concurrency moderate and
    /// raises `batch_size` so the first wave covers a lot of ground before
    /// per-request projection kicks in — an over-estimate that self-corrects via
    /// backfill if it hits a dense patch.
    pub fn sparse() -> Self {
        Self {
            concurrency: Self::default_concurrency(),
            batch_size: 20_000,
            ..Self::default()
        }
    }

    /// Preset for **archival / byte-heavy** workloads: full block + transaction
    /// pulls (e.g. `include_all_blocks` with wide field selection) where each
    /// response is many megabytes.
    ///
    /// These streams are bounded by the reorder buffer, not concurrency: a single
    /// response can dwarf `response_bytes_target`, so the adaptive
    /// `max_buffered_bytes` default (left unset here) is what keeps the pipeline
    /// full — in benchmarks it roughly doubled throughput versus a buffer sized to
    /// the target. Concurrency past ~10–15 gives little extra here.
    pub fn archival() -> Self {
        Self {
            concurrency: 12,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate() {
        let valid_cfg = ClientConfig {
            url: "https://hypersync.xyz".into(),
            api_token: "00000000-0000-0000-0000-000000000000".to_string(),
            ..Default::default()
        };

        assert!(valid_cfg.validate().is_ok(), "valid config");

        let cfg = ClientConfig {
            url: "https://hypersync.xyz".to_string(),
            api_token: "not a uuid".to_string(),
            ..Default::default()
        };

        assert!(cfg.validate().is_err(), "invalid uuid");

        let cfg = ClientConfig {
            url: "https://hypersync.xyz".to_string(),
            ..Default::default()
        };

        assert!(cfg.validate().is_err(), "missing api token");

        let cfg = ClientConfig {
            api_token: "00000000-0000-0000-0000-000000000000".to_string(),
            ..Default::default()
        };

        assert!(cfg.validate().is_err(), "missing url");
        let cfg = ClientConfig {
            http_req_timeout_millis: 0,
            ..valid_cfg
        };
        assert!(
            cfg.validate().is_err(),
            "http_req_timeout_millis must be greater than 0"
        );
    }

    #[test]
    fn test_stream_config_defaults() {
        let default_config = StreamConfig::default();

        // Check that all defaults are applied correctly
        assert_eq!(default_config.concurrency, 10);
        assert_eq!(default_config.batch_size, 1000);
        assert_eq!(default_config.max_batch_size, None);
        assert_eq!(default_config.min_batch_size, 200);
        assert_eq!(default_config.response_bytes_target, 400_000);
        assert_eq!(default_config.max_buffered_bytes, None);
        assert!(!default_config.reverse);
        assert_eq!(default_config.hex_output, HexOutput::NoEncode);
        assert!(default_config.column_mapping.is_none());
        assert!(default_config.event_signature.is_none());
        assert!(default_config.max_num_blocks.is_none());
        assert!(default_config.max_num_transactions.is_none());
        assert!(default_config.max_num_logs.is_none());
        assert!(default_config.max_num_traces.is_none());
    }

    #[test]
    fn test_stream_config_serde() {
        // Test serialization of default config
        let default_config = StreamConfig::default();
        let json = serde_json::to_string(&default_config).unwrap();
        let deserialized: StreamConfig = serde_json::from_str(&json).unwrap();

        // Verify round-trip works
        assert_eq!(deserialized.concurrency, default_config.concurrency);
        assert_eq!(deserialized.batch_size, default_config.batch_size);
        assert_eq!(deserialized.reverse, default_config.reverse);

        // Test partial JSON (missing some fields should use defaults)
        let partial_json = r#"{"reverse": true, "batch_size": 500}"#;
        let partial_config: StreamConfig = serde_json::from_str(partial_json).unwrap();

        assert!(partial_config.reverse);
        assert_eq!(partial_config.batch_size, 500);
        assert_eq!(partial_config.concurrency, 10); // should use default
        assert_eq!(partial_config.max_batch_size, None); // should use default
        assert_eq!(partial_config.response_bytes_target, 400_000); // should use default
        assert_eq!(partial_config.max_buffered_bytes, None); // should use default

        // Explicitly setting the new optional caps round-trips.
        let explicit_json = r#"{"max_batch_size": 50000, "response_bytes_target": 800000, "max_buffered_bytes": 1048576}"#;
        let explicit_config: StreamConfig = serde_json::from_str(explicit_json).unwrap();
        assert_eq!(explicit_config.max_batch_size, Some(50_000));
        assert_eq!(explicit_config.response_bytes_target, 800_000);
        assert_eq!(explicit_config.max_buffered_bytes, Some(1_048_576));
    }

    #[test]
    fn test_stream_config_presets() {
        // Dense: more parallelism, default target, adaptive buffer.
        let dense = StreamConfig::dense();
        assert_eq!(dense.concurrency, 20);
        assert_eq!(dense.response_bytes_target, 400_000);
        assert_eq!(dense.max_buffered_bytes, None);

        // Sparse: moderate concurrency, big first wave.
        let sparse = StreamConfig::sparse();
        assert_eq!(sparse.concurrency, 10);
        assert_eq!(sparse.batch_size, 20_000);
        assert_eq!(sparse.max_buffered_bytes, None);

        // Archival: modest concurrency, relies on adaptive buffer.
        let archival = StreamConfig::archival();
        assert_eq!(archival.concurrency, 12);
        assert_eq!(archival.max_buffered_bytes, None);

        // Presets keep the rest of the defaults.
        assert_eq!(dense.min_batch_size, StreamConfig::default_min_batch_size());
        assert!(!sparse.reverse);
    }
}
