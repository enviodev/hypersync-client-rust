use anyhow::Result;
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
            serialization_format: SerializationFormat::Json,
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
#[derive(Default, Clone, Copy, Debug, Serialize, Deserialize)]
pub enum SerializationFormat {
    /// Use JSON serialization (default)
    #[default]
    Json,
    /// Use Cap'n Proto binary serialization
    CapnProto {
        /// Whether to use query caching
        should_cache_queries: bool,
    },
}

/// Config for hypersync event streaming.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Column mapping for stream function output.
    /// It lets you map columns you want into the DataTypes you want.
    pub column_mapping: Option<ColumnMapping>,
    /// Event signature used to populate decode logs. Decode logs would be empty if set to None.
    pub event_signature: Option<String>,
    /// Trace signature used to populate decoded traces. Decoded traces would be empty if set to None.
    pub trace_signature: Option<String>,
    /// Determines formatting of binary columns numbers into utf8 hex.
    #[serde(default)]
    pub hex_output: HexOutput,
    /// Initial batch size. Size would be adjusted based on response size during execution.
    #[serde(default = "StreamConfig::default_batch_size")]
    pub batch_size: u64,
    /// Maximum batch size that could be used during dynamic adjustment.
    #[serde(default = "StreamConfig::default_max_batch_size")]
    pub max_batch_size: u64,
    /// Minimum batch size that could be used during dynamic adjustment.
    #[serde(default = "StreamConfig::default_min_batch_size")]
    pub min_batch_size: u64,
    /// Number of async threads that would be spawned to execute different block ranges of queries.
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
    /// Size of a response in bytes from which step size will be lowered
    #[serde(default = "StreamConfig::default_response_bytes_ceiling")]
    pub response_bytes_ceiling: u64,
    /// Size of a response in bytes from which step size will be increased
    #[serde(default = "StreamConfig::default_response_bytes_floor")]
    pub response_bytes_floor: u64,
    /// Stream data in reverse order
    #[serde(default = "StreamConfig::default_reverse")]
    pub reverse: bool,
}

/// Determines format of Binary column
#[derive(Default, Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
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
            max_batch_size: Self::default_max_batch_size(),
            min_batch_size: Self::default_min_batch_size(),
            concurrency: Self::default_concurrency(),
            max_num_blocks: None,
            max_num_transactions: None,
            max_num_logs: None,
            max_num_traces: None,
            response_bytes_ceiling: Self::default_response_bytes_ceiling(),
            response_bytes_floor: Self::default_response_bytes_floor(),
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

    /// Default maximum batch size
    pub const fn default_max_batch_size() -> u64 {
        200_000
    }

    /// Default minimum batch size
    pub const fn default_min_batch_size() -> u64 {
        200
    }

    /// Default response bytes ceiling for dynamic batch adjustment
    pub const fn default_response_bytes_ceiling() -> u64 {
        500_000
    }

    /// Default response bytes floor for dynamic batch adjustment
    pub const fn default_response_bytes_floor() -> u64 {
        250_000
    }

    /// Default reverse streaming setting
    pub const fn default_reverse() -> bool {
        false
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
        assert_eq!(default_config.max_batch_size, 200_000);
        assert_eq!(default_config.min_batch_size, 200);
        assert_eq!(default_config.response_bytes_ceiling, 500_000);
        assert_eq!(default_config.response_bytes_floor, 250_000);
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
        assert_eq!(partial_config.max_batch_size, 200_000); // should use default
    }
}
