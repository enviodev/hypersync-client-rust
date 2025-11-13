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
    /// HyperSync server bearer token.
    #[serde(default)]
    pub bearer_token: String,
    /// Milliseconds to wait for a response before timing out.
    #[serde(default = "default_http_req_timeout_millis")]
    pub http_req_timeout_millis: u64,
    /// Number of retries to attempt before returning error.
    #[serde(default = "default_max_num_retries")]
    pub max_num_retries: usize,
    /// Milliseconds that would be used for retry backoff increasing.
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    /// Initial wait time for request backoff.
    #[serde(default = "default_retry_base_ms")]
    pub retry_base_ms: u64,
    /// Ceiling time for request backoff.
    #[serde(default = "default_retry_ceiling_ms")]
    pub retry_ceiling_ms: u64,
    /// Query serialization format to use for HTTP requests.
    #[serde(default)]
    pub serialization_format: SerializationFormat,
}

const fn default_http_req_timeout_millis() -> u64 {
    30_000
}

const fn default_max_num_retries() -> usize {
    12
}

const fn default_retry_backoff_ms() -> u64 {
    500
}

const fn default_retry_base_ms() -> u64 {
    200
}

const fn default_retry_ceiling_ms() -> u64 {
    5_000
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: String::default(),
            bearer_token: String::default(),
            http_req_timeout_millis: default_http_req_timeout_millis(),
            max_num_retries: default_max_num_retries(),
            retry_backoff_ms: default_retry_backoff_ms(),
            retry_base_ms: default_retry_base_ms(),
            retry_ceiling_ms: default_retry_ceiling_ms(),
            serialization_format: SerializationFormat::Json,
        }
    }
}

impl ClientConfig {
    /// Validates the config
    pub fn validate(&self) -> Result<()> {
        if self.url.is_empty() {
            anyhow::bail!("url is required");
        }

        // validate that url is a valid url
        if Url::parse(&self.url).is_err() {
            anyhow::bail!("url is malformed");
        }

        if self.bearer_token.is_empty() {
            anyhow::bail!(
                "bearer_token is required - get one from https://envio.dev/app/api-tokens"
            );
        }
        // validate that bearer token is a uuid
        if uuid::Uuid::parse_str(self.bearer_token.as_str()).is_err() {
            anyhow::bail!("bearer_token is malformed - make sure its a token from https://envio.dev/app/api-tokens");
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
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Column mapping for stream function output.
    /// It lets you map columns you want into the DataTypes you want.
    pub column_mapping: Option<ColumnMapping>,
    /// Event signature used to populate decode logs. Decode logs would be empty if set to None.
    pub event_signature: Option<String>,
    /// Determines formatting of binary columns numbers into utf8 hex.
    #[serde(default)]
    pub hex_output: HexOutput,
    /// Initial batch size. Size would be adjusted based on response size during execution.
    pub batch_size: Option<u64>,
    /// Maximum batch size that could be used during dynamic adjustment.
    pub max_batch_size: Option<u64>,
    /// Minimum batch size that could be used during dynamic adjustment.
    pub min_batch_size: Option<u64>,
    /// Number of async threads that would be spawned to execute different block ranges of queries.
    pub concurrency: Option<usize>,
    /// Max number of blocks to fetch in a single request.
    pub max_num_blocks: Option<usize>,
    /// Max number of transactions to fetch in a single request.
    pub max_num_transactions: Option<usize>,
    /// Max number of logs to fetch in a single request.
    pub max_num_logs: Option<usize>,
    /// Max number of traces to fetch in a single request.
    pub max_num_traces: Option<usize>,
    /// Size of a response in bytes from which step size will be lowered
    pub response_bytes_ceiling: Option<u64>,
    /// Size of a response in bytes from which step size will be increased
    pub response_bytes_floor: Option<u64>,
    /// Stream data in reverse order
    pub reverse: Option<bool>,
}

/// Determines format of Binary column
#[derive(Default, Clone, Copy, Debug, Serialize, Deserialize)]
pub enum HexOutput {
    /// Binary column won't be formatted as hex
    #[default]
    NoEncode,
    /// Binary column would be formatted as prefixed hex i.e. 0xdeadbeef
    Prefixed,
    /// Binary column would be formatted as non prefixed hex i.e. deadbeef
    NonPrefixed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate() {
        let valid_cfg = ClientConfig {
            url: "https://hypersync.xyz".into(),
            bearer_token: "00000000-0000-0000-0000-000000000000".to_string(),
            ..Default::default()
        };

        assert!(valid_cfg.validate().is_ok(), "valid config");

        let cfg = ClientConfig {
            url: "https://hypersync.xyz".to_string(),
            bearer_token: "not a uuid".to_string(),
            ..Default::default()
        };

        assert!(cfg.validate().is_err(), "invalid uuid");

        let cfg = ClientConfig {
            url: "https://hypersync.xyz".to_string(),
            ..Default::default()
        };

        assert!(cfg.validate().is_err(), "missing bearer token");

        let cfg = ClientConfig {
            bearer_token: "00000000-0000-0000-0000-000000000000".to_string(),
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
}
