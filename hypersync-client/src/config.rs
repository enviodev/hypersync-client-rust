use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;
use url::Url;

use crate::ColumnMapping;

/// Configuration for the hypersync client.
#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct ClientConfig {
    /// HyperSync server URL.
    pub url: Option<Url>,
    /// HyperSync server bearer token.
    pub bearer_token: Option<String>,
    /// Milliseconds to wait for a response before timing out.
    pub http_req_timeout_millis: Option<NonZeroU64>,
    /// Number of retries to attempt before returning error.
    pub max_num_retries: Option<usize>,
    /// Milliseconds that would be used for retry backoff increasing.
    pub retry_backoff_ms: Option<u64>,
    /// Initial wait time for request backoff.
    pub retry_base_ms: Option<u64>,
    /// Ceiling time for request backoff.
    pub retry_ceiling_ms: Option<u64>,
    /// Query serialization format to use for HTTP requests.
    #[serde(default)]
    pub serialization_format: SerializationFormat,
}

/// Determines query serialization format for HTTP requests.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum SerializationFormat {
    /// Use JSON serialization (default)
    Json,
    /// Use Cap'n Proto binary serialization
    CapnProto,
}

impl Default for SerializationFormat {
    fn default() -> Self {
        // Keep this the default until all hs instances are upgraded to use Cap'n Proto endpoint
        Self::Json
    }
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
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum HexOutput {
    /// Binary column won't be formatted as hex
    NoEncode,
    /// Binary column would be formatted as prefixed hex i.e. 0xdeadbeef
    Prefixed,
    /// Binary column would be formatted as non prefixed hex i.e. deadbeef
    NonPrefixed,
}

impl Default for HexOutput {
    fn default() -> Self {
        Self::NoEncode
    }
}
