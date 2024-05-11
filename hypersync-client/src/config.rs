use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;
use url::Url;

use crate::ColumnMapping;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClientConfig {
    /// Url of the source hypersync instance
    pub url: Url,
    /// Optional bearer_token to put into http requests made to source hypersync instance
    pub bearer_token: Option<String>,
    /// Timeout treshold for a single http request in milliseconds
    pub http_req_timeout_millis: Option<NonZeroU64>,
    pub max_num_retries: Option<usize>,
    pub retry_backoff_ms: u64,
    pub retry_base_ms: u64,
    pub retry_ceiling_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Define type mapping for output columns
    #[serde(default)]
    pub column_mapping: ColumnMapping,
    /// Event signature to parse the logs with. example: Transfer(address indexed from, address indexed to, uint256 amount)
    pub event_signature: Option<String>,
    /// Convert binary output columns to hex
    #[serde(default)]
    pub hex_output: bool,
    pub batch_size: Option<u64>,
    pub concurrency: Option<usize>,
    pub max_num_blocks: Option<u64>,
    pub max_num_transactions: Option<u64>,
    pub max_num_logs: Option<u64>,
    pub max_num_traces: Option<u64>,
}
