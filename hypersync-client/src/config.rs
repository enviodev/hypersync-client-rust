use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;
use url::Url;

use crate::ColumnMapping;

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub struct ClientConfig {
    pub url: Option<Url>,
    pub bearer_token: Option<String>,
    pub http_req_timeout_millis: Option<NonZeroU64>,
    pub max_num_retries: Option<usize>,
    pub retry_backoff_ms: Option<u64>,
    pub retry_base_ms: Option<u64>,
    pub retry_ceiling_ms: Option<u64>,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub column_mapping: Option<ColumnMapping>,
    pub event_signature: Option<String>,
    #[serde(default)]
    pub hex_output: HexOutput,
    pub batch_size: Option<u64>,
    pub concurrency: Option<usize>,
    pub max_num_blocks: Option<usize>,
    pub max_num_transactions: Option<usize>,
    pub max_num_logs: Option<usize>,
    pub max_num_traces: Option<usize>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum HexOutput {
    NoEncode,
    Prefixed,
    NonPrefixed,
}

impl Default for HexOutput {
    fn default() -> Self {
        Self::NoEncode
    }
}
