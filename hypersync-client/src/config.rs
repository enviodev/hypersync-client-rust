use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;
use url::Url;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Url of the source hypersync instance
    pub url: Url,
    /// Optional bearer_token to put into http requests made to source hypersync instance
    pub bearer_token: Option<String>,
    /// Timout treshold for a single http request in milliseconds, default is 30 seconds (30_000ms)
    #[serde(default = "default_http_req_timeout_millis")]
    pub http_req_timeout_millis: NonZeroU64,
}

pub fn default_http_req_timeout_millis() -> NonZeroU64 {
    NonZeroU64::new(30000).unwrap()
}
