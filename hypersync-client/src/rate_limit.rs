/// Rate limit information extracted from response headers.
///
/// Envoy's rate limiter returns these headers in the IETF draft format:
/// - `x-ratelimit-limit`: e.g. `"60, 60;w=60"` (total quota for the window)
/// - `x-ratelimit-remaining`: e.g. `"57"` (requests left in window)
/// - `x-ratelimit-reset`: e.g. `"52"` (seconds until window resets)
/// - `retry-after`: e.g. `"5"` (seconds to wait, present on 429 responses)
#[derive(Debug, Clone, Default)]
pub struct RateLimitInfo {
    /// Total request quota for the current window.
    ///
    /// Parsed from `x-ratelimit-limit`. For IETF draft format like `"60, 60;w=60"`,
    /// the first integer before the comma is used.
    pub limit: Option<u64>,
    /// Remaining requests in the current window.
    ///
    /// Parsed from `x-ratelimit-remaining`.
    pub remaining: Option<u64>,
    /// Seconds until the rate limit window resets.
    ///
    /// Parsed from `x-ratelimit-reset`.
    pub reset_secs: Option<u64>,
    /// Seconds to wait before retrying, typically present on 429 responses.
    ///
    /// Parsed from `retry-after`.
    pub retry_after_secs: Option<u64>,
}

impl RateLimitInfo {
    /// Extracts rate limit information from HTTP response headers.
    ///
    /// All parsing is best-effort: missing or unparseable headers become `None`.
    pub(crate) fn from_response(res: &reqwest::Response) -> Self {
        Self {
            limit: Self::parse_limit_header(res),
            remaining: Self::parse_u64_header(res, "x-ratelimit-remaining"),
            reset_secs: Self::parse_u64_header(res, "x-ratelimit-reset"),
            retry_after_secs: Self::parse_u64_header(res, "retry-after"),
        }
    }

    /// Returns `true` if the rate limit quota has been exhausted.
    pub fn is_rate_limited(&self) -> bool {
        self.remaining == Some(0)
    }

    /// Returns the suggested number of seconds to wait before making another request.
    ///
    /// Prefers `retry-after` (explicit server instruction), falls back to `reset_secs`.
    pub fn suggested_wait_secs(&self) -> Option<u64> {
        self.retry_after_secs.or(self.reset_secs)
    }

    /// Parses `x-ratelimit-limit` which uses IETF draft format: `"60, 60;w=60"`.
    /// Extracts the first integer before the comma.
    fn parse_limit_header(res: &reqwest::Response) -> Option<u64> {
        let value = res.headers().get("x-ratelimit-limit")?.to_str().ok()?;
        // Take first value before comma: "60, 60;w=60" -> "60"
        let first = value.split(',').next()?.trim();
        first.parse().ok()
    }

    /// Parses a simple u64 header value.
    fn parse_u64_header(res: &reqwest::Response, name: &str) -> Option<u64> {
        res.headers().get(name)?.to_str().ok()?.trim().parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_limit_header_ietf_format() {
        // Simulate by testing the parsing logic directly
        let value = "60, 60;w=60";
        let first = value.split(',').next().unwrap().trim();
        assert_eq!(first.parse::<u64>().unwrap(), 60);
    }

    #[test]
    fn test_parse_limit_header_simple() {
        let value = "100";
        let first = value.split(',').next().unwrap().trim();
        assert_eq!(first.parse::<u64>().unwrap(), 100);
    }

    #[test]
    fn test_is_rate_limited() {
        let info = RateLimitInfo {
            remaining: Some(0),
            ..Default::default()
        };
        assert!(info.is_rate_limited());

        let info = RateLimitInfo {
            remaining: Some(5),
            ..Default::default()
        };
        assert!(!info.is_rate_limited());

        let info = RateLimitInfo::default();
        assert!(!info.is_rate_limited());
    }

    #[test]
    fn test_suggested_wait_secs() {
        // Prefers retry_after_secs
        let info = RateLimitInfo {
            retry_after_secs: Some(5),
            reset_secs: Some(30),
            ..Default::default()
        };
        assert_eq!(info.suggested_wait_secs(), Some(5));

        // Falls back to reset_secs
        let info = RateLimitInfo {
            reset_secs: Some(30),
            ..Default::default()
        };
        assert_eq!(info.suggested_wait_secs(), Some(30));

        // None when no info
        let info = RateLimitInfo::default();
        assert_eq!(info.suggested_wait_secs(), None);
    }
}
