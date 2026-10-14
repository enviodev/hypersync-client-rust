//! HTTP 402 Payment Required: the token's package has no credits left, or the
//! request carried no API token. The server answers with a human-readable
//! message and one or more MPP (Machine Payments Protocol) challenges in
//! `WWW-Authenticate: Payment ...` headers.
//!
//! Retrying cannot succeed until the balance is topped up (in the Envio
//! dashboard) or a challenge is paid, so the client returns this error at once
//! instead of retrying.

use std::fmt;

/// Details of a 402 Payment Required response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentRequiredInfo {
    /// The server's explanation, e.g. how to top up.
    pub message: String,
    /// Raw `WWW-Authenticate` values, each starting with `Payment `. An MPP
    /// client can pay one of them and retry with `Authorization: Payment`.
    pub challenges: Vec<String>,
}

impl PaymentRequiredInfo {
    /// Builds the info from a 402 response, consuming its body.
    pub(crate) async fn from_response(res: reqwest::Response) -> Self {
        let challenges = res
            .headers()
            .get_all(reqwest::header::WWW_AUTHENTICATE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .filter(|v| v.starts_with("Payment "))
            .map(str::to_owned)
            .collect();
        let body = res.text().await.unwrap_or_default();
        Self {
            message: message_from_body(&body),
            challenges,
        }
    }
}

/// The body is `{"error": "..."}`; fall back to the raw text.
fn message_from_body(body: &str) -> String {
    #[derive(serde::Deserialize)]
    struct ErrorBody {
        error: String,
    }
    match serde_json::from_str::<ErrorBody>(body) {
        Ok(b) => b.error,
        Err(_) if body.trim().is_empty() => {
            "payment required: no credits left on this token".to_owned()
        }
        Err(_) => body.trim().to_owned(),
    }
}

impl fmt::Display for PaymentRequiredInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)?;
        if !self.challenges.is_empty() {
            write!(
                f,
                " ({} MPP payment challenge(s) offered)",
                self.challenges.len()
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_json_error_message() {
        assert_eq!(
            message_from_body(r#"{"error":"Your token has used its monthly credits."}"#),
            "Your token has used its monthly credits."
        );
    }

    #[test]
    fn falls_back_to_text_or_default() {
        assert_eq!(message_from_body("plain text"), "plain text");
        assert!(message_from_body("").contains("payment required"));
    }

    #[test]
    fn display_mentions_challenges() {
        let info = PaymentRequiredInfo {
            message: "Out of credits.".into(),
            challenges: vec!["Payment id=\"x\", method=\"stripe\"".into()],
        };
        assert_eq!(
            info.to_string(),
            "Out of credits. (1 MPP payment challenge(s) offered)"
        );
    }
}
