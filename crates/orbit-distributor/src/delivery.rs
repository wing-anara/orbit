//! Delivery of batches to Durable Objects.

use std::time::Duration;

use orbit_protocol::cdc::{CdcBatch, CdcBatchAck};

#[derive(Debug, thiserror::Error)]
pub enum DeliveryError {
    /// The request did not complete (connection, timeout, 5xx). Safe to retry: the DO either
    /// applied the batch (then the retry is a duplicate) or did not.
    #[error("transport: {0}")]
    Transport(String),
    /// The endpoint answered with a non-success status that is not transient (4xx other than
    /// 408/429). Retried with backoff as well, but reported distinctly.
    #[error("http {status}: {body}")]
    Http { status: u16, body: String },
    /// The response body was not a valid `CdcBatchAck`.
    #[error("invalid ack: {0}")]
    InvalidAck(String),
}

impl DeliveryError {
    pub fn is_transient(&self) -> bool {
        matches!(self, DeliveryError::Transport(_))
            || matches!(self, DeliveryError::Http { status, .. } if *status == 408 || *status == 429 || *status >= 500)
    }
}

/// Where batches go. Implemented for HTTP and, in tests, for in-memory fakes.
#[async_trait::async_trait]
pub trait Sink: Send + Sync + 'static {
    async fn deliver(&self, batch: &CdcBatch) -> Result<CdcBatchAck, DeliveryError>;
}

/// Posts batches to `{base_url}/internal/cdc/{partition}` with a bearer secret.
pub struct HttpSink {
    client: reqwest::Client,
    base_url: String,
    secret: String,
}

impl HttpSink {
    pub fn new(
        base_url: impl Into<String>,
        secret: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, DeliveryError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| DeliveryError::Transport(e.to_string()))?;
        Ok(Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            secret: secret.into(),
        })
    }

    pub fn url_for(&self, partition: &str) -> String {
        format!("{}/internal/cdc/{}", self.base_url, urlencode(partition))
    }
}

#[async_trait::async_trait]
impl Sink for HttpSink {
    async fn deliver(&self, batch: &CdcBatch) -> Result<CdcBatchAck, DeliveryError> {
        let resp = self
            .client
            .post(self.url_for(&batch.partition))
            .bearer_auth(&self.secret)
            .json(batch)
            .send()
            .await
            .map_err(|e| DeliveryError::Transport(e.to_string()))?;
        let status = resp.status();
        let body = resp.text().await.map_err(|e| DeliveryError::Transport(e.to_string()))?;
        if !status.is_success() {
            return Err(DeliveryError::Http {
                status: status.as_u16(),
                body: body.chars().take(500).collect(),
            });
        }
        serde_json::from_str::<CdcBatchAck>(&body)
            .map_err(|e| DeliveryError::InvalidAck(format!("{e}: {}", body.chars().take(200).collect::<String>())))
    }
}

pub fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_partition_keys() {
        assert_eq!(urlencode("org_1-2.x~"), "org_1-2.x~");
        assert_eq!(urlencode("a b/ü"), "a%20b%2F%C3%BC");
    }

    #[test]
    fn transient_classification() {
        assert!(DeliveryError::Transport("x".into()).is_transient());
        assert!(
            DeliveryError::Http {
                status: 503,
                body: String::new()
            }
            .is_transient()
        );
        assert!(
            DeliveryError::Http {
                status: 429,
                body: String::new()
            }
            .is_transient()
        );
        assert!(
            !DeliveryError::Http {
                status: 401,
                body: String::new()
            }
            .is_transient()
        );
        assert!(!DeliveryError::InvalidAck("x".into()).is_transient());
    }
}
