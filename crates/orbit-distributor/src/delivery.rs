//! Delivery of batches to Durable Objects.

use std::sync::{Arc, Mutex};
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
    clients: Vec<reqwest::Client>,
    loads: Arc<Mutex<Vec<usize>>>,
    base_url: String,
    secret: String,
}

impl HttpSink {
    pub fn new(
        base_url: impl Into<String>,
        secret: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, DeliveryError> {
        Self::with_concurrency(base_url, secret, timeout, 64)
    }

    /// Each independent client owns one HTTP/2 connection pool. A single connection can be
    /// limited to 100 streams by the edge, even when the distributor admits more deliveries.
    /// Reserve headroom by targeting 64 deliveries per pool, capped at 16 pools. The caller's
    /// global delivery semaphore remains the actual concurrency bound.
    pub fn with_concurrency(
        base_url: impl Into<String>,
        secret: impl Into<String>,
        timeout: Duration,
        concurrency: usize,
    ) -> Result<Self, DeliveryError> {
        let count = concurrency.div_ceil(64).clamp(1, 16);
        let clients = (0..count)
            .map(|_| {
                reqwest::Client::builder()
                    .timeout(timeout)
                    .build()
                    .map_err(|e| DeliveryError::Transport(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            clients,
            loads: Arc::new(Mutex::new(vec![0; count])),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            secret: secret.into(),
        })
    }

    fn acquire_client(&self) -> ClientLease<'_> {
        let mut loads = self.loads.lock().expect("delivery pool lock");
        let index = loads
            .iter()
            .enumerate()
            .min_by_key(|(_, load)| **load)
            .expect("nonempty client pool")
            .0;
        loads[index] += 1;
        ClientLease {
            client: &self.clients[index],
            index,
            loads: self.loads.clone(),
        }
    }

    pub fn url_for(&self, partition: &str) -> String {
        format!("{}/internal/cdc/{}", self.base_url, urlencode(partition))
    }
}

// Holding the lease through response-body consumption also accounts for HTTP/2 flow control.
// Drop releases capacity on success, error, timeout, or cancellation.
struct ClientLease<'a> {
    client: &'a reqwest::Client,
    index: usize,
    loads: Arc<Mutex<Vec<usize>>>,
}
impl Drop for ClientLease<'_> {
    fn drop(&mut self) {
        self.loads.lock().expect("delivery pool lock")[self.index] -= 1;
    }
}

#[async_trait::async_trait]
impl Sink for HttpSink {
    async fn deliver(&self, batch: &CdcBatch) -> Result<CdcBatchAck, DeliveryError> {
        let lease = self.acquire_client();
        let resp = lease
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

    #[tokio::test]
    async fn independent_h2_connections_preserve_capacity_and_release_cancelled_leases() {
        use hyper::{Request, Response, body::Incoming, service::service_fn};
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use tokio::sync::{Semaphore, mpsc};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let permits = Arc::new(Semaphore::new(0));
        let (seen_tx, mut seen_rx) = mpsc::channel(8);
        let server_permits = permits.clone();
        let server = tokio::spawn(async move {
            let mut id = 0;
            while let Ok((socket, _)) = listener.accept().await {
                id += 1;
                let seen = seen_tx.clone();
                let gate = server_permits.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let seen = seen.clone();
                        let gate = gate.clone();
                        async move {
                            let warm = req.uri().path() == "/warm";
                            let _ = axum::body::to_bytes(axum::body::Body::new(req.into_body()), 65536)
                                .await
                                .unwrap();
                            if !warm {
                                seen.send(id).await.unwrap();
                                gate.acquire().await.unwrap().forget();
                            }
                            Ok::<_, std::convert::Infallible>(Response::new(axum::body::Body::from(
                                r#"{"status":"applied","applied_seq":1,"duplicates":0,"apply_ms":0}"#,
                            )))
                        }
                    });
                    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .max_concurrent_streams(1)
                        .serve_connection(TokioIo::new(socket), service)
                        .await;
                });
            }
        });
        let mut sink = HttpSink::with_concurrency(&url, "test-secret", Duration::from_secs(5), 256).unwrap();
        sink.clients = (0..4)
            .map(|_| {
                reqwest::Client::builder()
                    .no_proxy()
                    .http2_prior_knowledge()
                    .build()
                    .unwrap()
            })
            .collect();
        for client in &sink.clients {
            client
                .get(format!("{url}/warm"))
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
        }
        let sink = Arc::new(sink);
        let mut jobs = Vec::new();
        for i in 0..4 {
            let sink = sink.clone();
            jobs.push(tokio::spawn(async move {
                sink.deliver(&CdcBatch {
                    protocol_version: 1,
                    schema_hash: "test".into(),
                    stream_epoch: 1,
                    partition: format!("org-{i}"),
                    transactions: vec![],
                    delivery_id: i.to_string(),
                })
                .await
            }));
        }
        let mut connections = std::collections::BTreeSet::new();
        for _ in 0..4 {
            connections.insert(
                tokio::time::timeout(Duration::from_secs(2), seen_rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(
            connections.len(),
            4,
            "one H2 stream per connection must not serialize independent deliveries"
        );
        assert_eq!(*sink.loads.lock().unwrap(), vec![1, 1, 1, 1]);
        let cancelled = jobs.pop().unwrap();
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        assert_eq!(sink.loads.lock().unwrap().iter().sum::<usize>(), 3);
        permits.add_permits(4);
        for job in jobs {
            assert!(matches!(job.await.unwrap().unwrap(), CdcBatchAck::Applied { .. }));
        }
        assert_eq!(*sink.loads.lock().unwrap(), vec![0, 0, 0, 0]);
        server.abort();
    }

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
