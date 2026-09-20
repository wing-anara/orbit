//! Demand fill worker: long-polls the Worker for fill requests, runs copy-phase fills, and
//! uploads results as newline-delimited `FillChunk`s.
//!
//! The Rust server only makes outbound connections, so it can run anywhere that can reach the
//! Worker and Vitess. Fill requests are idempotent: the Durable Object ignores results for fill
//! ids it no longer expects.

use std::sync::Arc;
use std::time::Duration;

use orbit_protocol::errors::EngineError;
use orbit_protocol::fill::{FillChunk, FillPollResponse, FillRequest, FillResult};
use orbit_protocol::schema::SyncSchema;
use orbit_vstream::SubscriberConfig;
use orbit_vstream::fill::run_derived_fill_with_client;
use orbit_vstream::select_fill::run_select_fill_with_client;
use tokio::sync::{OnceCell, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub struct FillWorker {
    pub client: reqwest::Client,
    pub worker_url: String,
    pub secret: String,
    pub subscriber: SubscriberConfig,
    pub source_client: OnceCell<orbit_vstream::client::Client>,
    pub schema: Arc<SyncSchema>,
    pub concurrency: usize,
    pub timeout: Duration,
}

impl FillWorker {
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let capacity = self.concurrency.max(1);
        let permits = Arc::new(Semaphore::new(capacity));
        // Overlap registry round trips without leasing work beyond the execution budget.
        // Each lane owns its credits through polling, receipt acknowledgement and execution.
        let lanes = capacity.div_ceil(16).clamp(1, 8);
        let batch_size = (capacity / lanes).clamp(1, 16);
        let mut pollers = tokio::task::JoinSet::new();
        for _ in 0..lanes {
            let worker = self.clone();
            let permits = permits.clone();
            let cancel = cancel.clone();
            pollers.spawn(async move { worker.run_lane(permits, batch_size, cancel).await });
        }
        while let Some(result) = pollers.join_next().await {
            if let Err(error) = result {
                error!(%error, "fill polling lane failed");
            }
        }
    }

    async fn run_lane(self: Arc<Self>, permits: Arc<Semaphore>, batch_size: usize, cancel: CancellationToken) {
        let mut backoff = Duration::from_millis(250);
        loop {
            if cancel.is_cancelled() {
                return;
            }
            let mut permit = tokio::select! {
                _ = cancel.cancelled() => return,
                p = permits.clone().acquire_many_owned(batch_size as u32) => p.expect("semaphore"),
            };
            let poll = tokio::select! {
                _ = cancel.cancelled() => return,
                r = self.poll(batch_size) => r,
            };
            match poll {
                Ok(resp) => {
                    backoff = Duration::from_millis(250);
                    for req in resp.requests {
                        // Transfer reserved credits directly. Releasing then reacquiring lets a
                        // competing poller steal them and strand work whose lease is ticking.
                        let permit = permit.split(1).expect("response fits reserved capacity");
                        let me = self.clone();
                        let cancel = cancel.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            me.execute(req, &cancel).await;
                        });
                    }
                }
                Err(e) => {
                    drop(permit);
                    warn!(error = %e, backoff_ms = backoff.as_millis() as u64, "fill poll failed");
                    metrics::counter!("orbit_fill_poll_errors_total").increment(1);
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                }
            }
        }
    }

    async fn poll(&self, capacity: usize) -> Result<FillPollResponse, String> {
        let url = format!(
            "{}/internal/fills/next?wait=25&ack=1&limit={}",
            self.worker_url.trim_end_matches('/'),
            capacity.min(16)
        );
        let resp = self
            .client
            .get(&url)
            .bearer_auth(&self.secret)
            .timeout(Duration::from_secs(40))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let lease = resp.headers().get("x-orbit-fill-lease").cloned();
        let status = resp.status();
        let body = resp.text().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("http {status}: {}", body.chars().take(200).collect::<String>()));
        }
        let batch: FillPollResponse = serde_json::from_str(&body).map_err(|e| format!("invalid poll response: {e}"))?;
        if batch.requests.len() > capacity.min(16) {
            return Err("fill poll exceeded reserved capacity".into());
        }
        if let Some(lease) = lease.filter(|_| !batch.requests.is_empty()) {
            // Receiving the body, not sending the poll, establishes ownership. If this
            // acknowledgement is lost, still execute the received work: the registry
            // can redeliver it and fill completion is idempotent.
            let claim = self
                .client
                .post(format!(
                    "{}/internal/fills/claim",
                    self.worker_url.trim_end_matches('/')
                ))
                .bearer_auth(&self.secret)
                .header("x-orbit-fill-lease", lease)
                .timeout(Duration::from_secs(2))
                .send()
                .await;
            match claim {
                Ok(r) if r.status().is_success() => {}
                other => {
                    warn!(result = ?other.map(|r| r.status()), "fill receipt acknowledgement failed; executing received batch");
                    metrics::counter!("orbit_fill_claim_errors_total").increment(1);
                }
            }
        }
        Ok(batch)
    }

    async fn execute(&self, req: FillRequest, cancel: &CancellationToken) {
        let started = std::time::Instant::now();
        info!(fill_id = %req.fill_id, partition = %req.partition, table = %req.table, "fill started");
        metrics::counter!("orbit_fill_requests_total").increment(1);
        if req.schema_hash != self.schema.schema_hash {
            let err = EngineError::SchemaMismatch {
                table: req.table.clone(),
                message: format!(
                    "fill requested for schema {} but server runs {}",
                    req.schema_hash, self.schema.schema_hash
                ),
            };
            self.upload(&req, vec![], FillResult::Failed { error: err }).await;
            return;
        }
        // Derived and relation-routed tables need indexed joins. Direct tables use
        // keyset reads plus CDC reconciliation, avoiding copy-phase source locks.
        let derived = self
            .schema
            .table(&req.table)
            .is_some_and(|t| t.partition_parent.is_some());
        let routed = self
            .schema
            .table(&req.table)
            .is_some_and(|t| !t.partition_routes.is_empty());
        let outcome = retry_fill(self.timeout, cancel, || async {
            let client = self
                .source_client
                .get_or_try_init(|| self.subscriber.endpoint.connect())
                .await?
                .clone();
            if routed {
                orbit_vstream::routed_fill::run_routed_fill_with_client(
                    &self.subscriber,
                    &self.schema,
                    &req.table,
                    &req.partition,
                    self.timeout,
                    cancel,
                    client,
                )
                .await
            } else if derived {
                run_derived_fill_with_client(
                    &self.subscriber,
                    &self.schema,
                    &req.table,
                    &req.partition,
                    self.timeout,
                    cancel,
                    client,
                )
                .await
            } else {
                run_select_fill_with_client(
                    &self.subscriber,
                    &self.schema,
                    &req.table,
                    &req.partition,
                    self.timeout,
                    cancel,
                    client,
                )
                .await
            }
        })
        .await;
        match outcome {
            Ok(outcome) => {
                let row_count = outcome.rows.len() as u64;
                let (shard_id, position) = outcome
                    .positions
                    .iter()
                    .next()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .expect("fill has a position");
                if outcome.positions.len() > 1 {
                    // Multi-shard fills need per-shard positions in the DO; not supported yet.
                    let err = EngineError::Internal {
                        message: format!(
                            "fill spans {} shards; multi-shard partitions are not supported",
                            outcome.positions.len()
                        ),
                    };
                    self.upload(&req, vec![], FillResult::Failed { error: err }).await;
                    return;
                }
                let result = FillResult::Completed {
                    position,
                    keyspace: shard_id.keyspace,
                    shard: shard_id.shard,
                    row_count,
                    duration_ms: started.elapsed().as_millis() as u32,
                };
                self.upload(&req, outcome.rows, result).await;
            }
            Err(e) => {
                error!(fill_id = %req.fill_id, error = %e, "fill failed");
                metrics::counter!("orbit_fill_failed_total").increment(1);
                self.upload(
                    &req,
                    vec![],
                    FillResult::Failed {
                        error: e.to_engine_error(),
                    },
                )
                .await;
            }
        }
    }

    async fn upload(&self, req: &FillRequest, rows: Vec<orbit_protocol::value::Row>, result: FillResult) {
        let mut body = String::new();
        for chunk in rows.chunks(500) {
            let line = FillChunk::Rows {
                fill_id: req.fill_id.clone(),
                rows: chunk.to_vec(),
            };
            body.push_str(&serde_json::to_string(&line).expect("serializable"));
            body.push('\n');
        }
        body.push_str(
            &serde_json::to_string(&FillChunk::Done {
                fill_id: req.fill_id.clone(),
                result,
            })
            .expect("serializable"),
        );
        body.push('\n');
        let url = format!(
            "{}/internal/fills/{}",
            self.worker_url.trim_end_matches('/'),
            orbit_distributor::delivery::urlencode(&req.fill_id)
        );
        let mut attempt = 0;
        loop {
            attempt += 1;
            let resp = self
                .client
                .post(&url)
                .bearer_auth(&self.secret)
                .header("content-type", "application/x-ndjson")
                .body(body.clone())
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => {
                    info!(fill_id = %req.fill_id, "fill uploaded");
                    return;
                }
                Ok(r) if r.status().as_u16() == 404 || r.status().as_u16() == 409 => {
                    // The DO no longer expects this fill (it was cancelled or superseded). Drop the
                    // registry entry so an expired lease cannot hand the same fill out again.
                    warn!(fill_id = %req.fill_id, status = %r.status(), "fill upload not accepted; dropping it");
                    metrics::counter!("orbit_fill_upload_rejected_total").increment(1);
                    self.drop_fill(&req.fill_id).await;
                    return;
                }
                Ok(r) => warn!(fill_id = %req.fill_id, status = %r.status(), attempt, "fill upload failed"),
                Err(e) => warn!(fill_id = %req.fill_id, error = %e, attempt, "fill upload failed"),
            }
            if attempt >= 8 {
                error!(fill_id = %req.fill_id, "giving up on fill upload; the DO will re-request");
                metrics::counter!("orbit_fill_upload_failed_total").increment(1);
                return;
            }
            tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
        }
    }

    /// Removes a fill from the registry. Best effort: the registry also drops a fill when the DO
    /// reports it complete, and a leaked entry only costs one more rejected upload per lease.
    async fn drop_fill(&self, fill_id: &str) {
        let url = format!(
            "{}/internal/fills/{}",
            self.worker_url.trim_end_matches('/'),
            orbit_distributor::delivery::urlencode(fill_id)
        );
        match self.client.delete(&url).bearer_auth(&self.secret).send().await {
            Ok(r) if r.status().is_success() || r.status().as_u16() == 404 => {}
            Ok(r) => warn!(fill_id, status = %r.status(), "could not drop fill from registry"),
            Err(e) => warn!(fill_id, error = %e, "could not drop fill from registry"),
        }
    }
}

/// Retry an entire read-only snapshot on transient source errors. Every attempt
/// acquires its own position fence and rows; partial snapshots never escape.
async fn retry_fill<T, F, Fut>(
    timeout: Duration,
    cancel: &CancellationToken,
    mut run: F,
) -> Result<T, orbit_vstream::error::VStreamError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, orbit_vstream::error::VStreamError>>,
{
    use orbit_vstream::error::VStreamError;
    let deadline = tokio::time::Instant::now() + timeout;
    let mut backoff = Duration::from_millis(100);
    loop {
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(VStreamError::Cancelled),
            _ = tokio::time::sleep_until(deadline) => return Err(VStreamError::Timeout(timeout)),
            result = run() => result,
        };
        match result {
            Err(error) if error.is_retryable() => {
                metrics::counter!("orbit_fill_source_retries_total").increment(1);
                warn!(error = %error, backoff_ms = backoff.as_millis() as u64, "transient fill source failure; retrying snapshot");
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(VStreamError::Cancelled),
                    _ = tokio::time::sleep_until(deadline) => return Err(VStreamError::Timeout(timeout)),
                    _ = tokio::time::sleep(backoff) => {},
                }
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbit_vstream::error::VStreamError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn polling_worker(url: String, concurrency: usize) -> Arc<FillWorker> {
        Arc::new(FillWorker {
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
            worker_url: url,
            secret: "synthetic-test".into(),
            subscriber: SubscriberConfig::new(orbit_vstream::client::VitessEndpoint::new("http://127.0.0.1:1"), "test"),
            source_client: OnceCell::new(),
            schema: Arc::new(serde_json::from_str(include_str!("../../../schema/fixtures/SyncSchema.json")).unwrap()),
            concurrency,
            timeout: Duration::from_secs(120),
        })
    }

    #[tokio::test]
    async fn overlaps_slow_registry_polls_and_cancels_without_waiting_for_http_timeout() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (observed, ready) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 8192];
                let n = socket.read(&mut request).await.unwrap();
                assert!(String::from_utf8_lossy(&request[..n]).contains("ack=1&limit=16"));
                held.push(socket);
            }
            observed.send(()).unwrap();
            // Both full batches consume the 32-credit budget while the registry is slow.
            assert!(
                tokio::time::timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err()
            );
            drop(held);
        });
        let cancel = CancellationToken::new();
        let worker = polling_worker(format!("http://{addr}"), 32);
        let token = cancel.clone();
        let running = tokio::spawn(async move { worker.run(token).await });
        tokio::time::timeout(Duration::from_secs(2), ready)
            .await
            .unwrap()
            .unwrap();
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), running)
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_a_poll_response_larger_than_reserved_capacity() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 8192];
            socket.read(&mut request).await.unwrap();
            let body = serde_json::json!({"requests":(0..4).map(|i|serde_json::json!({
                "fill_id":format!("org:{i}"),"schema_hash":"test","partition":"org","table":"Chatbot","requested_at_ms":1
            })).collect::<Vec<_>>()}).to_string();
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let worker = polling_worker(format!("http://{addr}"), 3);
        assert_eq!(
            worker.poll(3).await.unwrap_err(),
            "fill poll exceeded reserved capacity"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn executes_received_work_when_claim_response_is_lost() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut poll, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 8192];
            let n = poll.read(&mut request).await.unwrap();
            assert!(String::from_utf8_lossy(&request[..n]).contains("ack=1&limit=3"));
            let body = r#"{"requests":[{"fill_id":"org:1","schema_hash":"test","partition":"org","table":"Chatbot","requested_at_ms":1}]}"#;
            poll.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nx-orbit-fill-lease: receipt\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            drop(poll);
            let (mut claim, _) = listener.accept().await.unwrap();
            let n = claim.read(&mut request).await.unwrap();
            let headers = String::from_utf8_lossy(&request[..n]);
            assert!(headers.starts_with("POST /internal/fills/claim"));
            assert!(headers.contains("x-orbit-fill-lease: receipt"));
            // The registry may already have committed the claim; lose only its response.
            drop(claim);
        });
        let worker = FillWorker {
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
            worker_url: format!("http://{addr}"),
            secret: "synthetic-test".into(),
            subscriber: SubscriberConfig::new(orbit_vstream::client::VitessEndpoint::new("http://127.0.0.1:1"), "test"),
            source_client: OnceCell::new(),
            schema: Arc::new(serde_json::from_str(include_str!("../../../schema/fixtures/SyncSchema.json")).unwrap()),
            concurrency: 3,
            timeout: Duration::from_secs(120),
        };
        let received = worker.poll(3).await.unwrap();
        assert_eq!(received.requests.len(), 1);
        assert_eq!(received.requests[0].fill_id, "org:1");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_transient_snapshot_from_scratch() {
        let attempts = AtomicUsize::new(0);
        let result = retry_fill(Duration::from_secs(2), &CancellationToken::new(), || async {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(VStreamError::EndedUnexpectedly)
            } else {
                Ok(vec!["complete snapshot"])
            }
        })
        .await
        .unwrap();
        assert_eq!(result, vec!["complete snapshot"]);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn never_retries_fatal_errors() {
        let attempts = AtomicUsize::new(0);
        let result: Result<(), _> = retry_fill(Duration::from_secs(1), &CancellationToken::new(), || async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(VStreamError::Unauthenticated)
        })
        .await;
        assert!(matches!(result, Err(VStreamError::Unauthenticated)));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn bounds_retries_and_inflight_reads_by_one_deadline() {
        let timeout = Duration::from_millis(20);
        let result: Result<(), _> = retry_fill(timeout, &CancellationToken::new(), || async {
            Err(VStreamError::EndedUnexpectedly)
        })
        .await;
        assert!(matches!(result, Err(VStreamError::Timeout(t)) if t == timeout));
        let result: Result<(), _> = retry_fill(timeout, &CancellationToken::new(), || std::future::pending()).await;
        assert!(matches!(result, Err(VStreamError::Timeout(t)) if t == timeout));
    }

    #[tokio::test]
    async fn cancellation_prevents_new_source_reads() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result: Result<(), _> = retry_fill(Duration::from_secs(1), &cancel, || async {
            panic!("cancelled fill must not start a source read")
        })
        .await;
        assert!(matches!(result, Err(VStreamError::Cancelled)));
    }
}
