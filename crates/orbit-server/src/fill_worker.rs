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
use orbit_vstream::fill::{run_derived_fill, run_fill};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub struct FillWorker {
    pub client: reqwest::Client,
    pub worker_url: String,
    pub secret: String,
    pub subscriber: SubscriberConfig,
    pub schema: Arc<SyncSchema>,
    pub concurrency: usize,
    pub timeout: Duration,
}

impl FillWorker {
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let permits = Arc::new(Semaphore::new(self.concurrency));
        let mut backoff = Duration::from_millis(250);
        loop {
            if cancel.is_cancelled() {
                return;
            }
            let permit = tokio::select! {
                _ = cancel.cancelled() => return,
                p = permits.clone().acquire_owned() => p.expect("semaphore"),
            };
            let poll = tokio::select! {
                _ = cancel.cancelled() => return,
                r = self.poll() => r,
            };
            match poll {
                Ok(resp) => {
                    backoff = Duration::from_millis(250);
                    drop(permit);
                    for req in resp.requests {
                        let permit = permits.clone().acquire_owned().await.expect("semaphore");
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

    async fn poll(&self) -> Result<FillPollResponse, String> {
        let url = format!("{}/internal/fills/next?wait=25", self.worker_url.trim_end_matches('/'));
        let resp = self
            .client
            .get(&url)
            .bearer_auth(&self.secret)
            .timeout(Duration::from_secs(40))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status();
        let body = resp.text().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("http {status}: {}", body.chars().take(200).collect::<String>()));
        }
        serde_json::from_str(&body).map_err(|e| format!("invalid poll response: {e}"))
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
        // A table with `partition_parent` cannot be filtered by the copy phase (it would need a
        // subquery on the parent), so it is read through vtgate `Execute` instead. See
        // `run_derived_fill` for why the position taken before the selects is exact.
        let derived = self
            .schema
            .table(&req.table)
            .is_some_and(|t| t.partition_parent.is_some());
        let outcome = if derived {
            run_derived_fill(
                &self.subscriber,
                &self.schema,
                &req.table,
                &req.partition,
                self.timeout,
                cancel,
            )
            .await
        } else {
            run_fill(
                &self.subscriber,
                &self.schema,
                &req.table,
                &req.partition,
                self.timeout,
                cancel,
            )
            .await
        };
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
