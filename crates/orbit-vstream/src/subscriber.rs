//! The live subscriber: opens a VStream from a checkpoint, assembles transactions, reconnects on
//! transient failures, and hands items to a bounded channel (backpressure).
//!
//! Invariants:
//! * The subscriber never advances its own notion of "done". The consumer decides when a
//!   position is durable and resumes the subscriber from that checkpoint after a restart.
//! * A reconnect always resumes from the last position the subscriber *emitted*, so the
//!   consumer may see a transaction twice but never misses one (at-least-once).
//! * Fatal errors (see [`VStreamError`]) stop the subscriber and are returned to the caller.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use orbit_gtid::GtidSet;
use orbit_protocol::schema::SyncSchema;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::checkpoint::{Checkpoint, ShardId};
use crate::client::VitessEndpoint;
use crate::error::VStreamError;
use crate::proto::binlogdata::{Filter, Rule, VEventType};
use crate::proto::topodata::TabletType;
use crate::proto::vtgate::{VStreamFlags, VStreamRequest};
use crate::stream::{Assembler, StepPolicy, StreamItem};

#[derive(Debug, Clone)]
pub struct SubscriberConfig {
    pub endpoint: VitessEndpoint,
    pub keyspace: String,
    /// Shards to stream. Empty means "all shards of the keyspace" (vtgate expands it).
    pub shards: Vec<String>,
    pub tablet_type: TabletType,
    /// Comma-separated cells passed to vtgate. PlanetScale connectors use `planetscale_operator_default`.
    pub cells: Option<String>,
    pub heartbeat_interval: Duration,
    /// Fail the stream when nothing (not even a heartbeat) arrives for this long.
    pub stall_timeout: Duration,
    /// Fail the stream when heartbeats arrive but the position does not move for this long while
    /// the source's current position has advanced. vtgate retries some tablet errors (for example
    /// purged binlogs) internally and keeps sending heartbeats, which would otherwise hang forever.
    pub progress_timeout: Duration,
    pub reconnect_backoff_min: Duration,
    pub reconnect_backoff_max: Duration,
    /// Give up after this many consecutive retryable failures without progress.
    pub max_consecutive_failures: u32,
    /// Channel capacity in items. When full, the gRPC stream is not read (HTTP/2 flow control).
    pub channel_capacity: usize,
    pub allow_merged_transactions: bool,
}

impl SubscriberConfig {
    pub fn new(endpoint: VitessEndpoint, keyspace: impl Into<String>) -> Self {
        Self {
            endpoint,
            keyspace: keyspace.into(),
            shards: vec![],
            tablet_type: TabletType::Primary,
            cells: None,
            heartbeat_interval: Duration::from_secs(5),
            stall_timeout: Duration::from_secs(30),
            progress_timeout: Duration::from_secs(120),
            reconnect_backoff_min: Duration::from_millis(250),
            reconnect_backoff_max: Duration::from_secs(30),
            max_consecutive_failures: u32::MAX,
            channel_capacity: 256,
            allow_merged_transactions: false,
        }
    }
}

/// Builds the VStream filter: one explicit `select * from T` rule per synced table.
///
/// Bare `match` rules without a `filter` query trigger a vtgate failure on compressed binlog
/// transactions ("failed to build table replication plan"), observed on PlanetScale. Always
/// emit the explicit form.
pub fn filter_for(schema: &SyncSchema) -> Filter {
    Filter {
        rules: schema
            .tables
            .iter()
            .map(|t| Rule {
                r#match: t.name.clone(),
                filter: format!("select * from {}", quote_ident(&t.name)),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Reads the server's current position for every shard by opening a stream at `current` and
/// taking the first VGTID. Used to validate checkpoints before resuming.
pub async fn current_position(config: &SubscriberConfig, schema: &SyncSchema) -> Result<Checkpoint, VStreamError> {
    let mut client = config.endpoint.connect().await?;
    let cp = Checkpoint::empty(0);
    let req = VStreamRequest {
        tablet_type: config.tablet_type as i32,
        vgtid: Some(cp.to_vgtid(&config.keyspace, &shards_or_all(config))),
        filter: Some(filter_for(schema)),
        flags: Some(flags_for(config)),
        ..Default::default()
    };
    let mut stream = client
        .v_stream(req)
        .await
        .map_err(|s| VStreamError::from_status(s, "current"))?
        .into_inner();
    let deadline = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => return Err(VStreamError::Timeout(Duration::from_secs(30))),
            msg = stream.message() => {
                let resp = msg.map_err(|s| VStreamError::from_status(s, "current"))?.ok_or(VStreamError::EndedUnexpectedly)?;
                for ev in &resp.events {
                    if ev.r#type == VEventType::Vgtid as i32
                        && let Some(v) = &ev.vgtid
                    {
                        return Ok(Checkpoint::from_vgtid(0, v));
                    }
                }
            }
        }
    }
}

pub(crate) fn shards_or_all(config: &SubscriberConfig) -> Vec<String> {
    if config.shards.is_empty() {
        vec![String::new()]
    } else {
        config.shards.clone()
    }
}

pub(crate) fn flags_for(config: &SubscriberConfig) -> VStreamFlags {
    VStreamFlags {
        heartbeat_interval: config.heartbeat_interval.as_secs().max(1) as u32,
        cells: config.cells.clone().unwrap_or_default(),
        stop_on_reshard: true,
        ..Default::default()
    }
}

/// Runs the subscriber until cancelled or a fatal error occurs.
///
/// Items are delivered in stream order. After a reconnect the stream resumes from the last
/// emitted position, so consumers must tolerate duplicates.
pub async fn run(
    config: SubscriberConfig,
    schema: SyncSchema,
    start: Checkpoint,
    tx: mpsc::Sender<StreamItem>,
    cancel: CancellationToken,
) -> Result<(), VStreamError> {
    // Validate the checkpoint against the server before streaming. A position "ahead" of the
    // server would otherwise produce silence forever.
    let current = current_position(&config, &schema).await?;
    if !start.is_contained_in(&current)? {
        return Err(VStreamError::InvalidCheckpoint {
            checkpoint: start.render(),
            current: current.render(),
        });
    }
    info!(checkpoint = %start.render(), current = %current.render(), "checkpoint validated");

    let mut resume = start;
    let mut failures: u32 = 0;
    let mut backoff = config.reconnect_backoff_min;
    loop {
        if cancel.is_cancelled() {
            return Err(VStreamError::Cancelled);
        }
        let before = resume.render();
        let result = run_once(&config, &schema, &mut resume, &tx, &cancel).await;
        match result {
            Ok(()) => return Ok(()),
            Err(e) if e.is_retryable() => {
                if resume.render() != before {
                    // The connection made progress before failing: reset the failure budget.
                    failures = 0;
                    backoff = config.reconnect_backoff_min;
                }
                failures += 1;
                metrics::counter!("orbit_vstream_reconnects_total").increment(1);
                if failures > config.max_consecutive_failures {
                    error!(error = %e, failures, "giving up after consecutive failures");
                    return Err(e);
                }
                let jitter = Duration::from_millis(rand::random::<u64>() % 250);
                warn!(error = %e, failures, backoff_ms = backoff.as_millis() as u64, resume = %resume.render(), "stream failed; reconnecting");
                tokio::select! {
                    _ = cancel.cancelled() => return Err(VStreamError::Cancelled),
                    _ = tokio::time::sleep(backoff + jitter) => {}
                }
                backoff = (backoff * 2).min(config.reconnect_backoff_max);
            }
            Err(VStreamError::Cancelled) => {
                info!("subscriber cancelled");
                return Err(VStreamError::Cancelled);
            }
            Err(e) => {
                error!(error = %e, "fatal stream error");
                return Err(e);
            }
        }
    }
}

async fn run_once(
    config: &SubscriberConfig,
    schema: &SyncSchema,
    resume: &mut Checkpoint,
    tx: &mpsc::Sender<StreamItem>,
    cancel: &CancellationToken,
) -> Result<(), VStreamError> {
    let mut client = config.endpoint.connect().await?;
    let vgtid = resume.to_vgtid(&config.keyspace, &shards_or_all(config));
    let requested = resume.render();
    let req = VStreamRequest {
        tablet_type: config.tablet_type as i32,
        vgtid: Some(vgtid),
        filter: Some(filter_for(schema)),
        flags: Some(flags_for(config)),
        ..Default::default()
    };
    info!(resume = %requested, "opening vstream");
    let mut stream = client
        .v_stream(req)
        .await
        .map_err(|s| VStreamError::from_status(s, &requested))?
        .into_inner();
    metrics::gauge!("orbit_vstream_connected").set(1.0);

    let start_positions: HashMap<ShardId, GtidSet> = resume
        .positions
        .iter()
        .filter(|(_, p)| p.as_str() != "current")
        .map(|(k, v)| Ok((k.clone(), GtidSet::parse_position(v)?)))
        .collect::<Result<_, VStreamError>>()?;
    let policy = if config.allow_merged_transactions {
        StepPolicy::AllowMerged
    } else {
        StepPolicy::Strict
    };
    let mut assembler = Assembler::new(schema, start_positions, policy);

    let mut last_progress = Instant::now();
    let result = async {
        loop {
            let msg = tokio::select! {
                _ = cancel.cancelled() => return Err(VStreamError::Cancelled),
                m = tokio::time::timeout(config.stall_timeout, stream.message()) => m,
            };
            let resp = match msg {
                Err(_) => return Err(VStreamError::Stalled(config.stall_timeout)),
                Ok(Err(status)) => return Err(VStreamError::from_status(status, &requested)),
                Ok(Ok(None)) => return Err(VStreamError::EndedUnexpectedly),
                Ok(Ok(Some(r))) => r,
            };
            if last_progress.elapsed() >= config.progress_timeout {
                check_progress(config, schema, resume, last_progress.elapsed()).await?;
                last_progress = Instant::now();
            }
            let received_at_ms = now_ms();
            metrics::counter!("orbit_vstream_responses_total").increment(1);
            for ev in &resp.events {
                metrics::counter!("orbit_vstream_events_total", "type" => event_name(ev.r#type)).increment(1);
                for item in assembler.push(ev, received_at_ms)? {
                    match &item {
                        StreamItem::Transaction(t) => {
                            let lag = (received_at_ms / 1000 - t.commit_timestamp).max(0);
                            metrics::gauge!("orbit_vstream_lag_seconds").set(lag as f64);
                            metrics::counter!("orbit_vstream_transactions_total").increment(1);
                            metrics::counter!("orbit_vstream_row_changes_total").increment(t.changes.len() as u64);
                            resume.set(
                                ShardId {
                                    keyspace: t.keyspace.clone(),
                                    shard: t.shard.clone(),
                                },
                                t.position.clone(),
                            );
                        }
                        StreamItem::Position { shard, position } | StreamItem::Ddl { shard, position, .. } => {
                            resume.set(shard.clone(), position.clone());
                        }
                        StreamItem::Heartbeat | StreamItem::CopyCompleted { .. } => {}
                    }
                    if !matches!(item, StreamItem::Heartbeat) {
                        last_progress = Instant::now();
                    }
                    if tx.send(item).await.is_err() {
                        return Err(VStreamError::ConsumerGone);
                    }
                }
            }
        }
    }
    .await;
    metrics::gauge!("orbit_vstream_connected").set(0.0);
    result
}

/// Called when only heartbeats arrived for `progress_timeout`. Compares the resume position with
/// the source's current position: an idle source is fine, a source that moved on while the stream
/// stayed put means the stream is silently stuck.
async fn check_progress(
    config: &SubscriberConfig,
    schema: &SyncSchema,
    resume: &Checkpoint,
    elapsed: Duration,
) -> Result<(), VStreamError> {
    if resume.positions.values().any(|p| p == "current") {
        return Ok(());
    }
    let current = current_position(config, schema).await?;
    if !resume.is_contained_in(&current)? {
        return Err(VStreamError::InvalidCheckpoint {
            checkpoint: resume.render(),
            current: current.render(),
        });
    }
    if current.is_contained_in(resume)? {
        // The source is idle: nothing to stream.
        return Ok(());
    }
    metrics::counter!("orbit_vstream_no_progress_total").increment(1);
    Err(VStreamError::NoProgress {
        position: resume.render(),
        current: current.render(),
        elapsed,
    })
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn event_name(ty: i32) -> &'static str {
    match VEventType::try_from(ty) {
        Ok(VEventType::Begin) => "begin",
        Ok(VEventType::Commit) => "commit",
        Ok(VEventType::Row) => "row",
        Ok(VEventType::Field) => "field",
        Ok(VEventType::Vgtid) => "vgtid",
        Ok(VEventType::Heartbeat) => "heartbeat",
        Ok(VEventType::Ddl) => "ddl",
        Ok(VEventType::Other) => "other",
        Ok(VEventType::CopyCompleted) => "copy_completed",
        Ok(VEventType::Journal) => "journal",
        _ => "other_type",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_uses_explicit_select_rules() {
        let schema = SyncSchema {
            format_version: 1,
            schema_hash: String::new(),
            app: "t".into(),
            keyspace: "ks".into(),
            partition: orbit_protocol::schema::PartitionConfig {
                name: "p".into(),
                key_kind: orbit_protocol::schema::ValueKind::String,
                placement: orbit_protocol::schema::PlacementConfig::OnePerPartition { version: 1 },
            },
            tables: vec![orbit_protocol::schema::TableSchema {
                name: "Weird`Name".into(),
                primary_key: vec!["id".into()],
                partition_column: "id".into(),
                partition_parent: None,
                columns: vec![orbit_protocol::schema::ColumnSchema {
                    name: "id".into(),
                    kind: orbit_protocol::schema::ValueKind::String,
                    nullable: false,
                    source_type: "varchar(1)".into(),
                    enum_values: None,
                    derived: None,
                }],
                relations: vec![],
            }],
        };
        let f = filter_for(&schema);
        assert_eq!(f.rules[0].r#match, "Weird`Name");
        assert_eq!(f.rules[0].filter, "select * from `Weird``Name`");
    }
}
