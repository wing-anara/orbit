//! The distributor loop.
//!
//! ```text
//! subscriber channel ──▶ route ──▶ per-partition queue ──▶ batch ──▶ Sink ──▶ ack
//!                        │                                            │
//!                        └── in-flight ledger ◀────────────────────────┘
//!                                    │
//!                                    └── contiguous acked prefix ──▶ checkpoint (SQLite)
//! ```
//!
//! Invariants enforced here:
//! * Per partition, batches are delivered one at a time and in `seq` order.
//! * A stream transaction is "done" when every partition slice derived from it is acked. The
//!   checkpoint advances only over a contiguous prefix of done transactions, so a restart can
//!   replay but never skip.
//! * Sequence numbers are assigned in stream order and persisted with the checkpoint, so replay
//!   re-derives the same numbers.
//! * Delivery failures retry with backoff. Permanent rejections (after `max_reject_attempts`)
//!   quarantine the partition's transactions durably and let the checkpoint move on, so one bad
//!   target cannot stall the whole stream silently: the quarantine is visible in metrics and
//!   through the operator CLI.
//! * Derived routing (`partition_parent`) reads the parent index. Parent changes of a transaction
//!   are applied to an in-memory overlay before the transaction is routed, so a child inserted
//!   with its parent resolves. The overlay entries reach SQLite in the same transaction as the
//!   checkpoint that covers them, so after a restart the persisted index is exactly the index
//!   as of the checkpoint and the replay re-derives the same routing.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orbit_protocol::cdc::{CdcBatch, CdcBatchAck, PartitionTransaction, RejectReason, SourceTransaction, TraceContext};
use orbit_protocol::schema::SyncSchema;
use orbit_vstream::checkpoint::{Checkpoint, ShardId};
use orbit_vstream::execute::execute;
use orbit_vstream::fill::sql_literal;
use orbit_vstream::subscriber::{SubscriberConfig, current_position, quote_ident};
use orbit_vstream::{StreamItem, VStreamError};
use tokio::sync::{Notify, OnceCell, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::delivery::Sink;
use crate::router::{RoutingError, no_parents, parent_key_kind, parent_key_of, partition_of, route};
use crate::state::{ParentEntry, StateError, StateStore};

/// Rows per page of the parent index bootstrap copy.
const BOOTSTRAP_PAGE_SIZE: usize = 5000;

#[derive(Debug, Clone)]
pub struct DistributorConfig {
    /// Max transactions per batch.
    pub max_batch_transactions: usize,
    /// Soft cap on batch body size (bytes of serialized JSON). A single oversized transaction is
    /// still sent alone.
    pub max_batch_bytes: usize,
    /// Max stream transactions routed but not yet checkpointed. Reaching it pauses consumption.
    pub max_inflight_transactions: usize,
    /// Max concurrent deliveries across partitions.
    pub max_concurrent_deliveries: usize,
    pub retry_backoff_min: Duration,
    pub retry_backoff_max: Duration,
    /// After this many consecutive *rejections* (not transport failures) of the same batch, the
    /// partition is quarantined.
    pub max_reject_attempts: u32,
    /// How often the checkpoint is flushed to SQLite when it has advanced.
    pub checkpoint_interval: Duration,
}

impl Default for DistributorConfig {
    fn default() -> Self {
        Self {
            max_batch_transactions: 200,
            max_batch_bytes: 4 * 1024 * 1024,
            max_inflight_transactions: 2000,
            max_concurrent_deliveries: 32,
            retry_backoff_min: Duration::from_millis(100),
            retry_backoff_max: Duration::from_secs(30),
            max_reject_attempts: 20,
            checkpoint_interval: Duration::from_millis(500),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DistributorError {
    #[error("routing: {0}")]
    Routing(#[from] RoutingError),
    #[error("state: {0}")]
    State(#[from] StateError),
    #[error("source: {0}")]
    VStream(#[from] VStreamError),
    #[error("stream closed")]
    StreamClosed,
    #[error("cancelled")]
    Cancelled,
}

/// One stream item that has been routed and awaits acknowledgement.
struct InflightTxn {
    /// Positions after this item, per shard (only the item's shard changes).
    shard: ShardId,
    position: String,
    /// (partition, seq) pairs derived from this item.
    assignments: Vec<(String, u64)>,
    pending: usize,
    /// Parent index entries written by this item, in row order. Persisted with the checkpoint
    /// that covers the item.
    parent_updates: Vec<ParentEntry>,
}

#[derive(Default)]
struct PartitionQueue {
    queue: VecDeque<(u64 /* stream index */, PartitionTransaction)>,
    /// A delivery task currently owns this partition.
    busy: bool,
    quarantined: bool,
}

struct Shared {
    schema: SyncSchema,
    config: DistributorConfig,
    sink: Arc<dyn Sink>,
    state: Mutex<StateStore>,
    inner: Mutex<Inner>,
    /// Signalled when a partition queue gains work or a delivery finishes.
    wake: Notify,
    /// Signalled when in-flight count drops (backpressure release).
    drained: Notify,
    delivery_permits: Arc<Semaphore>,
    epoch: u64,
    /// Tables named as `partition_parent` by some table. Their changes maintain the parent index.
    parent_tables: HashSet<String>,
}

struct Inner {
    next_seq: BTreeMap<String, u64>,
    partitions: HashMap<String, PartitionQueue>,
    inflight: BTreeMap<u64, InflightTxn>,
    next_stream_index: u64,
    checkpoint: Checkpoint,
    /// Counters that changed since the last persisted checkpoint.
    dirty_counters: BTreeMap<String, u64>,
    checkpoint_dirty: bool,
    quarantined: HashSet<String>,
    /// Parent index entries of routed transactions that the persisted index does not hold yet:
    /// parent table -> rendered key -> (partition, stream index of the writer).
    parent_overlay: HashMap<String, HashMap<String, (String, u64)>>,
    /// Parent index entries of checkpointed transactions not yet written to SQLite, in stream
    /// order, with the stream index of the writer.
    dirty_parents: Vec<(ParentEntry, u64)>,
}

/// Snapshot of runtime state for observability and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributorStatus {
    pub checkpoint: Checkpoint,
    pub inflight_transactions: usize,
    pub queued_partition_transactions: usize,
    pub quarantined_partitions: Vec<String>,
}

pub struct Distributor {
    shared: Arc<Shared>,
    fanout_source: Option<SubscriberConfig>,
    fanout_client: OnceCell<orbit_vstream::client::Client>,
}

impl Distributor {
    pub fn new(
        schema: SyncSchema,
        config: DistributorConfig,
        state: StateStore,
        sink: Arc<dyn Sink>,
    ) -> Result<Self, DistributorError> {
        let persisted = state.load_or_init(&schema.schema_hash)?;
        let quarantined: HashSet<String> = state.quarantined_partitions()?.into_iter().collect();
        let mut partitions = HashMap::new();
        for p in &quarantined {
            partitions.insert(
                p.clone(),
                PartitionQueue {
                    quarantined: true,
                    ..Default::default()
                },
            );
        }
        let epoch = persisted.checkpoint.epoch;
        let inner = Inner {
            next_seq: persisted.counters.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            partitions,
            inflight: BTreeMap::new(),
            next_stream_index: 0,
            checkpoint: persisted.checkpoint,
            dirty_counters: BTreeMap::new(),
            checkpoint_dirty: false,
            quarantined,
            parent_overlay: HashMap::new(),
            dirty_parents: Vec::new(),
        };
        let delivery_permits = Arc::new(Semaphore::new(config.max_concurrent_deliveries));
        let parent_tables = parent_tables(&schema).into_iter().collect();
        Ok(Self {
            fanout_source: None,
            fanout_client: OnceCell::new(),
            shared: Arc::new(Shared {
                schema,
                config,
                sink,
                state: Mutex::new(state),
                inner: Mutex::new(inner),
                wake: Notify::new(),
                drained: Notify::new(),
                delivery_permits,
                epoch,
                parent_tables,
            }),
        })
    }

    pub fn with_fanout_source(mut self, source: SubscriberConfig) -> Self {
        self.fanout_source = Some(source);
        self
    }

    /// The checkpoint to resume the subscriber from.
    pub fn checkpoint(&self) -> Checkpoint {
        self.shared.inner.lock().expect("lock").checkpoint.clone()
    }

    pub fn status(&self) -> DistributorStatus {
        let inner = self.shared.inner.lock().expect("lock");
        DistributorStatus {
            checkpoint: inner.checkpoint.clone(),
            inflight_transactions: inner.inflight.len(),
            queued_partition_transactions: inner.partitions.values().map(|p| p.queue.len()).sum(),
            quarantined_partitions: inner
                .quarantined
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
        }
    }

    /// Runs until the stream closes, cancellation, or a fatal error. Consumes `rx` in order.
    pub async fn run(
        &self,
        mut rx: mpsc::Receiver<StreamItem>,
        cancel: CancellationToken,
    ) -> Result<(), DistributorError> {
        let shared = self.shared.clone();
        let scheduler = tokio::spawn(scheduler_loop(shared.clone(), cancel.clone()));
        let flusher = tokio::spawn(checkpoint_flusher(shared.clone(), cancel.clone()));
        let result = self.consume(&mut rx, &cancel).await;
        cancel.cancel();
        let _ = scheduler.await;
        let _ = flusher.await;
        // Final flush so a clean shutdown resumes exactly where it stopped.
        flush_checkpoint(&shared)?;
        result
    }

    async fn consume(
        &self,
        rx: &mut mpsc::Receiver<StreamItem>,
        cancel: &CancellationToken,
    ) -> Result<(), DistributorError> {
        let shared = &self.shared;
        loop {
            // Backpressure: do not read more until in-flight work drains.
            loop {
                let inflight = shared.inner.lock().expect("lock").inflight.len();
                if inflight < shared.config.max_inflight_transactions {
                    break;
                }
                metrics::counter!("orbit_distributor_backpressure_waits_total").increment(1);
                tokio::select! {
                    _ = cancel.cancelled() => return Err(DistributorError::Cancelled),
                    _ = shared.drained.notified() => {}
                }
            }
            let item = tokio::select! {
                _ = cancel.cancelled() => return Err(DistributorError::Cancelled),
                item = rx.recv() => item.ok_or(DistributorError::StreamClosed)?,
            };
            match item {
                StreamItem::Transaction(tx) => tokio::select! {
                    _ = cancel.cancelled() => return Err(DistributorError::Cancelled),
                    result = self.route_transaction(tx) => result?,
                },
                StreamItem::Position { shard, position } | StreamItem::Ddl { shard, position, .. } => {
                    self.record_position(shard, position);
                }
                StreamItem::Heartbeat | StreamItem::CopyCompleted { .. } => {}
            }
        }
    }

    fn record_position(&self, shard: ShardId, position: String) {
        let shared = &self.shared;
        let mut inner = shared.inner.lock().expect("lock");
        let idx = inner.next_stream_index;
        inner.next_stream_index += 1;
        inner.inflight.insert(
            idx,
            InflightTxn {
                shard,
                position,
                assignments: vec![],
                pending: 0,
                parent_updates: vec![],
            },
        );
        advance_checkpoint(&mut inner);
    }

    /// The parent index entries a transaction writes: one per insert or update of a parent
    /// table row whose partition is not NULL. Deletes leave the index untouched.
    fn parent_updates_of(&self, tx: &SourceTransaction) -> Result<Vec<ParentEntry>, RoutingError> {
        let shared = &self.shared;
        let mut updates = Vec::new();
        if shared.parent_tables.is_empty() {
            return Ok(updates);
        }
        for change in &tx.changes {
            if !shared.parent_tables.contains(&change.table) {
                continue;
            }
            let Some(after) = &change.after else { continue };
            let parent = shared
                .schema
                .table(&change.table)
                .expect("parent table is in the schema");
            let key = parent_key_of(parent, after)?;
            let partition = partition_of(&shared.schema, parent, after, &no_parents)?;
            if let (Some(key), Some(partition)) = (key, partition) {
                updates.push(ParentEntry {
                    table: change.table.clone(),
                    key,
                    partition,
                });
            }
        }
        Ok(updates)
    }

    async fn route_transaction(&self, tx: SourceTransaction) -> Result<(), DistributorError> {
        let shared = &self.shared;
        let hydrate = shared.schema.tables.iter().any(|t| !t.partition_routes.is_empty())
            && !shared.state.lock().expect("lock").fanout_has_journal(&tx)?
            && !orbit_vstream::shared_projection::queries(&shared.schema, Some(&tx.changes))?.is_empty();
        let hydrated = if hydrate {
            let source = self
                .fanout_source
                .as_ref()
                .ok_or_else(|| VStreamError::Malformed("missing shared-item hydration source".into()))?;
            let started = Instant::now();
            let rows = tokio::time::timeout(Duration::from_secs(60), async {
                let client = self.fanout_client.get_or_try_init(|| source.endpoint.connect()).await?;
                orbit_vstream::shared_projection::load_with_client(
                    client,
                    &source.keyspace,
                    &shared.schema,
                    Some(&tx.changes),
                )
                .await
            })
            .await
            .map_err(|_| VStreamError::Timeout(Duration::from_secs(60)))??;
            let elapsed = started.elapsed();
            metrics::histogram!("orbit_distributor_shared_hydration_seconds").record(elapsed.as_secs_f64());
            if elapsed >= Duration::from_secs(1) {
                warn!(
                    ms = elapsed.as_millis() as u64,
                    changes = tx.changes.len(),
                    rows = rows.len(),
                    "shared-item hydration delayed routing"
                );
            }
            rows
        } else {
            vec![]
        };

        let parent_updates = self.parent_updates_of(&tx)?;
        let shard = ShardId {
            keyspace: tx.keyspace.clone(),
            shard: tx.shard.clone(),
        };
        let mut inner = shared.inner.lock().expect("lock");
        let idx = inner.next_stream_index;
        inner.next_stream_index += 1;
        // Index first, then route: a child inserted with its parent must resolve.
        for e in &parent_updates {
            inner
                .parent_overlay
                .entry(e.table.clone())
                .or_default()
                .insert(e.key.clone(), (e.partition.clone(), idx));
        }
        let (routed, stats) = {
            let state = shared.state.lock().expect("lock");
            let lookup_error: RefCell<Option<StateError>> = RefCell::new(None);
            let overlay = &inner.parent_overlay;
            let lookup = |table: &str, key: &str| -> Option<String> {
                if let Some((p, _)) = overlay.get(table).and_then(|m| m.get(key)) {
                    return Some(p.clone());
                }
                match state.parent_partition(table, key) {
                    Ok(p) => p,
                    Err(e) => {
                        *lookup_error.borrow_mut() = Some(e);
                        None
                    }
                }
            };
            let result = route(&shared.schema, &tx.changes, &lookup);
            if let Some(e) = lookup_error.into_inner() {
                return Err(e.into());
            }
            let (mut routed, mut stats) = result?;
            if shared.schema.tables.iter().any(|t| !t.partition_routes.is_empty()) {
                if !state.fanout_ready(&shared.schema.schema_hash)? {
                    return Err(StateError::Fanout(crate::fanout::FanoutError::Snapshot(
                        "fanout graph is not bootstrapped".into(),
                    ))
                    .into());
                }
                for (partition, changes) in state.route_fanout(&shared.schema, &tx, &hydrated)? {
                    stats.routed += changes.len() as u64;
                    routed.entry(partition).or_default().extend(changes);
                }
            }
            (routed, stats)
        };
        metrics::counter!("orbit_distributor_routed_rows_total").increment(stats.routed);
        metrics::counter!("orbit_distributor_unpartitioned_rows_total").increment(stats.unpartitioned);
        metrics::counter!("orbit_distributor_partition_moves_total").increment(stats.moves);
        metrics::counter!("orbit_distributor_unresolved_parent_total").increment(stats.unresolved_parent);
        if stats.unpartitioned > 0 {
            debug!(gtid = %tx.gtid, count = stats.unpartitioned, "rows with NULL partition key were not routed");
        }
        let mut assignments = Vec::with_capacity(routed.len());
        let mut pending = 0;
        for (partition, changes) in routed {
            let seq = *inner.next_seq.get(&partition).unwrap_or(&0) + 1;
            inner.next_seq.insert(partition.clone(), seq);
            assignments.push((partition.clone(), seq));
            let ptx = PartitionTransaction {
                seq,
                keyspace: tx.keyspace.clone(),
                shard: tx.shard.clone(),
                gtid: tx.gtid.clone(),
                position: tx.position.clone(),
                commit_timestamp: tx.commit_timestamp,
                changes,
                trace: TraceContext {
                    distributor_dispatched_at_ms: None,
                    ..tx.trace.clone()
                },
            };
            let pq = inner.partitions.entry(partition.clone()).or_default();
            if pq.quarantined {
                // The partition is quarantined: keep the transaction durably, count it as done.
                shared
                    .state
                    .lock()
                    .expect("lock")
                    .quarantine(&ptx, &partition, "partition_quarantined")?;
                metrics::counter!("orbit_distributor_quarantined_transactions_total").increment(1);
                continue;
            }
            pq.queue.push_back((idx, ptx));
            pending += 1;
        }
        inner.inflight.insert(
            idx,
            InflightTxn {
                shard,
                position: tx.position,
                assignments,
                pending,
                parent_updates,
            },
        );
        metrics::gauge!("orbit_distributor_inflight_transactions").set(inner.inflight.len() as f64);
        if pending == 0 {
            advance_checkpoint(&mut inner);
        }
        drop(inner);
        shared.wake.notify_one();
        Ok(())
    }
}

/// Picks idle partitions with queued work and spawns deliveries, bounded by the semaphore.
async fn scheduler_loop(shared: Arc<Shared>, cancel: CancellationToken) {
    loop {
        let mut ready: Vec<(String, CdcBatch, Vec<u64>)> = Vec::new();
        {
            let mut inner = shared.inner.lock().expect("lock");
            let partitions: Vec<String> = inner
                .partitions
                .iter()
                .filter(|(_, q)| !q.busy && !q.quarantined && !q.queue.is_empty())
                .map(|(k, _)| k.clone())
                .collect();
            for partition in partitions {
                let pq = inner.partitions.get_mut(&partition).expect("exists");
                let mut txns = Vec::new();
                let mut indexes = Vec::new();
                let mut bytes = 0usize;
                while let Some((idx, ptx)) = pq.queue.front() {
                    let size = serde_json::to_vec(ptx).map(|v| v.len()).unwrap_or(0);
                    if !txns.is_empty()
                        && (txns.len() >= shared.config.max_batch_transactions
                            || bytes + size > shared.config.max_batch_bytes)
                    {
                        break;
                    }
                    bytes += size;
                    indexes.push(*idx);
                    let (_, mut ptx) = pq.queue.pop_front().expect("front exists");
                    ptx.trace.distributor_dispatched_at_ms = Some(now_ms());
                    txns.push(ptx);
                }
                pq.busy = true;
                let batch = CdcBatch {
                    protocol_version: orbit_protocol::INTERNAL_PROTOCOL_VERSION,
                    schema_hash: shared.schema.schema_hash.clone(),
                    stream_epoch: shared.epoch,
                    partition: partition.clone(),
                    transactions: txns,
                    delivery_id: uuid::Uuid::new_v4().to_string(),
                };
                ready.push((partition, batch, indexes));
            }
        }
        for (partition, batch, indexes) in ready {
            let permit = tokio::select! {
                _ = cancel.cancelled() => return,
                p = shared.delivery_permits.clone().acquire_owned() => p.expect("semaphore open"),
            };
            let shared = shared.clone();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                let _permit = permit;
                deliver_with_retry(shared, partition, batch, indexes, cancel).await;
            });
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = shared.wake.notified() => {}
        }
    }
}

async fn deliver_with_retry(
    shared: Arc<Shared>,
    partition: String,
    batch: CdcBatch,
    indexes: Vec<u64>,
    cancel: CancellationToken,
) {
    let mut backoff = shared.config.retry_backoff_min;
    let mut rejects: u32 = 0;
    let first_seq = batch.first_seq().unwrap_or(0);
    let last_seq = batch.last_seq().unwrap_or(0);
    let started = Instant::now();
    loop {
        if cancel.is_cancelled() {
            // Put the work back so a restart-in-process (tests) keeps ordering; the persisted
            // checkpoint never includes these, so a real restart replays them anyway.
            let mut inner = shared.inner.lock().expect("lock");
            requeue(&mut inner, &partition, batch.transactions.clone(), &indexes);
            return;
        }
        let attempt_started = Instant::now();
        let result = shared.sink.deliver(&batch).await;
        metrics::histogram!("orbit_distributor_delivery_seconds").record(attempt_started.elapsed().as_secs_f64());
        match result {
            Ok(CdcBatchAck::Applied {
                applied_seq,
                duplicates,
                apply_ms,
            }) => {
                metrics::counter!("orbit_distributor_deliveries_total", "result" => "applied").increment(1);
                metrics::counter!("orbit_distributor_duplicate_transactions_total").increment(duplicates as u64);
                metrics::histogram!("orbit_distributor_do_apply_ms").record(apply_ms as f64);
                if applied_seq < last_seq {
                    // The DO claims success but is behind the batch: treat as a protocol violation.
                    error!(
                        partition,
                        applied_seq, last_seq, "DO acked below the batch's last seq; quarantining"
                    );
                    quarantine_batch(&shared, &partition, &batch, &indexes, "ack_below_batch");
                    return;
                }
                let mut inner = shared.inner.lock().expect("lock");
                for idx in &indexes {
                    if let Some(t) = inner.inflight.get_mut(idx) {
                        t.pending -= 1;
                    }
                }
                inner.partitions.get_mut(&partition).expect("exists").busy = false;
                advance_checkpoint(&mut inner);
                debug!(
                    partition,
                    first_seq,
                    last_seq,
                    ms = started.elapsed().as_millis() as u64,
                    "batch applied"
                );
                drop(inner);
                shared.wake.notify_one();
                shared.drained.notify_waiters();
                return;
            }
            Ok(CdcBatchAck::Rejected { reason }) => {
                rejects += 1;
                metrics::counter!("orbit_distributor_deliveries_total", "result" => "rejected").increment(1);
                let permanent = matches!(
                    reason,
                    RejectReason::WrongPartition { .. }
                        | RejectReason::StaleEpoch { .. }
                        | RejectReason::InvalidRow { .. }
                        | RejectReason::SequenceConflict { .. }
                );
                let gap = matches!(reason, RejectReason::SequenceGap { .. });
                warn!(partition, first_seq, last_seq, ?reason, rejects, "batch rejected");
                if permanent || rejects >= shared.config.max_reject_attempts {
                    let code = reject_code(&reason);
                    quarantine_batch(&shared, &partition, &batch, &indexes, code);
                    return;
                }
                // Gaps and schema/protocol mismatches can heal (DO redeploy, DO reset) so retry.
                let _ = gap;
            }
            Err(e) => {
                metrics::counter!("orbit_distributor_deliveries_total", "result" => "failed").increment(1);
                metrics::counter!("orbit_distributor_delivery_retries_total").increment(1);
                if !e.is_transient() {
                    rejects += 1;
                    if rejects >= shared.config.max_reject_attempts {
                        error!(partition, error = %e, "permanent delivery failure; quarantining");
                        quarantine_batch(&shared, &partition, &batch, &indexes, "delivery_failed");
                        return;
                    }
                }
                warn!(partition, first_seq, last_seq, error = %e, backoff_ms = backoff.as_millis() as u64, "delivery failed; retrying");
            }
        }
        let jitter = Duration::from_millis(rand::random::<u64>() % (backoff.as_millis() as u64 / 2).max(1));
        tokio::select! {
            _ = cancel.cancelled() => {}
            _ = tokio::time::sleep(backoff + jitter) => {}
        }
        backoff = (backoff * 2).min(shared.config.retry_backoff_max);
    }
}

fn reject_code(reason: &RejectReason) -> &'static str {
    match reason {
        RejectReason::ProtocolVersionMismatch { .. } => "protocol_version_mismatch",
        RejectReason::SchemaMismatch { .. } => "schema_mismatch",
        RejectReason::SequenceGap { .. } => "sequence_gap",
        RejectReason::SequenceConflict { .. } => "sequence_conflict",
        RejectReason::StaleEpoch { .. } => "stale_epoch",
        RejectReason::InvalidRow { .. } => "invalid_row",
        RejectReason::WrongPartition { .. } => "wrong_partition",
        RejectReason::Internal { .. } => "internal",
    }
}

fn requeue(inner: &mut Inner, partition: &str, txns: Vec<PartitionTransaction>, indexes: &[u64]) {
    let pq = inner.partitions.get_mut(partition).expect("exists");
    for (idx, ptx) in indexes.iter().zip(txns).rev() {
        pq.queue.push_front((*idx, ptx));
    }
    pq.busy = false;
}

fn quarantine_batch(shared: &Shared, partition: &str, batch: &CdcBatch, indexes: &[u64], reason: &str) {
    metrics::counter!("orbit_distributor_quarantined_transactions_total").increment(batch.transactions.len() as u64);
    let mut inner = shared.inner.lock().expect("lock");
    {
        let state = shared.state.lock().expect("lock");
        for ptx in &batch.transactions {
            if let Err(e) = state.quarantine(ptx, partition, reason) {
                // Losing the quarantine record would lose data: abort the process instead.
                error!(partition, error = %e, "failed to persist quarantine record");
                std::process::abort();
            }
        }
        // Everything still queued for this partition is quarantined too, in order.
        let queued: Vec<(u64, PartitionTransaction)> = inner
            .partitions
            .get_mut(partition)
            .expect("exists")
            .queue
            .drain(..)
            .collect();
        for (idx, ptx) in queued {
            if let Err(e) = state.quarantine(&ptx, partition, "partition_quarantined") {
                error!(partition, error = %e, "failed to persist quarantine record");
                std::process::abort();
            }
            if let Some(t) = inner.inflight.get_mut(&idx) {
                t.pending -= 1;
            }
        }
    }
    let pq = inner.partitions.get_mut(partition).expect("exists");
    pq.quarantined = true;
    pq.busy = false;
    inner.quarantined.insert(partition.to_string());
    metrics::gauge!("orbit_distributor_quarantined_partitions").set(inner.quarantined.len() as f64);
    for idx in indexes {
        if let Some(t) = inner.inflight.get_mut(idx) {
            t.pending -= 1;
        }
    }
    advance_checkpoint(&mut inner);
    drop(inner);
    shared.wake.notify_one();
    shared.drained.notify_waiters();
}

/// Moves the checkpoint over the contiguous prefix of fully acknowledged stream items.
fn advance_checkpoint(inner: &mut Inner) {
    while let Some((&idx, t)) = inner.inflight.iter().next() {
        if t.pending != 0 {
            break;
        }
        let t = inner.inflight.remove(&idx).expect("exists");
        inner.checkpoint.set(t.shard, t.position);
        for (p, seq) in t.assignments {
            inner.dirty_counters.insert(p, seq);
        }
        for e in t.parent_updates {
            inner.dirty_parents.push((e, idx));
        }
        inner.checkpoint_dirty = true;
    }
    metrics::gauge!("orbit_distributor_inflight_transactions").set(inner.inflight.len() as f64);
}

async fn checkpoint_flusher(shared: Arc<Shared>, cancel: CancellationToken) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(shared.config.checkpoint_interval) => {}
        }
        if let Err(e) = flush_checkpoint(&shared) {
            error!(error = %e, "failed to persist checkpoint");
        }
    }
}

fn flush_checkpoint(shared: &Shared) -> Result<(), StateError> {
    let (checkpoint, counters, parents) = {
        let mut inner = shared.inner.lock().expect("lock");
        if !inner.checkpoint_dirty {
            return Ok(());
        }
        inner.checkpoint_dirty = false;
        (
            inner.checkpoint.clone(),
            std::mem::take(&mut inner.dirty_counters),
            std::mem::take(&mut inner.dirty_parents),
        )
    };
    let entries: Vec<ParentEntry> = parents.iter().map(|(e, _)| e.clone()).collect();
    let committed = {
        let state = shared.state.lock().expect("lock");
        state.commit_checkpoint(&checkpoint, &counters, &entries)
    };
    let mut inner = shared.inner.lock().expect("lock");
    if let Err(e) = committed {
        // Keep the updates for the next flush; a newer counter value wins.
        for (p, seq) in counters {
            inner.dirty_counters.entry(p).or_insert(seq);
        }
        let mut restored = parents;
        restored.append(&mut inner.dirty_parents);
        inner.dirty_parents = restored;
        inner.checkpoint_dirty = true;
        return Err(e);
    }
    // The persisted index now holds these entries: drop them from the overlay unless a later
    // in-flight transaction overwrote the key.
    for (e, idx) in &parents {
        if let Some(m) = inner.parent_overlay.get_mut(&e.table) {
            if matches!(m.get(&e.key), Some((_, writer)) if writer == idx) {
                m.remove(&e.key);
            }
            if m.is_empty() {
                inner.parent_overlay.remove(&e.table);
            }
        }
    }
    drop(inner);
    metrics::counter!("orbit_distributor_checkpoints_total").increment(1);
    debug!(checkpoint = %checkpoint.render(), partitions = counters.len(), parents = entries.len(), "checkpoint persisted");
    Ok(())
}

/// Tables named as `partition_parent` by some table, in schema order, without duplicates.
pub fn parent_tables(schema: &SyncSchema) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for t in &schema.tables {
        if let Some(p) = &t.partition_parent
            && !out.contains(p)
        {
            out.push(p.clone());
        }
    }
    out
}

/// Fills the parent index for every parent table whose index is not marked ready, by reading
/// `SELECT <pk>, <partition_column> FROM <parent>` through vtgate in pages of
/// [`BOOTSTRAP_PAGE_SIZE`] rows with keyset pagination. Call it before the subscriber starts.
///
/// The marker position is read before the copy. The main stream resumes from the checkpoint,
/// which is at or before that position, so every parent change since the checkpoint is
/// replayed into the index; the upserts are idempotent. A parent table that is already ready
/// is skipped, so a restart, a new parent table or a changed `partition_parent` only copies
/// what is missing.
pub async fn bootstrap_parent_index(
    schema: &SyncSchema,
    state: &StateStore,
    config: &SubscriberConfig,
) -> Result<(), DistributorError> {
    for name in parent_tables(schema) {
        if state.parent_index_ready(&name)?.is_some() {
            continue;
        }
        let parent = schema
            .table(&name)
            .ok_or_else(|| RoutingError::UnknownTable { table: name.clone() })?;
        let started = Instant::now();
        let position = current_position(config, schema).await?.render();
        let pk = parent.primary_key.first().cloned().unwrap_or_default();
        let kind = parent_key_kind(parent);
        let mut rows_total = 0usize;
        let mut last_key: Option<String> = None;
        loop {
            let after = match &last_key {
                Some(k) => format!(" WHERE {} > {}", quote_ident(&pk), sql_literal(kind, k)?),
                None => String::new(),
            };
            let sql = format!(
                "SELECT {pk}, {partition} FROM {table}{after} ORDER BY {pk} LIMIT {limit}",
                pk = quote_ident(&pk),
                partition = quote_ident(&parent.partition_column),
                table = quote_ident(&name),
                limit = BOOTSTRAP_PAGE_SIZE,
            );
            let page = execute(&config.endpoint, &config.keyspace, &sql).await?;
            let count = page.len();
            let mut entries: Vec<(String, String)> = Vec::with_capacity(count);
            for row in page {
                let key = row.first().cloned().flatten().ok_or_else(|| {
                    VStreamError::Malformed(format!("table {name}: NULL primary key in bootstrap copy"))
                })?;
                if let Some(partition) = row.get(1).cloned().flatten() {
                    entries.push((key.clone(), partition));
                }
                last_key = Some(key);
            }
            rows_total += state.bulk_upsert_parents(&name, entries.iter().map(|(k, p)| (k.as_str(), p.as_str())))?;
            if count < BOOTSTRAP_PAGE_SIZE {
                break;
            }
        }
        state.mark_parent_index_ready(&name, &position)?;
        info!(
            table = %name,
            rows = rows_total,
            ms = started.elapsed().as_millis() as u64,
            position = %position,
            "parent index bootstrapped"
        );
    }
    Ok(())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
