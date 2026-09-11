//! Normalized change data capture types and the distributor to Durable Object batch protocol.
//!
//! Identity and ordering model:
//! * A source transaction is identified by its `gtid` (`uuid:gno`) within a `(keyspace, shard)`.
//! * The distributor assigns each partition a dense per-partition sequence (`seq`). Replay from a
//!   checkpoint re-derives identical sequences because routing is a pure function of the
//!   transaction content and the checkpointed counters.
//! * A Durable Object applies batches in `seq` order and treats `seq <= applied_seq` as a
//!   duplicate. A gap (`seq > applied_seq + 1`) is rejected, never skipped.
//! * `stream_epoch` increments when an operator resets the checkpoint. A Durable Object that sees
//!   a new epoch discards its cached scopes, because the stream may have skipped transactions.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::value::{Row, RowKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RowOp {
    Insert,
    Update,
    Delete,
}

/// One row change inside a source transaction. Images are full rows (Vitess requires
/// `binlog_row_image=FULL`), projected to the sync schema's columns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RowChange {
    pub table: String,
    pub op: RowOp,
    /// Primary key of the row *after* the change for insert/update, *before* for delete.
    pub key: RowKey,
    /// Present for update and delete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<Row>,
    /// Present for insert and update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Row>,
}

/// A committed source transaction as observed on one shard stream, after normalization and
/// projection to the sync schema. Transactions that touch no synced table still appear (with
/// no changes) so the checkpoint can advance; the distributor drops them before delivery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceTransaction {
    pub keyspace: String,
    pub shard: String,
    /// The single transaction id added by this step, `uuid:gno`.
    pub gtid: String,
    /// Full shard position after this transaction, `MySQL56/...`.
    pub position: String,
    /// Commit time from the binlog, unix seconds.
    pub commit_timestamp: i64,
    pub changes: Vec<RowChange>,
    /// Subscriber-side observability. Not part of identity.
    pub trace: TraceContext,
}

/// Timestamps collected as a change travels through the pipeline (unix milliseconds).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TraceContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscriber_received_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distributor_dispatched_at_ms: Option<i64>,
}

/// The slice of one source transaction that belongs to one logical partition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PartitionTransaction {
    /// Dense per-partition sequence assigned by the distributor.
    pub seq: u64,
    pub keyspace: String,
    pub shard: String,
    pub gtid: String,
    pub position: String,
    pub commit_timestamp: i64,
    pub changes: Vec<RowChange>,
    pub trace: TraceContext,
}

/// Delivery unit from the distributor to one Durable Object. Transactions are contiguous in
/// `seq` and in stream order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CdcBatch {
    pub protocol_version: u32,
    pub schema_hash: String,
    pub stream_epoch: u64,
    /// Partition key rendered as a string (`int`/`bigint` keys use decimal digits).
    pub partition: String,
    pub transactions: Vec<PartitionTransaction>,
    /// Unique id of this delivery attempt, for log correlation only.
    pub delivery_id: String,
}

impl CdcBatch {
    pub fn first_seq(&self) -> Option<u64> {
        self.transactions.first().map(|t| t.seq)
    }
    pub fn last_seq(&self) -> Option<u64> {
        self.transactions.last().map(|t| t.seq)
    }
}

/// Response of a Durable Object to a `CdcBatch`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum CdcBatchAck {
    /// Every transaction in the batch is now durably reflected (or was already).
    Applied {
        applied_seq: u64,
        /// Transactions the DO skipped as duplicates.
        duplicates: u32,
        /// Milliseconds the DO spent applying, for latency metrics.
        apply_ms: u32,
    },
    /// The DO refused the batch. The distributor must not advance past it.
    Rejected { reason: RejectReason },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RejectReason {
    ProtocolVersionMismatch {
        expected: u32,
        got: u32,
    },
    SchemaMismatch {
        do_schema_hash: String,
        got: String,
    },
    /// The batch's first `seq` is beyond `applied_seq + 1`.
    SequenceGap {
        applied_seq: u64,
        first_seq: u64,
    },
    /// A duplicate `seq` carried a different `gtid` than the one already applied.
    SequenceConflict {
        seq: u64,
        applied_gtid: String,
        got_gtid: String,
    },
    /// The DO is already on a newer epoch and refuses older streams.
    StaleEpoch {
        do_epoch: u64,
        got: u64,
    },
    /// A row failed validation against the sync schema.
    InvalidRow {
        table: String,
        seq: u64,
        message: String,
    },
    /// Partition in the batch does not match the DO's partition.
    WrongPartition {
        do_partition: String,
        got: String,
    },
    /// Any other failure with a stable, machine-readable code.
    Internal {
        code: String,
        message: String,
    },
}
