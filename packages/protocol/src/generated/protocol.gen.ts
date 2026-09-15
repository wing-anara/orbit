// GENERATED FILE. Do not edit.
// Source: schema/protocol.schema.json (exported from crates/orbit-protocol).
// Regenerate with `pnpm codegen`; CI fails when this file is stale.

import { Schema } from "effect"

export const INTERNAL_PROTOCOL_VERSION = 1 as const
export const SYNC_SCHEMA_FORMAT_VERSION = 1 as const

/** Any JSON value. Column cells are typed per table by the sync schema, not here. */
export type JsonValue = string | number | boolean | null | ReadonlyArray<JsonValue> | { readonly [key: string]: JsonValue }
export const JsonValue: Schema.Codec<JsonValue, JsonValue> = Schema.Union([
  Schema.String,
  Schema.Finite,
  Schema.Boolean,
  Schema.Null,
  Schema.Array(Schema.suspend((): Schema.Codec<JsonValue, JsonValue> => JsonValue)),
  Schema.Record(Schema.String, Schema.suspend((): Schema.Codec<JsonValue, JsonValue> => JsonValue)),
])

export const Int = Schema.Finite.check(Schema.isInt())
export const NonNegativeInt = Schema.Finite.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0))

export const RowOp = Schema.Literals(["insert", "update", "delete"])
export type RowOp = typeof RowOp.Type

/**
 * One row change inside a source transaction. Images are full rows (Vitess requires
 * `binlog_row_image=FULL`), projected to the sync schema's columns.
 */
export const RowChange = Schema.Struct({
  table: Schema.String,
  op: RowOp,
  /**
   * Primary key of the row *after* the change for insert/update, *before* for delete.
   */
  key: Schema.Array(JsonValue),
  /**
   * Present for update and delete.
   */
  before: Schema.optionalKey(Schema.NullOr(Schema.Record(Schema.String, JsonValue))),
  /**
   * Present for insert and update.
   */
  after: Schema.optionalKey(Schema.NullOr(Schema.Record(Schema.String, JsonValue))),
})
export type RowChange = typeof RowChange.Type

/**
 * Timestamps collected as a change travels through the pipeline (unix milliseconds).
 */
export const TraceContext = Schema.Struct({
  subscriber_received_at_ms: Schema.optionalKey(Schema.NullOr(Int)),
  distributor_dispatched_at_ms: Schema.optionalKey(Schema.NullOr(Int)),
})
export type TraceContext = typeof TraceContext.Type

/**
 * The slice of one source transaction that belongs to one logical partition.
 */
export const PartitionTransaction = Schema.Struct({
  /**
   * Dense per-partition sequence assigned by the distributor.
   */
  seq: NonNegativeInt,
  keyspace: Schema.String,
  shard: Schema.String,
  gtid: Schema.String,
  position: Schema.String,
  commit_timestamp: Int,
  changes: Schema.Array(RowChange),
  trace: TraceContext,
})
export type PartitionTransaction = typeof PartitionTransaction.Type

/**
 * Delivery unit from the distributor to one Durable Object. Transactions are contiguous in
 * `seq` and in stream order.
 */
export const CdcBatch = Schema.Struct({
  protocol_version: NonNegativeInt,
  schema_hash: Schema.String,
  stream_epoch: NonNegativeInt,
  /**
   * Partition key rendered as a string (`int`/`bigint` keys use decimal digits).
   */
  partition: Schema.String,
  transactions: Schema.Array(PartitionTransaction),
  /**
   * Unique id of this delivery attempt, for log correlation only.
   */
  delivery_id: Schema.String,
})
export type CdcBatch = typeof CdcBatch.Type

export const RejectReason = Schema.Union([
  Schema.Struct({
    expected: NonNegativeInt,
    got: NonNegativeInt,
    kind: Schema.Literal("protocol_version_mismatch"),
  }),
  Schema.Struct({
    do_schema_hash: Schema.String,
    got: Schema.String,
    kind: Schema.Literal("schema_mismatch"),
  }),
  Schema.Struct({
    applied_seq: NonNegativeInt,
    first_seq: NonNegativeInt,
    kind: Schema.Literal("sequence_gap"),
  }),
  Schema.Struct({
    seq: NonNegativeInt,
    applied_gtid: Schema.String,
    got_gtid: Schema.String,
    kind: Schema.Literal("sequence_conflict"),
  }),
  Schema.Struct({
    do_epoch: NonNegativeInt,
    got: NonNegativeInt,
    kind: Schema.Literal("stale_epoch"),
  }),
  Schema.Struct({
    table: Schema.String,
    seq: NonNegativeInt,
    message: Schema.String,
    kind: Schema.Literal("invalid_row"),
  }),
  Schema.Struct({
    do_partition: Schema.String,
    got: Schema.String,
    kind: Schema.Literal("wrong_partition"),
  }),
  Schema.Struct({
    code: Schema.String,
    message: Schema.String,
    kind: Schema.Literal("internal"),
  }),
])
export type RejectReason = typeof RejectReason.Type

/**
 * Response of a Durable Object to a `CdcBatch`.
 */
export const CdcBatchAck = Schema.Union([
  Schema.Struct({
    applied_seq: NonNegativeInt,
    /**
     * Transactions the DO skipped as duplicates.
     */
    duplicates: NonNegativeInt,
    /**
     * Milliseconds the DO spent applying, for latency metrics.
     */
    apply_ms: NonNegativeInt,
    status: Schema.Literal("applied"),
  }),
  Schema.Struct({
    reason: RejectReason,
    status: Schema.Literal("rejected"),
  }),
])
export type CdcBatchAck = typeof CdcBatchAck.Type

export const DerivedRule = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("not_null"),
  }),
  Schema.Struct({
    prefix: Schema.String,
    kind: Schema.Literal("starts_with"),
  }),
])
export type DerivedRule = typeof DerivedRule.Type

/**
 * How the engine computes a derived column. Both the live stream and every fill apply the rule
 * to the raw source cell, so the value is the same on every path.
 */
export const DerivedColumn = Schema.Struct({
  /**
   * Source column in the live table.
   */
  from: Schema.String,
  rule: DerivedRule,
})
export type DerivedColumn = typeof DerivedColumn.Type

/**
 * How a column's values are encoded on the wire and typed in consumers.
 */
export const ValueKind = Schema.Literals(["bool", "int", "bigint", "float", "decimal", "string", "bytes", "json", "datetime", "date", "time"])
export type ValueKind = typeof ValueKind.Type

export const ColumnSchema = Schema.Struct({
  name: Schema.String,
  kind: ValueKind,
  nullable: Schema.Boolean,
  /**
   * Full MySQL column type as introspected, for example `varchar(191)` or `enum('A','B')`.
   */
  source_type: Schema.String,
  /**
   * Allowed values for enum columns, in definition order (index 1 is the first value).
   */
  enum_values: Schema.optionalKey(Schema.NullOr(Schema.Array(Schema.String))),
  /**
   * Set when the engine computes this column from a source column instead of reading it.
   * A derived column is a non-nullable `bool`; the source column itself need not be synced,
   * so its value never leaves the engine.
   */
  derived: Schema.optionalKey(Schema.NullOr(DerivedColumn)),
})
export type ColumnSchema = typeof ColumnSchema.Type

/**
 * Error carried in fill results and internal HTTP error responses.
 */
export const EngineError = Schema.Union([
  Schema.Struct({
    message: Schema.String,
    retryable: Schema.Boolean,
    code: Schema.Literal("source_unavailable"),
  }),
  Schema.Struct({
    message: Schema.String,
    code: Schema.Literal("source_rejected"),
  }),
  Schema.Struct({
    table: Schema.String,
    message: Schema.String,
    code: Schema.Literal("normalization"),
  }),
  Schema.Struct({
    table: Schema.String,
    message: Schema.String,
    code: Schema.Literal("schema_mismatch"),
  }),
  Schema.Struct({
    table: Schema.String,
    code: Schema.Literal("unknown_table"),
  }),
  Schema.Struct({
    message: Schema.String,
    code: Schema.Literal("unauthorized"),
  }),
  Schema.Struct({
    after_ms: NonNegativeInt,
    code: Schema.Literal("timeout"),
  }),
  Schema.Struct({
    message: Schema.String,
    code: Schema.Literal("internal"),
  }),
])
export type EngineError = typeof EngineError.Type

export const FillResult = Schema.Union([
  Schema.Struct({
    /**
     * Shard position (`MySQL56/...`) at which the delivered rows are exact.
     */
    position: Schema.String,
    keyspace: Schema.String,
    shard: Schema.String,
    row_count: NonNegativeInt,
    duration_ms: NonNegativeInt,
    status: Schema.Literal("completed"),
  }),
  Schema.Struct({
    error: EngineError,
    status: Schema.Literal("failed"),
  }),
])
export type FillResult = typeof FillResult.Type

/**
 * One NDJSON line of a fill upload.
 */
export const FillChunk = Schema.Union([
  Schema.Struct({
    fill_id: Schema.String,
    rows: Schema.Array(Schema.Record(Schema.String, JsonValue)),
    type: Schema.Literal("rows"),
  }),
  Schema.Struct({
    fill_id: Schema.String,
    result: FillResult,
    type: Schema.Literal("done"),
  }),
])
export type FillChunk = typeof FillChunk.Type

export const FillRequest = Schema.Struct({
  /**
   * Unique id chosen by the requesting Durable Object.
   */
  fill_id: Schema.String,
  schema_hash: Schema.String,
  partition: Schema.String,
  table: Schema.String,
  /**
   * Unix milliseconds when the DO registered the request.
   */
  requested_at_ms: Int,
})
export type FillRequest = typeof FillRequest.Type

/**
 * Long-poll response listing fills the Rust server should execute next.
 */
export const FillPollResponse = Schema.Struct({
  requests: Schema.Array(FillRequest),
})
export type FillPollResponse = typeof FillPollResponse.Type

export const IntrospectedColumn = Schema.Struct({
  name: Schema.String,
  /**
   * `information_schema.COLUMNS.COLUMN_TYPE`, for example `varchar(191)`.
   */
  column_type: Schema.String,
  /**
   * `information_schema.COLUMNS.DATA_TYPE`, for example `varchar`.
   */
  data_type: Schema.String,
  nullable: Schema.Boolean,
  /**
   * The value kind the engine infers for this column. `tinyint(1)` maps to `bool`.
   */
  kind: ValueKind,
  enum_values: Schema.optionalKey(Schema.NullOr(Schema.Array(Schema.String))),
})
export type IntrospectedColumn = typeof IntrospectedColumn.Type

export const IntrospectedTable = Schema.Struct({
  name: Schema.String,
  primary_key: Schema.Array(Schema.String),
  columns: Schema.Array(IntrospectedColumn),
})
export type IntrospectedTable = typeof IntrospectedTable.Type

/**
 * Raw metadata read from `information_schema`. Produced by `orbit-server schema introspect`
 * and consumed by the TypeScript sync config DSL.
 */
export const IntrospectedSchema = Schema.Struct({
  keyspace: Schema.String,
  /**
   * Server version string, for example `8.0.43-Vitess`.
   */
  server_version: Schema.String,
  tables: Schema.Array(IntrospectedTable),
})
export type IntrospectedSchema = typeof IntrospectedSchema.Type

export const PlacementConfig = Schema.Struct({
    version: NonNegativeInt,
    strategy: Schema.Literal("one_per_partition"),
  })
export type PlacementConfig = typeof PlacementConfig.Type

/**
 * Logical partitioning and physical placement configuration.
 */
export const PartitionConfig = Schema.Struct({
  /**
   * Human name of the logical partition key, for example `organization`.
   */
  name: Schema.String,
  /**
   * Value kind of the partition key. Only `string`, `int` and `bigint` are allowed.
   */
  key_kind: ValueKind,
  /**
   * Physical placement strategy. Changing it invalidates every Durable Object's state.
   */
  placement: PlacementConfig,
})
export type PartitionConfig = typeof PartitionConfig.Type

export const RelationKind = Schema.Union([
  Schema.Literal("one"),
  Schema.Literal("many"),
])
export type RelationKind = typeof RelationKind.Type

export const RelationSchema = Schema.Struct({
  name: Schema.String,
  kind: RelationKind,
  target_table: Schema.String,
  from_columns: Schema.Array(Schema.String),
  to_columns: Schema.Array(Schema.String),
})
export type RelationSchema = typeof RelationSchema.Type

/**
 * A committed source transaction as observed on one shard stream, after normalization and
 * projection to the sync schema. Transactions that touch no synced table still appear (with
 * no changes) so the checkpoint can advance; the distributor drops them before delivery.
 */
export const SourceTransaction = Schema.Struct({
  keyspace: Schema.String,
  shard: Schema.String,
  /**
   * The single transaction id added by this step, `uuid:gno`.
   */
  gtid: Schema.String,
  /**
   * Full shard position after this transaction, `MySQL56/...`.
   */
  position: Schema.String,
  /**
   * Commit time from the binlog, unix seconds.
   */
  commit_timestamp: Int,
  changes: Schema.Array(RowChange),
  /**
   * Subscriber-side observability. Not part of identity.
   */
  trace: TraceContext,
})
export type SourceTransaction = typeof SourceTransaction.Type

export const TableSchema = Schema.Struct({
  /**
   * Source table name in the keyspace.
   */
  name: Schema.String,
  /**
   * Primary key column names in key order. Must be non-empty and be a subset of `columns`.
   */
  primary_key: Schema.Array(Schema.String),
  /**
   * Column that holds the logical partition key. Must be in `columns`. Rows with a NULL
   * partition value are not routed anywhere and are counted as `unpartitioned`.
   * 
   * When `partition_parent` is set, this column holds the primary key of a row of the parent
   * table instead, and the row's partition is derived from that parent row.
   */
  partition_column: Schema.String,
  /**
   * Derived partitioning. When set, `partition_column` holds the primary key value of a row of
   * this table (a synced table that is partitioned directly, with no `partition_parent` of its
   * own), and the row's partition is the partition of that parent row. The parent's primary
   * key must be a single `string`, `int` or `bigint` column, and `partition_column` must have
   * the same kind. Only one level is allowed. A row whose parent is NULL or unknown is not
   * routed and is counted as `unresolved_parent`.
   */
  partition_parent: Schema.optionalKey(Schema.NullOr(Schema.String)),
  /**
   * Synced columns, in order. Source columns not listed are ignored.
   */
  columns: Schema.Array(ColumnSchema),
  /**
   * Declared relations to other synced tables, usable in query includes.
   */
  relations: Schema.Array(RelationSchema),
})
export type TableSchema = typeof TableSchema.Type

/**
 * The compiled sync schema. `schema_hash` is computed by [`SyncSchema::compute_hash`] over the
 * canonical JSON of every other field.
 */
export const SyncSchema = Schema.Struct({
  /**
   * Artifact format version. See `SYNC_SCHEMA_FORMAT_VERSION`.
   */
  format_version: NonNegativeInt,
  /**
   * SHA-256 hex of the canonical artifact without this field.
   */
  schema_hash: Schema.String,
  /**
   * Application-chosen name, used in metrics and Durable Object namespaces.
   */
  app: Schema.String,
  /**
   * Vitess keyspace the tables live in.
   */
  keyspace: Schema.String,
  partition: PartitionConfig,
  /**
   * Tables in dependency-friendly order (parents before children where relations exist).
   */
  tables: Schema.Array(TableSchema),
})
export type SyncSchema = typeof SyncSchema.Type

export const PROTOCOL_ROOTS = ["SyncSchema", "IntrospectedSchema", "CdcBatch", "CdcBatchAck", "SourceTransaction", "FillRequest", "FillPollResponse", "FillChunk", "FillResult", "EngineError"] as const
