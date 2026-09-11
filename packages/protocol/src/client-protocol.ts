/**
 * Sync protocol between the browser client and a Sync Durable Object (over WebSocket, JSON).
 *
 * Versioning: `CLIENT_PROTOCOL_VERSION` is sent in `hello` and checked first. Every message is
 * a tagged union member and decoding is exhaustive, so an unknown message is an error, never a
 * silent no-op.
 *
 * Cursor model: `cursor` is the partition sequence (`applied_seq` of the Durable Object) at
 * which the client's local state is consistent. Deltas carry the cursor they advance to; the
 * client applies each delta in one local transaction and acknowledges the cursor. On reconnect
 * the client presents its cursor and subscriptions; the server replies with a fresh snapshot
 * per subscription taken at one consistent cursor (see `docs/protocol.md`).
 */

import { Schema } from "effect"

import { EngineError, JsonValue, NonNegativeInt, TraceContext } from "./generated/protocol.gen.ts"
import { Query, QueryRef } from "./query-ast.ts"

export const CLIENT_PROTOCOL_VERSION = 1 as const

export const RowKey = Schema.Array(JsonValue)
export type RowKey = typeof RowKey.Type

export const RowImage = Schema.Record(Schema.String, JsonValue)
export type RowImage = typeof RowImage.Type

/** A row the client must upsert (`row` present) or delete (`row` null). */
export const RowUpdate = Schema.Struct({
  table: Schema.String,
  key: RowKey,
  row: Schema.NullOr(RowImage),
})
export type RowUpdate = typeof RowUpdate.Type

export const MemberRef = Schema.Struct({ table: Schema.String, key: RowKey })
export type MemberRef = typeof MemberRef.Type

/** Column summary the client sends so the server can check additive compatibility. */
export const SchemaSummary = Schema.Struct({
  schemaHash: Schema.String,
  tables: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      columns: Schema.Array(Schema.String),
    }),
  ),
})
export type SchemaSummary = typeof SchemaSummary.Type

export const SubscribeMessage = Schema.Struct({
  type: Schema.Literal("subscribe"),
  id: Schema.String,
  /** A named query (the default) or a raw query when the server allows ad-hoc queries. */
  query: QueryRef,
})
export type SubscribeMessage = typeof SubscribeMessage.Type

export const ClientMessage = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("hello"),
    protocolVersion: NonNegativeInt,
    clientId: Schema.String,
    /** Opaque session token issued by the application backend. */
    token: Schema.String,
    /** The logical partition the client wants. Authorization happens server-side. */
    partition: Schema.String,
    schema: SchemaSummary,
    /** Cursor of the client's local state, or null when starting fresh. */
    cursor: Schema.NullOr(NonNegativeInt),
    /** Subscriptions to establish immediately (used on reconnect). */
    subscriptions: Schema.Array(SubscribeMessage),
  }),
  SubscribeMessage,
  Schema.Struct({ type: Schema.Literal("unsubscribe"), id: Schema.String }),
  Schema.Struct({ type: Schema.Literal("ack"), cursor: NonNegativeInt }),
  Schema.Struct({ type: Schema.Literal("ping"), sentAt: NonNegativeInt }),
])
export type ClientMessage = typeof ClientMessage.Type

export const SyncErrorCode = Schema.Literals([
  "protocol_version_mismatch",
  "schema_mismatch",
  "unauthorized",
  "partition_denied",
  "unsupported_query",
  "unknown_query",
  "invalid_message",
  "fill_failed",
  "overloaded",
  "internal",
])
export type SyncErrorCode = typeof SyncErrorCode.Type

export const SyncError = Schema.Struct({
  code: SyncErrorCode,
  message: Schema.String,
  /** Extra machine-readable context, for example the server's protocol version. */
  details: Schema.optionalKey(Schema.Record(Schema.String, JsonValue)),
  cause: Schema.optionalKey(EngineError),
})
export type SyncError = typeof SyncError.Type

/** Origin of a delta: the source transaction it came from, for end-to-end tracing. */
export const DeltaOrigin = Schema.Struct({
  gtid: Schema.String,
  commitTimestamp: NonNegativeInt,
  seq: NonNegativeInt,
  trace: TraceContext,
  /** Unix ms when the Durable Object applied the transaction. */
  appliedAt: NonNegativeInt,
})
export type DeltaOrigin = typeof DeltaOrigin.Type

export const MembershipChange = Schema.Struct({
  subscriptionId: Schema.String,
  added: Schema.Array(MemberRef),
  removed: Schema.Array(MemberRef),
})
export type MembershipChange = typeof MembershipChange.Type

export const ServerMessage = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("welcome"),
    protocolVersion: NonNegativeInt,
    sessionId: Schema.String,
    partition: Schema.String,
    schemaHash: Schema.String,
    /** The Durable Object's current cursor. */
    cursor: NonNegativeInt,
    serverTime: NonNegativeInt,
  }),
  /**
   * Full result of one subscription at `cursor`. Large results arrive in several messages with
   * `complete: false`; the client applies them as one unit when the final one arrives. The row
   * set replaces the client's previous membership for the subscription.
   */
  Schema.Struct({
    type: Schema.Literal("snapshot"),
    subscriptionId: Schema.String,
    cursor: NonNegativeInt,
    rows: Schema.Array(RowUpdate),
    members: Schema.Array(MemberRef),
    complete: Schema.Boolean,
  }),
  /**
   * Changes produced by exactly one source transaction. Applied atomically on the client.
   */
  Schema.Struct({
    type: Schema.Literal("delta"),
    cursor: NonNegativeInt,
    origin: DeltaOrigin,
    rows: Schema.Array(RowUpdate),
    memberships: Schema.Array(MembershipChange),
  }),
  Schema.Struct({
    type: Schema.Literal("subscribed"),
    id: Schema.String,
    /** `pending` while the Durable Object fills the scope from the source. */
    status: Schema.Literals(["pending", "live"]),
    /** The resolved, normalized query the server materializes for this subscription. */
    query: Query,
  }),
  Schema.Struct({ type: Schema.Literal("unsubscribed"), id: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("subscription_error"),
    id: Schema.String,
    error: SyncError,
  }),
  Schema.Struct({ type: Schema.Literal("error"), error: SyncError, fatal: Schema.Boolean }),
  Schema.Struct({
    type: Schema.Literal("pong"),
    sentAt: NonNegativeInt,
    serverTime: NonNegativeInt,
  }),
])
export type ServerMessage = typeof ServerMessage.Type

/** WebSocket close codes used by the server. */
export const CloseCode = {
  protocolVersionMismatch: 4400,
  unauthorized: 4401,
  partitionDenied: 4403,
  schemaMismatch: 4409,
  invalidMessage: 4422,
  overloaded: 4429,
  internal: 4500,
} as const

export const encodeClientMessage = Schema.encodeUnknownSync(ClientMessage)
export const decodeClientMessage = Schema.decodeUnknownSync(ClientMessage)
export const encodeServerMessage = Schema.encodeUnknownSync(ServerMessage)
export const decodeServerMessage = Schema.decodeUnknownSync(ServerMessage)
