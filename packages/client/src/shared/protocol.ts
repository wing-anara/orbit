import { Schema } from "effect"
import { canonicalJson } from "@orbit/schema"
import { JsonValue, MutationOutcome, NamedQueryRef, Query } from "@orbit/protocol"
import { SyncError } from "@orbit/protocol/client"

export const SHARED_PROTOCOL_VERSION = 1

export const WireQuery = Schema.Struct({ ast: Query, ref: Schema.optionalKey(NamedQueryRef) })
export type WireQuery = typeof WireQuery.Type
const decodeWireQuery = Schema.decodeUnknownSync(WireQuery)
export const queryKey = (query: WireQuery): string => canonicalJson(decodeWireQuery(query))
const number = Schema.Finite
const nullableNumber = Schema.NullOr(number)
const rows = Schema.Array(Schema.Record(Schema.String, Schema.Unknown))
export const Snapshot = Schema.Struct({
  status: Schema.Literals(["pending", "stale", "live", "error"]),
  rows,
  error: Schema.NullOr(SyncError),
  cursor: nullableNumber,
})
export type Snapshot = typeof Snapshot.Type
export const Status = Schema.Struct({
  connection: Schema.Union([
    Schema.Struct({ status: Schema.Literal("connecting"), attempt: number }),
    Schema.Struct({ status: Schema.Literal("open") }),
    Schema.Struct({
      status: Schema.Literal("reconnecting"),
      attempt: number,
      retryInMs: number,
      lastError: Schema.String,
    }),
    Schema.Struct({
      status: Schema.Literal("closed"),
      reason: Schema.String,
      fatal: Schema.Boolean,
      code: nullableNumber,
    }),
  ]),
  cursor: nullableNumber,
  partition: Schema.String,
  pendingSubscriptions: number,
  pendingMutations: number,
  lastDeltaAt: nullableNumber,
  lastCommitTimestamp: nullableNumber,
  storage: Schema.NullOr(
    Schema.Struct({ usage: nullableNumber, quota: nullableNumber, databaseBytes: nullableNumber }),
  ),
  storageMode: Schema.Literals(["opfs", "memory"]),
  fatalError: Schema.NullOr(SyncError),
})
export const MutationEvent = Schema.Struct({
  id: number,
  name: Schema.String,
  status: Schema.Literals(["applied_locally", "pushed", "confirmed", "failed"]),
  error: Schema.optionalKey(Schema.String),
})
export const Command = Schema.Union([
  Schema.Struct({ type: Schema.Literal("awaitMutation"), id: Schema.String, mutationId: number }),
  Schema.Struct({ type: Schema.Literal("subscribe"), id: Schema.String, query: WireQuery }),
  Schema.Struct({ type: Schema.Literal("release"), id: Schema.String }),
  Schema.Struct({ type: Schema.Literal("read"), id: Schema.String, query: WireQuery }),
  Schema.Struct({
    type: Schema.Literal("mutate"),
    id: Schema.String,
    name: Schema.String,
    args: Schema.Record(Schema.String, JsonValue),
  }),
])
export type Command = typeof Command.Type
export const Event = Schema.Union([
  Schema.Struct({ type: Schema.Literal("ready"), clientId: Schema.String, status: Status }),
  Schema.Struct({ type: Schema.Literal("status"), status: Status }),
  Schema.Struct({ type: Schema.Literal("snapshot"), id: Schema.String, snapshot: Snapshot }),
  Schema.Struct({ type: Schema.Literal("read"), id: Schema.String, rows }),
  Schema.Struct({ type: Schema.Literal("allocated"), id: Schema.String, mutationId: number }),
  Schema.Struct({
    type: Schema.Literal("local"),
    id: Schema.String,
    error: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({ type: Schema.Literal("outcome"), id: Schema.String, outcome: MutationOutcome }),
  Schema.Struct({ type: Schema.Literal("error"), id: Schema.String, message: Schema.String }),
  Schema.Struct({ type: Schema.Literal("mutation"), event: MutationEvent }),
])
export type Event = typeof Event.Type
export const Message = Schema.Union([
  Schema.Struct({ type: Schema.Literal("hello"), peer: Schema.String, schema: Schema.String }),
  Schema.Struct({ type: Schema.Literal("leave"), peer: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("owner"),
    peer: Schema.String,
    epoch: Schema.String,
    schema: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("command"),
    peer: Schema.String,
    epoch: Schema.String,
    command: Command,
  }),
  Schema.Struct({
    type: Schema.Literal("event"),
    peer: Schema.String,
    epoch: Schema.String,
    to: Schema.NullOr(Schema.String),
    event: Event,
  }),
  Schema.Struct({ type: Schema.Literal("stopped"), peer: Schema.String, epoch: Schema.String }),
])
export type Message = typeof Message.Type
export const decodeMessage = Schema.decodeUnknownSync(Message)
