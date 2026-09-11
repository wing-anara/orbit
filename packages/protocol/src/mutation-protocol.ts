/**
 * The push protocol between a client and the application's mutation endpoint (HTTP, JSON).
 *
 * A client applies a mutator locally, appends it to its persistent pending log with the next
 * `MutationId`, and pushes the log in order. The application runs the same mutator against the
 * authoritative database inside one transaction that also records the client's
 * `last_mutation_id` in the `orbit_clients` table. That row is synced like any other row, so
 * the client learns that its mutation is confirmed exactly when the mutation's effects arrive
 * through the sync protocol (see `docs/mutations.md`).
 *
 * Mutation ids are dense per client. The server rejects a gap as `out_of_order` and a replay as
 * `duplicate`, which makes retries safe.
 */

import { Schema } from "effect"

import { JsonValue, NonNegativeInt } from "./generated/protocol.gen.ts"

export const MUTATION_PROTOCOL_VERSION = 1 as const

/** Name of the table the application must create and sync (see `orbitClientsDdl`). */
export const ORBIT_CLIENTS_TABLE = "orbit_clients" as const

export const MutationId = NonNegativeInt
export type MutationId = typeof MutationId.Type

export const MutationRef = Schema.Struct({
  id: MutationId,
  name: Schema.String,
  args: Schema.Record(Schema.String, JsonValue),
})
export type MutationRef = typeof MutationRef.Type

export const PushRequest = Schema.Struct({
  protocolVersion: NonNegativeInt,
  clientId: Schema.String,
  partition: Schema.String,
  /** Ascending, contiguous mutation ids. */
  mutations: Schema.Array(MutationRef),
})
export type PushRequest = typeof PushRequest.Type

export const MutationOutcome = Schema.Union([
  Schema.Struct({ id: MutationId, status: Schema.Literal("applied") }),
  /** Already applied by an earlier push; the effects are (or were) synced. */
  Schema.Struct({ id: MutationId, status: Schema.Literal("duplicate") }),
  /**
   * The mutator threw or its arguments were invalid. Nothing was written, but the mutation id
   * was consumed so the client drops the mutation and reports `error`.
   */
  Schema.Struct({ id: MutationId, status: Schema.Literal("failed"), error: Schema.String }),
])
export type MutationOutcome = typeof MutationOutcome.Type

export const PushResponse = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("ok"),
    outcomes: Schema.Array(MutationOutcome),
    /** The server's `last_mutation_id` for the client after this push. */
    lastMutationId: MutationId,
  }),
  /**
   * The whole push was refused: the ids do not continue from the server's `last_mutation_id`
   * (`out_of_order`), the caller is not allowed to write to the partition (`unauthorized`), or the
   * versions differ. The client resynchronizes from `lastMutationId` when given.
   */
  Schema.Struct({
    type: Schema.Literal("refused"),
    reason: Schema.Literals(["out_of_order", "unauthorized", "protocol_version_mismatch"]),
    message: Schema.String,
    lastMutationId: Schema.optionalKey(MutationId),
  }),
])
export type PushResponse = typeof PushResponse.Type

export const encodePushRequest = Schema.encodeUnknownSync(PushRequest)
export const decodePushRequest = Schema.decodeUnknownSync(PushRequest)
export const encodePushResponse = Schema.encodeUnknownSync(PushResponse)
export const decodePushResponse = Schema.decodeUnknownSync(PushResponse)

/**
 * MySQL DDL for the client bookkeeping table. `partition_key` must be the partition column in
 * the sync schema so each client's row syncs to its own partition.
 */
export const orbitClientsDdl = (partitionKeyType = "varchar(191)"): string =>
  `CREATE TABLE IF NOT EXISTS \`${ORBIT_CLIENTS_TABLE}\` (` +
  `\`client_id\` varchar(64) NOT NULL, ` +
  `\`partition_key\` ${partitionKeyType} NOT NULL, ` +
  `\`last_mutation_id\` bigint NOT NULL DEFAULT 0, ` +
  `\`updated_at\` datetime(3) NOT NULL, ` +
  `PRIMARY KEY (\`client_id\`), KEY \`orbit_clients_partition\` (\`partition_key\`))`
