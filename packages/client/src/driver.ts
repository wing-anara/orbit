/**
 * Asynchronous SQL driver used by the browser store. The real implementation lives in a
 * dedicated Web Worker that owns the OPFS-backed SQLite database; tests use Node's SQLite.
 *
 * `batch` runs every statement inside one SQLite transaction: this is how a snapshot or a delta
 * becomes durable atomically. The store never needs read-modify-write inside a transaction;
 * every derived write (garbage collection, membership-gated upserts) is expressed in SQL.
 */

import { Data, Schema } from "effect"
import type { SqlValue } from "@orbit/query"

export interface Statement {
  readonly sql: string
  readonly params: ReadonlyArray<SqlValue>
}

export type SqlRecord = Readonly<Record<string, SqlValue>>

export class SqlDriverError extends Data.TaggedError("SqlDriverError")<{
  readonly message: string
  readonly code:
    | "quota_exceeded"
    | "locked"
    | "corrupt"
    | "unsupported"
    | "sql"
    | "worker"
    | "closed"
  readonly cause?: unknown
}> {}

export interface AsyncSqlDriver {
  readonly query: (
    sql: string,
    params?: ReadonlyArray<SqlValue>,
  ) => Promise<ReadonlyArray<SqlRecord>>
  readonly batch: (statements: ReadonlyArray<Statement>) => Promise<void>
  readonly close: () => Promise<void>
}

/** Storage back end for the browser. `opfs` needs a dedicated worker; `memory` is for tests and fallbacks. */
export type StorageMode = "opfs" | "memory"

// Worker RPC protocol. Every message is validated on both sides.
const SqlValueSchema = Schema.Union([
  Schema.String,
  Schema.Finite,
  Schema.Null,
  Schema.instanceOf(Uint8Array),
])

export const WorkerRequest = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("open"),
    id: Schema.Finite,
    name: Schema.String,
    mode: Schema.Literals(["opfs", "memory"]),
    /** The OPFS pool (directory) to hold; each tab holds its own (see `createOrbitClient`). */
    pool: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("query"),
    id: Schema.Finite,
    sql: Schema.String,
    params: Schema.Array(SqlValueSchema),
  }),
  Schema.Struct({
    type: Schema.Literal("batch"),
    id: Schema.Finite,
    statements: Schema.Array(
      Schema.Struct({ sql: Schema.String, params: Schema.Array(SqlValueSchema) }),
    ),
  }),
  Schema.Struct({ type: Schema.Literal("close"), id: Schema.Finite }),
  Schema.Struct({ type: Schema.Literal("estimate"), id: Schema.Finite }),
])
export type WorkerRequest = typeof WorkerRequest.Type

export const WorkerResponse = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("ok"),
    id: Schema.Finite,
    rows: Schema.optionalKey(Schema.Array(Schema.Record(Schema.String, SqlValueSchema))),
  }),
  Schema.Struct({
    type: Schema.Literal("estimate_result"),
    id: Schema.Finite,
    usage: Schema.NullOr(Schema.Finite),
    quota: Schema.NullOr(Schema.Finite),
    databaseBytes: Schema.NullOr(Schema.Finite),
  }),
  Schema.Struct({
    type: Schema.Literal("error"),
    id: Schema.Finite,
    code: Schema.Literals([
      "quota_exceeded",
      "locked",
      "corrupt",
      "unsupported",
      "sql",
      "worker",
      "closed",
    ]),
    message: Schema.String,
  }),
])
export type WorkerResponse = typeof WorkerResponse.Type
