/**
 * Session bookkeeping for connected clients, persisted in the Durable Object's SQLite so
 * hibernation and restarts lose nothing.
 *
 * | table       | purpose                                                     |
 * |-------------|-------------------------------------------------------------|
 * | sessions    | one row per WebSocket: subject, client id, schema summary   |
 * | client_subs | (session, client subscription id) -> engine subscription id  |
 */

import { Result, Schema } from "effect"
import type { SqlValue } from "@orbit/query"
import { QueryRef } from "@orbit/protocol"
import { SchemaSummary } from "@orbit/protocol/client"

import type { SqlDriver } from "./core/driver.ts"

/**
 * What a hibernating WebSocket carries across restarts (`serializeAttachment`). The shape is a
 * schema so the attachment is decoded, never cast, when the socket wakes up.
 */
export const SocketAttachment = Schema.Struct({
  session: Schema.String,
  subject: Schema.String,
  partition: Schema.String,
  hello: Schema.Boolean,
  /** Diagnostic client acknowledgment; recovery uses the durable engine/browser cursors. */
  ackCursor: Schema.optionalKey(Schema.Finite),
  /** When the grant behind the socket expires (unix ms); absent when it does not expire. */
  expiresAt: Schema.optionalKey(Schema.Finite),
})
export type SocketAttachment = typeof SocketAttachment.Type

export const encodeAttachment = Schema.encodeSync(SocketAttachment)
const decodeAttachment = Schema.decodeUnknownResult(Schema.NullOr(SocketAttachment))

/** The attachment of a socket, or null when it is missing or malformed. */
export const readAttachment = (ws: WebSocket): SocketAttachment | null => {
  const raw: unknown = ws.deserializeAttachment()
  const decoded = decodeAttachment(raw)
  return Result.isSuccess(decoded) ? decoded.success : null
}

const decodeQueryRef = Schema.decodeUnknownSync(Schema.fromJsonString(QueryRef))

const decodeSchemaSummary = Schema.decodeUnknownSync(Schema.fromJsonString(SchemaSummary))

export const SESSION_DDL: ReadonlyArray<string> = [
  `CREATE TABLE IF NOT EXISTS sessions (session TEXT PRIMARY KEY, client_id TEXT NOT NULL, subject TEXT NOT NULL, schema_json TEXT NOT NULL, identical_schema INTEGER NOT NULL, cursor INTEGER NOT NULL DEFAULT 0, connected_at INTEGER NOT NULL, last_seen_at INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS client_subs (session TEXT NOT NULL, client_sub_id TEXT NOT NULL, subscription TEXT NOT NULL, status TEXT NOT NULL, query_ref TEXT, PRIMARY KEY (session, client_sub_id))`,
  `CREATE INDEX IF NOT EXISTS client_subs_subscription ON client_subs (subscription)`,
]

export interface SessionRow {
  readonly session: string
  readonly clientId: string
  readonly subject: string
  readonly schema: SchemaSummary
  readonly identicalSchema: boolean
}

const str = (v: SqlValue | undefined): string => (typeof v === "string" ? v : "")
const num = (v: SqlValue | undefined): number => (typeof v === "number" ? v : 0)

export class Sessions {
  constructor(private readonly db: SqlDriver) {
    for (const stmt of SESSION_DDL) db.run(stmt)
    // Old sessions reconnect once to populate original query refs; never infer authorization
    // from a previously normalized AST. No row rewrite is needed for this nullable column.
    if (
      !db
        .query(`SELECT name FROM pragma_table_info('client_subs')`)
        .some((r) => r["name"] === "query_ref")
    )
      db.run(`ALTER TABLE client_subs ADD COLUMN query_ref TEXT`)
  }

  create(row: {
    session: string
    clientId: string
    subject: string
    schema: SchemaSummary
    identicalSchema: boolean
    now: number
  }): void {
    this.db.run(
      `INSERT INTO sessions (session, client_id, subject, schema_json, identical_schema, cursor, connected_at, last_seen_at) VALUES (?, ?, ?, ?, ?, 0, ?, ?) ON CONFLICT(session) DO UPDATE SET client_id = excluded.client_id, subject = excluded.subject, schema_json = excluded.schema_json, identical_schema = excluded.identical_schema, last_seen_at = excluded.last_seen_at`,
      [
        row.session,
        row.clientId,
        row.subject,
        JSON.stringify(row.schema),
        row.identicalSchema ? 1 : 0,
        row.now,
        row.now,
      ],
    )
  }

  get(session: string): SessionRow | null {
    const r = this.db.query(`SELECT * FROM sessions WHERE session = ?`, [session])[0]
    if (r === undefined) return null
    return {
      session,
      clientId: str(r["client_id"]),
      subject: str(r["subject"]),
      schema: decodeSchemaSummary(str(r["schema_json"])),
      identicalSchema: num(r["identical_schema"]) === 1,
    }
  }

  all(): ReadonlyArray<SessionRow> {
    return this.db
      .query(`SELECT session FROM sessions`)
      .map((r) => this.get(str(r["session"])))
      .filter((s): s is SessionRow => s !== null)
  }

  remove(session: string): void {
    this.db.run(`DELETE FROM client_subs WHERE session = ?`, [session])
    this.db.run(`DELETE FROM sessions WHERE session = ?`, [session])
  }

  setClientSub(
    session: string,
    clientSubId: string,
    subscription: string,
    status: "pending" | "live",
    queryRef?: QueryRef,
  ): void {
    this.db.run(
      `INSERT INTO client_subs (session, client_sub_id, subscription, status, query_ref) VALUES (?, ?, ?, ?, ?) ON CONFLICT(session, client_sub_id) DO UPDATE SET subscription = excluded.subscription, status = excluded.status, query_ref = excluded.query_ref WHERE client_subs.subscription IS NOT excluded.subscription OR client_subs.status IS NOT excluded.status OR client_subs.query_ref IS NOT excluded.query_ref`,
      [
        session,
        clientSubId,
        subscription,
        status,
        queryRef === undefined ? null : JSON.stringify(queryRef),
      ],
    )
  }

  markLive(subscription: string): void {
    this.db.run(
      `UPDATE client_subs SET status = 'live' WHERE subscription = ? AND status <> 'live'`,
      [subscription],
    )
  }

  markAllPending(): void {
    this.db.run(`UPDATE client_subs SET status = 'pending' WHERE status <> 'pending'`)
  }

  removeClientSub(session: string, clientSubId: string): string | null {
    const r = this.db.query(
      `SELECT subscription FROM client_subs WHERE session = ? AND client_sub_id = ?`,
      [session, clientSubId],
    )[0]
    if (r === undefined) return null
    this.db.run(`DELETE FROM client_subs WHERE session = ? AND client_sub_id = ?`, [
      session,
      clientSubId,
    ])
    return str(r["subscription"])
  }

  /** Client subscription ids in a session mapped to engine subscription ids. */
  clientSubsOf(session: string): ReadonlyArray<{
    readonly clientSubId: string
    readonly subscription: string
    readonly status: string
    readonly queryRef: QueryRef | null
  }> {
    return this.db
      .query(
        `SELECT client_sub_id, subscription, status, query_ref FROM client_subs WHERE session = ?`,
        [session],
      )
      .map((r) => ({
        clientSubId: str(r["client_sub_id"]),
        subscription: str(r["subscription"]),
        status: str(r["status"]),
        queryRef: typeof r["query_ref"] === "string" ? decodeQueryRef(r["query_ref"]) : null,
      }))
  }

  /** Sessions (with their client ids) that reference an engine subscription. */
  sessionsOf(
    subscription: string,
  ): ReadonlyArray<{ readonly session: string; readonly clientSubId: string }> {
    return this.db
      .query(`SELECT session, client_sub_id FROM client_subs WHERE subscription = ?`, [
        subscription,
      ])
      .map((r) => ({ session: str(r["session"]), clientSubId: str(r["client_sub_id"]) }))
  }

  referenceCount(subscription: string): number {
    return num(
      this.db.query(`SELECT COUNT(*) AS n FROM client_subs WHERE subscription = ?`, [
        subscription,
      ])[0]?.["n"],
    )
  }

  count(): number {
    return num(this.db.query(`SELECT COUNT(*) AS n FROM sessions`)[0]?.["n"])
  }
}
