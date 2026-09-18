/**
 * The Sync Durable Object engine core: relational cache, scopes, demand fills, incremental
 * maintenance of subscriptions, and cursor advancement. Pure synchronous code over a
 * `SqlDriver`; the Durable Object wrapper adds transport, sessions and alarms.
 *
 * Invariants (see `docs/consistency.md`):
 * 1. `applied_seq` advances only inside the transaction that also applied every row change of
 *    the corresponding source transaction and updated every affected subscription.
 * 2. A transaction with `seq <= applied_seq` is a duplicate and is skipped; its gtid must match
 *    the logged gtid for that seq, otherwise the batch is rejected.
 * 3. A scope is `live` only after a fill completed at position P and every held change with a
 *    gtid outside P was applied in sequence order. Changes inside P are skipped because the fill
 *    already reflects them.
 * 4. Membership rows of a subscription equal the result of re-running its query against the
 *    cache at every point where `applied_seq` is observable.
 */

import { Result, Schema } from "effect"
import {
  CdcBatch,
  INTERNAL_PROTOCOL_VERSION,
  Query,
  RowChange,
  type CdcBatchAck,
  type EngineError,
  type FillRequest,
  type FillResult,
  type PartitionTransaction,
  type RejectReason,
  type SyncSchema,
  type TableSchema,
} from "@orbit/protocol"
import type {
  DeltaOrigin,
  MemberRef,
  MembershipChange,
  RowImage,
  RowKey,
  RowUpdate,
  SyncError,
} from "@orbit/protocol/client"
import {
  compileIncludeSelect,
  compileSelect,
  deleteAllSql,
  deleteByKeySql,
  flattenIncludes,
  keyOfRecord,
  parseKey,
  planQuery,
  rowFromRecord,
  rowToParams,
  selectByKeySql,
  upsertSql,
  UnstorableValueError,
  type PlannedQuery,
  type SqlValue,
} from "@orbit/query"
import {
  canonicalJson,
  KEY_COLUMN,
  localTableName,
  quoteIdent,
  createIndexesSql,
  decodeRowSync,
  planMigration,
  SchemaRuntime,
} from "@orbit/schema"

import { listTables, type SqlDriver, type SqlRecord } from "./driver.ts"
import { gtidSetContains, parseGtidSet } from "./gtid.ts"
import { MAX_BOUND_PARAMS, pathOf, SubscriptionMaintainer } from "./maintain.ts"
import { ENGINE_DDL, ENGINE_UPGRADES, SEQ_LOG_RETENTION } from "./tables.ts"

export { MAX_BOUND_PARAMS }

export type ScopeState =
  | { readonly state: "absent" }
  | { readonly state: "filling"; readonly fillId: string; readonly holdFromSeq: number }
  | { readonly state: "live" }

export interface EngineDeps {
  readonly driver: SqlDriver
  readonly schema: SyncSchema
  readonly partition: string
  readonly now: () => number
  readonly newId: () => string
}

/** Something the transport layer must deliver after a core operation committed. */
export type EngineEvent =
  | {
      readonly type: "delta"
      readonly cursor: number
      readonly version?: string
      readonly origin: DeltaOrigin
      readonly rows: ReadonlyArray<RowUpdate>
      readonly memberships: ReadonlyArray<MembershipChange>
    }
  | {
      readonly type: "snapshot"
      readonly subscription: string
      readonly cursor: number
      readonly version?: string
      readonly rows: ReadonlyArray<RowUpdate>
      readonly members: ReadonlyArray<MemberRef>
      /**
       * When set, `rows` and `members` are only those the base subscription does not hold; the
       * full membership is the base's plus `members` (see `SubscribeOptions.basedOn`).
       */
      readonly basedOn?: string
    }
  | { readonly type: "fill_needed"; readonly request: FillRequest }
  | {
      readonly type: "subscription_failed"
      readonly subscription: string
      readonly error: SyncError
    }
  | { readonly type: "scopes_reset"; readonly reason: string }

export interface SubscribeOutcome {
  readonly resumed?: { readonly version: string; readonly cursor: number }
  readonly subscription: string
  readonly status: "pending" | "live"
  /** The normalized query the subscription materializes. */
  readonly query: Query
  readonly events: ReadonlyArray<EngineEvent>
}

export interface SubscribeOptions {
  readonly resume?: { readonly version: string; readonly query: Query }
  /**
   * A live subscription whose result the new one extends: the same query with a larger limit.
   * Its membership seeds the new subscription, so materialization writes only the extra
   * members, and the snapshot carries only those (`basedOn` on the event). When the base is not
   * a subset of the new result, the snapshot is complete and `basedOn` is absent.
   */
  readonly basedOn?: string
}

export interface EngineStatus {
  readonly partition: string
  readonly schemaHash: string
  readonly epoch: number
  readonly appliedSeq: number
  readonly scopes: ReadonlyArray<{
    readonly table: string
    readonly state: string
    readonly rows: number
  }>
  readonly subscriptions: number
  readonly heldChanges: number
}

interface SubscriptionRow {
  readonly id: string
  readonly planned: PlannedQuery
  readonly live: boolean
}

const engineErrorMessage = (e: EngineError): string => {
  switch (e.code) {
    case "unknown_table":
      return `unknown table ${e.table}`
    case "timeout":
      return `timeout after ${e.after_ms} ms`
    default:
      return e.message
  }
}

const asNumber = (v: SqlValue | undefined): number =>
  typeof v === "number" ? v : typeof v === "string" ? Number(v) : 0
/** Table and key joined with NUL, which cannot occur unescaped in JSON text. */
const NUL = String.fromCharCode(0)
const memberRef = (table: string, key: string): string => [table, key].join(NUL)

const asString = (v: SqlValue | undefined): string =>
  typeof v === "string" ? v : v === null || v === undefined ? "" : String(v)

/** JSON columns the engine wrote itself are still decoded through their codecs, never trusted. */
const decodeStoredChange = Schema.decodeUnknownSync(Schema.fromJsonString(RowChange))
const decodeStoredQuery = Schema.decodeUnknownSync(Schema.fromJsonString(Query))
const decodeStoredTables = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Array(Schema.String)),
)

interface ApplyOutcome {
  readonly ack: CdcBatchAck
  readonly events: ReadonlyArray<EngineEvent>
}

const reject = (reason: RejectReason): ApplyOutcome => ({
  ack: { status: "rejected", reason },
  events: [],
})

export class SyncEngine {
  readonly rt: SchemaRuntime
  private readonly plans = new Map<string, PlannedQuery>()
  private readonly codecs: ReadonlyMap<string, (input: unknown) => RowImage>

  constructor(private readonly deps: EngineDeps) {
    this.rt = new SchemaRuntime(deps.schema)
    const codecs = new Map<string, (input: unknown) => RowImage>()
    for (const [name, codec] of this.rt.codecs) codecs.set(name, decodeRowSync(codec))
    this.codecs = codecs
  }

  private get db(): SqlDriver {
    return this.deps.driver
  }

  // ---------------------------------------------------------------------------------------------
  // Initialization and migrations
  // ---------------------------------------------------------------------------------------------

  /** Creates or migrates every table. Safe to call on every Durable Object construction. */
  init(): ReadonlyArray<EngineEvent> {
    return this.db.transaction(() => {
      for (const stmt of ENGINE_DDL) this.db.run(stmt)
      const events: Array<EngineEvent> = []
      const existing = listTables(this.db)
      let membershipRebuilt = false
      for (const upgrade of ENGINE_UPGRADES) {
        const table = existing.find((t) => t.name === upgrade.table)
        if (table === undefined || table.columns.includes(upgrade.column)) continue
        for (const stmt of upgrade.statements) this.db.run(stmt)
        if (upgrade.table === "membership") membershipRebuilt = true
      }
      // After upgrades: older caches did not have orphaned_at. Active-view scans must not
      // visit the retained history of abandoned windows.
      this.db.run(
        `CREATE INDEX IF NOT EXISTS subscriptions_active ON subscriptions (orphaned_at, live)`,
      )
      const storedHash = this.meta("schema_hash")
      const storedPartition = this.meta("partition")
      if (storedPartition !== null && storedPartition !== this.deps.partition) {
        throw new Error(
          `durable object is bound to partition ${storedPartition}, not ${this.deps.partition}`,
        )
      }
      const plan = planMigration(
        this.deps.schema,
        existing.filter((t) => t.name.startsWith("t_")),
        storedHash,
      )
      if (plan.action !== "none") {
        for (const stmt of plan.statements) this.db.run(stmt)
      }
      // Relation indexes are `IF NOT EXISTS`; older caches gain the ones they lack.
      for (const stmt of createIndexesSql(this.deps.schema)) this.db.run(stmt)
      if (storedHash !== null && storedHash !== this.deps.schema.schema_hash) {
        // Cached rows were projected with the old schema (new columns would be missing).
        // Every scope is dropped and refilled on demand; subscriptions stay registered.
        this.resetScopes(`schema changed from ${storedHash} to ${this.deps.schema.schema_hash}`)
        events.push({ type: "scopes_reset", reason: "schema_changed" })
      } else if (membershipRebuilt) {
        // The membership table changed shape: live subscriptions are re-materialized from the
        // cache, which is still valid.
        for (const row of this.db.query(
          `SELECT id, query, tables, live FROM subscriptions WHERE live = 1 AND orphaned_at IS NULL`,
        )) {
          const sub = this.loadSubscription(row)
          if (sub !== null) events.push(this.materialize(sub))
        }
      }
      this.setMeta("schema_hash", this.deps.schema.schema_hash)
      this.setMeta("partition", this.deps.partition)
      if (this.meta("cache_identity") === null) this.setMeta("cache_identity", this.deps.newId())
      if (this.meta("epoch") === null) this.setMeta("epoch", "0")
      if (this.meta("applied_seq") === null) this.setMeta("applied_seq", "0")
      return events
    })
  }

  private meta(key: string): string | null {
    const rows = this.db.query(`SELECT value FROM meta WHERE key = ?`, [key])
    const v = rows[0]?.["value"]
    return typeof v === "string" ? v : null
  }

  private setMeta(key: string, value: string): void {
    this.db.run(
      `INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
      [key, value],
    )
  }

  /** Persisted cache incarnation plus source position. Fills invalidate even at the same cursor. */
  resumeVersion(cursor = this.appliedSeq): string {
    return JSON.stringify([
      this.deps.schema.schema_hash,
      this.deps.partition,
      this.meta("cache_identity"),
      this.epoch,
      cursor,
    ])
  }

  private invalidateResume(): void {
    this.setMeta("cache_identity", this.deps.newId())
  }

  get appliedSeq(): number {
    return Number(this.meta("applied_seq") ?? "0")
  }

  get epoch(): number {
    return Number(this.meta("epoch") ?? "0")
  }

  status(): EngineStatus {
    const scopes = this.db.query(`SELECT tbl, state FROM scopes`).map((s) => {
      const table = this.rt.table(asString(s["tbl"]))
      const rows =
        table === undefined
          ? 0
          : asNumber(this.db.query(`SELECT COUNT(*) AS n FROM "t_${table.name}"`)[0]?.["n"])
      return { table: asString(s["tbl"]), state: asString(s["state"]), rows }
    })
    return {
      partition: this.deps.partition,
      schemaHash: this.deps.schema.schema_hash,
      epoch: this.epoch,
      appliedSeq: this.appliedSeq,
      scopes,
      subscriptions: asNumber(this.db.query(`SELECT COUNT(*) AS n FROM subscriptions`)[0]?.["n"]),
      heldChanges: asNumber(this.db.query(`SELECT COUNT(*) AS n FROM held`)[0]?.["n"]),
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Scopes
  // ---------------------------------------------------------------------------------------------

  scope(table: string): ScopeState {
    const row = this.db.query(`SELECT state, fill_id, hold_from_seq FROM scopes WHERE tbl = ?`, [
      table,
    ])[0]
    if (row === undefined) return { state: "absent" }
    if (row["state"] === "live") return { state: "live" }
    return {
      state: "filling",
      fillId: asString(row["fill_id"]),
      holdFromSeq: asNumber(row["hold_from_seq"]),
    }
  }

  /** Drops every cached scope, held change and membership. Subscriptions become pending. */
  private resetScopes(reason: string): void {
    this.invalidateResume()
    for (const t of this.deps.schema.tables) this.db.run(deleteAllSql(t))
    this.db.run(`DELETE FROM scopes`)
    this.db.run(`DELETE FROM held`)
    this.db.run(`DELETE FROM membership`)
    this.db.run(`DELETE FROM fills`)
    this.db.run(`UPDATE subscriptions SET live = 0`)
    this.setMeta("last_reset_reason", reason)
  }

  /** Starts a fill for every absent scope among `tables`. Returns the fill requests to issue. */
  ensureScopes(tables: Iterable<string>): {
    readonly pending: ReadonlyArray<string>
    readonly requests: ReadonlyArray<FillRequest>
  } {
    const pending: Array<string> = []
    const requests: Array<FillRequest> = []
    for (const table of tables) {
      const state = this.scope(table)
      if (state.state === "live") continue
      pending.push(table)
      if (state.state === "filling") continue
      const fillId = `${this.deps.partition}:${this.deps.newId()}`
      const now = this.deps.now()
      this.db.run(
        `INSERT INTO scopes (tbl, state, fill_id, hold_from_seq, updated_at) VALUES (?, 'filling', ?, ?, ?)`,
        [table, fillId, this.appliedSeq, now],
      )
      this.db.run(`INSERT INTO fills (fill_id, tbl, requested_at) VALUES (?, ?, ?)`, [
        fillId,
        table,
        now,
      ])
      requests.push({
        fill_id: fillId,
        schema_hash: this.deps.schema.schema_hash,
        partition: this.deps.partition,
        table,
        requested_at_ms: now,
      })
    }
    return { pending, requests }
  }

  /** Fill requests that have not completed, for retry after restarts. */
  outstandingFills(): ReadonlyArray<FillRequest> {
    return this.db.query(`SELECT fill_id, tbl, requested_at FROM fills`).map((r) => ({
      fill_id: asString(r["fill_id"]),
      schema_hash: this.deps.schema.schema_hash,
      partition: this.deps.partition,
      table: asString(r["tbl"]),
      requested_at_ms: asNumber(r["requested_at"]),
    }))
  }

  /** Re-issues a fill for a scope whose fill never completed. Returns the new request. */
  retryFill(fillId: string): FillRequest | null {
    return this.db.transaction(() => {
      const row = this.db.query(`SELECT tbl, attempts FROM fills WHERE fill_id = ?`, [fillId])[0]
      if (row === undefined) return null
      const table = asString(row["tbl"])
      const next = `${this.deps.partition}:${this.deps.newId()}`
      const now = this.deps.now()
      this.db.run(`DELETE FROM fills WHERE fill_id = ?`, [fillId])
      this.db.run(`INSERT INTO fills (fill_id, tbl, requested_at, attempts) VALUES (?, ?, ?, ?)`, [
        next,
        table,
        now,
        asNumber(row["attempts"]) + 1,
      ])
      this.invalidateResume()
      // Rows uploaded by the abandoned fill are discarded: the new fill is a fresh snapshot.
      const t = this.rt.table(table)
      if (t !== undefined) this.db.run(deleteAllSql(t))
      this.db.run(
        `UPDATE scopes SET fill_id = ?, hold_from_seq = ?, rows_filled = 0, updated_at = ? WHERE tbl = ?`,
        [next, this.appliedSeq, now, table],
      )
      // Held changes since the original hold are kept: they are still needed after the new fill.
      return {
        fill_id: next,
        schema_hash: this.deps.schema.schema_hash,
        partition: this.deps.partition,
        table,
        requested_at_ms: now,
      }
    })
  }

  /** Stores a chunk of fill rows. Rows are validated against the sync schema. */
  applyFillRows(fillId: string, rows: ReadonlyArray<RowImage>): Result.Result<number, EngineError> {
    return this.db.transaction(() => {
      const scope = this.db.query(
        `SELECT tbl FROM scopes WHERE fill_id = ? AND state = 'filling'`,
        [fillId],
      )[0]
      if (scope === undefined)
        return Result.fail<EngineError>({
          code: "internal",
          message: `fill ${fillId} is not active`,
        })
      const table = this.rt.table(asString(scope["tbl"]))
      if (table === undefined)
        return Result.fail<EngineError>({ code: "unknown_table", table: asString(scope["tbl"]) })
      const decode = this.codecs.get(table.name)
      if (decode === undefined)
        return Result.fail<EngineError>({ code: "unknown_table", table: table.name })
      this.invalidateResume()
      const sql = upsertSql(table)
      let n = 0
      for (const raw of rows) {
        const row = this.decodeRow(decode, raw, table)
        if (Result.isFailure(row)) return Result.fail(row.failure)
        this.db.run(sql, rowToParams(table, row.success))
        n += 1
      }
      this.db.run(`UPDATE scopes SET rows_filled = rows_filled + ? WHERE fill_id = ?`, [n, fillId])
      return Result.succeed(n)
    })
  }

  private decodeRow(
    decode: (input: unknown) => RowImage,
    raw: unknown,
    table: TableSchema,
  ): Result.Result<RowImage, EngineError> {
    try {
      return Result.succeed(decode(raw))
    } catch (e) {
      return Result.fail<EngineError>({
        code: "normalization",
        table: table.name,
        message: e instanceof Error ? e.message : String(e),
      })
    }
  }

  /**
   * Finishes a fill. On success the scope becomes live: held changes outside the fill position
   * are applied in order, then every pending subscription on now-live tables is materialized.
   */
  completeFill(fillId: string, result: FillResult): ReadonlyArray<EngineEvent> {
    return this.db.transaction(() => {
      const scope = this.db.query(
        `SELECT tbl, hold_from_seq FROM scopes WHERE fill_id = ? AND state = 'filling'`,
        [fillId],
      )[0]
      if (scope === undefined) return []
      this.invalidateResume()
      const tableName = asString(scope["tbl"])
      const table = this.rt.table(tableName)
      if (table === undefined) return []
      if (result.status === "failed") {
        // The scope goes back to absent so the next subscription retries; pending subscriptions
        // on this table are told, and callers may retry with backoff.
        this.db.run(deleteAllSql(table))
        this.db.run(`DELETE FROM scopes WHERE tbl = ?`, [tableName])
        this.db.run(`DELETE FROM held WHERE tbl = ?`, [tableName])
        this.db.run(`DELETE FROM fills WHERE fill_id = ?`, [fillId])
        const failed: Array<EngineEvent> = []
        for (const sub of this.subscriptionsOn(tableName)) {
          failed.push({
            type: "subscription_failed",
            subscription: sub.id,
            error: {
              code: "fill_failed",
              message: `fill of ${tableName} failed`,
              cause: result.error,
            },
          })
        }
        return failed
      }
      const position = parseGtidSet(result.position)
      const held = this.db.query(
        `SELECT seq, ord, gtid, change FROM held WHERE tbl = ? ORDER BY seq, ord`,
        [tableName],
      )
      let applied = 0
      let skipped = 0
      const decode = this.codecs.get(tableName)
      for (const h of held) {
        if (gtidSetContains(position, asString(h["gtid"]))) {
          skipped += 1
          continue
        }
        const change = decodeStoredChange(asString(h["change"]))
        const r = this.applyChangeToCache(table, change, decode)
        if (Result.isFailure(r))
          throw new Error(`held change failed validation: ${JSON.stringify(r.failure)}`)
        applied += 1
      }
      this.db.run(`DELETE FROM held WHERE tbl = ?`, [tableName])
      this.db.run(
        `UPDATE scopes SET state = 'live', fill_position = ?, fill_id = NULL, updated_at = ? WHERE tbl = ?`,
        [result.position, this.deps.now(), tableName],
      )
      this.db.run(`DELETE FROM fills WHERE fill_id = ?`, [fillId])
      this.setMeta(
        `fill_stats:${tableName}`,
        JSON.stringify({
          rows: result.row_count,
          heldApplied: applied,
          heldSkipped: skipped,
          durationMs: result.duration_ms,
        }),
      )
      // Materialize subscriptions whose tables are now all live.
      const events: Array<EngineEvent> = []
      for (const sub of this.subscriptionsOn(tableName)) {
        if (sub.live) continue
        if ([...sub.planned.tables].every((t) => this.scope(t).state === "live")) {
          events.push(this.materialize(sub))
        }
      }
      return events
    })
  }

  // ---------------------------------------------------------------------------------------------
  // CDC application
  // ---------------------------------------------------------------------------------------------

  applyBatch(input: unknown): ApplyOutcome {
    const decoded = Schema.decodeUnknownResult(CdcBatch)(input)
    if (Result.isFailure(decoded)) {
      return reject({ kind: "internal", code: "invalid_batch", message: String(decoded.failure) })
    }
    const batch = decoded.success
    if (batch.protocol_version !== INTERNAL_PROTOCOL_VERSION)
      return reject({
        kind: "protocol_version_mismatch",
        expected: INTERNAL_PROTOCOL_VERSION,
        got: batch.protocol_version,
      })
    if (batch.schema_hash !== this.deps.schema.schema_hash)
      return reject({
        kind: "schema_mismatch",
        do_schema_hash: this.deps.schema.schema_hash,
        got: batch.schema_hash,
      })
    if (batch.partition !== this.deps.partition)
      return reject({
        kind: "wrong_partition",
        do_partition: this.deps.partition,
        got: batch.partition,
      })
    const started = this.deps.now()
    try {
      return this.db.transaction(() => {
        const events: Array<EngineEvent> = []
        const epoch = this.epoch
        if (batch.stream_epoch < epoch)
          return reject({ kind: "stale_epoch", do_epoch: epoch, got: batch.stream_epoch })
        if (batch.stream_epoch > epoch) {
          // History may have been skipped: nothing cached can be trusted.
          this.resetScopes(`stream epoch ${epoch} -> ${batch.stream_epoch}`)
          this.setMeta("epoch", String(batch.stream_epoch))
          this.setMeta("applied_seq", "0")
          this.db.run(`DELETE FROM seq_log`)
          events.push({ type: "scopes_reset", reason: "epoch_changed" })
        }
        let applied = this.appliedSeq
        let duplicates = 0
        for (const txn of batch.transactions) {
          if (applied !== 0 && txn.seq <= applied) {
            const logged = this.db.query(`SELECT gtid FROM seq_log WHERE seq = ?`, [txn.seq])[0]
            if (logged !== undefined && logged["gtid"] !== txn.gtid) {
              return reject({
                kind: "sequence_conflict",
                seq: txn.seq,
                applied_gtid: asString(logged["gtid"]),
                got_gtid: txn.gtid,
              })
            }
            duplicates += 1
            continue
          }
          if (applied !== 0 && txn.seq !== applied + 1)
            return reject({ kind: "sequence_gap", applied_seq: applied, first_seq: txn.seq })
          const r = this.applyTransaction(txn)
          if (Result.isFailure(r)) return reject(r.failure)
          applied = txn.seq
          this.setMeta("applied_seq", String(applied))
          this.db.run(`INSERT INTO seq_log (seq, gtid, applied_at) VALUES (?, ?, ?)`, [
            txn.seq,
            txn.gtid,
            this.deps.now(),
          ])
          events.push(r.success)
        }
        this.db.run(`DELETE FROM seq_log WHERE seq < ?`, [applied - SEQ_LOG_RETENTION])
        return {
          ack: {
            status: "applied",
            applied_seq: applied,
            duplicates,
            apply_ms: Math.max(0, this.deps.now() - started),
          },
          events,
        }
      })
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e)
      return reject({
        kind: "internal",
        code: e instanceof UnstorableValueError ? "unstorable_value" : "apply_failed",
        message,
      })
    }
  }

  /** Applies one source transaction: cache, held queue, and subscription maintenance. */
  private applyTransaction(txn: PartitionTransaction): Result.Result<EngineEvent, RejectReason> {
    const touched = new Map<string, Array<RowChange>>()
    for (let ord = 0; ord < txn.changes.length; ord++) {
      const change = txn.changes[ord]
      if (change === undefined) continue
      const table = this.rt.table(change.table)
      if (table === undefined)
        return Result.fail<RejectReason>({
          kind: "invalid_row",
          table: change.table,
          seq: txn.seq,
          message: "table is not in the sync schema",
        })
      const scope = this.scope(change.table)
      if (scope.state === "absent") continue
      if (scope.state === "filling") {
        this.db.run(`INSERT INTO held (seq, ord, tbl, gtid, change) VALUES (?, ?, ?, ?, ?)`, [
          txn.seq,
          ord,
          change.table,
          txn.gtid,
          JSON.stringify(change),
        ])
        continue
      }
      const r = this.applyChangeToCache(table, change, this.codecs.get(table.name))
      if (Result.isFailure(r))
        return Result.fail<RejectReason>({
          kind: "invalid_row",
          table: change.table,
          seq: txn.seq,
          message: engineErrorMessage(r.failure),
        })
      const list = touched.get(change.table) ?? []
      list.push(change)
      touched.set(change.table, list)
    }
    const { rows, memberships } = this.maintainSubscriptions(touched)
    const origin: DeltaOrigin = {
      gtid: txn.gtid,
      commitTimestamp: txn.commit_timestamp,
      seq: txn.seq,
      trace: txn.trace,
      appliedAt: this.deps.now(),
    }
    return Result.succeed({
      type: "delta",
      cursor: txn.seq,
      version: this.resumeVersion(txn.seq),
      origin,
      rows,
      memberships,
    })
  }

  private applyChangeToCache(
    table: TableSchema,
    change: RowChange,
    decode: ((input: unknown) => RowImage) | undefined,
  ): Result.Result<void, EngineError> {
    const key = SchemaRuntime.keyString(change.key)
    if (change.op === "delete") {
      this.db.run(deleteByKeySql(table), [key])
      return Result.succeed(undefined)
    }
    if (change.after === undefined || change.after === null)
      return Result.fail<EngineError>({
        code: "normalization",
        table: table.name,
        message: `${change.op} without an after image`,
      })
    if (decode === undefined)
      return Result.fail<EngineError>({ code: "unknown_table", table: table.name })
    const row = this.decodeRow(decode, change.after, table)
    if (Result.isFailure(row)) return Result.fail(row.failure)
    if (change.op === "update") {
      // The key may change on update (primary key update): remove the old identity first.
      const oldKey =
        change.before === undefined || change.before === null
          ? key
          : SchemaRuntime.keyString(this.rt.keyOf(table, change.before))
      if (oldKey !== key) this.db.run(deleteByKeySql(table), [oldKey])
    }
    this.db.run(upsertSql(table), rowToParams(table, row.success))
    return Result.succeed(undefined)
  }

  // ---------------------------------------------------------------------------------------------
  // Subscriptions and incremental maintenance
  // ---------------------------------------------------------------------------------------------

  private loadSubscription(row: SqlRecord): SubscriptionRow | null {
    const id = asString(row["id"])
    let planned = this.plans.get(id)
    if (planned === undefined) {
      const query = decodeStoredQuery(asString(row["query"]))
      const r = planQuery(this.rt, query)
      if (Result.isFailure(r)) return null
      planned = r.success
      this.plans.set(id, planned)
    }
    return { id, planned, live: asNumber(row["live"]) === 1 }
  }

  private subscriptionsOn(table: string): ReadonlyArray<SubscriptionRow> {
    const out: Array<SubscriptionRow> = []
    for (const row of this.db.query(
      `SELECT id, query, tables, live FROM subscriptions WHERE orphaned_at IS NULL`,
    )) {
      const tables = decodeStoredTables(asString(row["tables"]))
      if (!tables.includes(table)) continue
      const sub = this.loadSubscription(row)
      if (sub !== null) out.push(sub)
    }
    return out
  }

  subscription(id: string): SubscriptionRow | null {
    const row = this.db.query(
      `SELECT id, query, tables, CASE WHEN orphaned_at IS NULL THEN live ELSE 0 END AS live FROM subscriptions WHERE id = ?`,
      [id],
    )[0]
    return row === undefined ? null : this.loadSubscription(row)
  }

  /**
   * Registers a query. The subscription id is the query's canonical key, so identical queries
   * from different clients share one materialization.
   */
  subscribe(
    query: Query,
    options: SubscribeOptions = {},
  ): Result.Result<SubscribeOutcome, SyncError> {
    const planned = planQuery(this.rt, query)
    if (Result.isFailure(planned)) {
      return Result.fail<SyncError>({
        code: "unsupported_query",
        message: planned.failure.message,
        details: { problem: planned.failure.problem },
      })
    }
    const p = planned.success
    return Result.succeed(
      this.db.transaction(() => {
        const events: Array<EngineEvent> = []
        this.plans.set(p.key, p)
        const existing = this.subscription(p.key)
        const retainedVersion = existing === null ? null : this.meta(`orphan_version:${p.key}`)
        const base =
          options.basedOn === undefined || options.basedOn === p.key
            ? null
            : this.subscription(options.basedOn)
        const seed = base !== null && base.live
        if (existing === null) {
          this.db.run(
            `INSERT INTO subscriptions (id, query, tables, live, created_at) VALUES (?, ?, ?, 0, ?)`,
            [p.key, JSON.stringify(p.query), JSON.stringify([...p.tables]), this.deps.now()],
          )
          // The base's members are the new subscription's first guess: materialization then
          // writes only the members that differ instead of the whole window again.
          if (seed)
            this.db.run(
              `INSERT OR IGNORE INTO membership (subscription, path, tbl, key) SELECT ?, path, tbl, key FROM membership WHERE subscription = ?`,
              [p.key, base.id],
            )
        } else {
          this.db.run(`UPDATE subscriptions SET orphaned_at = NULL WHERE id = ?`, [p.key])
          this.db.run(`DELETE FROM meta WHERE key = ?`, [`orphan_version:${p.key}`])
        }
        const { pending, requests } = this.ensureScopes(p.tables)
        for (const request of requests) events.push({ type: "fill_needed", request })
        if (pending.length > 0)
          return { subscription: p.key, status: "pending", query: p.query, events }
        const resume = options.resume
        const unchanged =
          resume !== undefined &&
          resume.version === this.resumeVersion() &&
          canonicalJson(resume.query) === canonicalJson(p.query)
        // A held, live materialization needs neither row reads nor membership serialization.
        if (unchanged && (existing?.live === true || retainedVersion === resume.version)) {
          this.db.run(`UPDATE subscriptions SET live = 1 WHERE id = ?`, [p.key])
          return {
            subscription: p.key,
            status: "live",
            query: p.query,
            events,
            resumed: { version: resume.version, cursor: this.appliedSeq },
          }
        }
        // The base's members need no row images: the client has them.
        const grows =
          existing === null &&
          base !== null &&
          base.live &&
          base.planned.limit !== undefined &&
          p.limit !== undefined &&
          p.limit > base.planned.limit &&
          JSON.stringify({ ...base.planned.query, limit: undefined }) ===
            JSON.stringify({ ...p.query, limit: undefined })
        const held = !grows && seed && base !== null ? this.heldBy(base.id) : undefined
        const snapshot = grows
          ? this.materializeGrowth({ id: p.key, planned: p, live: false }, base)
          : existing?.live !== true
            ? this.materialize({ id: p.key, planned: p, live: false }, held)
            : this.snapshot(p.key, held)
        // An orphan may need its server membership rebuilt, but a validated client already has
        // the identical rows. Keep future CDC delivery correct without retransmitting that view.
        if (unchanged)
          return {
            subscription: p.key,
            status: "live",
            query: p.query,
            events,
            resumed: { version: resume.version, cursor: this.appliedSeq },
          }
        events.push(
          held === undefined || base === null ? snapshot : this.extend(snapshot, base.id, held),
        )
        return { subscription: p.key, status: "live", query: p.query, events }
      }),
    )
  }

  /**
   * Reduces a snapshot to the members the base subscription does not hold. When the base is not
   * a subset of the result (a different query, or a result that shrank), the snapshot stays
   * complete: the client then replaces its membership as usual.
   */
  private extend(event: EngineEvent, base: string, held: ReadonlySet<string>): EngineEvent {
    if (event.type !== "snapshot") return event
    const wanted = new Set(
      event.members.map((m) => memberRef(m.table, SchemaRuntime.keyString(m.key))),
    )
    // Without the subset property the reduced rows are incomplete: rebuild the full snapshot.
    for (const ref of held) if (!wanted.has(ref)) return this.snapshot(event.subscription)
    const fresh = (table: string, key: RowKey) =>
      !held.has(memberRef(table, SchemaRuntime.keyString(key)))
    return {
      ...event,
      rows: event.rows.filter((r) => fresh(r.table, r.key)),
      members: event.members.filter((m) => fresh(m.table, m.key)),
      basedOn: base,
    }
  }

  /** The member references a subscription holds, as `memberRef` strings. */
  private heldBy(id: string): ReadonlySet<string> {
    const held = new Set<string>()
    for (const row of this.db.query(
      `SELECT DISTINCT tbl, key FROM membership WHERE subscription = ?`,
      [id],
    ))
      held.add(memberRef(asString(row["tbl"]), asString(row["key"])))
    return held
  }

  unsubscribe(id: string): void {
    this.db.transaction(() => {
      this.db.run(`DELETE FROM membership WHERE subscription = ?`, [id])
      this.db.run(`DELETE FROM subscriptions WHERE id = ?`, [id])
      this.db.run(`DELETE FROM meta WHERE key = ?`, [`orphan_version:${id}`])
      this.plans.delete(id)
    })
  }

  /**
   * No session holds the subscription any more. Keep its membership during the grace period,
   * but stop maintaining it immediately: window/filter churn must not multiply CDC work.
   * A returning subscriber reconciles the retained membership against the current cache.
   * `sweepOrphans` drops it once the grace period has passed.
   */
  markOrphaned(id: string, now: number): void {
    this.db.transaction(() => {
      // A server-owned proof, not just the client's claim: this membership was
      // complete at exactly this cache version before maintenance was suspended.
      if (this.subscription(id)?.live === true)
        this.setMeta(`orphan_version:${id}`, this.resumeVersion())
      this.db.run(
        `UPDATE subscriptions SET orphaned_at = ?, live = 0 WHERE id = ? AND orphaned_at IS NULL`,
        [now, id],
      )
    })
  }

  /** Drops subscriptions orphaned at or before `before`. Returns the dropped ids. */
  sweepOrphans(before: number): ReadonlyArray<string> {
    return this.db.transaction(() => {
      const ids = this.db
        .query(`SELECT id FROM subscriptions WHERE orphaned_at IS NOT NULL AND orphaned_at <= ?`, [
          before,
        ])
        .map((r) => asString(r["id"]))
      for (const id of ids) this.unsubscribe(id)
      return ids
    })
  }

  /** The earliest time an orphaned subscription becomes due, or null when none is orphaned. */
  nextOrphanDue(graceMs: number): number | null {
    const v = this.db.query(`SELECT MIN(orphaned_at) AS m FROM subscriptions`)[0]?.["m"]
    return typeof v === "number" ? v + graceMs : null
  }

  /** Current members of a subscription, computed from the cache. */
  private evaluate(
    planned: PlannedQuery,
    skip?: ReadonlySet<string>,
    offset = 0,
  ): {
    readonly members: Array<{
      readonly path: string
      readonly table: string
      readonly key: string
      readonly record: SqlRecord
    }>
  } {
    const members: Array<{ path: string; table: string; key: string; record: SqlRecord }> = []
    const options = { keysOnly: skip !== undefined }
    const primary = compileSelect(
      offset === 0
        ? planned
        : {
            ...planned,
            limit: Math.max(0, (planned.limit ?? 0) - offset),
          },
      options,
    )
    for (const record of this.db.query(
      primary.sql + (offset > 0 ? ` OFFSET ${offset}` : ""),
      primary.params,
    ))
      members.push({ path: "", table: planned.table.name, key: keyOfRecord(record), record })
    const keysByPath = new Map<string, Array<string>>([["", members.map((m) => m.key)]])
    for (const include of flattenIncludes(planned)) {
      const path = pathOf(include.path)
      const parents = keysByPath.get(pathOf(include.path.slice(0, -1))) ?? []
      const inc = compileIncludeSelect(planned, include, options, parents)
      const keys: Array<string> = []
      for (const record of parents.length === 0 ? [] : this.db.query(inc.sql, inc.params)) {
        members.push({ path, table: include.target.name, key: keyOfRecord(record), record })
        keys.push(keyOfRecord(record))
      }
      keysByPath.set(path, keys)
    }
    if (skip !== undefined) {
      // Window growth needs all membership keys, but the client already has the
      // base's potentially large row images. Hydrate only rows the snapshot sends.
      const needed = new Map<string, Set<string>>()
      for (const m of members) {
        if (skip.has(memberRef(m.table, m.key))) continue
        const keys = needed.get(m.table) ?? new Set<string>()
        keys.add(m.key)
        needed.set(m.table, keys)
      }
      const records = new Map<string, SqlRecord>()
      for (const [table, keys] of needed) {
        for (const record of this.db.query(
          `SELECT * FROM ${quoteIdent(localTableName(table))} WHERE ${quoteIdent(KEY_COLUMN)} IN (SELECT value FROM json_each(?))`,
          [JSON.stringify([...keys])],
        ))
          records.set(memberRef(table, keyOfRecord(record)), record)
      }
      for (const m of members) {
        if (skip.has(memberRef(m.table, m.key))) continue
        const record = records.get(memberRef(m.table, m.key))
        if (record === undefined) throw new Error("snapshot lost a selected row")
        m.record = record
      }
    }
    return { members }
  }

  /** A live identical smaller limit is a current prefix, including its complete include tree. */
  private materializeGrowth(sub: SubscriptionRow, base: SubscriptionRow): EngineEvent {
    const offset = Number(
      this.db.query(`SELECT COUNT(*) AS n FROM membership WHERE subscription = ? AND path = ''`, [
        base.id,
      ])[0]?.["n"] ?? 0,
    )
    // Membership was seeded in SQL. Only evaluate roots beyond that prefix and their
    // includes; walking the entire old include graph on each page is quadratic work.
    // Sort and discard the prefix as keys, so SQLite does not copy large row
    // images into its ordering buffer before applying OFFSET.
    const { members } = this.evaluate(sub.planned, new Set(), offset)
    const candidates = [
      ...new Map(members.map((m) => [memberRef(m.table, m.key), [m.table, m.key]])).values(),
    ]
    const held = new Set<string>()
    if (candidates.length > 0) {
      for (const row of this.db.query(
        `SELECT DISTINCT m.tbl, m.key FROM json_each(?) r JOIN membership m ON m.tbl = json_extract(r.value, '$[0]') AND m.key = json_extract(r.value, '$[1]') WHERE m.subscription = ?`,
        [JSON.stringify(candidates), base.id],
      ))
        held.add(memberRef(asString(row["tbl"]), asString(row["key"])))
    }
    for (const m of members)
      this.db.run(
        `INSERT OR IGNORE INTO membership (subscription, path, tbl, key) VALUES (?, ?, ?, ?)`,
        [sub.id, m.path, m.table, m.key],
      )
    this.db.run(`UPDATE subscriptions SET live = 1 WHERE id = ?`, [sub.id])
    const snapshot = this.snapshotFrom(sub.id, members, held)
    return {
      ...snapshot,
      basedOn: base.id,
      members: snapshot.members.filter(
        (m) => !held.has(memberRef(m.table, SchemaRuntime.keyString(m.key))),
      ),
    }
  }

  /**
   * Full (re)materialization: membership becomes the evaluated result; emits a snapshot. Rows
   * already present are left alone so a re-materialization of an unchanged result writes
   * nothing but the rows that differ.
   */
  private materialize(sub: SubscriptionRow, skip?: ReadonlySet<string>): EngineEvent {
    const { members } = this.evaluate(sub.planned, skip)
    const wanted = new Set(members.map((m) => [m.path, m.table, m.key].join(" ")))
    const present = new Set<string>()
    for (const row of this.db.query(
      `SELECT path, tbl, key FROM membership WHERE subscription = ?`,
      [sub.id],
    )) {
      const path = asString(row["path"])
      const tbl = asString(row["tbl"])
      const key = asString(row["key"])
      const id = [path, tbl, key].join(" ")
      if (wanted.has(id)) {
        present.add(id)
        continue
      }
      this.db.run(
        `DELETE FROM membership WHERE subscription = ? AND path = ? AND tbl = ? AND key = ?`,
        [sub.id, path, tbl, key],
      )
    }
    for (const m of members) {
      if (present.has([m.path, m.table, m.key].join(" "))) continue
      this.db.run(
        `INSERT OR IGNORE INTO membership (subscription, path, tbl, key) VALUES (?, ?, ?, ?)`,
        [sub.id, m.path, m.table, m.key],
      )
    }
    this.db.run(`UPDATE subscriptions SET live = 1 WHERE id = ?`, [sub.id])
    return this.snapshotFrom(sub.id, members, skip)
  }

  /**
   * Snapshot of a live subscription at the current cursor. Members in `skip` are listed without
   * a row image (a base subscription the client already holds).
   */
  snapshot(id: string, skip?: ReadonlySet<string>): EngineEvent {
    const sub = this.subscription(id)
    if (sub === null)
      return {
        type: "subscription_failed",
        subscription: id,
        error: { code: "internal", message: "unknown subscription" },
      }
    if (!sub.live)
      return {
        type: "subscription_failed",
        subscription: id,
        error: { code: "internal", message: "subscription is pending" },
      }
    const { members } = this.evaluate(sub.planned, skip)
    return this.snapshotFrom(id, members, skip)
  }

  private snapshotFrom(
    id: string,
    members: ReadonlyArray<{
      readonly table: string
      readonly key: string
      readonly record: SqlRecord
    }>,
    skip?: ReadonlySet<string>,
  ): Extract<EngineEvent, { readonly type: "snapshot" }> {
    // A row can be reached as a primary row and through an include (self relations); membership
    // is a set, so it is listed once.
    const rows: Array<RowUpdate> = []
    const refs: Array<MemberRef> = []
    const seen = new Set<string>()
    for (const m of members) {
      const table = this.rt.table(m.table)
      if (table === undefined) continue
      const ref = memberRef(m.table, m.key)
      if (seen.has(ref)) continue
      seen.add(ref)
      refs.push({ table: m.table, key: parseKey(m.key) })
      if (skip?.has(ref)) continue
      rows.push({ table: m.table, key: parseKey(m.key), row: rowFromRecord(table, m.record) })
    }
    return {
      type: "snapshot",
      subscription: id,
      cursor: this.appliedSeq,
      version: this.resumeVersion(),
      rows,
      members: refs,
    }
  }

  /**
   * Incremental maintenance after a transaction touched `touched` tables: every live
   * subscription whose tables intersect is brought up to date by `SubscriptionMaintainer`,
   * whose work depends on the touched rows and not on the size of the cache (see `maintain.ts`
   * and `docs/ivm.md`).
   */
  private maintainSubscriptions(touched: ReadonlyMap<string, ReadonlyArray<RowChange>>): {
    readonly rows: ReadonlyArray<RowUpdate>
    readonly memberships: ReadonlyArray<MembershipChange>
  } {
    const memberships: Array<MembershipChange> = []
    const rowUpdates = new Map<string, RowUpdate>()
    if (touched.size === 0) return { rows: [], memberships }
    const subs = this.db.query(
      `SELECT id, query, tables, live FROM subscriptions WHERE live = 1 AND orphaned_at IS NULL`,
    )
    for (const row of subs) {
      const sub = this.loadSubscription(row)
      if (sub === null) continue
      if (![...touched.keys()].some((table) => sub.planned.tables.has(table))) continue
      const outcome = new SubscriptionMaintainer(this.db, this.rt, sub.id, sub.planned).apply(
        touched,
      )
      for (const m of outcome.added) {
        const table = this.rt.table(m.table)
        if (table === undefined) continue
        const key = SchemaRuntime.keyString(m.key)
        const record = this.db.query(selectByKeySql(table), [key])[0]
        if (record !== undefined)
          rowUpdates.set(memberRef(m.table, key), {
            table: m.table,
            key: m.key,
            row: rowFromRecord(table, record),
          })
      }
      if (outcome.added.length > 0 || outcome.removed.length > 0)
        memberships.push({
          subscriptionId: sub.id,
          added: outcome.added,
          removed: outcome.removed,
        })
    }
    // Rows that changed and are members of any subscription travel with their new image; deleted
    // rows travel as null so clients drop them regardless of membership. Changes are folded in
    // transaction order per key: a delete followed by an insert of the same key travels as the
    // final image, never as the delete.
    for (const [tableName, changes] of touched) {
      const table = this.rt.table(tableName)
      if (table === undefined) continue
      for (const change of changes) {
        const key = SchemaRuntime.keyString(change.key)
        const ref = memberRef(tableName, key)
        if (change.op === "delete") {
          rowUpdates.set(ref, { table: tableName, key: change.key, row: null })
          continue
        }
        const existing = rowUpdates.get(ref)
        if (existing !== undefined && existing.row !== null) continue
        const isMember =
          this.db.query(
            `SELECT 1 AS x FROM membership m JOIN subscriptions s ON s.id = m.subscription WHERE m.tbl = ? AND m.key = ? AND s.live = 1 AND s.orphaned_at IS NULL LIMIT 1`,
            [tableName, key],
          ).length > 0
        if (!isMember) continue
        const record = this.db.query(selectByKeySql(table), [key])[0]
        if (record !== undefined)
          rowUpdates.set(ref, {
            table: tableName,
            key: change.key,
            row: rowFromRecord(table, record),
          })
      }
    }
    return { rows: [...rowUpdates.values()], memberships }
  }

  /**
   * Which subscriptions reference each of the given rows, in one indexed query per chunk. Used
   * when a delta is fanned out to sessions, so the cost is bounded by the rows in the delta and
   * not by rows times sessions times membership size.
   */
  membershipIndex(
    refs: ReadonlyArray<{ readonly table: string; readonly key: RowKey }>,
  ): ReadonlyMap<string, ReadonlySet<string>> {
    const index = new Map<string, Set<string>>()
    const chunk = Math.floor(MAX_BOUND_PARAMS / 2)
    for (let i = 0; i < refs.length; i += chunk) {
      const part = refs.slice(i, i + chunk)
      const where = part.map(() => "(tbl = ? AND key = ?)").join(" OR ")
      const params = part.flatMap((r) => [r.table, SchemaRuntime.keyString(r.key)])
      for (const row of this.db.query(
        `SELECT subscription, tbl, key FROM membership JOIN subscriptions ON subscriptions.id = membership.subscription WHERE (${where}) AND subscriptions.live = 1 AND subscriptions.orphaned_at IS NULL`,
        params,
      )) {
        const ref = memberRef(asString(row["tbl"]), asString(row["key"]))
        const set = index.get(ref) ?? new Set<string>()
        set.add(asString(row["subscription"]))
        index.set(ref, set)
      }
    }
    return index
  }

  /** Membership of a subscription as stored, for tests and resume diagnostics. */
  membershipOf(id: string): ReadonlyArray<MemberRef> {
    return this.db
      .query(`SELECT DISTINCT tbl, key FROM membership WHERE subscription = ? ORDER BY tbl, key`, [
        id,
      ])
      .map((r) => ({ table: asString(r["tbl"]), key: parseKey(asString(r["key"])) }))
  }

  /** Full recomputation of a subscription's members straight from the cache (test oracle). */
  recompute(id: string): ReadonlyArray<MemberRef> {
    const sub = this.subscription(id)
    if (sub === null) return []
    const seen = new Set<string>()
    const out: Array<MemberRef> = []
    for (const m of this.evaluate(sub.planned).members) {
      const ref = memberRef(m.table, m.key)
      if (seen.has(ref)) continue
      seen.add(ref)
      out.push({ table: m.table, key: parseKey(m.key) })
    }
    return out
  }
}
