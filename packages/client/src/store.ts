/**
 * The local relational cache in the browser.
 *
 * Tables mirror the Durable Object cache (`@orbit/schema` DDL) plus:
 * | table             | purpose                                                         |
 * |-------------------|-----------------------------------------------------------------|
 * | meta              | schema hash, partition, cursor, client id                       |
 * | membership        | (subscription, table, key): rows each subscription needs        |
 * | subscriptions     | persisted refs and resolved queries, so a reload renders first  |
 * | pending_mutations | the client's mutation log, pushed in id order                   |
 * | o_<table>         | optimistic overlay: rows written by pending mutations           |
 * | v_<table>         | view: `t_<table>` with the overlay applied on top               |
 *
 * Every protocol message becomes one atomic batch of statements. A row is kept only while at
 * least one subscription references it; removing the last reference deletes the row (GC).
 *
 * Reads go through the `v_` views, so rows written by pending mutations show in live queries
 * before the server admits them. The overlay is derived state: it is a function of the canonical
 * rows and the pending log, and the engine rebuilds it (rebase) whenever either changes.
 */

import { Data, Effect, Result, Schema } from "effect"
import { JsonValue, Query, QueryRef, type SyncSchema, type TableSchema } from "@orbit/protocol"
import type { MembershipChange, RowUpdate, MemberRef } from "@orbit/protocol/client"
import {
  attachIncludes,
  compileIncludeSelect,
  compileSelect,
  deleteByKeySql,
  flattenIncludes,
  keyOfRecord,
  planQuery,
  rowFromRecord,
  rowToParams,
  sqliteDialect,
  upsertSql,
  upsertChangedWhere,
  type Dialect,
  type PlannedQuery,
  type ResultNode,
  type SelectOptions,
  type SqlParam,
  type SqlValue,
} from "@orbit/query"
import {
  allDdl,
  createIndexesSql,
  createTableSql,
  KEY_COLUMN,
  localTableName,
  planMigration,
  quoteIdent,
  SchemaRuntime,
  sqliteType,
} from "@orbit/schema"

import { SqlDriverError, type AsyncSqlDriver, type SqlRecord, type Statement } from "./driver.ts"

export class StoreError extends Data.TaggedError("StoreError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

/** Name of the overlay table for a synced table. */
export const overlayTableName = (table: string): string => `o_${table}`
/** Name of the view that applies the overlay on top of the cached rows. */
export const overlayViewName = (table: string): string => `v_${table}`

export const OP_COLUMN = "__op"
export const MUTATION_COLUMN = "__mutation"

/** Reads compile against the views, so pending mutations are visible. */
export const VIEW_DIALECT: Dialect = sqliteDialect((table) => quoteIdent(overlayViewName(table)))

/** Only evict completed undo history when no queued command could still need it. */
export const pruneUndoStatement = (): Statement => ({
  sql: `DELETE FROM local_undo WHERE NOT EXISTS (SELECT 1 FROM pending_mutations) AND mutation_id NOT IN (SELECT mutation_id FROM (SELECT mutation_id, SUM(bytes) OVER (ORDER BY mutation_id DESC) AS total FROM (SELECT mutation_id, SUM(length(images)) AS bytes FROM local_undo GROUP BY mutation_id)) WHERE total <= 33554432 ORDER BY mutation_id DESC LIMIT 100)`,
  params: [],
})

const STORE_DDL: ReadonlyArray<string> = [
  `CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS membership (subscription TEXT NOT NULL, tbl TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY (subscription, tbl, key))`,
  `CREATE INDEX IF NOT EXISTS membership_row ON membership (tbl, key)`,
  `CREATE TABLE IF NOT EXISTS subscriptions (id TEXT PRIMARY KEY, query TEXT NOT NULL, ref TEXT, cursor INTEGER NOT NULL DEFAULT 0, complete INTEGER NOT NULL DEFAULT 0, based_on TEXT, retired INTEGER NOT NULL DEFAULT 0)`,
  `CREATE TABLE IF NOT EXISTS local_undo (tbl TEXT NOT NULL, undo_group TEXT NOT NULL, mutation_id INTEGER NOT NULL, images TEXT NOT NULL, PRIMARY KEY (tbl, undo_group, mutation_id))`,
  `CREATE TABLE IF NOT EXISTS mutation_outcomes (id INTEGER PRIMARY KEY, outcome TEXT NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS pending_mutations (id INTEGER PRIMARY KEY, name TEXT NOT NULL, args TEXT NOT NULL, created_at INTEGER NOT NULL, pushed INTEGER NOT NULL DEFAULT 0)`,
]

const overlayTableSql = (table: TableSchema): string => {
  const cols = table.columns.map((c) => `${quoteIdent(c.name)} ${sqliteType(c.kind)}`)
  return `CREATE TABLE IF NOT EXISTS ${quoteIdent(overlayTableName(table.name))} (${[
    `${quoteIdent(KEY_COLUMN)} TEXT NOT NULL PRIMARY KEY`,
    ...cols,
    `${quoteIdent(OP_COLUMN)} TEXT NOT NULL`,
    `${quoteIdent(MUTATION_COLUMN)} INTEGER NOT NULL`,
  ].join(", ")}) WITHOUT ROWID`
}

const overlayViewSql = (table: TableSchema): string => {
  const cols = [KEY_COLUMN, ...table.columns.map((c) => c.name)].map(quoteIdent).join(", ")
  const t = quoteIdent(localTableName(table.name))
  const o = quoteIdent(overlayTableName(table.name))
  return `CREATE VIEW ${quoteIdent(overlayViewName(table.name))} AS SELECT ${cols} FROM ${t} WHERE ${quoteIdent(KEY_COLUMN)} NOT IN (SELECT ${quoteIdent(KEY_COLUMN)} FROM ${o}) UNION ALL SELECT ${cols} FROM ${o} WHERE ${quoteIdent(OP_COLUMN)} = 'upsert'`
}

/** `INSERT ... ON CONFLICT(__key) DO UPDATE` into an overlay table; params: `rowToParams`, op, mutation id. */
const overlayUpsertSql = (table: TableSchema): string => {
  const cols = [KEY_COLUMN, ...table.columns.map((c) => c.name), OP_COLUMN, MUTATION_COLUMN]
  const updates = cols
    .slice(1)
    .map((c) => `${quoteIdent(c)} = excluded.${quoteIdent(c)}`)
    .join(", ")
  return `INSERT INTO ${quoteIdent(overlayTableName(table.name))} (${cols.map(quoteIdent).join(", ")}) VALUES (${cols.map(() => "?").join(", ")}) ON CONFLICT(${quoteIdent(KEY_COLUMN)}) DO UPDATE SET ${updates}`
}

export interface OpenedStore {
  readonly action: "create" | "additive" | "reset" | "none"
  readonly cursor: number | null
  /** The persisted client id (created once per local database). */
  readonly clientId: string
}

export interface PersistedSubscription {
  readonly id: string
  /** What the client sends on the wire: a named query reference or a raw query. */
  readonly ref: QueryRef
  /** The resolved query the local plan is built from. */
  readonly query: Query
  readonly complete: boolean
}

export interface PendingMutation {
  readonly id: number
  readonly name: string
  readonly args: Record<string, JsonValue>
  readonly createdAt: number
  readonly pushed: boolean
}

const str = (v: SqlValue | undefined): string => (typeof v === "string" ? v : "")
const num = (v: SqlValue | undefined): number =>
  typeof v === "number" ? v : typeof v === "string" ? Number(v) : 0

/** Decodes a persisted subscription query from its JSON text in the `subscriptions` table. */
const decodeStoredQuery = Schema.decodeUnknownSync(Schema.fromJsonString(Query))
const decodeStoredRef = Schema.decodeUnknownSync(Schema.fromJsonString(QueryRef))
const decodeStoredArgs = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, JsonValue)),
)

export const META_UPSERT = `INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`

export class LocalStore {
  readonly rt: SchemaRuntime

  constructor(
    readonly driver: AsyncSqlDriver,
    readonly schema: SyncSchema,
    readonly partition: string,
  ) {
    this.rt = new SchemaRuntime(schema)
  }

  private fail(e: unknown): StoreError {
    if (e instanceof SqlDriverError)
      return new StoreError({ message: `${e.code}: ${e.message}`, cause: e })
    return new StoreError({ message: e instanceof Error ? e.message : String(e), cause: e })
  }

  run(statements: ReadonlyArray<Statement>): Effect.Effect<void, StoreError> {
    return Effect.tryPromise({
      try: () => this.driver.batch(statements),
      catch: (e) => this.fail(e),
    })
  }

  query(
    sql: string,
    params: ReadonlyArray<SqlValue> = [],
  ): Effect.Effect<ReadonlyArray<SqlRecord>, StoreError> {
    return Effect.tryPromise({
      try: () => this.driver.query(sql, params),
      catch: (e) => this.fail(e),
    })
  }

  /**
   * Creates or migrates the local schema. A reset wipes rows, memberships, the pending log and
   * the overlay. Overlay tables and views are recreated on every open: the overlay is derived
   * from the pending log and the engine rebuilds it right after opening.
   */
  open(options: { readonly clientId?: string } = {}): Effect.Effect<OpenedStore, StoreError> {
    return Effect.gen({ self: this }, function* () {
      yield* this.run(STORE_DDL.map((sql) => ({ sql, params: [] })))
      const subscriptionColumns = yield* this.query(`SELECT name FROM pragma_table_info(?)`, [
        "subscriptions",
      ])
      if (!subscriptionColumns.some((c) => str(c["name"]) === "ref"))
        yield* this.run([{ sql: `ALTER TABLE subscriptions ADD COLUMN ref TEXT`, params: [] }])
      if (!subscriptionColumns.some((c) => str(c["name"]) === "based_on"))
        yield* this.run([{ sql: `ALTER TABLE subscriptions ADD COLUMN based_on TEXT`, params: [] }])
      if (!subscriptionColumns.some((c) => str(c["name"]) === "retired"))
        yield* this.run([
          {
            sql: `ALTER TABLE subscriptions ADD COLUMN retired INTEGER NOT NULL DEFAULT 0`,
            params: [],
          },
        ])
      const meta = yield* this.query(`SELECT key, value FROM meta`)
      const stored = new Map(meta.map((r) => [str(r["key"]), str(r["value"])] as const))
      const storedPartition = stored.get("partition")
      const objects = yield* this.query(
        `SELECT name, type, sql FROM sqlite_master WHERE type IN ('table', 'view')`,
      )
      const existing: Array<{ name: string; columns: ReadonlyArray<string> }> = []
      const overlays: Array<string> = []
      const views: Array<string> = []
      for (const t of objects) {
        const name = str(t["name"])
        if (t["type"] === "view") {
          if (name.startsWith("v_")) views.push(name)
          continue
        }
        if (name.startsWith("o_")) {
          overlays.push(name)
          continue
        }
        if (!name.startsWith("t_")) continue
        const cols = yield* this.query(`SELECT name FROM pragma_table_info(?)`, [name])
        existing.push({ name, columns: cols.map((c) => str(c["name"])) })
      }
      const partitionChanged = storedPartition !== undefined && storedPartition !== this.partition
      const plan = partitionChanged
        ? {
            action: "reset" as const,
            statements: [
              ...existing.map((t) => `DROP TABLE IF EXISTS ${quoteIdent(t.name)}`),
              ...allDdl(this.schema, { rowid: true }),
            ],
            reason: "partition changed",
          }
        : planMigration(this.schema, existing, stored.get("schema_hash") ?? null, { rowid: true })
      const statements: Array<Statement> = []
      // Views reference the tables the migration may drop; overlays are rebuilt from the log.
      for (const v of views)
        statements.push({ sql: `DROP VIEW IF EXISTS ${quoteIdent(v)}`, params: [] })
      for (const o of overlays)
        statements.push({ sql: `DROP TABLE IF EXISTS ${quoteIdent(o)}`, params: [] })
      if (plan.action !== "none")
        for (const sql of plan.statements) statements.push({ sql, params: [] })
      if (plan.action !== "reset" && plan.action !== "create") {
        for (const table of this.schema.tables) {
          const name = localTableName(table.name)
          const object = objects.find((object) => object["name"] === name)
          if (typeof object?.["sql"] !== "string" || !/WITHOUT\s+ROWID\s*$/i.test(object["sql"]))
            continue
          const old = `${name}__orbit_layout_old`
          const columns = [KEY_COLUMN, ...table.columns.map((column) => column.name)]
            .map(quoteIdent)
            .join(", ")
          statements.push(
            { sql: `ALTER TABLE ${quoteIdent(name)} RENAME TO ${quoteIdent(old)}`, params: [] },
            { sql: createTableSql(table, { rowid: true }), params: [] },
            {
              sql: `INSERT INTO ${quoteIdent(name)} (${columns}) SELECT ${columns} FROM ${quoteIdent(old)}`,
              params: [],
            },
            { sql: `DROP TABLE ${quoteIdent(old)}`, params: [] },
          )
        }
      }
      // Relation indexes are `IF NOT EXISTS`: an existing cache gains the ones it lacks.
      for (const sql of createIndexesSql(this.schema)) statements.push({ sql, params: [] })
      const fresh = plan.action === "reset" || plan.action === "create"
      if (fresh) {
        statements.push(
          { sql: `DELETE FROM membership`, params: [] },
          { sql: `DELETE FROM subscriptions`, params: [] },
          { sql: `DELETE FROM pending_mutations`, params: [] },
          { sql: `DELETE FROM mutation_outcomes`, params: [] },
          { sql: `DELETE FROM local_undo`, params: [] },
          { sql: `DELETE FROM meta`, params: [] },
        )
      }
      for (const t of this.schema.tables) {
        statements.push({ sql: overlayTableSql(t), params: [] })
        statements.push({ sql: overlayViewSql(t), params: [] })
      }
      const clientId =
        options.clientId ?? (fresh ? undefined : stored.get("client_id")) ?? crypto.randomUUID()
      statements.push(
        { sql: META_UPSERT, params: ["schema_hash", this.schema.schema_hash] },
        { sql: META_UPSERT, params: ["partition", this.partition] },
        { sql: META_UPSERT, params: ["client_id", clientId] },
      )
      yield* this.run(statements)
      const cursorText = fresh ? undefined : stored.get("cursor")
      return {
        action: plan.action,
        cursor: cursorText === undefined ? null : Number(cursorText),
        clientId,
      }
    })
  }

  cursor(): Effect.Effect<number | null, StoreError> {
    return this.query(`SELECT value FROM meta WHERE key = 'cursor'`).pipe(
      Effect.map((rows) => (rows[0] === undefined ? null : Number(str(rows[0]["value"])))),
    )
  }

  /** Persisted subscriptions (ref and resolved query per client subscription id). */
  subscriptions(): Effect.Effect<ReadonlyArray<PersistedSubscription>, StoreError> {
    return this.query(`SELECT id, query, ref, complete FROM subscriptions WHERE retired = 0`).pipe(
      Effect.map((rows) =>
        rows.map((r) => {
          const query = decodeStoredQuery(str(r["query"]))
          const refText = r["ref"]
          return {
            id: str(r["id"]),
            ref: typeof refText === "string" ? decodeStoredRef(refText) : query,
            query,
            complete: r["complete"] === 1,
          }
        }),
      ),
    )
  }

  registerSubscription(id: string, ref: QueryRef, query: Query): Effect.Effect<void, StoreError> {
    return this.run([
      {
        sql: `INSERT INTO subscriptions (id, query, ref, cursor, complete) VALUES (?, ?, ?, 0, 0) ON CONFLICT(id) DO UPDATE SET query = excluded.query, ref = excluded.ref, retired = 0`,
        params: [id, JSON.stringify(query), JSON.stringify(ref)],
      },
    ])
  }

  /** Replaces the resolved query of a subscription (the server's resolution is authoritative). */
  updateSubscriptionQuery(id: string, query: Query): Effect.Effect<void, StoreError> {
    return this.run([
      {
        sql: `UPDATE subscriptions SET query = ? WHERE id = ?`,
        params: [JSON.stringify(query), id],
      },
    ])
  }

  /** Returns whether another bounded collection pass is needed. */
  removeSubscription(id: string): Effect.Effect<boolean, StoreError> {
    return this.run([
      { sql: `UPDATE subscriptions SET retired = 1 WHERE id = ?`, params: [id] },
    ]).pipe(Effect.andThen(this.collectRetired()))
  }

  /**
   * Retired windows can form a long inherited chain. Reclaim at most 128 row
   * references and 16 empty subscriptions per transaction, retaining live bases.
   * The engine schedules additional passes outside the foreground write lock.
   */
  collectRetired(): Effect.Effect<boolean, StoreError> {
    return Effect.gen({ self: this }, function* () {
      const rows = yield* this.query(`SELECT id, based_on, retired FROM subscriptions`)
      const nodes = new Map(
        rows.map((row) => [
          str(row["id"]),
          {
            parent: typeof row["based_on"] === "string" ? row["based_on"] : null,
            retired: row["retired"] === 1,
          },
        ]),
      )
      const dependents = new Map<string, number>()
      for (const node of nodes.values())
        if (node.parent !== null)
          dependents.set(node.parent, (dependents.get(node.parent) ?? 0) + 1)
      const ready = [...nodes]
        .filter(([id, node]) => node.retired && !dependents.get(id))
        .map(([id]) => id)
      const statements: Array<Statement> = []
      let budget = 128
      let visited = 0
      while (ready.length > 0 && budget > 0 && visited < 16) {
        const id = ready.pop()!
        const node = nodes.get(id)!
        visited += 1
        const candidates = yield* this.query(
          `SELECT tbl, key FROM membership WHERE subscription = ? LIMIT ?`,
          [id, budget + 1],
        )
        const selected = candidates.slice(0, budget)
        const byTable = new Map<string, Array<string>>()
        for (const row of selected) {
          const table = str(row["tbl"])
          const keys = byTable.get(table) ?? []
          keys.push(str(row["key"]))
          byTable.set(table, keys)
        }
        // A reopened partially collected view must never advertise completeness,
        // including if it closes again before its replacement snapshot arrives.
        statements.push({ sql: `UPDATE subscriptions SET complete = 0 WHERE id = ?`, params: [id] })
        for (const [table, keys] of byTable) {
          const name = quoteIdent(localTableName(table))
          if (this.rt.table(table) !== undefined)
            statements.push({
              sql: `DELETE FROM ${name} WHERE ${quoteIdent(KEY_COLUMN)} IN (SELECT value FROM json_each(?)) AND NOT EXISTS (SELECT 1 FROM membership WHERE tbl = ? AND key = ${name}.${quoteIdent(KEY_COLUMN)} AND subscription <> ?)`,
              params: [JSON.stringify(keys), table, id],
            })
          statements.push({
            sql: `DELETE FROM membership WHERE subscription = ? AND tbl = ? AND key IN (SELECT value FROM json_each(?))`,
            params: [id, table, JSON.stringify(keys)],
          })
        }
        budget -= Math.max(1, selected.length)
        if (candidates.length > selected.length) {
          ready.push(id)
          break
        }
        statements.push({ sql: `DELETE FROM subscriptions WHERE id = ?`, params: [id] })
        nodes.delete(id)
        if (node.parent !== null) {
          const remaining = (dependents.get(node.parent) ?? 0) - 1
          dependents.set(node.parent, remaining)
          if (remaining === 0 && nodes.get(node.parent)?.retired) ready.push(node.parent)
        }
      }
      if (statements.length > 0) yield* this.run(statements)
      return ready.length > 0
    })
  }

  /**
   * Hands the old rows to dependent windows before a new snapshot replaces this membership.
   * Retirement keeps the base in place instead, avoiding a growing copy on every window.
   */
  private transferStatements(id: string): ReadonlyArray<Statement> {
    return [
      {
        sql: `INSERT OR IGNORE INTO membership (subscription, tbl, key) SELECT d.id, m.tbl, m.key FROM membership m JOIN subscriptions d ON d.based_on = ? WHERE m.subscription = ?`,
        params: [id, id],
      },
      {
        sql: `UPDATE subscriptions SET based_on = (SELECT based_on FROM subscriptions WHERE id = ?) WHERE based_on = ?`,
        params: [id, id],
      },
    ]
  }

  /** Deletes rows referenced only by `subscription`, per table. Run before removing its membership. */
  private gcStatementsForSubscription(
    subscription: string,
    retained: ReadonlyArray<MemberRef> = [],
  ): ReadonlyArray<Statement> {
    const keys = new Map<string, Array<string>>()
    for (const ref of retained) {
      const list = keys.get(ref.table) ?? []
      list.push(SchemaRuntime.keyString(ref.key))
      keys.set(ref.table, list)
    }
    return this.schema.tables.map((t) => {
      const keep = keys.get(t.name)
      return {
        sql: `DELETE FROM ${quoteIdent(localTableName(t.name))} WHERE ${quoteIdent(KEY_COLUMN)} IN (SELECT key FROM membership WHERE subscription = ? AND tbl = ?) AND NOT EXISTS (SELECT 1 FROM membership WHERE tbl = ? AND key = ${quoteIdent(localTableName(t.name))}.${quoteIdent(KEY_COLUMN)} AND subscription <> ?)${keep === undefined ? "" : ` AND ${quoteIdent(KEY_COLUMN)} NOT IN (SELECT value FROM json_each(?))`}`,
        params: [
          subscription,
          t.name,
          t.name,
          subscription,
          ...(keep === undefined ? [] : [JSON.stringify(keep)]),
        ],
      }
    })
  }

  private upsertStatements(
    rows: ReadonlyArray<RowUpdate>,
    gated: boolean,
  ): ReadonlyArray<Statement> {
    const out: Array<Statement> = []
    for (const r of rows) {
      const table = this.rt.table(r.table)
      if (table === undefined) continue
      const key = SchemaRuntime.keyString(r.key)
      if (r.row === null) {
        out.push({ sql: deleteByKeySql(table), params: [key] })
        continue
      }
      const params = rowToParams(table, r.row)
      if (!gated) {
        out.push({ sql: upsertSql(table), params })
        continue
      }
      // Only rows some subscription references are kept; others would never be garbage collected.
      const cols = [KEY_COLUMN, ...table.columns.map((c) => c.name)]
      const updates = table.columns.map(
        (c) => `${quoteIdent(c.name)} = excluded.${quoteIdent(c.name)}`,
      )
      out.push({
        sql: `INSERT INTO ${quoteIdent(localTableName(table.name))} (${cols.map(quoteIdent).join(", ")}) SELECT ${cols.map(() => "?").join(", ")} WHERE EXISTS (SELECT 1 FROM membership WHERE tbl = ? AND key = ?) ON CONFLICT(${quoteIdent(KEY_COLUMN)}) DO UPDATE SET ${updates.join(", ")} WHERE ${upsertChangedWhere(table)}`,
        params: [...params, table.name, key],
      })
    }
    return out
  }

  /**
   * Applies a complete snapshot for one subscription: rows referenced only by the old membership
   * are dropped, the membership is replaced, then every snapshot row is written.
   */
  applySnapshot(
    subscription: string,
    cursor: number,
    rows: ReadonlyArray<RowUpdate>,
    members: ReadonlyArray<MemberRef>,
    // A subscription whose membership this snapshot extends: the rows stay under the base and
    // reads follow the link, so a grown window writes only its new members (see
    // `snapshot.basedOn` in the protocol).
    basedOn: string | null = null,
  ): Effect.Effect<boolean, StoreError> {
    const statements: Array<Statement> = [
      ...this.transferStatements(subscription),
      ...this.gcStatementsForSubscription(subscription, members),
      { sql: `DELETE FROM membership WHERE subscription = ?`, params: [subscription] },
      {
        sql: `UPDATE subscriptions SET based_on = ? WHERE id = ?`,
        params: [basedOn, subscription],
      },
      ...members.map((m) => ({
        sql: `INSERT OR IGNORE INTO membership (subscription, tbl, key) VALUES (?, ?, ?)`,
        params: [subscription, m.table, SchemaRuntime.keyString(m.key)],
      })),
      ...this.upsertStatements(rows, false),
      {
        sql: `UPDATE subscriptions SET cursor = ?, complete = 1 WHERE id = ?`,
        params: [cursor, subscription],
      },
      { sql: META_UPSERT, params: ["cursor", String(cursor)] },
    ]
    return this.run(statements).pipe(Effect.andThen(this.collectRetired()))
  }

  /** Applies one delta (one source transaction) atomically. */
  applyDelta(
    cursor: number,
    rows: ReadonlyArray<RowUpdate>,
    memberships: ReadonlyArray<MembershipChange>,
  ): Effect.Effect<void, StoreError> {
    return this.run(this.deltaStatements(cursor, rows, memberships))
  }

  /** A bounded queued burst commits together, preserving every transaction's statement order. */
  applyDeltas(
    deltas: ReadonlyArray<{
      readonly cursor: number
      readonly rows: ReadonlyArray<RowUpdate>
      readonly memberships: ReadonlyArray<MembershipChange>
    }>,
  ): Effect.Effect<void, StoreError> {
    return this.run(
      deltas.flatMap((delta) => this.deltaStatements(delta.cursor, delta.rows, delta.memberships)),
    )
  }

  private deltaStatements(
    cursor: number,
    rows: ReadonlyArray<RowUpdate>,
    memberships: ReadonlyArray<MembershipChange>,
  ): Array<Statement> {
    const statements: Array<Statement> = []
    const removed: Array<MemberRef> = []
    for (const m of memberships) {
      for (const ref of m.removed) {
        // A row that left a window also left the windows it extends (they are subsets).
        statements.push({
          sql: `DELETE FROM membership WHERE tbl = ? AND key = ? AND subscription IN (WITH RECURSIVE chain(id) AS (SELECT ? UNION SELECT s.based_on FROM subscriptions s JOIN chain ON s.id = chain.id WHERE s.based_on IS NOT NULL) SELECT id FROM chain)`,
          params: [ref.table, SchemaRuntime.keyString(ref.key), m.subscriptionId],
        })
        removed.push(ref)
      }
      for (const ref of m.added)
        statements.push({
          sql: `INSERT OR IGNORE INTO membership (subscription, tbl, key) VALUES (?, ?, ?)`,
          params: [m.subscriptionId, ref.table, SchemaRuntime.keyString(ref.key)],
        })
    }
    statements.push(...this.upsertStatements(rows, true))
    for (const ref of removed) {
      const table = this.rt.table(ref.table)
      if (table === undefined) continue
      const key = SchemaRuntime.keyString(ref.key)
      statements.push({
        sql: `DELETE FROM ${quoteIdent(localTableName(table.name))} WHERE ${quoteIdent(KEY_COLUMN)} = ? AND NOT EXISTS (SELECT 1 FROM membership WHERE tbl = ? AND key = ?)`,
        params: [key, ref.table, key],
      })
    }
    statements.push({ sql: META_UPSERT, params: ["cursor", String(cursor)] })
    return statements
  }

  /**
   * Runs a planned query against the rows a subscription references, with includes attached.
   * Rows written by pending mutations are admitted too, so optimistic writes show at once.
   */
  readSubscription(
    planned: PlannedQuery,
    subscription: string,
  ): Effect.Effect<ReadonlyArray<ResultNode>, StoreError> {
    return this.read(planned, {
      membershipOf: subscription,
      alsoAdmit: `t.${quoteIdent(KEY_COLUMN)} IN (SELECT ${quoteIdent(KEY_COLUMN)} FROM ${quoteIdent(overlayTableName(planned.table.name))})`,
    })
  }

  /** Reuse a current base window's result nodes; read full rows/includes only for new roots. */
  readGrowingSubscription(
    planned: PlannedQuery,
    subscription: string,
    base: ReadonlyArray<ResultNode>,
  ): Effect.Effect<ReadonlyArray<ResultNode>, StoreError> {
    return Effect.tryPromise({
      try: async () => {
        const options: SelectOptions = {
          membershipOf: subscription,
          alsoAdmit: `t.${quoteIdent(KEY_COLUMN)} IN (SELECT ${quoteIdent(KEY_COLUMN)} FROM ${quoteIdent(overlayTableName(planned.table.name))})`,
          dialect: VIEW_DIALECT,
        }
        const primary = compileSelect(planned, { ...options, keysOnly: true })
        const keys = (await this.driver.query(primary.sql, primary.params)).map(keyOfRecord)
        const nodes = new Map(base.map((node) => [node.key, node]))
        const added = keys.filter((key) => !nodes.has(key))
        if (added.length > 0) {
          const records = await this.driver.query(
            `SELECT * FROM ${quoteIdent(overlayViewName(planned.table.name))} WHERE ${quoteIdent(KEY_COLUMN)} IN (SELECT value FROM json_each(?))`,
            [JSON.stringify(added)],
          )
          for (const node of await readNodes(this.driver, planned, options, records))
            nodes.set(node.key, node)
        }
        return keys.map((key) => {
          const node = nodes.get(key)
          if (node === undefined) throw new Error("growing window lost a selected row")
          return node
        })
      },
      catch: (e) => this.fail(e),
    })
  }

  /**
   * The subscriptions whose membership references any of `refs`: the ones a delta over those
   * rows can change. A subscription that references none of them keeps its result as it is.
   */
  subscriptionsHolding(
    refs: ReadonlyArray<MemberRef>,
  ): Effect.Effect<ReadonlySet<string>, StoreError> {
    return Effect.tryPromise({
      try: async () => {
        const held = new Set<string>()
        const CHUNK = 200
        for (let i = 0; i < refs.length; i += CHUNK) {
          const chunk = refs.slice(i, i + CHUNK)
          // A subscription that extends a holder reads the row too.
          const rows = await this.driver.query(
            `WITH RECURSIVE dep(subscription) AS (SELECT DISTINCT subscription FROM membership WHERE ${chunk.map(() => "(tbl = ? AND key = ?)").join(" OR ")} UNION SELECT s.id FROM subscriptions s JOIN dep ON s.based_on = dep.subscription) SELECT subscription FROM dep`,
            chunk.flatMap((r) => [r.table, SchemaRuntime.keyString(r.key)]),
          )
          for (const row of rows)
            if (typeof row["subscription"] === "string") held.add(row["subscription"])
        }
        return held
      },
      catch: (e) => this.fail(e),
    })
  }

  /** Runs a planned query against everything cached locally (completeness is the caller's concern). */
  readLocal(planned: PlannedQuery): Effect.Effect<ReadonlyArray<ResultNode>, StoreError> {
    return this.read(planned, {})
  }

  private read(
    planned: PlannedQuery,
    options: SelectOptions,
  ): Effect.Effect<ReadonlyArray<ResultNode>, StoreError> {
    return Effect.tryPromise({
      try: () => readNodes(this.driver, planned, options),
      catch: (e) => this.fail(e),
    })
  }

  plan(query: Query): Result.Result<PlannedQuery, StoreError> {
    const r = planQuery(this.rt, query)
    return Result.isFailure(r)
      ? Result.fail(new StoreError({ message: r.failure.message }))
      : Result.succeed(r.success)
  }

  // ---------------------------------------------------------------------------------------------
  // Mutations: the pending log and the overlay
  // ---------------------------------------------------------------------------------------------

  pendingMutations(): Effect.Effect<ReadonlyArray<PendingMutation>, StoreError> {
    return Effect.tryPromise({
      try: () => readPendingMutations(this.driver),
      catch: (e) => this.fail(e),
    })
  }

  /** `last_mutation_id` of this client's synced `orbit_clients` row, or null when not cached. */
  lastMutationIdOf(
    clientsTable: string,
    clientId: string,
  ): Effect.Effect<number | null, StoreError> {
    const table = this.rt.table(clientsTable)
    if (table === undefined || !table.columns.some((c) => c.name === "last_mutation_id"))
      return Effect.succeed(null)
    return this.query(
      `SELECT "last_mutation_id" AS n FROM ${quoteIdent(localTableName(clientsTable))} WHERE "client_id" = ?`,
      [clientId],
    ).pipe(Effect.map((rows) => (rows[0] === undefined ? null : num(rows[0]["n"]))))
  }

  insertPendingStatement(mutation: Omit<PendingMutation, "pushed">): Statement {
    return {
      sql: `INSERT INTO pending_mutations (id, name, args, created_at, pushed) VALUES (?, ?, ?, ?, 0)`,
      params: [mutation.id, mutation.name, JSON.stringify(mutation.args), mutation.createdAt],
    }
  }

  deletePendingStatement(ids: ReadonlyArray<number>): Statement {
    return {
      sql: `DELETE FROM pending_mutations WHERE id IN (${ids.map(() => "?").join(", ")})`,
      params: [...ids],
    }
  }

  markPushedStatement(ids: ReadonlyArray<number>): Statement {
    return {
      sql: `UPDATE pending_mutations SET pushed = 1 WHERE id IN (${ids.map(() => "?").join(", ")})`,
      params: [...ids],
    }
  }

  clearOverlayStatements(): ReadonlyArray<Statement> {
    return this.schema.tables.map((t) => ({
      sql: `DELETE FROM ${quoteIdent(overlayTableName(t.name))}`,
      params: [],
    }))
  }

  /** Overlay upsert for a full row image (`op` is `upsert` or `delete`). */
  overlayStatement(
    table: TableSchema,
    row: ReadonlyArray<SqlParam>,
    op: "upsert" | "delete",
    mutationId: number,
  ): Statement {
    return { sql: overlayUpsertSql(table), params: [...row, op, mutationId] }
  }

  close(): Effect.Effect<void, StoreError> {
    return Effect.tryPromise({ try: () => this.driver.close(), catch: (e) => this.fail(e) })
  }
}

/** Reads the primary rows and every include level through the views, then nests them. */
export const readNodes = async (
  driver: AsyncSqlDriver,
  planned: PlannedQuery,
  options: SelectOptions,
  selected?: ReadonlyArray<SqlRecord>,
): Promise<ReadonlyArray<ResultNode>> => {
  const opts: SelectOptions = { ...options, dialect: VIEW_DIALECT }
  const primary = compileSelect(planned, opts)
  const records = selected ?? (await driver.query(primary.sql, primary.params))
  const rows = records.map((r) => ({ key: keyOfRecord(r), row: rowFromRecord(planned.table, r) }))
  const byPath = new Map<
    string,
    Array<{ readonly key: string; readonly row: ReturnType<typeof rowFromRecord> }>
  >()
  for (const include of flattenIncludes(planned)) {
    const parents =
      include.path.length === 1 ? rows : (byPath.get(include.path.slice(0, -1).join("/")) ?? [])
    const inc = compileIncludeSelect(
      planned,
      include,
      opts,
      parents.map((r) => r.key),
    )
    const related = parents.length === 0 ? [] : await driver.query(inc.sql, inc.params)
    byPath.set(
      include.path.join("/"),
      related.map((r) => ({ key: keyOfRecord(r), row: rowFromRecord(include.target, r) })),
    )
  }
  return attachIncludes(planned, rows, byPath)
}

export const readPendingMutations = async (
  driver: AsyncSqlDriver,
): Promise<ReadonlyArray<PendingMutation>> => {
  const rows = await driver.query(
    `SELECT id, name, args, created_at, pushed FROM pending_mutations ORDER BY id`,
  )
  return rows.map((r) => ({
    id: num(r["id"]),
    name: str(r["name"]),
    args: decodeStoredArgs(str(r["args"])),
    createdAt: num(r["created_at"]),
    pushed: r["pushed"] === 1,
  }))
}

/** The result of a local read: a primary row with its included relations nested. */
export type LocalResultRow = ResultNode
