/**
 * Test fixtures: a small sync schema that covers every value kind, mutators over it, and an
 * in-memory `PushDb` that records every statement per transaction.
 */

import { Effect, Schema } from "effect"
import type { SyncSchema } from "@orbit/protocol"
import { q } from "@orbit/query"
import { compileSyncSchema, defineSyncSchema, type IntrospectedShape } from "@orbit/schema"

import { defineMutator, defineMutators, type MutationContext } from "../../src/index.ts"
import type { PushDb, SqlParam, SqlTx } from "../../src/server/db.ts"
import { SELECT_LAST_MUTATION_SQL, UPSERT_CLIENT_SQL } from "../../src/server/push-handler.ts"

type Kind = IntrospectedShape["tables"][number]["columns"][number]["kind"]

const col = <const Name extends string, const K extends Kind, const Nullable extends boolean>(
  name: Name,
  column_type: string,
  kind: K,
  nullable: Nullable,
) => ({ name, column_type, data_type: column_type.replace(/\(.*$/, ""), nullable, kind }) as const

export const introspected = {
  keyspace: "orbit",
  server_version: "8.0.0",
  tables: [
    {
      name: "_orbit_mut",
      primary_key: ["id"],
      columns: [
        col("id", "varchar(64)", "string", false),
        col("org", "varchar(64)", "string", false),
        col("name", "varchar(191)", "string", false),
        col("flag", "tinyint(1)", "bool", false),
        col("n", "int", "int", true),
        col("big", "bigint", "bigint", true),
        col("price", "decimal(12,4)", "decimal", true),
        col("meta", "json", "json", true),
        col("at", "datetime(3)", "datetime", true),
        col("day", "date", "date", true),
        col("tm", "time", "time", true),
        col("data", "blob", "bytes", true),
        col("score", "double", "float", true),
        col("parentId", "varchar(64)", "string", true),
      ],
    },
    {
      name: "_orbit_mut_tag",
      primary_key: ["id"],
      columns: [
        col("id", "varchar(64)", "string", false),
        col("org", "varchar(64)", "string", false),
        col("itemId", "varchar(64)", "string", false),
        col("label", "varchar(191)", "string", false),
      ],
    },
  ],
} as const satisfies IntrospectedShape

export const sync = defineSyncSchema({
  app: "mutators-test",
  introspected,
  partition: { name: "org", kind: "string" },
  tables: {
    _orbit_mut: {
      partitionBy: "org",
      relations: {
        parent: { kind: "one", to: "_orbit_mut", from: ["parentId"], toColumns: ["id"] },
        tags: { kind: "many", to: "_orbit_mut_tag", from: ["id"], toColumns: ["itemId"] },
      },
    },
    _orbit_mut_tag: {
      partitionBy: "org",
      relations: {
        item: { kind: "one", to: "_orbit_mut", from: ["itemId"], toColumns: ["id"] },
      },
    },
  },
})

export type Sync = typeof sync

export const compileFixtureSchema = (): Promise<SyncSchema> =>
  Effect.runPromise(compileSyncSchema(sync))

/** MySQL DDL for the fixture tables (the integration test creates them). */
export const FIXTURE_DDL: ReadonlyArray<string> = [
  "CREATE TABLE IF NOT EXISTS `_orbit_mut` (" +
    "`id` varchar(64) NOT NULL, `org` varchar(64) NOT NULL, `name` varchar(191) NOT NULL DEFAULT '', " +
    "`flag` tinyint(1) NOT NULL DEFAULT 0, `n` int NULL, `big` bigint NULL, `price` decimal(12,4) NULL, " +
    "`meta` json NULL, `at` datetime(3) NULL, `day` date NULL, `tm` time NULL, `data` blob NULL, " +
    "`score` double NULL, `parentId` varchar(64) NULL, PRIMARY KEY (`id`))",
  "CREATE TABLE IF NOT EXISTS `_orbit_mut_tag` (" +
    "`id` varchar(64) NOT NULL, `org` varchar(64) NOT NULL, `itemId` varchar(64) NOT NULL, " +
    "`label` varchar(191) NOT NULL, PRIMARY KEY (`id`))",
]

const define = defineMutator(sync)

/** Contexts and query results the mutators observed, for assertions. */
export const observed: { contexts: Array<MutationContext>; rows: Array<unknown> } = {
  contexts: [],
  rows: [],
}

export const mutators = defineMutators(sync, {
  createItem: define(
    Schema.Struct({ id: Schema.String, name: Schema.String }),
    async (tx, { id, name }, ctx) => {
      observed.contexts.push(ctx)
      await tx.insert("_orbit_mut", { id, org: ctx.partition, name, flag: true, at: ctx.now })
    },
  ),
  rename: define(
    Schema.Struct({ id: Schema.String, name: Schema.String }),
    async (tx, { id, name }, ctx) => {
      const row = await tx.get("_orbit_mut", { id })
      if (row === null || row.org !== ctx.partition) throw new Error(`item ${id} not found`)
      await tx.update("_orbit_mut", { id }, { name })
    },
  ),
  remove: define(Schema.Struct({ id: Schema.String }), async (tx, { id }) => {
    await tx.delete("_orbit_mut", { id })
  }),
  /** Writes a row and then throws, so the write must roll back. */
  writeThenBoom: define(Schema.Struct({ id: Schema.String }), async (tx, { id }, ctx) => {
    await tx.insert("_orbit_mut", { id, org: ctx.partition, name: "doomed", flag: false })
    throw new Error("boom")
  }),
  /** Reads through `tx.query` and records the rows. */
  inspect: define(Schema.Struct({ flag: Schema.Boolean }), async (tx, { flag }) => {
    const rows = await tx.query(
      q(sync)
        .from("_orbit_mut")
        .where((c) => c.eq("flag", flag))
        .orderBy("name")
        .include("tags")
        .include("parent"),
    )
    observed.rows.push(...rows)
  }),
})

export interface Statement {
  readonly sql: string
  readonly params: ReadonlyArray<SqlParam>
}

export type Responder = (
  sql: string,
  params: ReadonlyArray<SqlParam>,
) => ReadonlyArray<Record<string, unknown>>

/**
 * Records statements per transaction. The `orbit_clients` statements are simulated so the
 * handler's bookkeeping works; every other read goes to `respond`.
 */
export class FakeDb implements PushDb {
  last = new Map<string, number>()
  committed: Array<Array<Statement>> = []
  rolledBack: Array<Array<Statement>> = []
  respond: Responder = () => []
  /** Statements whose SQL contains this text reject, simulating a database error. */
  rejectSqlContaining: string | null = null

  transaction = async <T>(f: (tx: SqlTx) => Promise<T>): Promise<T> => {
    const log: Array<Statement> = []
    const pendingLast = new Map(this.last)
    const record = (sql: string, params: ReadonlyArray<SqlParam>): void => {
      log.push({ sql, params })
      if (this.rejectSqlContaining !== null && sql.includes(this.rejectSqlContaining))
        throw new Error(`database rejected: ${sql}`)
    }
    const tx: SqlTx = {
      query: async (sql, params) => {
        record(sql, params)
        if (sql === SELECT_LAST_MUTATION_SQL) {
          const v = pendingLast.get(String(params[0]))
          return v === undefined ? [] : [{ last_mutation_id: v }]
        }
        return this.respond(sql, params)
      },
      execute: async (sql, params) => {
        record(sql, params)
        if (sql === UPSERT_CLIENT_SQL) pendingLast.set(String(params[0]), Number(params[2]))
      },
    }
    try {
      const out = await f(tx)
      this.last = pendingLast
      this.committed.push(log)
      return out
    } catch (e) {
      this.rolledBack.push(log)
      throw e
    }
  }
}

/** A standalone `SqlTx` that records statements, for `createMysqlTx` tests. */
export const fakeTx = (
  respond: Responder = () => [],
): { readonly tx: SqlTx; readonly log: Array<Statement> } => {
  const log: Array<Statement> = []
  return {
    log,
    tx: {
      query: async (sql, params) => {
        log.push({ sql, params })
        return respond(sql, params)
      },
      execute: async (sql, params) => {
        log.push({ sql, params })
      },
    },
  }
}
