import { Result } from "effect"
import { describe, expect, it } from "vitest"

import type { Predicate, Query } from "@orbit/protocol"
import type { RowImage } from "@orbit/protocol/client"
import { SchemaRuntime } from "@orbit/schema"

import { Schema } from "effect"

import {
  attachIncludes,
  compileChainCandidates,
  compileIncludeSelect,
  compileRowsByColumns,
  compileSelect,
  compileWhere,
  predicateChains,
  defineQueries,
  defineQuery,
  flattenIncludes,
  flattenNode,
  mysqlDialect,
  planQuery,
  resolveNamedQuery,
  TypedQuery,
  type PlannedQuery,
} from "../src/index.ts"
import { deleteRow, loadFixtureSchema, openDb, upsertRow } from "./support/db.ts"

const schema = loadFixtureSchema()
const rt = new SchemaRuntime(schema)

const chatbot = (id: string, over: Partial<RowImage> = {}): RowImage => ({
  id,
  organizationId: "org_1",
  groupId: null,
  type: "DOCUMENT",
  displayOrder: null,
  contents: null,
  createdAt: "2026-01-01 00:00:00",
  score: null,
  big: null,
  price: null,
  blob: null,
  day: null,
  at: null,
  ...over,
})

const plan = (q: Query) => {
  const r = planQuery(rt, q)
  if (Result.isFailure(r)) throw r.failure
  return r.success
}

describe("planQuery", () => {
  it("normalizes ordering with the primary key and sorts includes", () => {
    const p = plan({
      table: "Chatbot",
      orderBy: [{ column: "displayOrder", direction: "desc" }],
      include: ["organization", "folder"],
    })
    expect(p.orderBy).toEqual([
      { column: "displayOrder", direction: "desc" },
      { column: "id", direction: "asc" },
    ])
    expect(p.query.include).toEqual([{ relation: "folder" }, { relation: "organization" }])
    expect([...p.tables].sort()).toEqual(["Chatbot", "organization"])
    expect(p.key).toMatch(/^[a-f0-9]{64}$/)
    expect(plan(p.query).key).toBe(p.key)
    expect(plan({ ...p.query, limit: 2 }).key).not.toBe(p.key)
  })

  it("allows LIKE over a JSON column, which is stored as text", () => {
    const r = planQuery(rt, {
      table: "Chatbot",
      where: { op: "like", column: "contents", pattern: "%Ada%" },
    })
    expect(Result.isSuccess(r)).toBe(true)
  })

  it("rejects unsupported forms explicitly", () => {
    const failures = [
      { table: "nope" },
      { table: "Chatbot", where: { op: "eq", column: "missing", value: 1 } },
      { table: "Chatbot", where: { op: "eq", column: "displayOrder", value: "1" } },
      { table: "Chatbot", where: { op: "gt", column: "contents", value: "x" } },
      { table: "Chatbot", where: { op: "lt", column: "price", value: "1" } },
      { table: "Chatbot", where: { op: "like", column: "displayOrder", pattern: "%" } },
      { table: "Chatbot", where: { op: "in", column: "id", values: [] } },
      { table: "Chatbot", orderBy: [{ column: "contents", direction: "asc" }] },
      { table: "Chatbot", limit: 1_000_000 },
      { table: "Chatbot", include: ["nope"] },
      { table: "Chatbot", where: { op: "exists", relation: "nope" } },
      {
        table: "Chatbot",
        where: {
          op: "exists",
          relation: "documents",
          where: {
            op: "exists",
            relation: "documents",
            where: {
              op: "exists",
              relation: "documents",
              where: { op: "exists", relation: "documents" },
            },
          },
        },
      },
    ] satisfies ReadonlyArray<Query>
    const reasons = failures.map((q) => {
      const r = planQuery(rt, q)
      return Result.isFailure(r) ? r.failure.problem.reason : "accepted"
    })
    expect(reasons).toEqual([
      "unknown_table",
      "unknown_column",
      "type_mismatch",
      "not_comparable",
      "not_comparable",
      "not_comparable",
      "empty_in",
      "not_orderable",
      "limit_too_large",
      "unknown_relation",
      "unknown_relation",
      "too_deep",
    ])
  })

  it("collects the tables of relation predicates and nested includes", () => {
    const p = plan({
      table: "Chatbot",
      where: { op: "exists", relation: "organization" },
      include: [{ relation: "documents", include: ["folder"] }],
    })
    expect([...p.predicateTables]).toEqual(["organization"])
    expect([...p.tables].sort()).toEqual(["Chatbot", "organization"])
    expect(flattenIncludes(p).map((i) => i.path.join("/"))).toEqual([
      "documents",
      "documents/folder",
    ])
  })
})

describe("SQL compilation against SQLite", () => {
  const db = openDb(schema)
  upsertRow(db, rt, "organization", {
    id: "org_1",
    name: "Acme",
    created_at: "2026-01-01 00:00:00",
    hipaa_enabled: false,
  })
  upsertRow(db, rt, "Chatbot", chatbot("f1", { type: "GROUP", displayOrder: 1 }))
  upsertRow(
    db,
    rt,
    "Chatbot",
    chatbot("d1", {
      groupId: "f1",
      displayOrder: 2,
      score: 0.5,
      big: "9007199254740993",
      contents: { a: 1 },
    }),
  )
  upsertRow(db, rt, "Chatbot", chatbot("d2", { groupId: "f1", displayOrder: 1, score: 1.5 }))
  upsertRow(db, rt, "Chatbot", chatbot("d3", { displayOrder: 3, organizationId: "org_1" }))

  it("selects, filters, orders, limits and returns wire-shaped values", () => {
    const p = plan({
      table: "Chatbot",
      where: {
        op: "and",
        args: [
          { op: "eq", column: "type", value: "DOCUMENT" },
          { op: "isNotNull", column: "groupId" },
        ],
      },
      orderBy: [{ column: "displayOrder", direction: "asc" }],
      limit: 5,
    })
    const { sql, params } = compileSelect(p)
    const rows = db.prepare(sql).all(...params) as Array<Record<string, unknown>>
    expect(rows.map((r) => r["id"])).toEqual(["d2", "d1"])
    expect(rows[1]?.["big"]).toBe("9007199254740993")
    expect(rows[1]?.["__key"]).toBe('["d1"]')
  })

  it("includes related rows via declared relations", () => {
    const p = plan({
      table: "Chatbot",
      where: { op: "eq", column: "type", value: "DOCUMENT" },
      include: ["folder", "organization"],
    })
    const folder = p.includes.find((i) => i.relation.name === "folder")!
    const org = p.includes.find((i) => i.relation.name === "organization")!
    const folders = db
      .prepare(compileIncludeSelect(p, folder).sql)
      .all(...compileIncludeSelect(p, folder).params) as Array<Record<string, unknown>>
    expect(folders.map((r) => r["id"])).toEqual(["f1"])
    const orgs = db
      .prepare(compileIncludeSelect(p, org).sql)
      .all(...compileIncludeSelect(p, org).params) as Array<Record<string, unknown>>
    expect(orgs.map((r) => r["id"])).toEqual(["org_1"])
    const many = plan({
      table: "Chatbot",
      where: { op: "eq", column: "id", value: "f1" },
      include: ["documents"],
    })
    const docs = many.includes[0]!
    const docRows = db
      .prepare(compileIncludeSelect(many, docs).sql)
      .all(...compileIncludeSelect(many, docs).params) as Array<Record<string, unknown>>
    expect(docRows.map((r) => r["id"]).sort()).toEqual(["d1", "d2"])
  })

  it("restricts to a subscription's membership, following the based_on chain", () => {
    db.prepare(`INSERT INTO membership VALUES ('s1', 'Chatbot', '["d1"]')`).run()
    db.prepare(`INSERT INTO membership VALUES ('s2', 'Chatbot', '["d2"]')`).run()
    db.prepare(`INSERT INTO subscriptions VALUES ('s1', NULL), ('s2', 's1'), ('s3', 's2')`).run()
    const p = plan({ table: "Chatbot", orderBy: [{ column: "id", direction: "asc" }] })
    const read = (subscription: string) => {
      const { sql, params } = compileSelect(p, { membershipOf: subscription })
      return (db.prepare(sql).all(...params) as Array<Record<string, unknown>>).map((r) => r["id"])
    }
    expect(read("s1")).toEqual(["d1"])
    expect(read("s2")).toEqual(["d1", "d2"])
    expect(read("s3")).toEqual(["d1", "d2"])
  })

  it("finds the rows an include level references for one parent row", () => {
    const p = plan({
      table: "Chatbot",
      where: { op: "eq", column: "type", value: "DOCUMENT" },
      include: ["folder"],
    })
    const folder = p.includes[0]!
    const rows = (values: Array<string | null>) =>
      (
        db
          .prepare(
            compileRowsByColumns(p, folder.target, folder.relation.to_columns, values, undefined)
              .sql,
          )
          .all(
            ...compileRowsByColumns(p, folder.target, folder.relation.to_columns, values, undefined)
              .params,
          ) as Array<Record<string, unknown>>
      ).map((r) => r["id"])
    expect(rows(["f1"])).toEqual(["f1"])
    expect(rows(["zzz"])).toEqual([])
    // A NULL relation column references nothing.
    expect(rows([null])).toEqual([])
    // With a filter on the include, rows outside the filter are not referenced.
    const filtered = plan({
      table: "Chatbot",
      include: [{ relation: "folder", where: { op: "eq", column: "type", value: "DOCUMENT" } }],
    })
    const inc = filtered.includes[0]!
    const q = compileRowsByColumns(filtered, inc.target, inc.relation.to_columns, ["f1"], inc.where)
    expect(db.prepare(q.sql).all(...q.params)).toEqual([])
  })

  it("rejects bigints outside the signed 64-bit range instead of storing them lossily", () => {
    expect(() =>
      upsertRow(db, rt, "Chatbot", chatbot("big", { big: "18446744073709551615" })),
    ).toThrow(/64-bit/)
    upsertRow(db, rt, "Chatbot", chatbot("big", { big: "9223372036854775807" }))
    const p = plan({ table: "Chatbot", where: { op: "eq", column: "id", value: "big" } })
    const rows = db.prepare(compileSelect(p).sql).all(...compileSelect(p).params) as Array<
      Record<string, unknown>
    >
    expect(rows[0]?.["big"]).toBe("9223372036854775807")
    deleteRow(db, "Chatbot", '["big"]')
  })

  it("deleting a row removes it from results", () => {
    deleteRow(db, "Chatbot", '["d3"]')
    const p = plan({ table: "Chatbot" })
    const rows = db.prepare(compileSelect(p).sql).all(...compileSelect(p).params) as Array<
      Record<string, unknown>
    >
    expect(rows.map((r) => r["id"])).toEqual(["d1", "d2", "f1"])
  })
})

/**
 * Incremental maintenance evaluates the predicate over one stored row by key (`compileWhere`
 * over alias `t`). It must agree with the full select, otherwise a change could be classified
 * wrongly. Randomized comparison over generated predicates and rows.
 */
describe("predicate over one row agrees with the full select", () => {
  const db = openDb(schema)
  let seed = 12345
  const rand = (): number => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff
    return seed / 0x7fffffff
  }
  const pick = <T>(xs: ReadonlyArray<T>): T => xs[Math.floor(rand() * xs.length)]!
  const strings = ["", "a", "b", "abc", "ABC", "a%", "日本", "zz", "a_c"]
  const numbers = [0, 1, -1, 2.5, 100, -7]
  const genRow = (id: string): RowImage =>
    chatbot(id, {
      groupId: rand() < 0.3 ? null : pick(strings),
      type: pick(["DOCUMENT", "GROUP"]),
      displayOrder: rand() < 0.3 ? null : pick([0, 1, -1, 100, -7]),
      score: rand() < 0.3 ? null : pick(numbers),
      big: rand() < 0.3 ? null : pick(["1", "-5", "9007199254740993", "-9223372036854775808"]),
      createdAt: pick(["2026-01-01 00:00:00", "2026-02-01 00:00:00", "2025-12-31 23:59:59"]),
    })
  const genPredicate = (depth: number): Predicate => {
    const r = rand()
    if (depth < 2 && r < 0.2)
      return { op: "and", args: [genPredicate(depth + 1), genPredicate(depth + 1)] }
    if (depth < 2 && r < 0.35)
      return { op: "or", args: [genPredicate(depth + 1), genPredicate(depth + 1)] }
    if (depth < 2 && r < 0.45) return { op: "not", arg: genPredicate(depth + 1) }
    const column = pick(["groupId", "type", "displayOrder", "score", "big", "createdAt"] as const)
    const kind = rt.column("Chatbot", column)!.kind
    const op = pick([
      "eq",
      "ne",
      "lt",
      "lte",
      "gt",
      "gte",
      "in",
      "isNull",
      "isNotNull",
      "like",
    ] as const)
    if (op === "isNull" || op === "isNotNull") return { op, column }
    if (op === "like")
      return kind === "string"
        ? { op, column, pattern: pick(["a%", "%b%", "_", "%", "日%", "A%"]) }
        : { op: "isNull", column }
    const value =
      kind === "string" || kind === "datetime"
        ? pick(kind === "datetime" ? ["2026-01-01 00:00:00", "2026-01-15 00:00:00"] : strings)
        : kind === "bigint"
          ? pick(["1", "-5", "9007199254740993"])
          : kind === "int"
            ? pick([0, 1, -1, 100, -7])
            : pick(numbers)
    if (op === "in")
      return {
        op,
        column,
        values: [
          value,
          kind === "string"
            ? "zz"
            : kind === "datetime"
              ? "2026-03-01 00:00:00"
              : kind === "bigint"
                ? "2"
                : 100,
        ],
      }
    return { op, column, value }
  }

  it("holds for 500 random predicate/row pairs", () => {
    for (let i = 0; i < 500; i++) {
      const row = genRow(`r${i}`)
      upsertRow(db, rt, "Chatbot", row)
      const predicate = genPredicate(0)
      const p = plan({ table: "Chatbot", where: predicate })
      const viaTable = (
        db.prepare(compileSelect(p).sql).all(...compileSelect(p).params) as Array<{ id: string }>
      ).some((r) => r.id === row["id"])
      const where = compileWhere(p, p.table, predicate, "t")
      const viaKey =
        db
          .prepare(`SELECT 1 AS x FROM "t_Chatbot" t WHERE t."__key" = ? AND ${where.sql}`)
          .get(`["r${i}"]`, ...where.params) !== undefined
      expect(viaKey, `predicate ${JSON.stringify(predicate)} row ${JSON.stringify(row)}`).toBe(
        viaTable,
      )
      deleteRow(db, "Chatbot", `["r${i}"]`)
    }
  })
})

describe("relation predicates and nested includes", () => {
  const db = openDb(schema)
  upsertRow(db, rt, "organization", {
    id: "org_1",
    name: "Acme",
    created_at: "2026-01-01 00:00:00",
    hipaa_enabled: false,
  })
  upsertRow(db, rt, "Chatbot", chatbot("f1", { type: "GROUP", displayOrder: 1 }))
  upsertRow(db, rt, "Chatbot", chatbot("f2", { type: "GROUP", displayOrder: 2 }))
  upsertRow(db, rt, "Chatbot", chatbot("d1", { groupId: "f1", displayOrder: 1, score: 2 }))
  upsertRow(db, rt, "Chatbot", chatbot("d2", { groupId: "f1", displayOrder: 2, score: 0.5 }))
  upsertRow(db, rt, "Chatbot", chatbot("d3", { groupId: "f2", displayOrder: 3, score: 0.1 }))

  const all = (c: { sql: string; params: ReadonlyArray<unknown> }) =>
    db.prepare(c.sql).all(...(c.params as Array<string | number | null>)) as Array<
      Record<string, unknown>
    >

  it("filters on related rows with exists and not exists", () => {
    const withHighScore = plan({
      table: "Chatbot",
      where: {
        op: "and",
        args: [
          { op: "eq", column: "type", value: "GROUP" },
          { op: "exists", relation: "documents", where: { op: "gt", column: "score", value: 1 } },
        ],
      },
    })
    expect(all(compileSelect(withHighScore)).map((r) => r["id"])).toEqual(["f1"])
    const withoutDocs = plan({
      table: "Chatbot",
      where: {
        op: "and",
        args: [
          { op: "eq", column: "type", value: "GROUP" },
          { op: "not", arg: { op: "exists", relation: "documents" } },
        ],
      },
    })
    expect(all(compileSelect(withoutDocs)).map((r) => r["id"])).toEqual([])
    const nested = plan({
      table: "Chatbot",
      where: {
        op: "exists",
        relation: "folder",
        where: {
          op: "exists",
          relation: "documents",
          where: { op: "eq", column: "id", value: "d3" },
        },
      },
    })
    expect(all(compileSelect(nested)).map((r) => r["id"])).toEqual(["d3"])
  })

  it("walks exists chains back to the primary rows a related change can affect", () => {
    const p = plan({
      table: "Chatbot",
      where: {
        op: "exists",
        relation: "documents",
        where: { op: "gt", column: "score", value: 1 },
      },
    })
    const chains = predicateChains(p)
    expect(chains.map((c) => c.map((r) => r.name))).toEqual([["documents"]])
    // A change to d3 (groupId f2) can only affect f2.
    const viaDoc = compileChainCandidates(p, chains[0]!, chatbot("d3", { groupId: "f2" }))
    expect(all(viaDoc).map((r) => r["id"])).toEqual(["f2"])
    // A document without a folder affects no primary row.
    const none = compileChainCandidates(p, chains[0]!, chatbot("dx", { groupId: null }))
    expect(all(none)).toEqual([])
    // Depth 2: a change to a document reaches, through its folder, every document of that folder.
    const nested = plan({
      table: "Chatbot",
      where: {
        op: "exists",
        relation: "folder",
        where: {
          op: "exists",
          relation: "documents",
          where: { op: "eq", column: "id", value: "d3" },
        },
      },
    })
    const deep = predicateChains(nested)
    expect(deep.map((c) => c.map((r) => r.name))).toEqual([["folder"], ["folder", "documents"]])
    const viaFolder = compileChainCandidates(nested, deep[1]!, chatbot("d1", { groupId: "f1" }))
    expect(all(viaFolder).map((r) => r["id"])).toEqual(["d1", "d2"])
  })

  it("compiles nested and filtered includes, and attaches them", () => {
    const p = plan({
      table: "Chatbot",
      where: { op: "eq", column: "type", value: "GROUP" },
      include: [
        {
          relation: "documents",
          where: { op: "gte", column: "score", value: 0.5 },
          include: ["organization"],
        },
      ],
    })
    const primary = all(compileSelect(p)).map((r) => ({
      key: String(r["__key"]),
      row: rowOf(p, "Chatbot", r),
    }))
    const byPath = new Map<string, Array<{ key: string; row: RowImage }>>()
    for (const inc of flattenIncludes(p)) {
      byPath.set(
        inc.path.join("/"),
        all(compileIncludeSelect(p, inc)).map((r) => ({
          key: String(r["__key"]),
          row: rowOf(p, inc.target.name, r),
        })),
      )
    }
    expect(byPath.get("documents")?.map((r) => r.row["id"])).toEqual(["d1", "d2"])
    expect(byPath.get("documents/organization")?.map((r) => r.row["id"])).toEqual(["org_1"])
    const nodes = attachIncludes(p, primary, byPath)
    const flat = nodes.map(flattenNode) as Array<{
      id: string
      documents: Array<{ id: string; organization: { id: string } | null }>
    }>
    expect(flat.map((f) => f.id)).toEqual(["f1", "f2"])
    expect(flat[0]?.documents.map((d) => d.id)).toEqual(["d1", "d2"])
    expect(flat[0]?.documents[0]?.organization?.id).toBe("org_1")
    expect(flat[1]?.documents).toEqual([])
    // Rows of a depth-2 level for one parent row: the organization of document d1.
    const deep = flattenIncludes(p)[1]!
    const orgs = compileRowsByColumns(
      p,
      deep.target,
      deep.relation.to_columns,
      ["org_1"],
      deep.where,
    )
    expect(all(orgs).map((r) => r["id"])).toEqual(["org_1"])
    const missing = compileRowsByColumns(
      p,
      deep.target,
      deep.relation.to_columns,
      ["org_2"],
      deep.where,
    )
    expect(all(missing)).toEqual([])
  })

  it("computes derived columns from their source in MySQL and reads them stored in SQLite", () => {
    const withDerived: typeof schema = {
      ...schema,
      tables: schema.tables.map((t) =>
        t.name !== "Chatbot"
          ? t
          : {
              ...t,
              columns: [
                ...t.columns,
                {
                  name: "hasContents",
                  kind: "bool",
                  nullable: false,
                  source_type: "derived",
                  derived: { from: "contents", rule: { kind: "not_null" } },
                },
                {
                  name: "isGroupType",
                  kind: "bool",
                  nullable: false,
                  source_type: "derived",
                  derived: { from: "type", rule: { kind: "starts_with", prefix: "GR'OU\\P" } },
                },
              ],
            },
      ),
    }
    const drt = new SchemaRuntime(withDerived)
    const planned = planQuery(drt, {
      table: "Chatbot",
      where: { op: "eq", column: "hasContents", value: true },
    })
    if (Result.isFailure(planned)) throw new Error(planned.failure.message)
    const mysql = compileSelect(planned.success, { dialect: mysqlDialect })
    expect(mysql.sql).toContain("(t.`contents` IS NOT NULL) AS `hasContents`")
    expect(mysql.sql).toContain(
      "(t.`type` IS NOT NULL AND LEFT(t.`type`, 7) = 'GR''OU\\\\P') AS `isGroupType`",
    )
    expect(mysql.sql).toContain("WHERE (t.`hasContents` = ?)")
    const sqlite = compileSelect(planned.success)
    expect(sqlite.sql).toContain('t."hasContents", t."isGroupType" FROM')
    expect(sqlite.sql).not.toContain("IS NOT NULL) AS")
  })

  it("orders text columns case-insensitively in the cache only", () => {
    const p = plan({
      table: "Chatbot",
      orderBy: [
        { column: "type", direction: "asc" },
        { column: "displayOrder", direction: "desc" },
      ],
    })
    expect(compileSelect(p).sql).toContain(
      'ORDER BY t."type" COLLATE NOCASE ASC, t."displayOrder" DESC',
    )
    expect(compileSelect(p, { dialect: mysqlDialect }).sql).toContain(
      "ORDER BY t.`type` ASC, t.`displayOrder` DESC",
    )
  })

  it("compiles the MySQL dialect without engine columns", () => {
    const p = plan({
      table: "Chatbot",
      where: {
        op: "and",
        args: [
          { op: "eq", column: "type", value: "GROUP" },
          { op: "exists", relation: "documents", where: { op: "gt", column: "big", value: "5" } },
        ],
      },
      limit: 3,
    })
    const c = compileSelect(p, { dialect: mysqlDialect })
    expect(c.sql).toBe(
      "SELECT t.`id`, t.`organizationId`, t.`groupId`, t.`type`, t.`displayOrder`, t.`contents`, t.`createdAt`, t.`score`, t.`big`, t.`price`, t.`blob`, t.`day`, t.`at` FROM `Chatbot` t WHERE ((t.`type` = ?) AND EXISTS (SELECT 1 FROM `Chatbot` r0 WHERE t.`id` = r0.`groupId` AND (r0.`big` > ?))) ORDER BY t.`id` ASC LIMIT 3",
    )
    expect(c.params).toEqual(["GROUP", 5])
    expect(() => compileSelect(p, { dialect: mysqlDialect, membershipOf: "s" })).toThrow(
      /membership/,
    )
  })
})

const rowOf = (p: PlannedQuery, table: string, r: Record<string, unknown>): RowImage => {
  const t = p.rt.table(table)!
  const out: Record<string, RowImage[string]> = {}
  for (const c of t.columns) out[c.name] = (r[c.name] ?? null) as RowImage[string]
  return out
}

describe("named queries", () => {
  const definition = { _tag: "SyncSchemaDefinition" } as const
  const define = defineQuery(definition)
  const queries = defineQueries(definition, {
    folders: define(
      Schema.Struct({ withDocuments: Schema.Boolean }),
      // The fixture has no typed definition, so the query is built from its AST directly.
      (ctx, args) =>
        new TypedQuery<typeof definition, "Chatbot">({
          table: "Chatbot",
          where: {
            op: "and",
            args: [
              { op: "eq", column: "organizationId", value: ctx.partition },
              ...(args.withDocuments ? [{ op: "exists" as const, relation: "documents" }] : []),
            ],
          },
        }),
    ),
  })

  it("binds arguments, resolves locally, and resolves on the server with its own context", () => {
    const call = queries.folders({ withDocuments: true })
    expect(call.ref).toEqual({ name: "folders", args: { withDocuments: true } })
    const ctx = { partition: "org_1", subject: "alice", clientId: null }
    expect(call.resolve(ctx).ast.where).toEqual({
      op: "and",
      args: [
        { op: "eq", column: "organizationId", value: "org_1" },
        { op: "exists", relation: "documents" },
      ],
    })
    const resolved = resolveNamedQuery(queries, call.ref, { ...ctx, partition: "org_2" })
    expect(Result.isSuccess(resolved) && resolved.success.where).toEqual({
      op: "and",
      args: [
        { op: "eq", column: "organizationId", value: "org_2" },
        { op: "exists", relation: "documents" },
      ],
    })
    const unknown = resolveNamedQuery(queries, { name: "nope", args: {} }, ctx)
    expect(Result.isFailure(unknown) && unknown.failure.reason).toBe("unknown_query")
    const bad = resolveNamedQuery(queries, { name: "folders", args: { withDocuments: 1 } }, ctx)
    expect(Result.isFailure(bad) && bad.failure.reason).toBe("invalid_args")
  })
})
