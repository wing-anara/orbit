import * as fs from "node:fs"
import * as path from "node:path"
import { fileURLToPath } from "node:url"

import { Effect } from "effect"
import { describe, expect, it } from "vitest"

import {
  canonicalJson,
  compileSyncSchema,
  decodeRowSync,
  defineSyncSchema,
  planMigration,
  rowCodecFor,
  SchemaRuntime,
  compatibility,
} from "../src/index.ts"
import type { RowOf } from "../src/index.ts"

const here = path.dirname(fileURLToPath(import.meta.url))
const fixtureDir = path.resolve(here, "../../../schema/fixtures")

/** Introspection input that produces the Rust fixture `SyncSchema.json`. */
const introspected = {
  keyspace: "ks",
  server_version: "8.0.43-Vitess",
  tables: [
    {
      name: "organization",
      primary_key: ["id"],
      columns: [
        {
          name: "id",
          column_type: "varchar(191)",
          data_type: "varchar",
          nullable: false,
          kind: "string",
        },
        { name: "name", column_type: "text", data_type: "text", nullable: false, kind: "string" },
        {
          name: "created_at",
          column_type: "timestamp",
          data_type: "timestamp",
          nullable: false,
          kind: "datetime",
        },
        {
          name: "hipaa_enabled",
          column_type: "tinyint(1)",
          data_type: "tinyint",
          nullable: false,
          kind: "bool",
        },
        { name: "ignored", column_type: "text", data_type: "text", nullable: true, kind: "string" },
      ],
    },
    {
      name: "Chatbot",
      primary_key: ["id"],
      columns: [
        {
          name: "id",
          column_type: "varchar(191)",
          data_type: "varchar",
          nullable: false,
          kind: "string",
        },
        {
          name: "organizationId",
          column_type: "varchar(191)",
          data_type: "varchar",
          nullable: true,
          kind: "string",
        },
        {
          name: "groupId",
          column_type: "varchar(191)",
          data_type: "varchar",
          nullable: true,
          kind: "string",
        },
        {
          name: "type",
          column_type: "enum('DOCUMENT','GROUP')",
          data_type: "enum",
          nullable: false,
          kind: "string",
          enum_values: ["DOCUMENT", "GROUP"],
        },
        { name: "displayOrder", column_type: "int", data_type: "int", nullable: true, kind: "int" },
        { name: "contents", column_type: "json", data_type: "json", nullable: true, kind: "json" },
        {
          name: "createdAt",
          column_type: "datetime(3)",
          data_type: "datetime",
          nullable: false,
          kind: "datetime",
        },
        {
          name: "score",
          column_type: "double",
          data_type: "double",
          nullable: true,
          kind: "float",
        },
        {
          name: "big",
          column_type: "bigint unsigned",
          data_type: "bigint",
          nullable: true,
          kind: "bigint",
        },
        {
          name: "price",
          column_type: "decimal(10,2)",
          data_type: "decimal",
          nullable: true,
          kind: "decimal",
        },
        { name: "blob", column_type: "blob", data_type: "blob", nullable: true, kind: "bytes" },
        { name: "day", column_type: "date", data_type: "date", nullable: true, kind: "date" },
        { name: "at", column_type: "time(3)", data_type: "time", nullable: true, kind: "time" },
      ],
    },
    {
      name: "NotSynced",
      primary_key: ["id"],
      columns: [{ name: "id", column_type: "int", data_type: "int", nullable: false, kind: "int" }],
    },
  ],
} as const

const definition = defineSyncSchema({
  app: "fixture",
  introspected,
  partition: { name: "org", kind: "string" },
  tables: {
    organization: { partitionBy: "id", columns: ["id", "name", "created_at", "hipaa_enabled"] },
    Chatbot: {
      partitionBy: "organizationId",
      relations: {
        folder: { kind: "one", to: "Chatbot", from: ["groupId"], toColumns: ["id"] },
        organization: {
          kind: "one",
          to: "organization",
          from: ["organizationId"],
          toColumns: ["id"],
        },
        documents: { kind: "many", to: "Chatbot", from: ["id"], toColumns: ["groupId"] },
      },
    },
  },
})

type ChatbotRow = RowOf<typeof definition, "Chatbot">
type OrganizationRow = RowOf<typeof definition, "organization">

describe("compileSyncSchema", () => {
  it("produces the same artifact and hash as the Rust crate", async () => {
    const artifact = await Effect.runPromise(compileSyncSchema(definition))
    const expected: unknown = JSON.parse(
      fs.readFileSync(path.join(fixtureDir, "SyncSchema.json"), "utf8"),
    )
    const expectedHash = fs.readFileSync(path.join(fixtureDir, "SyncSchema.hash"), "utf8").trim()
    expect(artifact.schema_hash).toBe(expectedHash)
    expect(artifact).toEqual(expected)
  })

  it("derives row types from the definition", () => {
    const row: ChatbotRow = {
      id: "c1",
      organizationId: null,
      groupId: "g",
      type: "DOCUMENT",
      displayOrder: 1,
      contents: { a: [1] },
      createdAt: "2026-01-01 00:00:00",
      score: 1.5,
      big: "1",
      price: "1.00",
      blob: "AA==",
      day: "2026-01-01",
      at: "00:00:00",
    }
    const org: OrganizationRow = { id: "o", name: "n", created_at: "x", hipaa_enabled: false }
    // @ts-expect-error `ignored` is not a synced column
    const bad: OrganizationRow = { ...org, ignored: "x" }
    // @ts-expect-error wrong kind
    const bad2: ChatbotRow = { ...row, displayOrder: "1" }
    expect([row, org, bad, bad2].length).toBe(4)
  })

  it("rejects definitions that reference unknown tables, columns or non-key relations", async () => {
    const broken = defineSyncSchema({
      app: "x",
      introspected,
      partition: { name: "org", kind: "string" },
      tables: {
        Chatbot: {
          // @ts-expect-error not a column
          partitionBy: "nope",
          relations: {
            r: { kind: "one", to: "Chatbot", from: ["groupId"], toColumns: ["groupId"] },
          },
        },
      },
    })
    const result = await Effect.runPromise(Effect.result(compileSyncSchema(broken)))
    expect(result._tag).toBe("Failure")
    if (result._tag === "Failure") {
      expect(result.failure.problems.some((p) => p.includes("partition column nope"))).toBe(true)
      expect(result.failure.problems.some((p) => p.includes("full primary key"))).toBe(true)
    }
  })

  it("canonical json sorts keys like serde", () => {
    expect(canonicalJson({ b: [1, { z: 1, a: "x" }], a: null })).toBe(
      '{"a":null,"b":[1,{"a":"x","z":1}]}',
    )
  })
})

describe("runtime", () => {
  it("row codecs validate cells per kind", async () => {
    const artifact = await Effect.runPromise(compileSyncSchema(definition))
    const rt = new SchemaRuntime(artifact)
    const decode = decodeRowSync(rowCodecFor(rt.table("Chatbot")!))
    const ok = {
      id: "c",
      organizationId: "o",
      groupId: null,
      type: "DOCUMENT",
      displayOrder: 2,
      contents: null,
      createdAt: "x",
      score: null,
      big: "123",
      price: null,
      blob: null,
      day: null,
      at: null,
    }
    expect(decode(ok)).toEqual(ok)
    expect(() => decode({ ...ok, displayOrder: "2" })).toThrow()
    expect(() => decode({ ...ok, big: "12a" })).toThrow()
    expect(() => decode({ ...ok, extra: 1 })).toThrow()
    expect(() => decode({ ...ok, createdAt: null })).toThrow()
    expect(() => decode({ ...ok, id: undefined })).toThrow()
    expect(rt.keyOf(rt.table("Chatbot")!, ok)).toEqual(["c"])
  })

  it("additive compatibility", async () => {
    const artifact = await Effect.runPromise(compileSyncSchema(definition))
    const rt = new SchemaRuntime(artifact)
    const same = compatibility(artifact, rt.summary())
    expect(same).toEqual({ compatible: true, identical: true })
    const older = {
      schemaHash: "old",
      tables: [{ name: "Chatbot", columns: ["id", "organizationId"] }],
    }
    expect(compatibility(artifact, older)).toEqual({ compatible: true, identical: false })
    const newer = { schemaHash: "new", tables: [{ name: "Chatbot", columns: ["id", "brandNew"] }] }
    expect(compatibility(artifact, newer)).toMatchObject({ compatible: false })
  })

  it("derived columns compile to non-nullable bools with their rule and type as booleans", async () => {
    const withDerived = defineSyncSchema({
      app: "fixture",
      introspected,
      partition: { name: "org", kind: "string" },
      tables: {
        Chatbot: {
          partitionBy: "organizationId",
          columns: ["id", "organizationId", "type"],
          derived: {
            hasContents: { from: "contents", rule: { kind: "not_null" } },
            isDocument: { from: "type", rule: { kind: "starts_with", prefix: "DOC" } },
          },
        },
      },
    })
    const artifact = await Effect.runPromise(compileSyncSchema(withDerived))
    const table = artifact.tables.find((t) => t.name === "Chatbot")
    expect(table?.columns.map((c) => c.name)).toEqual([
      "id",
      "organizationId",
      "type",
      "hasContents",
      "isDocument",
    ])
    expect(table?.columns.find((c) => c.name === "hasContents")).toEqual({
      name: "hasContents",
      kind: "bool",
      nullable: false,
      source_type: "derived",
      derived: { from: "contents", rule: { kind: "not_null" } },
    })
    // The source column is not synced: only the booleans reach the cache.
    expect(table?.columns.some((c) => c.name === "contents")).toBe(false)
    type Row = RowOf<typeof withDerived, "Chatbot">
    const row: Row = {
      id: "c1",
      organizationId: "o",
      type: "DOCUMENT",
      hasContents: true,
      isDocument: false,
    }
    // @ts-expect-error a derived column is a boolean
    const bad: Row = { ...row, hasContents: "yes" }
    expect([row, bad].length).toBe(2)

    const broken = defineSyncSchema({
      app: "fixture",
      introspected,
      partition: { name: "org", kind: "string" },
      tables: {
        Chatbot: {
          partitionBy: "organizationId",
          derived: {
            type: { from: "contents", rule: { kind: "not_null" } },
            // @ts-expect-error not a column
            missing: { from: "nope", rule: { kind: "not_null" } },
          },
        },
      },
    })
    const result = await Effect.runPromise(Effect.result(compileSyncSchema(broken)))
    expect(result._tag).toBe("Failure")
    if (result._tag === "Failure") {
      expect(result.failure.problems).toContain(
        "table Chatbot: derived column type has the name of a source column",
      )
      expect(result.failure.problems).toContain(
        "table Chatbot: derived column missing reads unknown column nope",
      )
    }
  })

  it("migration planning", async () => {
    const artifact = await Effect.runPromise(compileSyncSchema(definition))
    expect(planMigration(artifact, [], null).action).toBe("create")
    expect(
      planMigration(artifact, [{ name: "t_Chatbot", columns: ["id"] }], artifact.schema_hash)
        .action,
    ).toBe("none")
    const existing = artifact.tables.map((t) => ({
      name: `t_${t.name}`,
      columns: t.columns.filter((c) => c.name !== "score").map((c) => c.name),
    }))
    const plan = planMigration(artifact, existing, "old")
    expect(plan.action).toBe("additive")
    if (plan.action === "additive") expect(plan.addedColumns).toEqual(["Chatbot.score"])
    const withExtra = artifact.tables.map((t) => ({
      name: `t_${t.name}`,
      columns: [...t.columns.map((c) => c.name), "gone"],
    }))
    expect(planMigration(artifact, withExtra, "old").action).toBe("reset")
  })
})
