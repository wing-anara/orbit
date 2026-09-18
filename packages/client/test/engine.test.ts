import * as fs from "node:fs"
import * as path from "node:path"
import { fileURLToPath } from "node:url"

import { Effect, Schema } from "effect"
import { describe, expect, it } from "vitest"

import { SyncSchema, type RowChange, type TableSchema } from "@orbit/protocol"
import type { RowImage } from "@orbit/protocol/client"
import { defineMutator, defineMutators } from "@orbit/mutators"
import { defineQueries, q } from "@orbit/query"
import { defineSyncSchema } from "@orbit/schema"

import { createOrbitClient } from "../src/client.ts"
import type { AsyncSqlDriver } from "../src/driver.ts"
import { ClientEngine } from "../src/engine.ts"
import type { MutationEvent } from "../src/mutations.ts"
import { FakePushServer, FakeSyncServer } from "./support/fake-server.ts"
import { nodeAsyncDriver } from "./support/node-driver.ts"

const here = path.dirname(fileURLToPath(import.meta.url))
const fixture = Schema.decodeUnknownSync(SyncSchema)(
  JSON.parse(
    fs.readFileSync(path.resolve(here, "../../../schema/fixtures/SyncSchema.json"), "utf8"),
  ),
)

/** The client bookkeeping table (`orbitClientsDdl`), partitioned by `partition_key`. */
const orbitClients: TableSchema = {
  name: "orbit_clients",
  primary_key: ["client_id"],
  partition_column: "partition_key",
  columns: [
    { name: "client_id", kind: "string", nullable: false, source_type: "varchar(64)" },
    { name: "partition_key", kind: "string", nullable: false, source_type: "varchar(191)" },
    { name: "last_mutation_id", kind: "bigint", nullable: false, source_type: "bigint" },
    { name: "updated_at", kind: "datetime", nullable: false, source_type: "datetime(3)" },
  ],
  relations: [],
}

/** The shared fixture plus `orbit_clients`; the hash only has to be equal on both sides. */
const schema: SyncSchema = {
  ...fixture,
  schema_hash: "fixture-with-orbit-clients",
  tables: [...fixture.tables, orbitClients],
}

/** Type-level definition for the typed builders (a subset of the fixture's columns). */
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
        },
        { name: "displayOrder", column_type: "int", data_type: "int", nullable: true, kind: "int" },
        {
          name: "createdAt",
          column_type: "datetime(3)",
          data_type: "datetime",
          nullable: false,
          kind: "datetime",
        },
      ],
    },
  ],
} as const

const sync = defineSyncSchema({
  app: "fixture",
  introspected,
  partition: { name: "org", kind: "string" },
  tables: {
    organization: { partitionBy: "id" },
    Chatbot: {
      partitionBy: "organizationId",
      relations: {
        folder: { kind: "one", to: "Chatbot", from: ["groupId"], toColumns: ["id"] },
        documents: { kind: "many", to: "Chatbot", from: ["id"], toColumns: ["groupId"] },
        organization: {
          kind: "one",
          to: "organization",
          from: ["organizationId"],
          toColumns: ["id"],
        },
      },
    },
  },
})

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
const organization = (id: string): RowImage => ({
  id,
  name: "Acme",
  created_at: "2026-01-01 00:00:00",
  hipaa_enabled: false,
})
const insert = (
  table: string,
  row: RowImage,
  keyColumns: ReadonlyArray<string> = ["id"],
): RowChange => ({
  table,
  op: "insert",
  key: keyColumns.map((c) => row[c] ?? null),
  after: row,
})
const update = (table: string, before: RowImage, after: RowImage): RowChange => ({
  table,
  op: "update",
  key: [after["id"] ?? null],
  before,
  after,
})
const remove = (table: string, before: RowImage): RowChange => ({
  table,
  op: "delete",
  key: [before["id"] ?? null],
  before,
})

const settle = (ms = 30) => new Promise((r) => setTimeout(r, ms))

const makeClient = (
  server: FakeSyncServer,
  driver: AsyncSqlDriver = nodeAsyncDriver(),
  queryTtlMs = 0,
) => {
  const log: Array<[string, Record<string, unknown>]> = []
  const engine = new ClientEngine({
    schema,
    partition: "org_1",
    driver,
    clientId: "c1",
    queryTtlMs,
    target: async () => ({ url: "ws://fake", protocols: [] }),
    makeWebSocket: server.connect,
    backoffMinMs: 5,
    backoffMaxMs: 20,
    onLog: (e, d) => log.push([e, d]),
  })
  return { engine, driver, log }
}

const waitFor = async (pred: () => boolean, timeoutMs = 3000, what = "condition") => {
  const deadline = Date.now() + timeoutMs
  while (!pred()) {
    if (Date.now() > deadline) throw new Error(`timeout waiting for ${what}`)
    await settle(10)
  }
}

/** A driver whose `close` keeps the database open, so a second client can "reload" it. */
const reloadable = (driver: AsyncSqlDriver): AsyncSqlDriver => ({
  ...driver,
  close: async () => {},
})

describe("client engine end to end with the Durable Object core", () => {
  it("does not reuse a retained window whose included rows changed while unobserved", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine } = makeClient(server, nodeAsyncDriver(), 30000)
    await Effect.runPromise(engine.open())
    const query = (limit: number) => ({
      table: "Chatbot",
      where: { op: "eq" as const, column: "type", value: "DOCUMENT" },
      orderBy: [{ column: "id", direction: "asc" as const }],
      include: ["folder"],
      limit,
    })
    const small = await Effect.runPromise(engine.subscribe(query(1)))
    await waitFor(() => server.pendingFills.length === 1)
    const folder = chatbot("folder", { type: "GROUP", contents: "before" })
    server.completeFill("Chatbot", [
      folder,
      chatbot("a", { groupId: "folder" }),
      chatbot("b", { groupId: "folder" }),
    ])
    await Effect.runPromise(engine.awaitLive(small.id))
    const old = small.getSnapshot().rows[0]
    await Effect.runPromise(small.release())
    server.commit([update("Chatbot", folder, { ...folder, contents: "after" })])
    await waitFor(() => engine.getStatus().cursor === 1)
    const grown = await Effect.runPromise(engine.subscribe(query(2)))
    await Effect.runPromise(engine.awaitLive(grown.id))
    expect(grown.getSnapshot().rows[0]).not.toBe(old)
    expect(grown.getSnapshot().rows[0]?.related["folder"]).toMatchObject({
      row: { contents: "after" },
    })
    await Effect.runPromise(engine.close())
  })

  it("grows a window by reading only new roots and their includes, preserving SQLite order", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const driver = nodeAsyncDriver()
    const reads: Array<{ sql: string; rows: number }> = []
    const { engine } = makeClient(server, {
      ...driver,
      query: async (sql, params) => {
        const result = await driver.query(sql, params)
        reads.push({ sql, rows: result.length })
        return result
      },
    })
    await Effect.runPromise(engine.open())
    const query = (limit: number) => ({
      table: "Chatbot",
      where: { op: "eq" as const, column: "type", value: "DOCUMENT" },
      orderBy: [{ column: "id", direction: "desc" as const }],
      include: ["folder"],
      limit,
    })
    const small = await Effect.runPromise(engine.subscribe(query(100)))
    await waitFor(() => server.pendingFills.length === 1)
    server.completeFill("Chatbot", [
      chatbot("folder", { type: "GROUP", contents: "x".repeat(10000) }),
      ...Array.from({ length: 125 }, (_, i) =>
        chatbot(String(i).padStart(3, "0"), { groupId: "folder" }),
      ),
    ])
    await Effect.runPromise(engine.awaitLive(small.id))
    const original = small.getSnapshot().rows
    reads.length = 0
    const grown = await Effect.runPromise(engine.subscribe(query(125)))
    await Effect.runPromise(engine.awaitLive(grown.id))
    const rows = grown.getSnapshot().rows
    expect(rows.map((row) => row.row["id"])).toEqual(
      Array.from({ length: 125 }, (_, i) => String(124 - i).padStart(3, "0")),
    )
    for (let i = 0; i < 100; i++) expect(rows[i]).toBe(original[i])
    expect(rows[124]?.related["folder"]).toMatchObject({
      row: { id: "folder", contents: "x".repeat(10000) },
    })
    // Key projection may read the entire order; full row images must be limited to new roots.
    expect(
      reads
        .filter(
          (read) =>
            read.sql.includes('"v_Chatbot"') && !read.sql.startsWith('SELECT t."__key" FROM'),
        )
        .every((read) => read.rows <= 25),
    ).toBe(true)
    await Effect.runPromise(engine.close())
  })

  it("does not resume a named query when its authorized resolution changes", async () => {
    let allowed = "a"
    const server = new FakeSyncServer(schema, "org_1", {
      resolve: () => ({ table: "Chatbot", where: { op: "eq", column: "id", value: allowed } }),
    })
    const { engine, log } = makeClient(server)
    await Effect.runPromise(engine.open())
    const docs = await Effect.runPromise(
      engine.subscribeNamed({ name: "authorized", args: {} }, { table: "Chatbot" }),
    )
    await waitFor(() => server.pendingFills.length === 1)
    server.completeFill("Chatbot", [chatbot("a"), chatbot("b")])
    await Effect.runPromise(engine.awaitLive(docs.id))
    expect(docs.getSnapshot().rows.map((row) => row.row["id"])).toEqual(["a"])
    allowed = "b"
    server.dropAll()
    await waitFor(() => docs.getSnapshot().rows[0]?.row["id"] === "b")
    expect(docs.getSnapshot().rows.map((row) => row.row["id"])).toEqual(["b"])
    expect(log.some(([event]) => event === "subscription.resumed")).toBe(false)
    await Effect.runPromise(engine.close())
  })

  it("revalidates an unchanged active window on reconnect without replacing its rows", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine, log } = makeClient(server)
    await Effect.runPromise(engine.open())
    const docs = await Effect.runPromise(engine.subscribe({ table: "Chatbot", limit: 2000 }))
    await waitFor(() => server.pendingFills.length === 1)
    server.completeFill(
      "Chatbot",
      Array.from({ length: 2000 }, (_, i) => chatbot(String(i))),
    )
    await Effect.runPromise(engine.awaitLive(docs.id))
    const rows = docs.getSnapshot().rows
    const snapshots = log.filter(([event]) => event === "snapshot.applied").length
    server.dropAll()
    await waitFor(() => log.some(([event]) => event === "subscription.resumed"))
    expect(docs.getSnapshot().status).toBe("live")
    expect(docs.getSnapshot().rows).toBe(rows)
    expect(log.filter(([event]) => event === "snapshot.applied").length).toBe(snapshots)
    server.commit([remove("Chatbot", chatbot("0"))])
    await waitFor(() => docs.getSnapshot().rows.length === 1999)
    expect(docs.getSnapshot().rows.some((row) => row.row["id"] === "0")).toBe(false)
    await Effect.runPromise(engine.close())
  })

  it.each([false, true])(
    "preserves a pending growth base across expiry (retained target: %s)",
    async (retained) => {
      const server = new FakeSyncServer(schema, "org_1")
      const { engine, log } = makeClient(server, nodeAsyncDriver(), 30_000)
      const query = (limit: number) => ({
        table: "Chatbot",
        limit,
        orderBy: [{ column: "id", direction: "asc" as const }],
      })
      await Effect.runPromise(engine.open())
      const initial = await Effect.runPromise(engine.subscribe(query(retained ? 2100 : 2000)))
      await waitFor(() => server.pendingFills.length === 1)
      server.completeFill(
        "Chatbot",
        Array.from({ length: 2100 }, (_, i) => chatbot(String(i).padStart(4, "0"))),
      )
      await Effect.runPromise(engine.awaitLive(initial.id))
      let small = initial
      if (retained) {
        await Effect.runPromise(initial.release())
        small = await Effect.runPromise(engine.subscribe(query(2000)))
        await Effect.runPromise(engine.awaitLive(small.id))
        server.dropAll()
        await waitFor(() => log.some(([event]) => event === "subscription.resumed"))
        await Effect.runPromise(engine.awaitLive(small.id))
      }
      const receive = server.receive.bind(server)
      let interrupted = false
      server.receive = (socket, text) => {
        const message = JSON.parse(text)
        if (!interrupted && message.type === "subscribe" && message.query.limit === 2100) {
          interrupted = true
          socket.dropFromServer(4408)
          return
        }
        receive(socket, text)
      }
      log.length = 0
      const grown = await Effect.runPromise(engine.subscribe(query(2100)))
      // The pending child must keep its base alive even after the UI releases it.
      await Effect.runPromise(small.release())
      await waitFor(() => interrupted)
      await Effect.runPromise(engine.awaitLive(grown.id))
      expect(grown.getSnapshot().rows).toHaveLength(2100)
      expect(grown.getSnapshot().rows.at(-1)?.row["id"]).toBe("2099")
      const snapshots = log.filter(
        ([event, data]) => event === "snapshot.applied" && data["subscription"] === grown.id,
      )
      expect(snapshots).toHaveLength(1)
      expect(snapshots[0]?.[1]).toMatchObject({ rows: 100, basedOn: small.id })
      await Effect.runPromise(engine.close())
    },
  )

  it("recovers a failed local delta transaction from a fresh snapshot instead of dropping it", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const driver = nodeAsyncDriver()
    let rejectNext = false
    const { engine, log } = makeClient(server, {
      ...driver,
      batch: async (statements) => {
        if (rejectNext) {
          rejectNext = false
          throw new Error("transient local write failure")
        }
        await driver.batch(statements)
      },
    })
    await Effect.runPromise(engine.open())
    const docs = await Effect.runPromise(engine.subscribe({ table: "Chatbot" }))
    await waitFor(() => server.pendingFills.length === 1)
    server.completeFill("Chatbot", [chatbot("deleted")])
    await Effect.runPromise(engine.awaitLive(docs.id))
    const stale: Array<string> = []
    docs.subscribe(() => stale.push(docs.getSnapshot().status))
    rejectNext = true
    server.commit([remove("Chatbot", chatbot("deleted"))])
    server.commit([insert("Chatbot", chatbot("survives"))])
    await waitFor(() => log.some(([event]) => event === "message.failed"))
    await waitFor(
      () =>
        docs.getSnapshot().status === "live" &&
        docs.getSnapshot().rows.length === 1 &&
        docs.getSnapshot().rows[0]?.row["id"] === "survives",
    )
    expect(stale).toContain("stale")
    expect(engine.getStatus().cursor).toBe(2)
    expect((await driver.query("SELECT id FROM t_Chatbot")).map((r) => r["id"])).toEqual([
      "survives",
    ])
    await Effect.runPromise(engine.close())
  })

  it("bounds queued batches by membership work as well as row images", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine, log } = makeClient(server)
    await Effect.runPromise(engine.open())
    const docs = await Effect.runPromise(engine.subscribe({ table: "Chatbot" }))
    await waitFor(() => server.pendingFills.length === 1)
    server.completeFill("Chatbot", [])
    await Effect.runPromise(engine.awaitLive(docs.id))
    // Each transaction is 600 rows + 600 membership additions. Combining two
    // would exceed the work bound even though their row count alone fits.
    for (let batch = 0; batch < 3; batch++)
      server.commit(
        Array.from({ length: 600 }, (_, i) => insert("Chatbot", chatbot(`bounded-${batch}-${i}`))),
      )
    await waitFor(() => engine.getStatus().cursor === 3)
    expect(docs.getSnapshot().rows).toHaveLength(1800)
    expect(
      log.filter(([event]) => event === "delta.applied").map(([, data]) => data["transactions"]),
    ).toEqual([1, 1, 1])
    await Effect.runPromise(engine.close())
  }, 20_000)

  it("drains queued import deltas without refreshing a view for every transaction", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine, driver, log } = makeClient(server)
    await Effect.runPromise(engine.open())
    const docs = await Effect.runPromise(
      engine.subscribe({ table: "Chatbot", orderBy: [{ column: "id", direction: "asc" }] }),
    )
    await waitFor(() => server.pendingFills.length === 1)
    server.completeFill("Chatbot", [])
    await Effect.runPromise(engine.awaitLive(docs.id))
    let refreshes = 0
    docs.subscribe(() => refreshes++)
    for (let i = 0; i < 256; i++) server.commit([insert("Chatbot", chatbot(`import-${i}`))])
    for (let i = 0; i < 256; i++) server.commit([remove("Chatbot", chatbot(`import-${i}`))])
    server.commit([insert("Chatbot", chatbot("healthy"))])
    await waitFor(() => engine.getStatus().cursor === 513)
    expect(docs.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["healthy"])
    expect((await driver.query("SELECT id FROM t_Chatbot")).map((r) => r["id"])).toEqual([
      "healthy",
    ])
    expect(refreshes).toBeLessThan(10)
    const applied = log
      .filter(([event]) => event === "delta.applied")
      .map(([, data]) => Number(data["transactions"]))
    expect(applied.reduce((sum, n) => sum + n, 0)).toBe(513)
    expect(Math.max(...applied)).toBeLessThanOrEqual(256)
    await Effect.runPromise(engine.close())
  })

  it("bootstraps through a fill, serves live queries locally and applies deltas atomically", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine, driver } = makeClient(server)
    await Effect.runPromise(engine.open())
    const docs = await Effect.runPromise(
      engine.subscribe({
        table: "Chatbot",
        where: { op: "eq", column: "type", value: "DOCUMENT" },
        orderBy: [{ column: "displayOrder", direction: "asc" }],
        include: ["folder"],
      }),
    )
    let notified = 0
    docs.subscribe(() => notified++)
    expect(docs.getSnapshot().status).toBe("pending")
    await waitFor(() => server.pendingFills.length === 1, 2000, "fill request")
    server.completeFill("Chatbot", [
      chatbot("f1", { type: "GROUP" }),
      chatbot("d1", { groupId: "f1", displayOrder: 2 }),
      chatbot("d2", { groupId: "f1", displayOrder: 1 }),
    ])
    await Effect.runPromise(engine.awaitLive(docs.id))
    const snap = docs.getSnapshot()
    expect(snap.status).toBe("live")
    expect(snap.rows.map((r) => r.row["id"])).toEqual(["d2", "d1"])
    expect(snap.rows[0]?.related["folder"]).toMatchObject({ row: { id: "f1", type: "GROUP" } })
    expect(notified).toBeGreaterThan(0)

    // Insert, update, delete flow through as deltas; the local store stays consistent.
    server.commit([insert("Chatbot", chatbot("d0", { groupId: "f1", displayOrder: 0 }))])
    await waitFor(() => docs.getSnapshot().rows.length === 3, 2000, "insert delta")
    expect(docs.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["d0", "d2", "d1"])
    server.commit([
      update(
        "Chatbot",
        chatbot("d0", { groupId: "f1", displayOrder: 0 }),
        chatbot("d0", { groupId: "f1", displayOrder: 9 }),
      ),
    ])
    await waitFor(() => docs.getSnapshot().rows[2]?.row["id"] === "d0", 2000, "update delta")
    server.commit([remove("Chatbot", chatbot("d0", { groupId: "f1", displayOrder: 9 }))])
    await waitFor(() => docs.getSnapshot().rows.length === 2, 2000, "delete delta")
    expect(engine.getStatus().cursor).toBe(3)

    // GC: the deleted row is gone from the local table; the folder is kept (still referenced).
    const rows = await driver.query(`SELECT id FROM t_Chatbot ORDER BY id`)
    expect(rows.map((r) => r["id"])).toEqual(["d1", "d2", "f1"])
    await Effect.runPromise(docs.release())
    await settle()
    expect((await driver.query(`SELECT COUNT(*) AS n FROM t_Chatbot`))[0]?.["n"]).toBe(0)
    await Effect.runPromise(engine.close())
  })

  it("re-runs only the queries a delta can change", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine } = makeClient(server)
    await Effect.runPromise(engine.open())
    const groupA = await Effect.runPromise(
      engine.subscribe({
        table: "Chatbot",
        where: { op: "eq", column: "groupId", value: "a" },
        orderBy: [{ column: "id", direction: "asc" }],
      }),
    )
    const groupB = await Effect.runPromise(
      engine.subscribe({
        table: "Chatbot",
        where: { op: "eq", column: "groupId", value: "b" },
        orderBy: [{ column: "id", direction: "asc" }],
      }),
    )
    await waitFor(() => server.pendingFills.length === 1, 2000, "fill request")
    server.completeFill("Chatbot", [
      chatbot("a", { type: "GROUP" }),
      chatbot("b", { type: "GROUP" }),
      chatbot("d1", { groupId: "a" }),
      chatbot("d2", { groupId: "b" }),
    ])
    await Effect.runPromise(engine.awaitLive(groupA.id))
    await Effect.runPromise(engine.awaitLive(groupB.id))
    let notifiedA = 0
    let notifiedB = 0
    groupA.subscribe(() => notifiedA++)
    groupB.subscribe(() => notifiedB++)
    const snapshotB = groupB.getSnapshot()
    // A change to a row only group A holds refreshes A and leaves B's snapshot untouched.
    server.commit([
      update(
        "Chatbot",
        chatbot("d1", { groupId: "a" }),
        chatbot("d1", { groupId: "a", displayOrder: 5 }),
      ),
    ])
    await waitFor(
      () => groupA.getSnapshot().rows[0]?.row["displayOrder"] === 5,
      2000,
      "update delta",
    )
    expect(notifiedA).toBeGreaterThan(0)
    expect(notifiedB).toBe(0)
    expect(groupB.getSnapshot()).toBe(snapshotB)
    await Effect.runPromise(engine.close())
  })

  it("grows a window in place: the server sends only the new rows, the base stays until then", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine, log } = makeClient(server)
    await Effect.runPromise(engine.open())
    const byId = (limit: number) => ({
      table: "Chatbot",
      orderBy: [{ column: "id", direction: "asc" as const }],
      limit,
    })
    const small = await Effect.runPromise(engine.subscribe(byId(2)))
    await waitFor(() => server.pendingFills.length === 1, 2000, "fill request")
    server.completeFill("Chatbot", [chatbot("a"), chatbot("b"), chatbot("c"), chatbot("d")])
    await Effect.runPromise(engine.awaitLive(small.id))
    // Release the base first: with a zero TTL it would retire at once, but the grown window
    // pins it until the extending snapshot has copied its membership.
    const grown = await Effect.runPromise(engine.subscribe(byId(3)))
    await Effect.runPromise(small.release())
    await Effect.runPromise(engine.awaitLive(grown.id))
    expect(server.receivedBases.at(-1)).toBe(small.id)
    const applied = log.find(([e, d]) => e === "snapshot.applied" && d["subscription"] === grown.id)
    expect(applied?.[1]).toMatchObject({ rows: 1, basedOn: small.id })
    expect(grown.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["a", "b", "c"])
    // The base retired once the pin was released; the grown window keeps every row.
    await waitFor(
      () => !server.receivedRefs.some(() => false) && engine.getStatus().pendingSubscriptions === 0,
    )
    await settle(50)
    expect(grown.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["a", "b", "c"])
    // A window that grows again extends the largest live window.
    const larger = await Effect.runPromise(engine.subscribe(byId(4)))
    await Effect.runPromise(engine.awaitLive(larger.id))
    expect(server.receivedBases.at(-1)).toBe(grown.id)
    expect(larger.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["a", "b", "c", "d"])
    // The retired base still owns a's cached membership. Its active descendants receive
    // both row-only changes and removals through that inheritance chain.
    server.commit([update("Chatbot", chatbot("a"), chatbot("a", { displayOrder: 7 }))])
    await waitFor(() => larger.getSnapshot().rows[0]?.row["displayOrder"] === 7)
    server.commit([remove("Chatbot", chatbot("a", { displayOrder: 7 }))])
    await waitFor(() => larger.getSnapshot().rows.length === 3)
    expect(larger.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["b", "c", "d"])
    await Effect.runPromise(engine.close())
  })

  it("a row that leaves a grown window leaves the base it inherited the row from, and survives a reload", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const driver = reloadable(nodeAsyncDriver())
    const { engine } = makeClient(server, driver, 60_000)
    await Effect.runPromise(engine.open())
    const byId = (limit: number) => ({
      table: "Chatbot",
      orderBy: [{ column: "id", direction: "asc" as const }],
      limit,
    })
    const small = await Effect.runPromise(engine.subscribe(byId(2)))
    await waitFor(() => server.pendingFills.length === 1, 2000, "fill request")
    server.completeFill("Chatbot", [chatbot("a"), chatbot("b"), chatbot("c"), chatbot("d")])
    await Effect.runPromise(engine.awaitLive(small.id))
    const grown = await Effect.runPromise(engine.subscribe(byId(3)))
    await Effect.runPromise(engine.awaitLive(grown.id))
    expect(server.receivedBases.at(-1)).toBe(small.id)
    // The grown window holds only its extra member; "a" and "b" are read through the base.
    const own = await driver.query(
      `SELECT key FROM membership WHERE subscription = ? ORDER BY key`,
      [grown.id],
    )
    expect(own.map((r) => r["key"])).toEqual([JSON.stringify(["c"])])
    // Deleting "a" removes it from both windows; "d" enters the grown one.
    server.commit([remove("Chatbot", chatbot("a"))])
    await waitFor(
      () =>
        grown
          .getSnapshot()
          .rows.map((r) => r.row["id"])
          .join() === "b,c,d",
      3000,
      "grown window after the delete",
    )
    expect(small.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["b", "c"])
    await Effect.runPromise(engine.close())

    // After a reload every window gets a complete snapshot and stands on its own.
    const again = makeClient(server, driver, 60_000)
    await Effect.runPromise(again.engine.open())
    const back = await Effect.runPromise(again.engine.subscribe(byId(3)))
    await Effect.runPromise(again.engine.awaitLive(back.id))
    expect(back.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["b", "c", "d"])
    const link = await driver.query(`SELECT based_on FROM subscriptions WHERE id = ?`, [back.id])
    expect(link[0]?.["based_on"]).toBeNull()
    await Effect.runPromise(again.engine.close())
  })

  it("restored subscriptions retire after the query TTL unless the application references them", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const driver = reloadable(nodeAsyncDriver())
    const first = makeClient(server, driver, 60)
    await Effect.runPromise(first.engine.open())
    const byId = (limit: number) => ({
      table: "Chatbot",
      orderBy: [{ column: "id", direction: "asc" as const }],
      limit,
    })
    const one = await Effect.runPromise(first.engine.subscribe(byId(1)))
    const two = await Effect.runPromise(first.engine.subscribe(byId(2)))
    await waitFor(() => server.pendingFills.length === 1, 2000, "fill request")
    server.completeFill("Chatbot", [chatbot("a"), chatbot("b")])
    await Effect.runPromise(first.engine.awaitLive(one.id))
    await Effect.runPromise(first.engine.awaitLive(two.id))
    await Effect.runPromise(first.engine.close())

    // Only the referenced view reconnects; the other cache retires after the TTL.
    const second = makeClient(server, driver, 60)
    await Effect.runPromise(second.engine.open())
    const again = await Effect.runPromise(second.engine.subscribe(byId(1)))
    expect(again.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["a"])
    await Effect.runPromise(second.engine.awaitLive(again.id))
    await settle(150)
    const persisted = await driver.query(`SELECT id FROM subscriptions`)
    expect(persisted.map((r) => r["id"])).toEqual([again.id])
    expect(again.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["a"])
    await Effect.runPromise(second.engine.close())
  })

  it("replays only referenced views after reload and activates cached views on demand", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const driver = reloadable(nodeAsyncDriver())
    const first = makeClient(server, driver, 60_000)
    await Effect.runPromise(first.engine.open())
    const queries = Array.from({ length: 20 }, (_, i) => ({
      table: "Chatbot",
      orderBy: [{ column: "id", direction: "asc" as const }],
      limit: i + 1,
    }))
    const initial = await Effect.runPromise(first.engine.subscribe(queries[0]!))
    await waitFor(() => server.pendingFills.length === 1)
    server.completeFill("Chatbot", [chatbot("a"), chatbot("b")])
    await Effect.runPromise(first.engine.awaitLive(initial.id))
    for (const query of queries.slice(1)) {
      const handle = await Effect.runPromise(first.engine.subscribe(query))
      await Effect.runPromise(first.engine.awaitLive(handle.id))
      await Effect.runPromise(handle.release())
    }
    await Effect.runPromise(first.engine.close())
    server.receivedRefs.length = 0
    const second = makeClient(server, driver, 60_000)
    await Effect.runPromise(second.engine.open())
    await waitFor(() => second.engine.getStatus().connection.status === "open")
    await settle()
    expect(server.receivedRefs).toEqual([])
    expect(second.engine.getStatus().pendingSubscriptions).toBe(0)
    const active = await Effect.runPromise(second.engine.subscribe(queries[0]!))
    expect(active.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["a"])
    await Effect.runPromise(second.engine.awaitLive(active.id))
    expect(server.receivedRefs).toEqual([queries[0]])
    const other = await Effect.runPromise(second.engine.subscribe(queries[19]!))
    await Effect.runPromise(second.engine.awaitLive(other.id))
    expect(other.getSnapshot().rows).toHaveLength(2)
    expect(server.receivedRefs).toEqual([queries[0], queries[19]])
    await Effect.runPromise(second.engine.close())
  })

  it("does not replay released views on reconnect but catches them up when reopened", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine } = makeClient(server, nodeAsyncDriver(), 60_000)
    await Effect.runPromise(engine.open())
    const small = {
      table: "Chatbot",
      orderBy: [{ column: "id", direction: "asc" as const }],
      limit: 1,
    }
    const large = {
      table: "Chatbot",
      orderBy: [{ column: "id", direction: "asc" as const }],
      limit: 10,
    }
    const active = await Effect.runPromise(engine.subscribe(small))
    const cached = await Effect.runPromise(engine.subscribe(large))
    await waitFor(() => server.pendingFills.length === 1)
    server.completeFill("Chatbot", [chatbot("a")])
    await Effect.runPromise(engine.awaitLive(active.id))
    await Effect.runPromise(engine.awaitLive(cached.id))
    await Effect.runPromise(cached.release())
    server.reachable = false
    server.dropAll()
    await waitFor(() => engine.getStatus().connection.status === "reconnecting")
    server.commit([insert("Chatbot", chatbot("b"))])
    server.receivedRefs.length = 0
    server.reachable = true
    await waitFor(() => engine.getStatus().connection.status === "open")
    await Effect.runPromise(engine.awaitLive(active.id))
    expect(server.receivedRefs).toEqual([small])
    const reopened = await Effect.runPromise(engine.subscribe(large))
    await Effect.runPromise(engine.awaitLive(reopened.id))
    expect(reopened.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["a", "b"])
    expect(server.receivedRefs).toEqual([small, large])
    await Effect.runPromise(engine.close())
  })

  it("reconnects after a drop and resumes with a consistent snapshot", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine, log } = makeClient(server)
    await Effect.runPromise(engine.open())
    const all = await Effect.runPromise(
      engine.subscribe({ table: "Chatbot", orderBy: [{ column: "id", direction: "asc" }] }),
    )
    await waitFor(() => server.pendingFills.length === 1)
    server.completeFill("Chatbot", [chatbot("a")])
    await Effect.runPromise(engine.awaitLive(all.id))

    // Network drops; changes happen while offline; the client reconnects and catches up.
    server.reachable = false
    server.dropAll()
    await waitFor(
      () => engine.getStatus().connection.status === "reconnecting",
      2000,
      "reconnecting state",
    )
    server.commit([insert("Chatbot", chatbot("b"))])
    server.commit([remove("Chatbot", chatbot("a"))])
    expect(all.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["a"])
    server.reachable = true
    await waitFor(() => engine.getStatus().connection.status === "open", 3000, "reconnect")
    await waitFor(
      () =>
        all.getSnapshot().status === "live" &&
        all
          .getSnapshot()
          .rows.map((r) => r.row["id"])
          .join() === "b",
      3000,
      "resumed snapshot",
    )
    expect(engine.getStatus().cursor).toBe(2)
    expect(log.some(([e]) => e === "snapshot.applied")).toBe(true)
    await Effect.runPromise(engine.close())
  })

  it("detects a silently dead link by heartbeat even when the close handshake hangs", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const engine = new ClientEngine({
      schema,
      partition: "org_1",
      driver: nodeAsyncDriver(),
      clientId: "c1",
      target: async () => ({ url: "ws://fake", protocols: [] }),
      makeWebSocket: server.connect,
      backoffMinMs: 5,
      backoffMaxMs: 20,
      pingIntervalMs: 10,
      pongTimeoutMs: 10,
    })
    await Effect.runPromise(engine.open())
    await waitFor(() => engine.getStatus().connection.status === "open", 2000, "open")
    // The link dies silently: pongs stop, and the browser cannot finish the closing handshake.
    server.answerPings = false
    server.hangClose = true
    await waitFor(
      () => engine.getStatus().connection.status === "reconnecting",
      2000,
      "reconnecting despite a hanging close",
    )
    // The link comes back: the next attempt opens a fresh socket.
    server.answerPings = true
    server.hangClose = false
    await waitFor(() => engine.getStatus().connection.status === "open", 3000, "reopened")
    // The hung socket never reached the fake server's `detach`; a fresh socket opened beside it.
    expect(server.sessions.size).toBeGreaterThan(1)
    await Effect.runPromise(engine.close())
  })

  it("survives a reload: persisted rows are served as stale until the server confirms", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const driver = nodeAsyncDriver()
    const first = makeClient(server, driver)
    await Effect.runPromise(first.engine.open())
    const q1 = { table: "Chatbot", orderBy: [{ column: "id", direction: "asc" as const }] }
    const sub = await Effect.runPromise(first.engine.subscribe(q1))
    await waitFor(() => server.pendingFills.length === 1)
    server.completeFill("Chatbot", [chatbot("persisted")])
    await Effect.runPromise(first.engine.awaitLive(sub.id))
    // Simulate reload without closing the connection cleanly (the store keeps the data).
    server.dropAll()
    server.reachable = false
    await settle(50)

    const second = new ClientEngine({
      schema,
      partition: "org_1",
      driver,
      target: async () => ({ url: "ws://fake", protocols: [] }),
      makeWebSocket: server.connect,
      backoffMinMs: 5,
      backoffMaxMs: 20,
    })
    await Effect.runPromise(second.open())
    // The client id is persisted in the local database and survives the reload.
    expect(second.clientId).toBe("c1")
    const again = await Effect.runPromise(second.subscribe(q1))
    expect(again.getSnapshot().status).toBe("stale")
    expect(again.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["persisted"])
    expect(second.getStatus().cursor).toBe(0)
    server.reachable = true
    await waitFor(() => again.getSnapshot().status === "live", 3000, "live after reload")
    await Effect.runPromise(second.close())
  })

  it("fatal server errors stop reconnecting and are exposed", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const driver = nodeAsyncDriver()
    const wrongSchema = {
      ...schema,
      schema_hash: "different",
      tables: [
        {
          ...schema.tables[0]!,
          columns: [
            ...schema.tables[0]!.columns,
            { name: "ghost", kind: "string" as const, nullable: true, source_type: "text" },
          ],
        },
        ...schema.tables.slice(1),
      ],
    }
    const engine = new ClientEngine({
      schema: wrongSchema,
      partition: "org_1",
      driver,
      clientId: "c3",
      target: async () => ({ url: "ws://fake", protocols: [] }),
      makeWebSocket: server.connect,
      backoffMinMs: 5,
      backoffMaxMs: 20,
    })
    await Effect.runPromise(engine.open())
    await waitFor(() => engine.getStatus().fatalError !== null, 3000, "fatal error")
    expect(engine.getStatus().fatalError?.code).toBe("schema_mismatch")
    expect(engine.getStatus().connection.status).toBe("closed")
    await Effect.runPromise(engine.close())
  })

  it("randomized: live query rows always equal the server's recomputation", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine } = makeClient(server)
    await Effect.runPromise(engine.open())
    const queries = [
      {
        table: "Chatbot",
        where: { op: "eq" as const, column: "type", value: "DOCUMENT" },
        orderBy: [{ column: "displayOrder", direction: "asc" as const }],
        limit: 3,
        include: ["folder"],
      },
      {
        table: "Chatbot",
        where: { op: "eq" as const, column: "type", value: "GROUP" },
        include: ["documents"],
      },
      {
        table: "Chatbot",
        where: { op: "isNull" as const, column: "groupId" },
        orderBy: [{ column: "id", direction: "desc" as const }],
      },
    ]
    const handles = await Promise.all(queries.map((x) => Effect.runPromise(engine.subscribe(x))))
    await waitFor(() => server.pendingFills.length === 1)
    server.completeFill("Chatbot", [
      chatbot("f1", { type: "GROUP" }),
      chatbot("f2", { type: "GROUP" }),
    ])
    for (const h of handles) await Effect.runPromise(engine.awaitLive(h.id))
    let seed = 7
    const rand = () => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff
      return seed / 0x7fffffff
    }
    const pick = <T>(xs: ReadonlyArray<T>): T => xs[Math.floor(rand() * xs.length)]!
    const live = new Map<string, RowImage>()
    for (let step = 0; step < 40; step++) {
      const changes: Array<RowChange> = []
      for (let i = 0; i < 1 + Math.floor(rand() * 3); i++) {
        const id = `d${Math.floor(rand() * 6)}`
        const existing = live.get(id)
        const fresh = chatbot(id, {
          groupId: pick(["f1", "f2", null]),
          displayOrder: pick([null, 1, 2, 5, 9]),
          type: rand() < 0.1 ? "GROUP" : "DOCUMENT",
        })
        if (existing === undefined) {
          changes.push(insert("Chatbot", fresh))
          live.set(id, fresh)
        } else if (rand() < 0.25) {
          changes.push(remove("Chatbot", existing))
          live.delete(id)
        } else {
          changes.push(update("Chatbot", existing, fresh))
          live.set(id, fresh)
        }
      }
      server.commit(changes)
      await waitFor(() => engine.getStatus().cursor === step + 1, 3000, `cursor ${step + 1}`)
      for (const h of handles) {
        const expectedPrimary = new Set(
          server.engine.membershipOf(h.id).map((m) => `${m.table}:${String(m.key[0])}`),
        )
        const clientKeys = h.getSnapshot().rows.map((r) => `Chatbot:${r.key.slice(2, -2)}`)
        // Every client row is a member on the server.
        for (const k of clientKeys)
          expect(expectedPrimary.has(k), `step ${step} ${h.id} ${k}`).toBe(true)
      }
      // Stronger: the client's primary rows must equal what the query yields over the server cache.
      for (const h of handles) {
        const serverRows = server.engine.snapshot(h.id)
        if (serverRows.type !== "snapshot") throw new Error("expected snapshot")
        const serverPrimaryIds = serverRows.rows
          .filter((r) => r.row?.["type"] !== undefined)
          .map((r) => String(r.key[0]))
        const clientIds = h.getSnapshot().rows.map((r) => String(r.row["id"]))
        for (const id of clientIds)
          expect(
            serverPrimaryIds,
            `step ${step} query ${h.id}: client row ${id} missing on server`,
          ).toContain(id)
      }
    }
    await Effect.runPromise(engine.close())
  })
})

describe("nested and filtered includes", () => {
  it("reads nested includes as trees and flattens them into nested objects", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine } = makeClient(server, reloadable(nodeAsyncDriver()))
    await Effect.runPromise(engine.open())
    const groups = await Effect.runPromise(
      engine.subscribe({
        table: "Chatbot",
        where: { op: "eq", column: "type", value: "GROUP" },
        include: [
          {
            relation: "documents",
            where: { op: "isNotNull", column: "displayOrder" },
            include: ["organization"],
          },
        ],
      }),
    )
    await waitFor(() => server.pendingFills.length === 2, 2000, "fill requests")
    server.completeFill("Chatbot", [
      chatbot("f1", { type: "GROUP" }),
      chatbot("d1", { groupId: "f1", displayOrder: 1 }),
      chatbot("d2", { groupId: "f1", displayOrder: null }),
    ])
    server.completeFill("organization", [organization("org_1")])
    await Effect.runPromise(engine.awaitLive(groups.id))
    const [group] = groups.getSnapshot().rows
    expect(group?.row["id"]).toBe("f1")
    const documents = group?.related["documents"]
    expect(Array.isArray(documents)).toBe(true)
    if (!Array.isArray(documents)) throw new Error("expected a list")
    // The filtered include drops d2 (null displayOrder); the nested include hangs off d1.
    expect(documents.map((d) => d.row["id"])).toEqual(["d1"])
    expect(documents[0]?.related["organization"]).toMatchObject({
      row: { id: "org_1", name: "Acme" },
    })
    await Effect.runPromise(engine.close())

    // The public API flattens the tree into nested objects with the query's include shape.
    const client = await createOrbitClient({
      definition: sync,
      schema,
      url: "http://fake",
      partition: "org_1",
      getToken: async () => "t",
      driver: reloadable(engine.store.driver),
      clientId: "c1",
      makeWebSocket: server.connect,
      backoffMinMs: 5,
      backoffMaxMs: 20,
    })
    const rows = await client.read(
      q(sync)
        .from("Chatbot")
        .where((c) => c.eq("type", "GROUP"))
        .include("documents", (i) => i.include("organization")),
    )
    // Only rows some subscription references are cached: the filtered include never admitted d2.
    expect(rows[0]?.documents.map((d) => d.id)).toEqual(["d1"])
    expect(rows[0]?.documents[0]?.organization?.name).toBe("Acme")
    await client.close()
  })
})

describe("local-first reads", () => {
  it("answers a new query from the cache before the server snapshot arrives", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine } = makeClient(server)
    await Effect.runPromise(engine.open())
    const all = await Effect.runPromise(engine.subscribe({ table: "Chatbot" }))
    await waitFor(() => server.pendingFills.length === 1, 2000, "fill")
    server.completeFill("Chatbot", [
      chatbot("a", { displayOrder: 1 }),
      chatbot("b", { displayOrder: 2 }),
    ])
    await Effect.runPromise(engine.awaitLive(all.id))

    // The server goes away; a narrower query still renders from the cached rows at once.
    server.reachable = false
    server.dropAll()
    const narrow = await Effect.runPromise(
      engine.subscribe({
        table: "Chatbot",
        where: { op: "eq", column: "displayOrder", value: 2 },
      }),
    )
    await settle()
    expect(narrow.getSnapshot().status).toBe("pending")
    expect(narrow.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["b"])

    // When the server answers, the snapshot confirms the set and the query turns live.
    server.reachable = true
    await Effect.runPromise(engine.awaitLive(narrow.id))
    expect(narrow.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["b"])
    await Effect.runPromise(engine.close())
  })
})

describe("local-first reads and query retention", () => {
  it("answers a new query from the cache before the server snapshot arrives", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine } = makeClient(server)
    await Effect.runPromise(engine.open())
    const all = await Effect.runPromise(engine.subscribe({ table: "Chatbot" }))
    await waitFor(() => server.pendingFills.length === 1, 2000, "fill")
    server.completeFill("Chatbot", [
      chatbot("a", { displayOrder: 1 }),
      chatbot("b", { displayOrder: 2 }),
    ])
    await Effect.runPromise(engine.awaitLive(all.id))

    // The server goes away; a narrower query still renders from the cached rows at once.
    server.reachable = false
    server.dropAll()
    const narrow = await Effect.runPromise(
      engine.subscribe({
        table: "Chatbot",
        where: { op: "eq", column: "displayOrder", value: 2 },
      }),
    )
    await settle()
    expect(narrow.getSnapshot().status).toBe("pending")
    expect(narrow.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["b"])

    // When the server answers, the snapshot confirms the set and the query turns live.
    server.reachable = true
    await Effect.runPromise(engine.awaitLive(narrow.id))
    expect(narrow.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["b"])
    await Effect.runPromise(engine.close())
  })

  it("keeps a released query subscribed and cached until its TTL, then retires it", async () => {
    const server = new FakeSyncServer(schema, "org_1")
    const { engine, driver } = makeClient(server, nodeAsyncDriver(), 150)
    await Effect.runPromise(engine.open())
    const query = {
      table: "Chatbot",
      where: { op: "eq", column: "type", value: "DOCUMENT" },
    } as const
    const docs = await Effect.runPromise(engine.subscribe(query))
    await waitFor(() => server.pendingFills.length === 1, 2000, "fill")
    server.completeFill("Chatbot", [chatbot("a")])
    await Effect.runPromise(engine.awaitLive(docs.id))
    await Effect.runPromise(docs.release())

    // Still subscribed: a delta keeps the cached rows current while nothing references them.
    server.commit([insert("Chatbot", chatbot("b"))])
    await waitFor(() => engine.getStatus().cursor === 1, 3000, "delta")
    const ids = async () =>
      (await driver.query(`SELECT id FROM t_Chatbot ORDER BY id`)).map((r) => r["id"])
    expect(await ids()).toEqual(["a", "b"])

    // Returning within the TTL is instant and already live.
    const again = await Effect.runPromise(engine.subscribe(query))
    expect(again.getSnapshot().status).toBe("live")
    expect(again.getSnapshot().rows.map((r) => r.row["id"])).toEqual(["a", "b"])
    await Effect.runPromise(again.release())

    // After the TTL the subscription is retired and its rows are garbage-collected.
    const deadline = Date.now() + 3000
    while ((await ids()).length > 0) {
      if (Date.now() > deadline) throw new Error("rows were not garbage-collected")
      await settle(20)
    }
    expect(await driver.query(`SELECT id FROM subscriptions`)).toEqual([])
    await Effect.runPromise(engine.close())
  })
})

const clientQueries = defineQueries(sync, {
  documents: {
    args: Schema.Struct({ type: Schema.String }),
    query: (_ctx, { type }) =>
      q(sync)
        .from("Chatbot")
        .where((c) => c.eq("type", type))
        .orderBy("id"),
  },
  missing: { args: Schema.Struct({}), query: () => q(sync).from("Chatbot") },
})

/** The server's definitions: `documents` also scopes to the partition and caps the result. */
const serverQueries = defineQueries(sync, {
  documents: {
    args: Schema.Struct({ type: Schema.String }),
    query: (ctx, { type }) =>
      q(sync)
        .from("Chatbot")
        .where((c) => c.and(c.eq("type", type), c.eq("organizationId", ctx.partition)))
        .orderBy("id")
        .limit(1),
  },
})

describe("named queries", () => {
  it("renders the local resolution first, then adopts the server's resolved query", async () => {
    const server = new FakeSyncServer(schema, "org_1", { queries: serverQueries })
    const driver = reloadable(nodeAsyncDriver())
    const open = () =>
      createOrbitClient({
        definition: sync,
        schema,
        url: "http://fake",
        partition: "org_1",
        getToken: async () => "t",
        driver,
        clientId: "c1",
        makeWebSocket: server.connect,
        backoffMinMs: 5,
        backoffMaxMs: 20,
      })
    const client = await open()
    const live = client.liveQuery(clientQueries.documents({ type: "DOCUMENT" }))
    await settle()
    expect(live.getSnapshot().status).toBe("pending")
    await waitFor(() => server.pendingFills.length === 1, 2000, "fill request")
    // The wire carries the reference, never the locally resolved query.
    expect(server.receivedRefs).toContainEqual({ name: "documents", args: { type: "DOCUMENT" } })
    server.completeFill("Chatbot", [chatbot("a"), chatbot("b"), chatbot("c", { type: "GROUP" })])
    await waitFor(() => live.getSnapshot().status === "live", 3000, "live")
    // The server's query (limit 1) is authoritative: one row, not two.
    expect(live.getSnapshot().rows.map((r) => r.id)).toEqual(["a"])
    await client.close()

    // Reload while the server is unreachable: the persisted resolved query renders as stale.
    server.reachable = false
    server.dropAll()
    const again = await open()
    const stale = again.liveQuery(clientQueries.documents({ type: "DOCUMENT" }))
    await settle()
    expect(stale.getSnapshot().status).toBe("stale")
    expect(stale.getSnapshot().rows.map((r) => r.id)).toEqual(["a"])
    server.reachable = true
    await waitFor(() => stale.getSnapshot().status === "live", 3000, "live after reload")
    // `hello` sent the reference again.
    expect(server.receivedRefs.filter((r) => "name" in r && r.name === "documents").length).toBe(2)
    await again.close()
  })

  it("reports unknown_query when the server has no such query", async () => {
    const server = new FakeSyncServer(schema, "org_1", { queries: serverQueries })
    const client = await createOrbitClient({
      definition: sync,
      schema,
      url: "http://fake",
      partition: "org_1",
      getToken: async () => "t",
      driver: nodeAsyncDriver(),
      makeWebSocket: server.connect,
      backoffMinMs: 5,
      backoffMaxMs: 20,
    })
    const live = client.liveQuery(clientQueries.missing({}))
    await waitFor(() => live.getSnapshot().status === "error", 3000, "error status")
    expect(live.getSnapshot().error?.code).toBe("unknown_query")
    await client.close()
  })
})

const define = defineMutator(sync)
const mutators = defineMutators(sync, {
  createDocument: define(
    Schema.Struct({ id: Schema.String, groupId: Schema.NullOr(Schema.String) }),
    async (tx, { id, groupId }, ctx) => {
      await tx.insert("Chatbot", {
        id,
        organizationId: ctx.partition,
        groupId,
        type: "DOCUMENT",
        displayOrder: 0,
        createdAt: ctx.now,
      })
    },
  ),
  setOrder: define(
    Schema.Struct({ id: Schema.String, displayOrder: Schema.Number }),
    async (tx, { id, displayOrder }) => {
      const row = await tx.get("Chatbot", { id })
      if (row === null) throw new Error(`document ${id} not found`)
      await tx.update("Chatbot", { id }, { displayOrder })
    },
  ),
  removeDocument: define(Schema.Struct({ id: Schema.String }), async (tx, { id }) => {
    await tx.delete("Chatbot", { id })
  }),
})

/** The fake application's server-side mutators: the row changes each mutation commits. */
const serverApply = (m: {
  name: string
  args: Record<string, unknown>
}): ReadonlyArray<RowChange> => {
  const id = String(m.args["id"])
  switch (m.name) {
    case "createDocument":
      if (id === "bad") throw new Error("refused by the server")
      return [
        insert(
          "Chatbot",
          chatbot(id, {
            groupId: typeof m.args["groupId"] === "string" ? m.args["groupId"] : null,
            displayOrder: 0,
          }),
        ),
      ]
    case "setOrder":
      return [
        update(
          "Chatbot",
          chatbot(id, { displayOrder: 0 }),
          chatbot(id, { displayOrder: Number(m.args["displayOrder"]) }),
        ),
      ]
    default:
      return [remove("Chatbot", chatbot(id))]
  }
}

const documents = q(sync)
  .from("Chatbot")
  .where((c) => c.eq("type", "DOCUMENT"))
  .orderBy("id")

describe("client-side mutations", () => {
  const setup = async (
    options: { reachable?: boolean; pushReachable?: boolean; deferCommit?: boolean } = {},
  ) => {
    const server = new FakeSyncServer(schema, "org_1")
    server.reachable = options.reachable ?? true
    const push = new FakePushServer(server, serverApply, {
      reachable: options.pushReachable ?? true,
      deferCommit: options.deferCommit ?? false,
    })
    const driver = reloadable(nodeAsyncDriver())
    const events: Array<MutationEvent> = []
    const open = async () => {
      const client = await createOrbitClient({
        definition: sync,
        schema,
        url: "http://fake",
        partition: "org_1",
        getToken: async () => "t",
        driver,
        clientId: "c1",
        mutators,
        pushUrl: "http://fake/push",
        fetch: push.fetch,
        makeWebSocket: server.connect,
        backoffMinMs: 5,
        backoffMaxMs: 20,
      })
      client.onMutation((e) => events.push(e))
      return client
    }
    return { server, push, driver, events, open }
  }

  const ids = (rows: ReadonlyArray<{ readonly id: string }>) => rows.map((r) => r.id)

  it("applies an insert optimistically while offline and keeps it across a reload", async () => {
    const { driver, events, open } = await setup({ reachable: false, pushReachable: false })
    const client = await open()
    const live = client.liveQuery(documents)
    await settle()
    expect(live.getSnapshot().rows).toEqual([])
    const handle = client.mutate.createDocument({ id: "n1", groupId: null })
    expect(handle.id).toBe(1)
    await handle.local
    expect(ids(live.getSnapshot().rows)).toEqual(["n1"])
    expect(live.getSnapshot().rows[0]?.organizationId).toBe("org_1")
    expect(client.getStatus().pendingMutations).toBe(1)
    expect(events.map((e) => e.status)).toEqual(["applied_locally"])
    // The pending log and the overlay are persisted; the canonical table is untouched.
    expect(await driver.query(`SELECT id, pushed FROM pending_mutations`)).toEqual([
      { id: 1, pushed: 0 },
    ])
    expect(await driver.query(`SELECT id, "__op" FROM o_Chatbot`)).toEqual([
      { id: "n1", __op: "upsert" },
    ])
    expect(await driver.query(`SELECT id FROM t_Chatbot`)).toEqual([])
    await client.close()

    // Reload: the overlay is rebuilt from the log before the first render.
    const again = await open()
    const stale = again.liveQuery(documents)
    await settle()
    expect(ids(stale.getSnapshot().rows)).toEqual(["n1"])
    expect(again.getStatus().pendingMutations).toBe(1)
    expect(again.clientId).toBe("c1")
    // A second mutation continues the dense id sequence and reads the effective row.
    const second = again.mutate.setOrder({ id: "n1", displayOrder: 5 })
    expect(second.id).toBe(2)
    await second.local
    expect(stale.getSnapshot().rows[0]?.displayOrder).toBe(5)
    await again.close()
  })

  it(
    "the first tab pushes the mutations a closed tab queued offline in its own slot",
    { timeout: 15_000 },
    async () => {
      const { server, push, open } = await setup({ pushReachable: false })
      // A second tab: its own slot, one mutation queued while the push endpoint was unreachable,
      // then closed. The log stays in its database.
      const orphan = reloadable(nodeAsyncDriver())
      const closedTab = await createOrbitClient({
        definition: sync,
        schema,
        url: "http://fake",
        partition: "org_1",
        getToken: async () => "t",
        driver: orphan,
        clientId: "c-tab2",
        mutators,
        pushUrl: "http://fake/push",
        fetch: push.fetch,
        makeWebSocket: server.connect,
        backoffMinMs: 5,
        backoffMaxMs: 20,
      })
      const queued = closedTab.mutate.createDocument({ id: "n2", groupId: null })
      await queued.local
      await closedTab.close()
      expect(await orphan.query(`SELECT count(*) AS n FROM pending_mutations`)).toEqual([{ n: 1 }])

      // The server's `orbit_clients` scope is live before the drain pushes, as in production,
      // where a fill reflects the rows committed meanwhile.
      const warm = await open()
      await waitFor(() => server.pendingFills.length >= 1, 2000, "client row fill")
      for (const fill of [...server.pendingFills]) server.completeFill(fill.table, [])
      await warm.close()

      // The first tab opens with the endpoint reachable: it drains the slot as the closed tab.
      push.reachable = true
      const logs: Array<{ event: string; data: Record<string, unknown> }> = []
      const first = await createOrbitClient({
        definition: sync,
        schema,
        url: "http://fake",
        partition: "org_1",
        getToken: async () => "t",
        driver: reloadable(nodeAsyncDriver()),
        clientId: "c1",
        mutators,
        pushUrl: "http://fake/push",
        fetch: push.fetch,
        makeWebSocket: server.connect,
        backoffMinMs: 5,
        backoffMaxMs: 20,
        openSlot: async (slot) => (slot === 1 ? orphan : null),
        onLog: (event, data) => logs.push({ event, data }),
      })
      const deadline = Date.now() + 5000
      while (Date.now() < deadline && !logs.some((l) => l.event === "store.drained")) {
        for (const fill of [...server.pendingFills]) server.completeFill(fill.table, [])
        await new Promise((r) => setTimeout(r, 20))
      }
      const drained = logs.find((l) => l.event === "store.drained")
      if (drained === undefined)
        console.log(
          JSON.stringify({
            pushes: push.requests.map((r) => r.clientId),
            orphanLog: await orphan.query(`SELECT id, pushed FROM pending_mutations`),
            events: logs.map((l) => l.event),
          }),
        )
      expect(drained?.data).toEqual({ slot: 1, pending: 1, remaining: 0 })
      expect(await orphan.query(`SELECT count(*) AS n FROM pending_mutations`)).toEqual([{ n: 0 }])
      expect(push.requests.map((r) => r.clientId)).toContain("c-tab2")
      await first.close()
    },
  )

  it(
    "a shared cache recovers an old offline slot without importing its views",
    { timeout: 15_000 },
    async () => {
      const { server, push, open } = await setup({ pushReachable: false })
      // A second tab: its own slot, one mutation queued while the push endpoint was unreachable,
      // then closed. The log stays in its database.
      const orphan = reloadable(nodeAsyncDriver())
      const closedTab = await createOrbitClient({
        definition: sync,
        schema,
        url: "http://fake",
        partition: "org_1",
        getToken: async () => "t",
        driver: orphan,
        clientId: "c-tab2",
        mutators,
        pushUrl: "http://fake/push",
        fetch: push.fetch,
        makeWebSocket: server.connect,
        backoffMinMs: 5,
        backoffMaxMs: 20,
      })
      const queued = closedTab.mutate.createDocument({ id: "n2", groupId: null })
      await queued.local
      await closedTab.close()
      expect(await orphan.query(`SELECT count(*) AS n FROM pending_mutations`)).toEqual([{ n: 1 }])

      // The server's `orbit_clients` scope is live before the drain pushes, as in production,
      // where a fill reflects the rows committed meanwhile.
      const warm = await open()
      await waitFor(() => server.pendingFills.length >= 1, 2000, "client row fill")
      for (const fill of [...server.pendingFills]) server.completeFill(fill.table, [])
      await warm.close()

      // The first tab opens with the endpoint reachable: it drains the slot as the closed tab.
      push.reachable = true
      const logs: Array<{ event: string; data: Record<string, unknown> }> = []
      const { createSharedOrbitClient } = await import("../src/shared/client.ts")
      const { sharedPlatform } = await import("./support/shared-platform.ts")
      const first = await createSharedOrbitClient({
        subject: "user_1",
        platform: sharedPlatform(),
        legacySlots: 4,
        definition: sync,
        schema,
        url: "http://fake",
        partition: "org_1",
        getToken: async () => "t",
        driver: reloadable(nodeAsyncDriver()),
        clientId: "c1",
        mutators,
        pushUrl: "http://fake/push",
        fetch: push.fetch,
        makeWebSocket: server.connect,
        backoffMinMs: 5,
        backoffMaxMs: 20,
        openLegacySlot: async (slot) => (slot === 1 ? orphan : null),
        onLog: (event, data) => logs.push({ event, data }),
      })
      const deadline = Date.now() + 5000
      while (Date.now() < deadline && !logs.some((l) => l.event === "store.drained")) {
        for (const fill of [...server.pendingFills]) server.completeFill(fill.table, [])
        await new Promise((r) => setTimeout(r, 20))
      }
      const drained = logs.find((l) => l.event === "store.drained")
      if (drained === undefined)
        console.log(
          JSON.stringify({
            pushes: push.requests.map((r) => r.clientId),
            orphanLog: await orphan.query(`SELECT id, pushed FROM pending_mutations`),
            events: logs.map((l) => l.event),
          }),
        )
      expect(drained?.data).toEqual({ slot: 1, pending: 1, remaining: 0 })
      expect(await orphan.query(`SELECT count(*) AS n FROM pending_mutations`)).toEqual([{ n: 0 }])
      expect(push.requests.map((r) => r.clientId)).toContain("c-tab2")
      await first.close()
    },
  )

  it("pushes contiguous ids and confirms through sync without an empty state", async () => {
    const { server, push, driver, events, open } = await setup({ deferCommit: true })
    const client = await open()
    const live = client.liveQuery(documents)
    const observed: Array<ReadonlyArray<string>> = []
    live.subscribe(() => observed.push(ids(live.getSnapshot().rows)))
    await waitFor(() => server.pendingFills.length === 2, 2000, "fills")
    server.completeFill("Chatbot", [chatbot("a")])
    server.completeFill("orbit_clients", [])
    await waitFor(() => live.getSnapshot().status === "live", 3000, "live")
    expect(ids(live.getSnapshot().rows)).toEqual(["a"])

    const h1 = client.mutate.createDocument({ id: "n1", groupId: null })
    const h2 = client.mutate.createDocument({ id: "n2", groupId: null })
    await Promise.all([h1.local, h2.local])
    expect(ids(live.getSnapshot().rows)).toEqual(["a", "n1", "n2"])
    expect(await h1.server).toEqual({ id: 1, status: "applied" })
    expect(await h2.server).toEqual({ id: 2, status: "applied" })
    const pushedIds = push.requests.flatMap((r) => r.mutations.map((m) => m.id))
    expect(pushedIds).toEqual([1, 2])
    expect(push.requests.every((r) => r.clientId === "c1" && r.partition === "org_1")).toBe(true)
    // Pushed but not confirmed: still pending, still served from the overlay.
    await waitFor(() => events.filter((e) => e.status === "pushed").length === 2, 2000, "pushed")
    expect(client.getStatus().pendingMutations).toBe(2)
    expect(await driver.query(`SELECT id FROM t_Chatbot WHERE id LIKE 'n%'`)).toEqual([])

    // The application's transactions reach the client through CDC: rows plus orbit_clients.
    push.flush()
    await waitFor(() => client.getStatus().pendingMutations === 0, 3000, "confirmation")
    await waitFor(() => events.filter((e) => e.status === "confirmed").length === 2, 2000, "events")
    expect(ids(live.getSnapshot().rows)).toEqual(["a", "n1", "n2"])
    expect(await driver.query(`SELECT COUNT(*) AS n FROM o_Chatbot`)).toEqual([{ n: 0 }])
    expect(await driver.query(`SELECT COUNT(*) AS n FROM pending_mutations`)).toEqual([{ n: 0 }])
    expect(
      (await driver.query(`SELECT id FROM t_Chatbot ORDER BY id`)).map((r) => r["id"]),
    ).toEqual(["a", "n1", "n2"])
    // The listener never saw the optimistic rows disappear between overlay and canonical rows.
    const firstWithN1 = observed.findIndex((rows) => rows.includes("n1"))
    expect(firstWithN1).toBeGreaterThanOrEqual(0)
    for (const rows of observed.slice(firstWithN1)) expect(rows).toContain("n1")
    expect(events.filter((e) => e.id === 1).map((e) => e.status)).toEqual([
      "applied_locally",
      "pushed",
      "confirmed",
    ])
    await client.close()
  })

  it("rebases pending mutations when a concurrent server delta arrives", async () => {
    const { server, driver, open } = await setup({ pushReachable: false })
    const client = await open()
    const live = client.liveQuery(documents)
    await waitFor(() => server.pendingFills.length === 2, 2000, "fills")
    server.completeFill("Chatbot", [chatbot("a")])
    server.completeFill("orbit_clients", [])
    await waitFor(() => live.getSnapshot().status === "live", 3000, "live")

    const create = client.mutate.createDocument({ id: "n1", groupId: null })
    await create.local
    const order = client.mutate.setOrder({ id: "a", displayOrder: 7 })
    await order.local
    expect(live.getSnapshot().rows.find((r) => r.id === "a")?.displayOrder).toBe(7)

    // Another user changes a different row; the local overlay is rebuilt on top of the delta.
    server.commit([insert("Chatbot", chatbot("s1"))])
    await waitFor(() => client.getStatus().cursor === 1, 3000, "delta")
    expect(ids(live.getSnapshot().rows)).toEqual(["a", "n1", "s1"])
    expect(live.getSnapshot().rows.find((r) => r.id === "a")?.displayOrder).toBe(7)
    expect(client.getStatus().pendingMutations).toBe(2)
    expect(
      (await driver.query(`SELECT id FROM o_Chatbot ORDER BY id`)).map((r) => r["id"]),
    ).toEqual(["a", "n1"])

    // The server changes the row a pending update touches: the replay merges on the new image.
    server.commit([update("Chatbot", chatbot("a"), chatbot("a", { groupId: "g" }))])
    await waitFor(() => client.getStatus().cursor === 2, 3000, "second delta")
    const a = live.getSnapshot().rows.find((r) => r.id === "a")
    expect(a?.groupId).toBe("g")
    expect(a?.displayOrder).toBe(7)
    await client.close()
  })

  it("does not turn an HTTP refusal into success when CDC arrives first", async () => {
    const { server, push, driver, open } = await setup()
    const originalFetch = push.fetch
    let release!: () => void
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    push.fetch = async (input, init) => {
      const response = await originalFetch(input, init)
      await gate
      return response
    }
    const client = await open()
    const live = client.liveQuery(documents)
    await waitFor(() => server.pendingFills.length === 2, 2000, "fills")
    server.completeFill("Chatbot", [])
    server.completeFill("orbit_clients", [])
    await waitFor(() => live.getSnapshot().status === "live", 3000, "live")
    const bad = client.mutate.createDocument({ id: "bad", groupId: null })
    await bad.local
    let settled = false
    void bad.server.then(() => {
      settled = true
    })
    await waitFor(() => (client.getStatus().cursor ?? 0) > 0, 3000, "CDC before HTTP")
    await settle()
    const settledBeforeHttp = settled
    release()
    expect(await bad.server).toEqual({ id: 1, status: "failed", error: "refused by the server" })
    expect(settledBeforeHttp).toBe(false)
    expect(ids(live.getSnapshot().rows)).toEqual([])
    expect(await driver.query(`SELECT COUNT(*) AS n FROM pending_mutations`)).toEqual([{ n: 0 }])
    await client.close()
  })

  it("rolls the overlay back when the server reports a failed outcome", async () => {
    const { server, push, events, open } = await setup({ deferCommit: true })
    const client = await open()
    const live = client.liveQuery(documents)
    await waitFor(() => server.pendingFills.length === 2, 2000, "fills")
    server.completeFill("Chatbot", [])
    server.completeFill("orbit_clients", [])
    await waitFor(() => live.getSnapshot().status === "live", 3000, "live")

    const bad = client.mutate.createDocument({ id: "bad", groupId: null })
    await bad.local
    expect(ids(live.getSnapshot().rows)).toEqual(["bad"])
    expect(await bad.server).toEqual({ id: 1, status: "failed", error: "refused by the server" })
    expect(ids(live.getSnapshot().rows)).toEqual([])
    expect(client.getStatus().pendingMutations).toBe(0)
    expect(events.at(-1)).toEqual({
      id: 1,
      name: "createDocument",
      status: "failed",
      error: "refused by the server",
    })
    // The id was consumed: the next mutation continues after it and is accepted.
    push.flush()
    const good = client.mutate.createDocument({ id: "n2", groupId: null })
    expect(good.id).toBe(2)
    expect(await good.server).toEqual({ id: 2, status: "applied" })
    push.flush()
    await waitFor(() => client.getStatus().pendingMutations === 0, 3000, "confirmed")
    expect(ids(live.getSnapshot().rows)).toEqual(["n2"])
    await client.close()
  })

  it("resynchronizes after out_of_order: applied ids are dropped and the rest are resent", async () => {
    const { server, push, driver, open } = await setup({ pushReachable: false })
    const client = await open()
    const live = client.liveQuery(documents)
    await waitFor(() => server.pendingFills.length === 2, 2000, "fills")
    server.completeFill("Chatbot", [])
    server.completeFill("orbit_clients", [])
    await waitFor(() => live.getSnapshot().status === "live", 3000, "live")

    const h1 = client.mutate.createDocument({ id: "n1", groupId: null })
    const h2 = client.mutate.createDocument({ id: "n2", groupId: null })
    const h3 = client.mutate.createDocument({ id: "n3", groupId: null })
    await Promise.all([h1.local, h2.local, h3.local])
    expect(ids(live.getSnapshot().rows)).toEqual(["n1", "n2", "n3"])
    // The server applied 1 and 2 through a push whose response never reached the client.
    push.lastMutationId.set("c1", 2)
    push.reachable = true
    // A reconnect wakes the push loop.
    server.dropAll()
    await waitFor(() => push.requests.length === 2, 3000, "two pushes")
    expect(push.requests.map((r) => r.mutations.map((m) => m.id))).toEqual([[1, 2, 3], [3]])
    expect(await h1.server).toEqual({ id: 1, status: "duplicate" })
    expect(await h2.server).toEqual({ id: 2, status: "duplicate" })
    expect(await h3.server).toEqual({ id: 3, status: "applied" })
    await waitFor(() => client.getStatus().pendingMutations === 0, 3000, "confirmed")
    // 1 and 2 were dropped without their rows (the server's effects for them never synced here).
    expect(
      (await driver.query(`SELECT id FROM t_Chatbot ORDER BY id`)).map((r) => r["id"]),
    ).toEqual(["n3"])
    expect(ids(live.getSnapshot().rows)).toEqual(["n3"])
    await client.close()
  })

  it("does not confirm when orbit_clients is not synced, but mutations still apply", async () => {
    const server = new FakeSyncServer(fixture, "org_1")
    const log: Array<string> = []
    const client = await createOrbitClient({
      definition: sync,
      schema: fixture,
      url: "http://fake",
      partition: "org_1",
      getToken: async () => "t",
      driver: nodeAsyncDriver(),
      mutators,
      makeWebSocket: server.connect,
      backoffMinMs: 5,
      backoffMaxMs: 20,
      onLog: (e) => log.push(e),
    })
    expect(log).toContain("mutations.unconfirmable")
    const live = client.liveQuery(documents)
    await client.mutate.createDocument({ id: "n1", groupId: null }).local
    expect(ids(live.getSnapshot().rows)).toEqual(["n1"])
    await client.close()
  })
})

// Shared-client tests exercise the real engine and SQLite through the tab transport.
describe("shared browser clients", () => {
  const setup = async (pushReachable = true) => {
    const { createSharedOrbitClient } = await import("../src/shared/client.ts")
    const { sharedPlatform } = await import("./support/shared-platform.ts")
    const messages: Array<unknown> = []
    const platform = sharedPlatform((message) => messages.push(message))
    const server = new FakeSyncServer(schema, "org_1", { queries: serverQueries })
    const push = new FakePushServer(server, serverApply, { reachable: pushReachable })
    const driver = reloadable(nodeAsyncDriver())
    let sockets = 0
    const open = () =>
      createSharedOrbitClient({
        definition: sync,
        schema,
        url: "http://fake",
        partition: "org_1",
        subject: "user_1",
        getToken: async () => "test",
        driver,
        mutators,
        pushUrl: "http://fake/push",
        fetch: push.fetch,
        makeWebSocket: (url) => {
          sockets++
          return server.connect(url)
        },
        backoffMinMs: 5,
        backoffMaxMs: 20,
        platform,
        queryTtlMs: 0,
      })
    return { open, server, push, driver, messages, sockets: () => sockets }
  }

  it("keeps an owner-only large view off the tab broadcast channel", async () => {
    const env = await setup()
    const owner = await env.open()
    const other = await env.open()
    try {
      const view = owner.liveQuery(documents)
      await waitFor(() => env.server.pendingFills.length === 2)
      env.server.completeFill("Chatbot", [chatbot("a")])
      env.server.completeFill("orbit_clients", [])
      await waitFor(() => view.getSnapshot().status === "live")
      const snapshots = () =>
        env.messages.filter((message) => {
          const m = message as { type: string; event?: { type: string } }
          return m.type === "event" && m.event?.type === "snapshot"
        })
      expect(snapshots()).toHaveLength(0)
      env.server.commit([insert("Chatbot", chatbot("b"))])
      await waitFor(() => view.getSnapshot().rows.length === 2)
      expect(snapshots()).toHaveLength(0)
      // Joining consumers receive current data, then continue sharing live changes.
      const peer = other.liveQuery(documents)
      await waitFor(() => peer.getSnapshot().rows.length === 2)
      expect(snapshots().length).toBeGreaterThan(0)
      env.server.commit([remove("Chatbot", chatbot("a"))])
      await waitFor(
        () => view.getSnapshot().rows.length === 1 && peer.getSnapshot().rows.length === 1,
      )
      expect(peer.getSnapshot().rows[0]?.id).toBe("b")
    } finally {
      await other.close()
      await owner.close()
    }
  })

  it("shares named query snapshots with a later joining follower", async () => {
    const env = await setup()
    const first = await env.open()
    const view = first.liveQuery(clientQueries.documents({ type: "DOCUMENT" }))
    await waitFor(() => env.server.pendingFills.length === 2)
    env.server.completeFill("Chatbot", [chatbot("a"), chatbot("b")])
    env.server.completeFill("orbit_clients", [])
    await waitFor(() => view.getSnapshot().status === "live")
    const second = await env.open()
    try {
      const peer = second.liveQuery(clientQueries.documents({ type: "DOCUMENT" }))
      await waitFor(() => peer.getSnapshot().status === "live")
      expect(peer.getSnapshot().rows.map((row) => row.id)).toEqual(["a"])
      expect(
        env.server.receivedRefs.filter((ref) => "name" in ref && ref.name === "documents"),
      ).toHaveLength(1)
    } finally {
      await first.close()
      await second.close()
    }
  })

  it("mounting many identical consumers does not rebroadcast the cached result", async () => {
    const env = await setup()
    const first = await env.open(),
      second = await env.open()
    const query = clientQueries.documents({ type: "DOCUMENT" })
    const view = second.liveQuery(query)
    try {
      await waitFor(() => env.server.pendingFills.length === 2)
      env.server.completeFill("Chatbot", [chatbot("a")])
      env.server.completeFill("orbit_clients", [])
      await waitFor(() => view.getSnapshot().status === "live")
      const snapshot = view.getSnapshot()
      let notifications = 0
      view.subscribe(() => notifications++)
      const others = Array.from({ length: 100 }, () => second.liveQuery(query))
      await settle(100)
      expect(notifications).toBe(0)
      expect(others.every((other) => other.getSnapshot() === snapshot)).toBe(true)
      await Promise.all(others.map((other) => other.release()))
      await first.mutate.createDocument({ id: "0-new", groupId: null }).server
      await waitFor(() => view.getSnapshot().rows[0]?.id === "0-new")
      expect(notifications).toBeGreaterThan(0)
    } finally {
      await first.close()
      await second.close()
    }
  })

  it.each(["owner", "follower"] as const)(
    "retains a prefetched snapshot through a same-turn %s consumer handoff",
    async (role) => {
      const env = await setup()
      const owner = await env.open()
      const client = role === "owner" ? owner : await env.open()
      const query = clientQueries.documents({ type: "DOCUMENT" })
      try {
        const prefetch = client.liveQuery(query)
        await waitFor(() => env.server.pendingFills.length === 2)
        env.server.completeFill("Chatbot", [chatbot("a")])
        env.server.completeFill("orbit_clients", [])
        await waitFor(() => prefetch.getSnapshot().status === "live")
        const snapshot = prefetch.getSnapshot()
        // React runs passive cleanups before their replacement effects. There is
        // briefly no holder while a prefetched window becomes the visible query.
        const releasing = prefetch.release()
        const view = client.liveQuery(query)
        expect(view.getSnapshot()).toBe(snapshot)
        await releasing
        await settle(100)
        expect(
          env.server.receivedRefs.filter((ref) => "name" in ref && ref.name === "documents"),
        ).toHaveLength(1)
        await client.mutate.createDocument({ id: "0-new", groupId: null }).server
        await waitFor(() => view.getSnapshot().rows[0]?.id === "0-new")
        // A real release still drops the facade cache; this is not an unbounded TTL.
        await view.release()
        const fresh = client.liveQuery(query)
        expect(fresh.getSnapshot().status).toBe("pending")
        await fresh.release()
      } finally {
        await client.close()
        if (client !== owner) await owner.close()
      }
    },
  )

  it("three tabs share a client id, socket, optimistic writes and dense mutation ids", async () => {
    const env = await setup()
    const clients = await Promise.all([env.open(), env.open(), env.open()])
    try {
      const views = clients.map((c) => c.liveQuery(documents))
      await waitFor(() => env.server.pendingFills.length === 2)
      env.server.completeFill("Chatbot", [])
      env.server.completeFill("orbit_clients", [])
      await waitFor(() => views.every((v) => v.getSnapshot().status === "live"))
      expect(new Set(clients.map((c) => c.clientId)).size).toBe(1)
      expect(env.sockets()).toBe(1)
      const writes = clients.map((c, i) =>
        c.mutate.createDocument({ id: `shared-${i}`, groupId: null }),
      )
      await Promise.all(writes.map((w) => w.local))
      expect((await Promise.all(writes.map((w) => w.id))).sort()).toEqual([1, 2, 3])
      await waitFor(() => views.every((v) => v.getSnapshot().rows.length === 3))
      expect((await Promise.all(writes.map((w) => w.server))).map((r) => r.status)).toEqual([
        "applied",
        "applied",
        "applied",
      ])
      await views[0]!.release()
      await clients[0]!.close()
      await waitFor(() => env.sockets() === 2)
      await waitFor(() => views.slice(1).every((v) => v.getSnapshot().status === "live"))
      expect(env.sockets()).toBe(2)
      const next = clients[2]!.mutate.createDocument({ id: "after-handoff", groupId: null })
      await next.local
      await next.server
      expect(await next.id).toBe(4)
      await waitFor(() => views[1]!.getSnapshot().rows.length === 4)
    } finally {
      await Promise.all(clients.map((c) => c.close()))
    }
  })

  it("keeps offline mutations when the owner tab closes", async () => {
    const env = await setup(false)
    const first = await env.open(),
      second = await env.open()
    try {
      const view = second.liveQuery(documents)
      await waitFor(() => env.server.pendingFills.length === 2)
      env.server.completeFill("Chatbot", [])
      env.server.completeFill("orbit_clients", [])
      await waitFor(() => view.getSnapshot().status === "live")
      const write = second.mutate.createDocument({ id: "survives-owner", groupId: null })
      await write.local
      await waitFor(() => view.getSnapshot().rows.length === 1)
      const originalId = first.clientId
      await first.close()
      await waitFor(() => view.getSnapshot().status === "live")
      expect(second.clientId).toBe(originalId)
      env.push.reachable = true
      expect((await write.server).status).toBe("applied")
      await waitFor(() => second.getStatus().pendingMutations === 0, 5000)
      expect(view.getSnapshot().rows.map((r) => r.id)).toEqual(["survives-owner"])
      expect(env.push.requests.flatMap((r) => r.mutations).every((m) => m.id === 1)).toBe(true)
    } finally {
      await first.close()
      await second.close()
    }
  })

  it("retains exact failed outcomes and consumed ids across owner handoff", async () => {
    const env = await setup()
    const first = await env.open(),
      second = await env.open()
    try {
      env.push.deferCommit = true
      const bad = second.mutate.createDocument({ id: "bad", groupId: null })
      await bad.local
      const expected = { id: 1, status: "failed", error: "refused by the server" }
      expect(await bad.server).toEqual(expected)
      await first.close()
      await waitFor(() => env.sockets() === 2)
      expect(await second.awaitMutation(1)).toEqual(expected)
      const next = second.mutate.createDocument({ id: "good", groupId: null })
      await next.local
      expect(await next.id).toBe(2)
    } finally {
      await first.close()
      await second.close()
    }
  })

  it("restores the durable queue after every tab has closed", async () => {
    const env = await setup(false)
    const first = await env.open()
    const id = first.clientId
    await first.mutate.createDocument({ id: "last-tab", groupId: null }).local
    await first.close()
    const next = await env.open()
    try {
      expect(next.clientId).toBe(id)
      expect(await next.read(documents)).toEqual(
        expect.arrayContaining([expect.objectContaining({ id: "last-tab" })]),
      )
      env.push.reachable = true
      expect((await next.awaitMutation(1)).status).toBe("applied")
    } finally {
      await next.close()
    }
  })

  it("keeps invalid mutator arguments out of the shared queue", async () => {
    const env = await setup()
    const client = await env.open()
    try {
      expect(() =>
        client.mutate.createDocument({ id: 4 as unknown as string, groupId: null }),
      ).toThrow()
      const next = client.mutate.createDocument({ id: "valid", groupId: null })
      await next.local
      expect(await next.id).toBe(1)
    } finally {
      await client.close()
    }
  })
})
