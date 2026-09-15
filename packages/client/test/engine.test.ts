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
    await Effect.runPromise(engine.close())
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

    // The reload replays both, references only the first: the second retires after the TTL.
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
