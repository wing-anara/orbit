/**
 * The real Durable Object in workerd: HTTP internal endpoints, hibernating WebSockets, the fill
 * registry, eviction, and alarms.
 */

import { env, SELF } from "cloudflare:test"
import { evictDurableObject, runDurableObjectAlarm } from "cloudflare:test"
import { Effect, Schema } from "effect"
import { afterEach, describe, expect, it } from "vitest"

import {
  CdcBatch,
  INTERNAL_PROTOCOL_VERSION,
  type FillChunk,
  type PartitionTransaction,
  type RowChange,
} from "@orbit/protocol"
import {
  CLIENT_PROTOCOL_VERSION,
  decodeServerMessage,
  encodeClientMessage,
  type ClientMessage,
  type ServerMessage,
} from "@orbit/protocol/client"

import { signToken } from "../../src/authorizer.ts"
import { durableObjectNameFor } from "../../src/placement.ts"
import { schema } from "../worker/index.ts"

const SECRET = "test-secret"
const U1 = "a2523813-adbe-11f1-b19c-0a2250a7ed6c"
const base = "https://orbit.test/orbit"

const internal = (path: string, init: RequestInit = {}) =>
  SELF.fetch(`${base}${path}`, {
    ...init,
    headers: {
      authorization: `Bearer ${SECRET}`,
      "content-type": "application/json",
      ...(init.headers ?? {}),
    },
  })

const token = (partitions: ReadonlyArray<string> | "*", sub = "user-1", expInSeconds = 3600) =>
  Effect.runPromise(
    signToken("test-token-secret", {
      sub,
      partitions,
      exp: Math.floor(Date.now() / 1000) + expInSeconds,
    }),
  )

const chatbot = (id: string, over: Record<string, unknown> = {}) => ({
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

const insert = (table: string, row: Record<string, unknown>): RowChange => ({
  table,
  op: "insert",
  key: [row["id"] as string],
  after: row as never,
})

const txn = (seq: number, changes: ReadonlyArray<RowChange>): PartitionTransaction => ({
  seq,
  keyspace: "ks",
  shard: "0",
  gtid: `${U1}:${seq}`,
  position: `MySQL56/${U1}:1-${seq}`,
  commit_timestamp: 1_789_000_000 + seq,
  changes: [...changes],
  trace: {},
})

const batch = (
  partition: string,
  transactions: ReadonlyArray<PartitionTransaction>,
  epoch = 0,
): CdcBatch => ({
  protocol_version: INTERNAL_PROTOCOL_VERSION,
  schema_hash: schema.schema_hash,
  stream_epoch: epoch,
  partition,
  transactions: [...transactions],
  delivery_id: `d${transactions[0]?.seq ?? 0}`,
})

const deliver = async (
  partition: string,
  transactions: ReadonlyArray<PartitionTransaction>,
  epoch = 0,
) => {
  const res = await internal(`/internal/cdc/${encodeURIComponent(partition)}`, {
    method: "POST",
    body: JSON.stringify(Schema.encodeSync(CdcBatch)(batch(partition, transactions, epoch))),
  })
  return { status: res.status, body: (await res.json()) as Record<string, unknown> }
}

const pollFills = async () => {
  const res = await internal("/internal/fills/next?wait=1")
  expect(res.status).toBe(200)
  return (await res.json()) as {
    requests: Array<{ fill_id: string; table: string; partition: string }>
  }
}

const uploadFill = async (
  fillId: string,
  rows: ReadonlyArray<Record<string, unknown>>,
  gno: number,
  status: "completed" | "failed" = "completed",
) => {
  const lines: Array<FillChunk> = []
  if (rows.length > 0) lines.push({ type: "rows", fill_id: fillId, rows: rows as never })
  lines.push({
    type: "done",
    fill_id: fillId,
    result:
      status === "completed"
        ? {
            status: "completed",
            position: gno === 0 ? "" : `MySQL56/${U1}:1-${gno}`,
            keyspace: "ks",
            shard: "0",
            row_count: rows.length,
            duration_ms: 1,
          }
        : { status: "failed", error: { code: "timeout", after_ms: 1 } },
  })
  const res = await internal(`/internal/fills/${encodeURIComponent(fillId)}`, {
    method: "POST",
    headers: { "content-type": "application/x-ndjson" },
    body: lines.map((l) => JSON.stringify(l)).join("\n") + "\n",
  })
  return { status: res.status, body: (await res.json()) as Record<string, unknown> }
}

/** Minimal WebSocket client with a message queue. */
class Client {
  readonly messages: Array<ServerMessage> = []
  private waiters: Array<() => void> = []
  closed: { code: number; reason: string } | null = null
  constructor(readonly ws: WebSocket) {
    ws.addEventListener("message", (e) => {
      this.messages.push(decodeServerMessage(JSON.parse(String(e.data))))
      const w = this.waiters
      this.waiters = []
      for (const f of w) f()
    })
    ws.addEventListener("close", (e) => {
      this.closed = { code: e.code, reason: e.reason }
      const w = this.waiters
      this.waiters = []
      for (const f of w) f()
    })
  }
  send(m: ClientMessage): void {
    this.ws.send(JSON.stringify(encodeClientMessage(m)))
  }
  async next<T extends ServerMessage["type"]>(
    type: T,
    timeoutMs = 5000,
  ): Promise<Extract<ServerMessage, { type: T }>> {
    const deadline = Date.now() + timeoutMs
    for (;;) {
      const i = this.messages.findIndex((m) => m.type === type)
      if (i >= 0) return this.messages.splice(i, 1)[0] as Extract<ServerMessage, { type: T }>
      if (this.closed !== null)
        throw new Error(
          `socket closed ${this.closed.code} ${this.closed.reason} while waiting for ${type}`,
        )
      const remaining = deadline - Date.now()
      if (remaining <= 0)
        throw new Error(
          `timeout waiting for ${type}; have ${this.messages.map((m) => m.type).join(",")}`,
        )
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, remaining)
        this.waiters.push(() => {
          clearTimeout(t)
          resolve()
        })
      })
    }
  }
  async waitClosed(timeoutMs = 5000): Promise<{ code: number; reason: string }> {
    const deadline = Date.now() + timeoutMs
    while (this.closed === null) {
      if (Date.now() > deadline) throw new Error("socket did not close")
      await new Promise<void>((resolve) => {
        const t = setTimeout(resolve, 50)
        this.waiters.push(() => {
          clearTimeout(t)
          resolve()
        })
      })
    }
    return this.closed
  }
}

const connect = async (partition: string, tok: string): Promise<Client> => {
  const res = await SELF.fetch(
    `${base}/ws?partition=${encodeURIComponent(partition)}&token=${encodeURIComponent(tok)}`,
    { headers: { upgrade: "websocket" } },
  )
  expect(res.status).toBe(101)
  const ws = res.webSocket
  if (ws === null) throw new Error("no websocket")
  ws.accept()
  return new Client(ws)
}

const summary = () => ({
  schemaHash: schema.schema_hash,
  tables: schema.tables.map((t) => ({ name: t.name, columns: t.columns.map((c) => c.name) })),
})

const hello = (
  partition: string,
  subscriptions: Array<{ id: string; query: { table: string; [k: string]: unknown } }> = [],
  over: Partial<Extract<ClientMessage, { type: "hello" }>> = {},
): ClientMessage => ({
  type: "hello",
  protocolVersion: CLIENT_PROTOCOL_VERSION,
  clientId: "c1",
  token: "unused-at-do-level",
  partition,
  schema: summary(),
  cursor: null,
  subscriptions: subscriptions.map((s) => ({
    type: "subscribe" as const,
    id: s.id,
    query: s.query as never,
  })),
  ...over,
})

let counter = 0
const freshPartition = () => `org_${Date.now()}_${counter++}`

afterEach(async () => {
  // Nothing: storage isolation per test file; partitions are unique per test.
})

describe("internal endpoints", () => {
  it("rejects requests without the internal secret", async () => {
    const res = await SELF.fetch(`${base}/internal/status/org_x`)
    expect(res.status).toBe(401)
  })

  it("applies CDC batches with dedupe and reports status", async () => {
    const p = freshPartition()
    // Nothing is cached yet: rows for absent scopes are dropped, but the cursor advances.
    const first = await deliver(p, [txn(1, [insert("Chatbot", chatbot("a"))]), txn(2, [])])
    expect(first).toEqual({
      status: 200,
      body: { status: "applied", applied_seq: 2, duplicates: 0, apply_ms: expect.any(Number) },
    })
    const dup = await deliver(p, [txn(2, []), txn(3, [])])
    expect(dup.body).toMatchObject({ status: "applied", applied_seq: 3, duplicates: 1 })
    const gap = await deliver(p, [txn(9, [])])
    expect(gap.body).toEqual({
      status: "rejected",
      reason: { kind: "sequence_gap", applied_seq: 3, first_seq: 9 },
    })
    const status = await internal(`/internal/status/${p}`)
    expect(await status.json()).toMatchObject({
      partition: p,
      appliedSeq: 3,
      scopes: [],
      sessions: 0,
    })
  })

  it("rejects batches for the wrong partition and unknown fill uploads", async () => {
    const p = freshPartition()
    const res = await internal(`/internal/cdc/${p}`, {
      method: "POST",
      body: JSON.stringify(Schema.encodeSync(CdcBatch)(batch("other", [txn(1, [])]))),
    })
    expect(await res.json()).toMatchObject({
      status: "rejected",
      reason: { kind: "wrong_partition" },
    })
    const up = await uploadFill(`${p}:nope`, [], 0)
    expect(up.status).toBe(409)
  })
})

describe("client sessions", () => {
  it("rejects bad tokens and partitions the token does not grant", async () => {
    const denied = await SELF.fetch(
      `${base}/ws?partition=org_secret&token=${encodeURIComponent(await token(["org_1"]))}`,
      { headers: { upgrade: "websocket" } },
    )
    expect(denied.status).toBe(403)
    const bad = await SELF.fetch(`${base}/ws?partition=org_1&token=garbage`, {
      headers: { upgrade: "websocket" },
    })
    expect(bad.status).toBe(401)
    const expired = await SELF.fetch(
      `${base}/ws?partition=org_1&token=${encodeURIComponent(await token(["org_1"], "u", -10))}`,
      { headers: { upgrade: "websocket" } },
    )
    expect(expired.status).toBe(401)
  })

  it("closes on protocol version mismatch and schema mismatch", async () => {
    const p = freshPartition()
    const c1 = await connect(p, await token([p]))
    c1.send(hello(p, [], { protocolVersion: 99 }))
    const err = await c1.next("error")
    expect(err.error.code).toBe("protocol_version_mismatch")
    expect((await c1.waitClosed()).code).toBe(4400)

    const c2 = await connect(p, await token([p]))
    c2.send(
      hello(p, [], {
        schema: { schemaHash: "other", tables: [{ name: "Chatbot", columns: ["id", "nope"] }] },
      }),
    )
    expect((await c2.next("error")).error.code).toBe("schema_mismatch")
    expect((await c2.waitClosed()).code).toBe(4409)

    const c3 = await connect(p, await token([p]))
    c3.send(hello("someone-else"))
    expect((await c3.next("error")).error.code).toBe("partition_denied")
  })

  it("bootstraps a subscription through a demand fill, then streams deltas", async () => {
    const p = freshPartition()
    // The stream already delivered a transaction before any client showed up.
    await deliver(p, [txn(1, [insert("Chatbot", chatbot("early"))])])

    const client = await connect(p, await token([p]))
    client.send(
      hello(p, [
        {
          id: "docs",
          query: {
            table: "Chatbot",
            where: { op: "eq", column: "type", value: "DOCUMENT" },
            orderBy: [{ column: "displayOrder", direction: "asc" }],
          },
        },
      ]),
    )
    const welcome = await client.next("welcome")
    expect(welcome).toMatchObject({ partition: p, cursor: 1, schemaHash: schema.schema_hash })
    expect(await client.next("subscribed")).toMatchObject({
      type: "subscribed",
      id: "docs",
      status: "pending",
    })

    // The Rust fill worker polls the registry and finds the request.
    const poll = await pollFills()
    const req = poll.requests.find((r) => r.partition === p)
    expect(req).toMatchObject({ table: "Chatbot", partition: p })

    // Meanwhile a transaction lands: it is held because the scope is filling.
    await deliver(p, [txn(2, [insert("Chatbot", chatbot("during", { displayOrder: 2 }))])])

    // The fill returns the state at gtid 1 (contains "early" but not "during").
    const up = await uploadFill(req!.fill_id, [chatbot("early", { displayOrder: 1 })], 1)
    expect(up.status).toBe(200)
    expect(await client.next("subscribed")).toMatchObject({
      type: "subscribed",
      id: "docs",
      status: "live",
    })
    const snap = await client.next("snapshot")
    expect(snap.cursor).toBe(2)
    expect(snap.rows.map((r) => r.key[0])).toEqual(["early", "during"])
    expect(snap.members).toHaveLength(2)

    // Live delta: insert, update, delete, each atomically, in order.
    await deliver(p, [txn(3, [insert("Chatbot", chatbot("late", { displayOrder: 0 }))])])
    const d3 = await client.next("delta")
    expect(d3.cursor).toBe(3)
    expect(d3.origin.gtid).toBe(`${U1}:3`)
    expect(d3.memberships).toEqual([
      { subscriptionId: "docs", added: [{ table: "Chatbot", key: ["late"] }], removed: [] },
    ])
    expect(d3.rows.map((r) => r.key[0])).toEqual(["late"])

    await deliver(p, [
      txn(4, [
        {
          table: "Chatbot",
          op: "update",
          key: ["late"],
          before: chatbot("late", { displayOrder: 0 }) as never,
          after: chatbot("late", { displayOrder: 9, contents: { v: 2 } }) as never,
        },
      ]),
    ])
    const d4 = await client.next("delta")
    expect(d4.memberships).toEqual([])
    expect(d4.rows).toEqual([
      {
        table: "Chatbot",
        key: ["late"],
        row: expect.objectContaining({ displayOrder: 9, contents: { v: 2 } }),
      },
    ])

    await deliver(p, [
      txn(5, [
        {
          table: "Chatbot",
          op: "delete",
          key: ["early"],
          before: chatbot("early", { displayOrder: 1 }) as never,
        },
      ]),
    ])
    const d5 = await client.next("delta")
    expect(d5.memberships[0]).toEqual({
      subscriptionId: "docs",
      added: [],
      removed: [{ table: "Chatbot", key: ["early"] }],
    })
    expect(d5.rows).toEqual([{ table: "Chatbot", key: ["early"], row: null }])

    client.send({ type: "ack", cursor: 5 })
    client.send({ type: "ping", sentAt: 1 })
    expect((await client.next("pong")).sentAt).toBe(1)
    client.send({ type: "unsubscribe", id: "docs" })
    expect(await client.next("unsubscribed")).toEqual({ type: "unsubscribed", id: "docs" })
    // The subscription outlives its last session for the grace period, so a reload reuses it.
    const status = await (await internal(`/internal/status/${p}`)).json()
    expect(status).toMatchObject({ subscriptions: 1, sessions: 1 })
    await new Promise((r) => setTimeout(r, 1_600))
    await runDurableObjectAlarm(
      env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName(durableObjectNameFor(schema, p))),
    )
    const swept = await (await internal(`/internal/status/${p}`)).json()
    expect(swept).toMatchObject({ subscriptions: 0, sessions: 1 })
    client.ws.close(1000, "done")
  })

  it("survives eviction: sessions and subscriptions come back from storage", async () => {
    const p = freshPartition()
    const client = await connect(p, await token([p]))
    client.send(hello(p, [{ id: "all", query: { table: "Chatbot" } }]))
    await client.next("welcome")
    const req = (await pollFills()).requests.find((r) => r.partition === p)!
    await uploadFill(req.fill_id, [chatbot("x")], 0)
    await client.next("snapshot")

    const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName(durableObjectNameFor(schema, p)))
    await evictDurableObject(stub, { webSockets: "hibernate" })

    await deliver(p, [txn(1, [insert("Chatbot", chatbot("after-eviction"))])])
    const d = await client.next("delta")
    expect(d.memberships[0]?.added.map((m) => m.key[0])).toEqual(["after-eviction"])
    client.send({ type: "ping", sentAt: 2 })
    expect((await client.next("pong")).sentAt).toBe(2)
    client.ws.close(1000, "done")
  })

  it("multiple clients share one materialization and each gets deltas", async () => {
    const p = freshPartition()
    const a = await connect(p, await token([p], "alice"))
    const b = await connect(p, await token("*", "bob"))
    a.send(
      hello(p, [
        { id: "a-sub", query: { table: "Chatbot", orderBy: [{ column: "id", direction: "asc" }] } },
      ]),
    )
    b.send(
      hello(p, [
        { id: "b-sub", query: { table: "Chatbot", orderBy: [{ column: "id", direction: "asc" }] } },
      ]),
    )
    await a.next("welcome")
    await b.next("welcome")
    const req = (await pollFills()).requests.find((r) => r.partition === p)!
    await uploadFill(req.fill_id, [chatbot("shared")], 0)
    expect((await a.next("snapshot")).subscriptionId).toBe("a-sub")
    expect((await b.next("snapshot")).subscriptionId).toBe("b-sub")
    const status = await (await internal(`/internal/status/${p}`)).json()
    expect(status).toMatchObject({ subscriptions: 1, sessions: 2 })
    await deliver(p, [txn(1, [insert("Chatbot", chatbot("new"))])])
    expect((await a.next("delta")).memberships[0]?.subscriptionId).toBe("a-sub")
    expect((await b.next("delta")).memberships[0]?.subscriptionId).toBe("b-sub")
    a.ws.close(1000)
    b.ws.close(1000)
  })

  it("large snapshots are chunked and applied as one unit", async () => {
    const p = freshPartition()
    const client = await connect(p, await token([p]))
    client.send(hello(p, [{ id: "all", query: { table: "Chatbot" } }]))
    await client.next("welcome")
    const req = (await pollFills()).requests.find((r) => r.partition === p)!
    await uploadFill(
      req.fill_id,
      Array.from({ length: 7 }, (_, i) => chatbot(`r${i}`)),
      0,
    )
    const chunks = [
      await client.next("snapshot"),
      await client.next("snapshot"),
      await client.next("snapshot"),
    ]
    expect(chunks.map((c) => c.complete)).toEqual([false, false, true])
    expect(chunks.flatMap((c) => c.rows).length).toBe(7)
    expect(chunks[2]?.members.length).toBe(7)
    client.ws.close(1000)
  })

  it("fill failures are reported and retried by the alarm, then give up", async () => {
    const p = freshPartition()
    const client = await connect(p, await token([p]))
    client.send(hello(p, [{ id: "all", query: { table: "Chatbot" } }]))
    await client.next("welcome")
    const req = (await pollFills()).requests.find((r) => r.partition === p)!
    await uploadFill(req.fill_id, [], 0, "failed")
    const err = await client.next("subscription_error")
    expect(err.error.code).toBe("fill_failed")

    // Subscribe again: a new fill is requested; simulate the fill worker never answering.
    client.send({ type: "subscribe", id: "again", query: { table: "Chatbot" } })
    expect((await client.next("subscribed")).status).toBe("pending")
    const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName(durableObjectNameFor(schema, p)))
    // Each alarm run past the timeout re-issues the fill until attempts are exhausted.
    for (let i = 0; i < 4; i++) {
      await new Promise((r) => setTimeout(r, 2_100))
      await runDurableObjectAlarm(stub)
    }
    const giveUp = await client.next("subscription_error", 10_000)
    expect(giveUp.error.code).toBe("fill_failed")
    client.ws.close(1000)
  }, 20_000)

  it("resolves named queries with the session identity and reports unknown ones", async () => {
    const p = freshPartition()
    await deliver(p, [
      txn(1, [
        insert("Chatbot", chatbot("f1", { type: "GROUP" })),
        insert("Chatbot", chatbot("d1", { groupId: "f1", displayOrder: 1 })),
        insert("Chatbot", chatbot("d2", { groupId: "f2", displayOrder: 2 })),
      ]),
    ])
    const client = await connect(p, await token([p], "alice"))
    client.send(
      hello(p, [
        { id: "docs", query: { name: "documentsInFolder", args: { folderId: "f1" } } as never },
      ]),
    )
    await client.next("welcome")
    const subscribed = await client.next("subscribed")
    // The server resolved the query with the authorized partition, not with client input.
    expect(subscribed).toMatchObject({
      id: "docs",
      query: {
        table: "Chatbot",
        where: {
          op: "and",
          args: [
            { op: "eq", column: "organizationId", value: p },
            { op: "eq", column: "groupId", value: "f1" },
          ],
        },
      },
    })
    const req = (await pollFills()).requests.find((r) => r.partition === p)!
    await uploadFill(
      req.fill_id,
      [
        chatbot("f1", { type: "GROUP", organizationId: p }),
        chatbot("d1", { groupId: "f1", displayOrder: 1, organizationId: p }),
        chatbot("d2", { groupId: "f2", displayOrder: 2, organizationId: p }),
      ],
      1,
    )
    const snap = await client.next("snapshot")
    expect(snap.rows.map((r) => r.key[0])).toEqual(["d1"])

    client.send({ type: "subscribe", id: "nope", query: { name: "missing", args: {} } })
    expect((await client.next("subscription_error")).error.code).toBe("unknown_query")
    client.send({
      type: "subscribe",
      id: "bad",
      query: { name: "documentsInFolder", args: { folderId: 1 } },
    })
    expect((await client.next("subscription_error")).error.code).toBe("unsupported_query")
    // The engine's own query needs the orbit_clients table, which this fixture lacks.
    client.send({ type: "subscribe", id: "me", query: { name: "$orbit.client", args: {} } })
    expect((await client.next("subscription_error")).error.code).toBe("unsupported_query")
    client.ws.close(1000)
  })

  it("epoch changes reset scopes and re-bootstrap subscriptions", async () => {
    const p = freshPartition()
    const client = await connect(p, await token([p]))
    client.send(hello(p, [{ id: "all", query: { table: "Chatbot" } }]))
    await client.next("welcome")
    const req = (await pollFills()).requests.find((r) => r.partition === p)!
    await uploadFill(req.fill_id, [chatbot("old")], 0)
    await client.next("snapshot")
    await deliver(p, [txn(50, [])], 1)
    expect(await client.next("subscribed")).toMatchObject({
      type: "subscribed",
      id: "all",
      status: "pending",
    })
    const again = (await pollFills()).requests.find((r) => r.partition === p)!
    await uploadFill(again.fill_id, [chatbot("fresh")], 0)
    const snap = await client.next("snapshot")
    expect(snap.rows.map((r) => r.key[0])).toEqual(["fresh"])
    client.ws.close(1000)
  })
})
