/**
 * The real Durable Object in workerd: HTTP internal endpoints, hibernating WebSockets, the fill
 * registry, eviction, and alarms.
 */

import { env, SELF, runInDurableObject } from "cloudflare:test"
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
  WS_HEARTBEAT_REQUEST,
  WS_HEARTBEAT_RESPONSE,
  decodeServerMessage,
  encodeClientMessage,
  type ClientMessage,
  type ServerMessage,
} from "@orbit/protocol/client"

import { readAttachment, encodeAttachment } from "../../src/sessions.ts"
import { signToken } from "../../src/authorizer.ts"
import { durableObjectNameFor } from "../../src/placement.ts"
import { schema, revokedQuerySubjects } from "../worker/index.ts"

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
  streamEpoch?: number,
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
    headers: {
      "content-type": "application/x-ndjson",
      ...(streamEpoch === undefined ? {} : { "x-orbit-stream-epoch": String(streamEpoch) }),
    },
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
      if (e.data === WS_HEARTBEAT_RESPONSE) return
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

  it("serves the compiled sync schema to the engine", async () => {
    const res = await internal("/internal/schema")
    expect(res.status).toBe(200)
    const body = (await res.json()) as { schema_hash: string; tables: Array<unknown> }
    expect(body.schema_hash).toBe(schema.schema_hash)
    expect(body.tables.length).toBe(schema.tables.length)
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

  it("takes the token from the subprotocol and echoes the Orbit subprotocol", async () => {
    const p = freshPartition()
    const res = await SELF.fetch(`${base}/ws?partition=${encodeURIComponent(p)}`, {
      headers: {
        upgrade: "websocket",
        "sec-websocket-protocol": `orbit, orbit.token.${encodeURIComponent(await token([p]))}`,
      },
    })
    expect(res.status).toBe(101)
    expect(res.headers.get("sec-websocket-protocol")).toBe("orbit")
    const ws = res.webSocket
    if (ws === null) throw new Error("no websocket")
    ws.accept()
    const client = new Client(ws)
    client.send(hello(p))
    await client.next("welcome")
    client.ws.close(1000)

    const missing = await SELF.fetch(`${base}/ws?partition=${encodeURIComponent(p)}`, {
      headers: { upgrade: "websocket", "sec-websocket-protocol": "orbit" },
    })
    expect(missing.status).toBe(401)
  })

  it(
    "renews the same session with fresh authorization and zero SQL writes, including after hibernation",
    { timeout: 20000 },
    async () => {
      const p = freshPartition()
      const client = await connect(p, await token([p], "user-1", 120))
      const query = { name: "mine", args: {} }
      client.send(hello(p, [], { subscriptions: [{ type: "subscribe", id: "mine", query }] }))
      const welcome = await client.next("welcome")
      expect(welcome.heartbeat).toBe("static-v1")
      expect(welcome.sessionRenewal?.expiresAt).toBeGreaterThan(Date.now())
      const fill = (await pollFills()).requests.find((r) => r.partition === p)!
      await uploadFill(
        fill.fill_id,
        [chatbot("visible", { groupId: "user-1" }), chatbot("private-peer", { groupId: "user-2" })],
        0,
      )
      const snap = await client.next("snapshot")
      expect(snap.rows.map((r) => r.key[0])).toEqual(["visible"])
      const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName(durableObjectNameFor(schema, p)))
      const inspect = () =>
        runInDurableObject(stub, async (_object, state) => ({
          changes: state.storage.sql.exec(`SELECT total_changes() AS n`).one()["n"],
          sessions: state.storage.sql.exec(`SELECT * FROM sessions`).toArray(),
          subs: state.storage.sql.exec(`SELECT * FROM client_subs`).toArray(),
          expiry: state.getWebSockets().map((ws) => readAttachment(ws)?.expiresAt),
        }))
      const renew = async (
        tok: string,
        subscriptions = [{ id: "mine", query }],
        sessionId = welcome.sessionId,
        partition = p,
      ) => {
        const response = await SELF.fetch(
          `${base}/session/renew?partition=${encodeURIComponent(partition)}`,
          {
            method: "POST",
            headers: {
              authorization: `Bearer ${tok}`,
              "content-type": "application/json",
              "x-orbit-subject": "user-1",
              "x-orbit-expires": String(Date.now() + 99999999),
            },
            body: JSON.stringify({ sessionId, subscriptions }),
          },
        )
        const body = await response.text()
        return new Response(body, { status: response.status, headers: response.headers })
      }
      const original = await inspect()
      expect((await renew("invalid")).status).toBe(401)
      expect((await renew(await token([p], "user-2"))).status).toBe(403)
      expect((await renew(await token(["other"]))).status).toBe(403)
      expect((await renew(await token([p]), undefined, "missing")).status).toBe(409)
      expect((await renew(await token([p], "user-1", -10))).status).toBe(401)
      expect(await inspect()).toEqual(original)
      const preflight = await SELF.fetch(`${base}/session/renew?partition=${p}`, {
        method: "OPTIONS",
      })
      expect(preflight.status).toBe(204)
      expect(preflight.headers.get("access-control-allow-headers")).toContain("Authorization")
      for (const hibernate of [false, true]) {
        if (hibernate) await evictDurableObject(stub, { webSockets: "hibernate" })
        const before = await inspect()
        const response = await renew(await token([p], "user-1", hibernate ? 7200 : 3600))
        expect(response.status).toBe(200)
        await response.json()
        expect(response.headers.get("cache-control")).toBe("no-store")
        const after = await inspect()
        expect(after.changes).toBe(before.changes)
        expect(after.sessions).toEqual(original.sessions)
        expect(after.subs).toEqual(original.subs)
        expect(after.expiry[0]).toBeGreaterThan(before.expiry[0]!)
      }
      // The same subscribed session still receives exactly the rows authorized by its subject.
      await deliver(p, [
        txn(1, [
          insert("Chatbot", chatbot("after-renew", { groupId: "user-1" })),
          insert("Chatbot", chatbot("denied", { groupId: "user-2" })),
        ]),
      ])
      const delta = await client.next("delta")
      expect(delta.rows.map((r) => r.key[0])).toEqual(["after-renew"])
      // Change the server resolver without changing the client's ref, subject or schema.
      revokedQuerySubjects.add("user-1")
      try {
        expect((await renew(await token([p]))).status).toBe(409)
        expect((await client.waitClosed()).code).toBe(4408)
      } finally {
        revokedQuerySubjects.delete("user-1")
      }
    },
  )

  it("never resurrects an expired session and native heartbeats cannot extend authorization", async () => {
    const p = freshPartition()
    const client = await connect(p, await token([p]))
    client.send(hello(p, []))
    const welcome = await client.next("welcome")
    const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName(durableObjectNameFor(schema, p)))
    const ping = () =>
      new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(Error("native heartbeat timeout")), 2000)
        const listener = (e: MessageEvent) => {
          if (e.data !== WS_HEARTBEAT_RESPONSE) return
          clearTimeout(timer)
          client.ws.removeEventListener("message", listener)
          resolve()
        }
        client.ws.addEventListener("message", listener)
        client.ws.send(WS_HEARTBEAT_REQUEST)
      })
    for (const hibernate of [false, true]) {
      if (hibernate) await evictDurableObject(stub, { webSockets: "hibernate" })
      const before = await runInDurableObject(stub, async (_object, state) => ({
        changes: state.storage.sql.exec(`SELECT total_changes() AS n`).one()["n"],
        attachment: readAttachment(state.getWebSockets()[0]!),
      }))
      for (let i = 0; i < 100; i++) await ping()
      const after = await runInDurableObject(stub, async (_object, state) => ({
        changes: state.storage.sql.exec(`SELECT total_changes() AS n`).one()["n"],
        attachment: readAttachment(state.getWebSockets()[0]!),
        native: state.getWebSocketAutoResponseTimestamp(state.getWebSockets()[0]!) !== null,
      }))
      expect(after).toEqual({ ...before, native: true })
    }
    await runInDurableObject(stub, async (_object, state) => {
      const ws = state.getWebSockets()[0]!
      const a = readAttachment(ws)!
      ws.serializeAttachment(encodeAttachment({ ...a, expiresAt: Date.now() - 1 }))
    })
    const response = await SELF.fetch(`${base}/session/renew?partition=${p}`, {
      method: "POST",
      headers: { authorization: `Bearer ${await token([p])}`, "content-type": "application/json" },
      body: JSON.stringify({ sessionId: welcome.sessionId, subscriptions: [] }),
    })
    expect(response.status).toBe(409)
    expect((await client.waitClosed()).code).toBe(4408)
  })

  it("blocks outgoing data after expiry even if no client frame or expiry alarm arrives", async () => {
    const p = freshPartition()
    const client = await connect(p, await token([p]))
    client.send(hello(p, [{ id: "all", query: { table: "Chatbot" } }]))
    await client.next("welcome")
    const fill = (await pollFills()).requests.find((r) => r.partition === p)!
    await uploadFill(fill.fill_id, [chatbot("first")], 0)
    await client.next("snapshot")
    const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName(durableObjectNameFor(schema, p)))
    await runInDurableObject(stub, async (_object, state) => {
      const ws = state.getWebSockets()[0]!
      ws.serializeAttachment(
        encodeAttachment({ ...readAttachment(ws)!, expiresAt: Date.now() - 1 }),
      )
    })
    await deliver(p, [txn(1, [insert("Chatbot", chatbot("must-not-leak"))])])
    expect((await client.waitClosed()).code).toBe(4408)
    expect(client.messages.filter((m) => m.type === "delta")).toEqual([])
  })

  it(
    "handles 100 changing grants without session churn or SQL writes and honors shorter expiry",
    { timeout: 20000 },
    async () => {
      const p = freshPartition()
      const client = await connect(p, await token([p], "user-1", 60))
      client.send(hello(p, []))
      const welcome = await client.next("welcome")
      const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName(durableObjectNameFor(schema, p)))
      const inspect = () =>
        runInDurableObject(stub, async (_object, state) => ({
          changes: state.storage.sql.exec(`SELECT total_changes() AS n`).one()["n"],
          count: state.storage.sql.exec(`SELECT COUNT(*) AS n FROM sessions`).one()["n"],
          attachment: readAttachment(state.getWebSockets()[0]!),
        }))
      const before = await inspect()
      for (let i = 0; i < 100; i++) {
        const response = await SELF.fetch(`${base}/session/renew?partition=${p}`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${await token([p], "user-1", 120 + i)}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({ sessionId: welcome.sessionId }),
        })
        expect(response.status).toBe(200)
        await response.json()
      }
      const after = await inspect()
      expect(after.changes).toBe(before.changes)
      expect(after.count).toBe(1)
      expect(after.attachment?.session).toBe(welcome.sessionId)
      const shortened = await SELF.fetch(`${base}/session/renew?partition=${p}`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${await token([p], "user-1", 30)}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ sessionId: welcome.sessionId }),
      })
      expect(shortened.status).toBe(200)
      await shortened.json()
      expect((await inspect()).attachment!.expiresAt!).toBeLessThan(after.attachment!.expiresAt!)
      client.ws.close(1000)
    },
  )

  it("closes a socket with 4408 once the grant behind it expires", async () => {
    const p = freshPartition()
    const client = await connect(p, await token([p], "user-1", 1))
    client.send(hello(p))
    await client.next("welcome")
    const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName(durableObjectNameFor(schema, p)))
    // The alarm is due at the expiry; before then it leaves the socket alone.
    await runDurableObjectAlarm(stub)
    client.send({ type: "ping", sentAt: Date.now() })
    await client.next("pong")
    await new Promise((r) => setTimeout(r, 1_100))
    await runDurableObjectAlarm(stub)
    const err = await client.next("error")
    expect(err.error.code).toBe("session_expired")
    expect(err.fatal).toBe(false)
    expect((await client.waitClosed()).code).toBe(4408)
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

  it("keeps heartbeats write-free across hibernation and keeps acknowledgments in the hibernation attachment", async () => {
    const p = freshPartition()
    const client = await connect(p, await token([p]))
    client.send(hello(p, []))
    await client.next("welcome")
    const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName(durableObjectNameFor(schema, p)))
    const stateOf = () =>
      runInDurableObject(stub, async (_object, state) => ({
        changes: state.storage.sql.exec(`SELECT total_changes() AS n`).one()["n"],
        session: state.storage.sql.exec(`SELECT cursor, last_seen_at FROM sessions`).one(),
        ack: readAttachment(state.getWebSockets()[0]!)?.ackCursor,
      }))
    for (const hibernate of [false, true]) {
      if (hibernate) await evictDurableObject(stub, { webSockets: "hibernate" })
      // Capture after reopening SQLite; total_changes is connection-local.
      const before = await stateOf()
      for (let i = 0; i < 100; i++) {
        client.send({ type: "ping", sentAt: i })
        expect((await client.next("pong")).sentAt).toBe(i)
      }
      expect(await stateOf()).toEqual(before)
    }
    client.send({ type: "ack", cursor: 12 })
    client.send({ type: "ping", sentAt: 101 })
    await client.next("pong")
    const beforeDuplicates = await stateOf()
    expect(beforeDuplicates.session["cursor"]).toBe(0)
    expect(beforeDuplicates.ack).toBe(12)
    for (let i = 0; i < 100; i++) client.send({ type: "ack", cursor: 12 })
    client.send({ type: "ping", sentAt: 102 })
    await client.next("pong")
    expect(await stateOf()).toEqual(beforeDuplicates)
    await evictDurableObject(stub, { webSockets: "hibernate" })
    expect((await stateOf()).session).toEqual(beforeDuplicates.session)
    expect((await stateOf()).ack).toBe(12)
    const beforeReset = await stateOf()
    client.send({ type: "ack", cursor: 0 })
    client.send({ type: "ping", sentAt: 103 })
    await client.next("pong")
    const reset = await stateOf()
    expect(reset.changes).toBe(beforeReset.changes)
    expect(reset.ack).toBe(0)
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

  it("revalidates a complete view across eviction and falls back after a source change", async () => {
    const p = freshPartition()
    const first = await connect(p, await token([p]))
    first.send(hello(p, [{ id: "all", query: { table: "Chatbot" } }]))
    await first.next("welcome")
    const subscribed = await first.next("subscribed")
    const req = (await pollFills()).requests.find((r) => r.partition === p)!
    await uploadFill(req.fill_id, [chatbot("x")], 0)
    const snapshot = await first.next("snapshot")
    if (snapshot.version === undefined) throw Error("snapshot has no cache version")
    const resume = { version: snapshot.version, query: subscribed.query }
    const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName(durableObjectNameFor(schema, p)))
    await evictDurableObject(stub, { webSockets: "hibernate" })
    const second = await connect(p, await token([p]))
    second.send(
      hello(p, [], {
        subscriptions: [{ type: "subscribe", id: "resumed", query: { table: "Chatbot" }, resume }],
      }),
    )
    await second.next("welcome")
    expect((await second.next("subscribed")).resumed).toEqual({
      version: resume.version,
      cursor: 0,
    })
    second.send({ type: "ping", sentAt: 1 })
    await second.next("pong")
    expect(second.messages.some((m) => m.type === "snapshot")).toBe(false)
    await deliver(p, [txn(1, [insert("Chatbot", chatbot("new"))])])
    const delta = await second.next("delta")
    expect(delta.version).toBeDefined()
    expect(delta.version).not.toBe(resume.version)
    second.send({ type: "subscribe", id: "stale", query: { table: "Chatbot" }, resume })
    expect((await second.next("subscribed")).resumed).toBeUndefined()
    expect((await second.next("snapshot")).members.map((m) => m.key[0]).sort()).toEqual([
      "new",
      "x",
    ])
    first.ws.close(1000, "done")
    second.ws.close(1000, "done")
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

  it("keeps small views warm by default even with a custom short grace period", async () => {
    const p = freshPartition()
    const stub = env.ORBIT_WARM_SYNC.get(env.ORBIT_WARM_SYNC.idFromName(p))
    // Direct DO requests stand in for the authenticated Worker in this configuration test.
    const response = await stub.fetch("https://orbit.test/ws", {
      headers: { upgrade: "websocket", "x-orbit-partition": p, "x-orbit-subject": "user-1" },
    })
    const ws = response.webSocket
    if (ws === null) throw new Error("no websocket")
    ws.accept()
    const client = new Client(ws)
    client.send(hello(p))
    await client.next("welcome")
    await runInDurableObject(stub, async (_object, state) => {
      // An empty, completed scope avoids involving the separately bound fill registry.
      state.storage.sql
        .exec("INSERT INTO scopes(tbl,state,updated_at) VALUES ('Chatbot','live',0)")
        .toArray()
    })
    client.send({ type: "subscribe", id: "warm", query: { table: "Chatbot" } })
    expect((await client.next("subscribed")).status).toBe("live")
    await client.next("snapshot")
    client.send({ type: "unsubscribe", id: "warm" })
    await client.next("unsubscribed")
    const deadline = await runInDurableObject(stub, async (_object, state) => {
      const row = state.storage.sql
        .exec<{ orphaned_at: number; retire_at: number }>(
          "SELECT orphaned_at, retire_at FROM subscriptions",
        )
        .one()
      return row.retire_at - row.orphaned_at
    })
    expect(deadline).toBe(36 * 60 * 60_000)
    await evictDurableObject(stub)
    client.send({ type: "subscribe", id: "again", query: { table: "Chatbot" } })
    expect((await client.next("subscribed")).status).toBe("live")
    await client.next("snapshot")
    await runInDurableObject(stub, async (_object, state) => {
      for (const socket of state.getWebSockets()) socket.close(1000)
    })
    await client.waitClosed()
    client.ws.close(1000)
  })

  it("reopens an orphaned view with current membership and resumes deltas", async () => {
    const p = freshPartition()
    const client = await connect(p, await token([p]))
    const query = { table: "Chatbot" }
    client.send(hello(p, [{ id: "old", query }]))
    await client.next("welcome")
    expect((await client.next("subscribed")).status).toBe("pending")
    const req = (await pollFills()).requests.find((r) => r.partition === p)!
    await uploadFill(req.fill_id, [chatbot("before")], 0)
    expect((await client.next("subscribed")).status).toBe("live")
    await client.next("snapshot")
    client.send({ type: "unsubscribe", id: "old" })
    await client.next("unsubscribed")
    await deliver(p, [
      txn(1, [
        { table: "Chatbot", op: "delete", key: ["before"], before: chatbot("before") as never },
        insert("Chatbot", chatbot("after")),
      ]),
    ])
    client.send({ type: "subscribe", id: "new", query })
    expect((await client.next("subscribed")).status).toBe("live")
    const snapshot = await client.next("snapshot")
    expect(snapshot.subscriptionId).toBe("new")
    expect(snapshot.members.map((m) => m.key)).toEqual([["after"]])
    await deliver(p, [txn(2, [insert("Chatbot", chatbot("later"))])])
    const delta = await client.next("delta")
    expect(delta.memberships).toEqual([
      { subscriptionId: "new", added: [{ table: "Chatbot", key: ["later"] }], removed: [] },
    ])
    client.ws.close(1000)
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

  it("extends a window in place when the client names its base", async () => {
    const p = freshPartition()
    const client = await connect(p, await token([p]))
    const byId = (limit: number) => ({
      table: "Chatbot",
      orderBy: [{ column: "id", direction: "asc" }],
      limit,
    })
    client.send(hello(p, [{ id: "w1", query: byId(2) }]))
    await client.next("welcome")
    const req = (await pollFills()).requests.find((r) => r.partition === p)!
    await uploadFill(req.fill_id, [chatbot("a"), chatbot("b"), chatbot("c"), chatbot("d")], 0)
    expect((await client.next("snapshot")).rows.map((r) => r.key[0])).toEqual(["a", "b"])
    client.send({ type: "subscribe", id: "w2", query: byId(3) as never, basedOn: "w1" })
    // The queue still holds w1's own `subscribed` messages.
    for (;;) {
      const subscribed = await client.next("subscribed")
      if (subscribed.id !== "w2") continue
      expect(subscribed.status).toBe("live")
      break
    }
    const grown = await client.next("snapshot")
    expect(grown.basedOn).toBe("w1")
    expect(grown.rows.map((r) => r.key[0])).toEqual(["c"])
    expect(grown.members.map((m) => m.key[0])).toEqual(["c"])
    // A base the session does not hold is ignored: the snapshot is complete.
    client.send({ type: "subscribe", id: "w3", query: byId(4) as never, basedOn: "nope" })
    await client.next("subscribed")
    const chunks = [await client.next("snapshot")]
    while (!chunks[chunks.length - 1]?.complete) chunks.push(await client.next("snapshot"))
    expect(chunks.every((c) => c.basedOn === undefined)).toBe(true)
    expect(chunks.flatMap((c) => c.rows).length).toBe(4)
    client.ws.close(1000)
  })

  it("learns the epoch from a verified fill and delivers the first write without another fill", async () => {
    const p = freshPartition()
    const client = await connect(p, await token([p]))
    client.send(hello(p, [{ id: "all", query: { table: "Chatbot" } }]))
    await client.next("welcome")
    const req = (await pollFills()).requests.find((r) => r.partition === p)!
    const malformed = await internal(`/internal/fills/${encodeURIComponent(req.fill_id)}`, {
      method: "POST",
      headers: { "x-orbit-stream-epoch": "NaN" },
      body: "",
    })
    expect(malformed.status).toBe(400)
    expect((await uploadFill(req.fill_id, [chatbot("first")], 10, "completed", 7)).status).toBe(200)
    const snap = await client.next("snapshot")
    expect(snap.rows.map((r) => r.key[0])).toEqual(["first"])
    await deliver(p, [txn(50, [insert("Chatbot", chatbot("second"))])], 7)
    expect(await client.next("delta")).toMatchObject({ cursor: 50 })
    expect((await pollFills()).requests.filter((r) => r.partition === p)).toEqual([])
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

describe("durable fill registration", () => {
  it.each(["http", "throw"])(
    "retries a %s registry failure with the same fill id before the fill timeout",
    async (mode) => {
      const p = freshPartition()
      const registry = env.ORBIT_FILL_REGISTRY.get(env.ORBIT_FILL_REGISTRY.idFromName("registry"))
      await registry.fetch(
        new Request("https://registry/test/fail-next", {
          method: "POST",
          body: JSON.stringify({ partition: p, mode }),
        }),
      )
      const client = await connect(p, await token([p]))
      client.send(hello(p, [{ id: "all", query: { table: "Chatbot" } }]))
      await client.next("welcome")
      let rejected: { id: string | null } = { id: null }
      for (let i = 0; i < 20 && rejected.id === null; i++) {
        rejected = await (
          await registry.fetch(`https://registry/test/rejected?partition=${p}`)
        ).json()
        if (rejected.id === null) await new Promise((r) => setTimeout(r, 10))
      }
      expect(rejected.id).not.toBeNull()
      const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName(durableObjectNameFor(schema, p)))
      if (mode === "throw") await evictDurableObject(stub, { webSockets: "hibernate" })
      await new Promise((r) => setTimeout(r, 150))
      await runDurableObjectAlarm(stub)
      const fill = (await pollFills()).requests.find((r) => r.partition === p)
      expect(fill?.fill_id).toBe(rejected.id)
      await uploadFill(fill!.fill_id, [chatbot("recovered", { organizationId: p })], 0)
      const snapshot = await client.next("snapshot")
      expect(snapshot.rows.map((r) => r.key[0])).toEqual(["recovered"])
      expect(client.messages.filter((m) => m.type === "subscription_error")).toEqual([])
      client.ws.close(1000)
    },
  )
})
