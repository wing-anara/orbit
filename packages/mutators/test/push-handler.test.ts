import { beforeEach, describe, expect, it } from "vitest"

import { decodePushResponse, MUTATION_PROTOCOL_VERSION, type PushRequest } from "@orbit/protocol"

import {
  createPushHandler,
  SELECT_LAST_MUTATION_SQL,
  UPSERT_CLIENT_SQL,
} from "../src/server/index.ts"
import {
  compileFixtureSchema,
  FakeDb,
  mutators,
  observed,
  type Statement,
} from "./support/fixture.ts"

const schema = await compileFixtureSchema()

const WIRE_DATETIME = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/

const request = (body: unknown): Request =>
  new Request("http://app.test/orbit/push", {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer alice" },
    body: typeof body === "string" ? body : JSON.stringify(body),
  })

const push = (
  mutations: PushRequest["mutations"],
  over: Partial<PushRequest> = {},
): PushRequest => ({
  protocolVersion: MUTATION_PROTOCOL_VERSION,
  clientId: "c1",
  partition: "org_1",
  mutations,
  ...over,
})

const selectLast = (clientId = "c1"): Statement => ({
  sql: SELECT_LAST_MUTATION_SQL,
  params: [clientId],
})

const upsert = (id: number, clientId = "c1", partition = "org_1") => ({
  sql: UPSERT_CLIENT_SQL,
  params: [clientId, partition, id, expect.stringMatching(WIRE_DATETIME)],
})

const insertItem = (id: string, name: string, flag: 0 | 1) => ({
  sql: "INSERT INTO `_orbit_mut` (`id`, `org`, `name`, `flag`, `at`) VALUES (?, ?, ?, ?, ?)",
  params: [id, "org_1", name, flag, expect.stringMatching(WIRE_DATETIME)],
})

const setup = () => {
  const db = new FakeDb()
  const handler = createPushHandler({
    schema,
    mutators,
    db,
    authorize: async (req, body) =>
      req.headers.get("authorization") === "Bearer alice" && body.partition === "org_1"
        ? { subject: "user_alice" }
        : null,
  })
  const send = async (body: unknown) => {
    const response = await handler(request(body))
    return { status: response.status, body: await response.json() }
  }
  return { db, handler, send }
}

beforeEach(() => {
  observed.contexts.length = 0
  observed.rows.length = 0
})

describe("createPushHandler request handling", () => {
  it("returns 400 for a body that is not a push request", async () => {
    const { send, db } = setup()
    expect((await send("not json")).status).toBe(400)
    const bad = await send({ clientId: "c1" })
    expect(bad.status).toBe(400)
    expect(bad.body).toMatchObject({ error: expect.stringContaining("invalid push request") })
    expect(db.committed).toEqual([])
  })

  it("refuses a protocol version mismatch before authorization", async () => {
    const { send, db } = setup()
    const r = await send(push([], { protocolVersion: 99 }))
    expect(r.status).toBe(200)
    expect(decodePushResponse(r.body)).toEqual({
      type: "refused",
      reason: "protocol_version_mismatch",
      message: `server speaks mutation protocol ${MUTATION_PROTOCOL_VERSION}, client sent 99`,
    })
    expect(db.committed).toEqual([])
  })

  it("refuses an unauthorized push without touching the database", async () => {
    const { send, db } = setup()
    const r = await send(
      push([{ id: 1, name: "createItem", args: { id: "i1", name: "x" } }], { partition: "org_2" }),
    )
    expect(decodePushResponse(r.body)).toEqual({
      type: "refused",
      reason: "unauthorized",
      message: "not allowed to write to partition org_2",
    })
    expect(db.committed).toEqual([])
    expect(db.rolledBack).toEqual([])
  })

  it("answers an empty push with the server's last mutation id", async () => {
    const { send, db } = setup()
    db.last.set("c1", 4)
    const r = await send(push([]))
    expect(decodePushResponse(r.body)).toEqual({ type: "ok", outcomes: [], lastMutationId: 4 })
  })
})

describe("createPushHandler mutation flow", () => {
  it("applies a mutation and the bookkeeping row in one transaction", async () => {
    const { send, db } = setup()
    const r = await send(push([{ id: 1, name: "createItem", args: { id: "i1", name: "first" } }]))
    expect(r.status).toBe(200)
    expect(decodePushResponse(r.body)).toEqual({
      type: "ok",
      outcomes: [{ id: 1, status: "applied" }],
      lastMutationId: 1,
    })
    expect(db.rolledBack).toEqual([])
    expect(db.committed).toEqual([[selectLast(), insertItem("i1", "first", 1), upsert(1)]])
    expect(db.last.get("c1")).toBe(1)
    expect(observed.contexts).toEqual([
      {
        partition: "org_1",
        subject: "user_alice",
        clientId: "c1",
        mutationId: 1,
        now: expect.stringMatching(WIRE_DATETIME),
        side: "server",
      },
    ])
  })

  it("runs each mutation of a push in its own transaction, in order", async () => {
    const { send, db } = setup()
    db.last.set("c1", 2)
    const r = await send(
      push([
        { id: 3, name: "createItem", args: { id: "i1", name: "a" } },
        { id: 4, name: "createItem", args: { id: "i2", name: "b" } },
      ]),
    )
    expect(decodePushResponse(r.body)).toEqual({
      type: "ok",
      outcomes: [
        { id: 3, status: "applied" },
        { id: 4, status: "applied" },
      ],
      lastMutationId: 4,
    })
    expect(db.committed).toEqual([
      [selectLast(), insertItem("i1", "a", 1), upsert(3)],
      [selectLast(), insertItem("i2", "b", 1), upsert(4)],
    ])
  })

  it("reports a replayed mutation as duplicate and writes nothing", async () => {
    const { send, db } = setup()
    db.last.set("c1", 3)
    const r = await send(
      push([
        { id: 2, name: "createItem", args: { id: "i1", name: "old" } },
        { id: 3, name: "createItem", args: { id: "i2", name: "old" } },
        { id: 4, name: "createItem", args: { id: "i3", name: "new" } },
      ]),
    )
    expect(decodePushResponse(r.body)).toEqual({
      type: "ok",
      outcomes: [
        { id: 2, status: "duplicate" },
        { id: 3, status: "duplicate" },
        { id: 4, status: "applied" },
      ],
      lastMutationId: 4,
    })
    expect(db.committed).toEqual([
      [selectLast()],
      [selectLast()],
      [selectLast(), insertItem("i3", "new", 1), upsert(4)],
    ])
    expect(observed.contexts.map((c) => c.mutationId)).toEqual([4])
  })

  it("refuses a gap as out_of_order and stops the push", async () => {
    const { send, db } = setup()
    db.last.set("c1", 1)
    const r = await send(
      push([
        { id: 2, name: "createItem", args: { id: "i1", name: "a" } },
        { id: 4, name: "createItem", args: { id: "i2", name: "b" } },
        { id: 5, name: "createItem", args: { id: "i3", name: "c" } },
      ]),
    )
    expect(decodePushResponse(r.body)).toEqual({
      type: "refused",
      reason: "out_of_order",
      message: "mutation 4 does not follow last_mutation_id 2",
      lastMutationId: 2,
    })
    expect(db.committed).toEqual([
      [selectLast(), insertItem("i1", "a", 1), upsert(2)],
      [selectLast()],
    ])
    expect(db.last.get("c1")).toBe(2)
    expect(observed.contexts.map((c) => c.mutationId)).toEqual([2])
  })

  it("rolls back a mutator that throws, then consumes its id in a second transaction", async () => {
    const { send, db } = setup()
    const r = await send(
      push([
        { id: 1, name: "writeThenBoom", args: { id: "doomed" } },
        { id: 2, name: "createItem", args: { id: "i1", name: "after" } },
      ]),
    )
    expect(decodePushResponse(r.body)).toEqual({
      type: "ok",
      outcomes: [
        { id: 1, status: "failed", error: "boom" },
        { id: 2, status: "applied" },
      ],
      lastMutationId: 2,
    })
    expect(db.rolledBack).toEqual([
      [
        selectLast(),
        {
          sql: "INSERT INTO `_orbit_mut` (`id`, `org`, `name`, `flag`) VALUES (?, ?, ?, ?)",
          params: ["doomed", "org_1", "doomed", 0],
        },
      ],
    ])
    expect(db.committed).toEqual([
      [upsert(1)],
      [selectLast(), insertItem("i1", "after", 1), upsert(2)],
    ])
  })

  it("treats invalid arguments and unknown mutators as failed and consumes the ids", async () => {
    const { send, db } = setup()
    const r = await send(
      push([
        { id: 1, name: "createItem", args: { id: 5 } },
        { id: 2, name: "nope", args: {} },
        { id: 3, name: "createItem", args: { value: { id: "i1", name: "ok" } } },
      ]),
    )
    const decoded = decodePushResponse(r.body)
    expect(decoded).toMatchObject({
      type: "ok",
      outcomes: [
        { id: 1, status: "failed" },
        { id: 2, status: "failed", error: "unknown mutator nope" },
        { id: 3, status: "applied" },
      ],
      lastMutationId: 3,
    })
    expect(db.rolledBack).toEqual([[selectLast()], [selectLast()]])
    expect(db.committed).toEqual([
      [upsert(1)],
      [upsert(2)],
      [selectLast(), insertItem("i1", "ok", 1), upsert(3)],
    ])
  })

  it("reports a database error inside the mutator as failed", async () => {
    const { send, db } = setup()
    db.rejectSqlContaining = "INSERT INTO `_orbit_mut`"
    const r = await send(push([{ id: 1, name: "createItem", args: { id: "i1", name: "x" } }]))
    expect(decodePushResponse(r.body)).toEqual({
      type: "ok",
      outcomes: [
        {
          id: 1,
          status: "failed",
          error:
            "database rejected: INSERT INTO `_orbit_mut` (`id`, `org`, `name`, `flag`, `at`) VALUES (?, ?, ?, ?, ?)",
        },
      ],
      lastMutationId: 1,
    })
    expect(db.committed).toEqual([[upsert(1)]])
  })

  it("truncates long error messages to 500 characters", async () => {
    const { send } = setup()
    const r = await send(
      push([{ id: 1, name: "createItem", args: { id: "i1", name: 7, extra: "x".repeat(2000) } }]),
    )
    const decoded = decodePushResponse(r.body)
    if (decoded.type !== "ok") throw new Error("expected ok")
    const outcome = decoded.outcomes[0]
    if (outcome?.status !== "failed") throw new Error("expected failed")
    expect(outcome.error.length).toBeLessThanOrEqual(500)
  })

  it("answers 500 when the bookkeeping transaction itself fails", async () => {
    const { send, db } = setup()
    db.rejectSqlContaining = "orbit_clients"
    const r = await send(push([{ id: 1, name: "createItem", args: { id: "i1", name: "x" } }]))
    expect(r.status).toBe(500)
    expect(r.body).toMatchObject({ error: expect.stringContaining("database rejected") })
  })

  it("lets a mutator read through get and query inside the transaction", async () => {
    const { send, db } = setup()
    db.respond = (sql) => {
      if (sql.startsWith("SELECT `id`, `org`"))
        return [
          {
            id: "i1",
            org: "org_1",
            name: "n",
            flag: 1,
            n: null,
            big: null,
            price: null,
            meta: null,
            at: null,
            day: null,
            tm: null,
            data: null,
            score: null,
            parentId: null,
          },
        ]
      return []
    }
    const r = await send(
      push([
        { id: 1, name: "rename", args: { id: "i1", name: "renamed" } },
        { id: 2, name: "rename", args: { id: "i1", name: "again" } },
        { id: 3, name: "inspect", args: { flag: true } },
      ]),
    )
    expect(decodePushResponse(r.body)).toEqual({
      type: "ok",
      outcomes: [
        { id: 1, status: "applied" },
        { id: 2, status: "applied" },
        { id: 3, status: "applied" },
      ],
      lastMutationId: 3,
    })
    expect(db.committed[0]!.map((s) => s.sql)).toEqual([
      SELECT_LAST_MUTATION_SQL,
      "SELECT `id`, `org`, `name`, `flag`, `n`, `big`, `price`, `meta`, `at`, `day`, `tm`, `data`, `score`, `parentId` FROM `_orbit_mut` WHERE `id` = ?",
      "UPDATE `_orbit_mut` SET `name` = ? WHERE `id` = ?",
      UPSERT_CLIENT_SQL,
    ])
    expect(db.committed[2]!.map((s) => s.sql.slice(0, 6))).toEqual([
      "SELECT",
      "SELECT",
      "SELECT",
      "SELECT",
      "INSERT",
    ])
    expect(observed.rows).toEqual([])
  })
})
