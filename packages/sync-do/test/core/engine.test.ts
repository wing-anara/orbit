import { Result } from "effect"
import { describe, expect, it } from "vitest"

import type { Query } from "@orbit/protocol"
import type { RowUpdate } from "@orbit/protocol/client"

import type { EngineEvent } from "../../src/core/engine.ts"
import {
  batch,
  chatbot,
  fixtureSchema,
  insert,
  makeEngine,
  organization,
  remove,
  txn,
  update,
  U1,
} from "./support/fixture.ts"

const schema = fixtureSchema()

const liveScope = (
  engine: ReturnType<typeof makeEngine>["engine"],
  table: string,
  rows: ReadonlyArray<Record<string, unknown>>,
  gno = 0,
) => {
  const { requests } = engine.ensureScopes([table])
  const req = requests[0]!
  expect(Result.isSuccess(engine.applyFillRows(req.fill_id, rows as never))).toBe(true)
  return engine.completeFill(req.fill_id, {
    status: "completed",
    position: gno === 0 ? "" : `MySQL56/${U1}:1-${gno}`,
    keyspace: "ks",
    shard: "0",
    row_count: rows.length,
    duration_ms: 1,
  })
}

const deltas = (events: ReadonlyArray<EngineEvent>) =>
  events.filter((e): e is Extract<EngineEvent, { type: "delta" }> => e.type === "delta")

describe("SyncEngine: cursor and deduplication", () => {
  it("applies in order, skips duplicates, rejects gaps and conflicting duplicates", () => {
    const { engine } = makeEngine()
    liveScope(engine, "Chatbot", [])
    const t1 = txn(1, [insert("Chatbot", chatbot("a"))])
    const t2 = txn(2, [insert("Chatbot", chatbot("b"))])
    const first = engine.applyBatch(batch(schema, "org_1", [t1, t2]))
    expect(first.ack).toEqual({
      status: "applied",
      applied_seq: 2,
      duplicates: 0,
      apply_ms: expect.any(Number),
    })
    expect(engine.appliedSeq).toBe(2)

    const dup = engine.applyBatch(
      batch(schema, "org_1", [t1, t2, txn(3, [insert("Chatbot", chatbot("c"))])]),
    )
    expect(dup.ack).toMatchObject({ status: "applied", applied_seq: 3, duplicates: 2 })
    expect(dup.events).toHaveLength(1)

    const gap = engine.applyBatch(batch(schema, "org_1", [txn(5, [])]))
    expect(gap.ack).toEqual({
      status: "rejected",
      reason: { kind: "sequence_gap", applied_seq: 3, first_seq: 5 },
    })

    const conflict = engine.applyBatch(batch(schema, "org_1", [txn(2, [], 99)]))
    expect(conflict.ack).toMatchObject({
      status: "rejected",
      reason: { kind: "sequence_conflict", seq: 2 },
    })
    expect(engine.appliedSeq).toBe(3)
    expect(engine.status().scopes).toEqual([{ table: "Chatbot", state: "live", rows: 3 }])
  })

  it("rejects wrong protocol version, schema, partition and stale epoch, and resets on a new epoch", () => {
    const { engine } = makeEngine()
    liveScope(engine, "Chatbot", [chatbot("x")])
    expect(
      engine.applyBatch({ ...batch(schema, "org_1", []), protocol_version: 2 }).ack,
    ).toMatchObject({ status: "rejected", reason: { kind: "protocol_version_mismatch" } })
    expect(
      engine.applyBatch({ ...batch(schema, "org_1", []), schema_hash: "nope" }).ack,
    ).toMatchObject({ status: "rejected", reason: { kind: "schema_mismatch" } })
    expect(engine.applyBatch(batch(schema, "org_2", [])).ack).toMatchObject({
      status: "rejected",
      reason: { kind: "wrong_partition" },
    })
    expect(engine.applyBatch(batch(schema, "org_1", [txn(1, [])], 0)).ack).toMatchObject({
      status: "applied",
    })
    const bumped = engine.applyBatch(batch(schema, "org_1", [txn(7, [])], 1))
    expect(bumped.ack).toMatchObject({ status: "applied", applied_seq: 7 })
    expect(bumped.events[0]).toEqual({ type: "scopes_reset", reason: "epoch_changed" })
    expect(engine.status().scopes).toEqual([])
    expect(engine.applyBatch(batch(schema, "org_1", [txn(8, [])], 0)).ack).toMatchObject({
      status: "rejected",
      reason: { kind: "stale_epoch" },
    })
    expect(engine.applyBatch({ not: "a batch" }).ack).toMatchObject({
      status: "rejected",
      reason: { kind: "internal", code: "invalid_batch" },
    })
  })

  it("rejects invalid rows without advancing the cursor", () => {
    const { engine } = makeEngine()
    liveScope(engine, "Chatbot", [])
    const bad = txn(1, [insert("Chatbot", { ...chatbot("a"), displayOrder: "not a number" })])
    const r = engine.applyBatch(batch(schema, "org_1", [bad]))
    expect(r.ack).toMatchObject({
      status: "rejected",
      reason: { kind: "invalid_row", table: "Chatbot", seq: 1 },
    })
    expect(engine.appliedSeq).toBe(0)
    expect(engine.status().scopes[0]?.rows).toBe(0)
  })

  it("ignores changes for absent scopes and survives re-initialization", () => {
    const { engine, deps } = makeEngine()
    expect(
      engine.applyBatch(batch(schema, "org_1", [txn(1, [insert("Chatbot", chatbot("a"))])])).ack,
    ).toMatchObject({ status: "applied", applied_seq: 1 })
    expect(engine.status().scopes).toEqual([])
    const again = new (engine.constructor as new (d: typeof deps) => typeof engine)(deps)
    again.init()
    expect(again.appliedSeq).toBe(1)
  })
})

describe("SyncEngine: bootstrap race (hold and skip)", () => {
  it("holds changes while filling, then applies only those outside the fill position", () => {
    const { engine } = makeEngine()
    // Stream is at seq 10 when the fill is requested.
    for (let s = 1; s <= 10; s++) engine.applyBatch(batch(schema, "org_1", [txn(s, [])]))
    const { requests } = engine.ensureScopes(["Chatbot"])
    const req = requests[0]!
    expect(engine.scope("Chatbot")).toEqual({
      state: "filling",
      fillId: req.fill_id,
      holdFromSeq: 10,
    })

    // While the fill runs, the stream keeps flowing: gtids 11 and 12 commit. The fill snapshot
    // (taken at position 1-11) already reflects gtid 11 but not gtid 12.
    engine.applyBatch(
      batch(schema, "org_1", [txn(11, [insert("Chatbot", chatbot("a", { displayOrder: 1 }))])]),
    )
    engine.applyBatch(
      batch(schema, "org_1", [
        txn(12, [
          update("Chatbot", chatbot("a", { displayOrder: 1 }), chatbot("a", { displayOrder: 2 })),
          insert("Chatbot", chatbot("b")),
        ]),
      ]),
    )
    expect(engine.status().heldChanges).toBe(3)

    // The fill delivers the state as of position 1-11: row a with displayOrder 1.
    expect(
      Result.isSuccess(engine.applyFillRows(req.fill_id, [chatbot("a", { displayOrder: 1 })])),
    ).toBe(true)
    engine.completeFill(req.fill_id, {
      status: "completed",
      position: `MySQL56/${U1}:1-11`,
      keyspace: "ks",
      shard: "0",
      row_count: 1,
      duration_ms: 1,
    })

    expect(engine.scope("Chatbot")).toEqual({ state: "live" })
    expect(engine.status().heldChanges).toBe(0)
    const r = engine.subscribe({ table: "Chatbot", orderBy: [{ column: "id", direction: "asc" }] })
    expect(Result.isSuccess(r)).toBe(true)
    const snap = Result.isSuccess(r)
      ? r.success.events.find((e) => e.type === "snapshot")
      : undefined
    expect(
      snap?.type === "snapshot" && snap.rows.map((x) => [x.key[0], x.row?.["displayOrder"]]),
    ).toEqual([
      ["a", 2],
      ["b", null],
    ])
  })

  it("a stale fill (older than the hold) is impossible by construction: held changes are re-applied over any snapshot", () => {
    const { engine } = makeEngine()
    const { requests } = engine.ensureScopes(["Chatbot"])
    const req = requests[0]!
    // Stream delivers delete of a row the fill will still contain (fill position is behind).
    engine.applyBatch(batch(schema, "org_1", [txn(1, [remove("Chatbot", chatbot("gone"))])]))
    expect(
      Result.isSuccess(engine.applyFillRows(req.fill_id, [chatbot("gone"), chatbot("stays")])),
    ).toBe(true)
    engine.completeFill(req.fill_id, {
      status: "completed",
      position: "",
      keyspace: "ks",
      shard: "0",
      row_count: 2,
      duration_ms: 1,
    })
    const r = engine.subscribe({ table: "Chatbot" })
    const snap = Result.isSuccess(r)
      ? r.success.events.find((e) => e.type === "snapshot")
      : undefined
    expect(snap?.type === "snapshot" && snap.rows.map((x) => x.key[0])).toEqual(["stays"])
  })

  it("failed fills release the scope and report pending subscriptions", () => {
    const { engine } = makeEngine()
    const sub = engine.subscribe({ table: "Chatbot" })
    expect(Result.isSuccess(sub) && sub.success.status).toBe("pending")
    const fillNeeded = Result.isSuccess(sub)
      ? sub.success.events.find((e) => e.type === "fill_needed")
      : undefined
    expect(fillNeeded?.type).toBe("fill_needed")
    const fillId = fillNeeded?.type === "fill_needed" ? fillNeeded.request.fill_id : ""
    const events = engine.completeFill(fillId, {
      status: "failed",
      error: { code: "timeout", after_ms: 5 },
    })
    expect(events).toEqual([
      {
        type: "subscription_failed",
        subscription: expect.any(String),
        error: {
          code: "fill_failed",
          message: "fill of Chatbot failed",
          cause: { code: "timeout", after_ms: 5 },
        },
      },
    ])
    expect(engine.scope("Chatbot")).toEqual({ state: "absent" })
    // Subscribing again requests a fresh fill.
    const again = engine.subscribe({ table: "Chatbot" })
    expect(
      Result.isSuccess(again) && again.success.events.some((e) => e.type === "fill_needed"),
    ).toBe(true)
  })

  it("retryFill restarts a fill while keeping held changes", () => {
    const { engine } = makeEngine()
    const first = engine.ensureScopes(["Chatbot"]).requests[0]!
    engine.applyBatch(batch(schema, "org_1", [txn(1, [insert("Chatbot", chatbot("held"))])]))
    engine.applyFillRows(first.fill_id, [chatbot("partial")])
    const second = engine.retryFill(first.fill_id)!
    expect(second.fill_id).not.toBe(first.fill_id)
    expect(engine.status().heldChanges).toBe(1)
    expect(engine.status().scopes[0]?.rows).toBe(0)
    engine.applyFillRows(second.fill_id, [])
    engine.completeFill(second.fill_id, {
      status: "completed",
      position: "",
      keyspace: "ks",
      shard: "0",
      row_count: 0,
      duration_ms: 1,
    })
    const r = engine.subscribe({ table: "Chatbot" })
    const snap = Result.isSuccess(r)
      ? r.success.events.find((e) => e.type === "snapshot")
      : undefined
    expect(snap?.type === "snapshot" && snap.rows.map((x) => x.key[0])).toEqual(["held"])
  })
})

describe("SyncEngine: subscriptions and incremental maintenance", () => {
  const documentsInFolder = (folder: string): Query => ({
    table: "Chatbot",
    where: {
      op: "and",
      args: [
        { op: "eq", column: "type", value: "DOCUMENT" },
        { op: "eq", column: "groupId", value: folder },
      ],
    },
    orderBy: [{ column: "displayOrder", direction: "asc" }],
    limit: 2,
    include: ["folder", "organization"],
  })

  it("materializes, then emits deltas with membership changes and row images", () => {
    const { engine } = makeEngine()
    liveScope(engine, "organization", [organization("org_1")])
    liveScope(engine, "Chatbot", [
      chatbot("f1", { type: "GROUP" }),
      chatbot("d1", { groupId: "f1", displayOrder: 1 }),
      chatbot("d2", { groupId: "f1", displayOrder: 5 }),
      chatbot("d3", { groupId: "f1", displayOrder: 9 }),
    ])
    const sub = engine.subscribe(documentsInFolder("f1"))
    expect(Result.isSuccess(sub)).toBe(true)
    if (!Result.isSuccess(sub)) return
    expect(sub.success.status).toBe("live")
    const snap = sub.success.events.find((e) => e.type === "snapshot")
    expect(
      snap?.type === "snapshot" && snap.members.map((m) => `${m.table}:${m.key[0]}`).sort(),
    ).toEqual(["Chatbot:d1", "Chatbot:d2", "Chatbot:f1", "organization:org_1"])
    const id = sub.success.subscription

    // d3 moves into the window (displayOrder 3), pushing d2 out.
    const r1 = engine.applyBatch(
      batch(schema, "org_1", [
        txn(1, [
          update(
            "Chatbot",
            chatbot("d3", { groupId: "f1", displayOrder: 9 }),
            chatbot("d3", { groupId: "f1", displayOrder: 3 }),
          ),
        ]),
      ]),
    )
    const d1 = deltas(r1.events)[0]!
    expect(d1.memberships).toEqual([
      {
        subscriptionId: id,
        added: [{ table: "Chatbot", key: ["d3"] }],
        removed: [{ table: "Chatbot", key: ["d2"] }],
      },
    ])
    expect(d1.rows.map((r) => r.key[0])).toEqual(["d3"])
    expect(engine.membershipOf(id)).toEqual(
      [...engine.recompute(id)].sort((a, b) =>
        `${a.table}${String(a.key[0])}` < `${b.table}${String(b.key[0])}` ? -1 : 1,
      ),
    )

    // A change to an unrelated row produces no membership change and no row.
    const r2 = engine.applyBatch(
      batch(schema, "org_1", [
        txn(2, [insert("Chatbot", chatbot("other", { groupId: "f9", displayOrder: 0 }))]),
      ]),
    )
    expect(deltas(r2.events)[0]!.memberships).toEqual([])
    expect(deltas(r2.events)[0]!.rows).toEqual([])

    // Updating a member row (no membership change) ships the new image.
    const r3 = engine.applyBatch(
      batch(schema, "org_1", [
        txn(3, [
          update(
            "Chatbot",
            chatbot("d1", { groupId: "f1", displayOrder: 1 }),
            chatbot("d1", { groupId: "f1", displayOrder: 1, contents: { edited: true } }),
          ),
        ]),
      ]),
    )
    expect(deltas(r3.events)[0]!.memberships).toEqual([])
    expect(deltas(r3.events)[0]!.rows).toEqual([
      {
        table: "Chatbot",
        key: ["d1"],
        row: expect.objectContaining({ contents: { edited: true } }),
      },
    ])

    // Updating the included folder row ships it too.
    const r4 = engine.applyBatch(
      batch(schema, "org_1", [
        txn(4, [
          update(
            "Chatbot",
            chatbot("f1", { type: "GROUP" }),
            chatbot("f1", { type: "GROUP", contents: { renamed: true } }),
          ),
        ]),
      ]),
    )
    expect(deltas(r4.events)[0]!.rows.map((r) => r.key[0])).toEqual(["f1"])

    // Deleting a member ships a null row and a removal; the next candidate enters.
    const r5 = engine.applyBatch(
      batch(schema, "org_1", [
        txn(5, [
          remove(
            "Chatbot",
            chatbot("d1", { groupId: "f1", displayOrder: 1, contents: { edited: true } }),
          ),
        ]),
      ]),
    )
    const d5 = deltas(r5.events)[0]!
    expect(d5.memberships[0]).toEqual({
      subscriptionId: id,
      added: [{ table: "Chatbot", key: ["d2"] }],
      removed: [{ table: "Chatbot", key: ["d1"] }],
    })
    expect(d5.rows).toEqual(
      expect.arrayContaining([
        { table: "Chatbot", key: ["d1"], row: null },
        expect.objectContaining({ table: "Chatbot", key: ["d2"] }),
      ]),
    )
    expect(
      engine
        .membershipOf(id)
        .map((m) => m.key[0])
        .sort(),
    ).toEqual(["d2", "d3", "f1", "org_1"])
  })

  it("rejects unsupported queries explicitly and shares identical subscriptions", () => {
    const { engine } = makeEngine()
    liveScope(engine, "Chatbot", [])
    const bad = engine.subscribe({
      table: "Chatbot",
      where: { op: "gt", column: "contents", value: "x" },
    })
    expect(Result.isFailure(bad) && bad.failure.code).toBe("unsupported_query")
    const a = engine.subscribe({ table: "Chatbot", orderBy: [{ column: "id", direction: "asc" }] })
    const b = engine.subscribe({ table: "Chatbot", orderBy: [{ column: "id", direction: "asc" }] })
    expect(
      Result.isSuccess(a) &&
        Result.isSuccess(b) &&
        a.success.subscription === b.success.subscription,
    ).toBe(true)
    expect(engine.status().subscriptions).toBe(1)
    engine.unsubscribe(Result.isSuccess(a) ? a.success.subscription : "")
    expect(engine.status().subscriptions).toBe(0)
  })

  it("keeps subscriptions pending until every table they touch is live", () => {
    const { engine } = makeEngine()
    liveScope(engine, "Chatbot", [chatbot("d1", { groupId: "f1" })])
    const sub = engine.subscribe({ table: "Chatbot", include: ["organization"] })
    expect(Result.isSuccess(sub) && sub.success.status).toBe("pending")
    const req = Result.isSuccess(sub)
      ? sub.success.events.find((e) => e.type === "fill_needed")
      : undefined
    expect(req?.type === "fill_needed" && req.request.table).toBe("organization")
    const fillId = req?.type === "fill_needed" ? req.request.fill_id : ""
    engine.applyFillRows(fillId, [organization("org_1")])
    const events = engine.completeFill(fillId, {
      status: "completed",
      position: "",
      keyspace: "ks",
      shard: "0",
      row_count: 1,
      duration_ms: 1,
    })
    const snap = events.find((e) => e.type === "snapshot")
    expect(snap?.type === "snapshot" && snap.members.map((m) => m.table).sort()).toEqual([
      "Chatbot",
      "organization",
    ])
  })
})

describe("SyncEngine: large transactions", () => {
  it("applies 300 inserts in one transaction while predicate subscriptions are live", () => {
    const { engine } = makeEngine()
    liveScope(engine, "Chatbot", [chatbot("f1", { type: "GROUP" })])
    const sub = engine.subscribe({
      table: "Chatbot",
      where: {
        op: "and",
        args: [
          { op: "eq", column: "type", value: "DOCUMENT" },
          { op: "eq", column: "groupId", value: "f1" },
        ],
      },
      orderBy: [{ column: "displayOrder", direction: "asc" }],
      limit: 200,
      include: ["folder"],
    })
    expect(Result.isSuccess(sub) && sub.success.status).toBe("live")
    const changes = Array.from({ length: 300 }, (_, i) =>
      insert("Chatbot", chatbot(`bulk${i}`, { groupId: "f1", displayOrder: i + 1 })),
    )
    const r = engine.applyBatch(batch(schema, "org_1", [txn(1, changes)]))
    expect(r.ack).toMatchObject({ status: "applied", applied_seq: 1 })
    const id = Result.isSuccess(sub) ? sub.success.subscription : ""
    expect(engine.membershipOf(id).filter((m) => String(m.key[0]).startsWith("bulk"))).toHaveLength(
      200,
    )
    expect(engine.membershipOf(id)).toEqual(
      engine
        .recompute(id)
        .slice()
        .sort((a, b) =>
          `${a.table}${String(a.key[0])}` < `${b.table}${String(b.key[0])}` ? -1 : 1,
        ),
    )
    // And a multi-row delete of all of them.
    const deletes = changes.map((c) => remove("Chatbot", c.after as never))
    const d = engine.applyBatch(batch(schema, "org_1", [txn(2, deletes)]))
    expect(d.ack).toMatchObject({ status: "applied", applied_seq: 2 })
    expect(engine.membershipOf(id).filter((m) => String(m.key[0]).startsWith("bulk"))).toHaveLength(
      0,
    )
  })
})

describe("SyncEngine: randomized incremental maintenance equals full recomputation", () => {
  it("holds across random inserts, updates, deletes and query shapes", { timeout: 60_000 }, () => {
    let seed = 42
    const rand = (): number => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff
      return seed / 0x7fffffff
    }
    const pick = <T>(xs: ReadonlyArray<T>): T => xs[Math.floor(rand() * xs.length)]!
    for (let round = 0; round < 12; round++) {
      const { engine } = makeEngine()
      liveScope(engine, "organization", [organization("org_1")])
      liveScope(engine, "Chatbot", [
        chatbot("f1", { type: "GROUP" }),
        chatbot("f2", { type: "GROUP" }),
      ])
      const queries: Array<Query> = [
        {
          table: "Chatbot",
          where: { op: "eq", column: "type", value: "DOCUMENT" },
          orderBy: [{ column: "displayOrder", direction: "asc" }],
          limit: 3,
          include: ["folder"],
        },
        {
          table: "Chatbot",
          where: {
            op: "and",
            args: [
              { op: "eq", column: "groupId", value: "f1" },
              { op: "isNotNull", column: "displayOrder" },
            ],
          },
          orderBy: [{ column: "displayOrder", direction: "desc" }],
          limit: 2,
        },
        {
          table: "Chatbot",
          where: {
            op: "or",
            args: [
              { op: "gt", column: "displayOrder", value: 5 },
              { op: "isNull", column: "groupId" },
            ],
          },
        },
        {
          table: "Chatbot",
          where: { op: "eq", column: "type", value: "GROUP" },
          include: ["documents", "organization"],
        },
        {
          table: "Chatbot",
          where: { op: "like", column: "id", pattern: "d%" },
          orderBy: [
            { column: "createdAt", direction: "desc" },
            { column: "displayOrder", direction: "asc" },
          ],
          limit: 4,
        },
        // Relation predicate: folders that contain a document with a display order above 4.
        {
          table: "Chatbot",
          where: {
            op: "and",
            args: [
              { op: "eq", column: "type", value: "GROUP" },
              {
                op: "exists",
                relation: "documents",
                where: { op: "gt", column: "displayOrder", value: 4 },
              },
            ],
          },
        },
        // Negated relation predicate: documents whose folder has no other document in it.
        {
          table: "Chatbot",
          where: {
            op: "and",
            args: [
              { op: "eq", column: "type", value: "DOCUMENT" },
              {
                op: "not",
                arg: {
                  op: "exists",
                  relation: "folder",
                  where: {
                    op: "exists",
                    relation: "documents",
                    where: { op: "isNull", column: "displayOrder" },
                  },
                },
              },
            ],
          },
        },
        // A window over documents of folders that hold a document without a display order, with
        // the folder and the folder's other documents included (self relation at two levels).
        {
          table: "Chatbot",
          where: {
            op: "and",
            args: [
              { op: "eq", column: "type", value: "DOCUMENT" },
              {
                op: "exists",
                relation: "folder",
                where: {
                  op: "exists",
                  relation: "documents",
                  where: { op: "isNull", column: "displayOrder" },
                },
              },
            ],
          },
          orderBy: [{ column: "displayOrder", direction: "desc" }],
          limit: 2,
          include: [{ relation: "folder", include: ["documents"] }],
        },
        // Filtered and nested includes: folders with their ordered documents and each document's folder.
        {
          table: "Chatbot",
          where: { op: "eq", column: "type", value: "GROUP" },
          include: [
            {
              relation: "documents",
              where: { op: "isNotNull", column: "displayOrder" },
              include: ["folder", "organization"],
            },
          ],
        },
      ]
      const ids = queries.map((q) => {
        const r = engine.subscribe(q)
        if (!Result.isSuccess(r)) throw new Error("subscribe failed")
        return r.success.subscription
      })
      const live = new Map<string, Record<string, unknown>>([
        ["f1", chatbot("f1", { type: "GROUP" })],
        ["f2", chatbot("f2", { type: "GROUP" })],
      ])
      let seq = 0
      for (let step = 0; step < 80; step++) {
        const changes = []
        const n = 1 + Math.floor(rand() * 3)
        for (let i = 0; i < n; i++) {
          // Folders change too: they are include targets and chain hops, and can become documents.
          const id = rand() < 0.15 ? pick(["f1", "f2"]) : `d${Math.floor(rand() * 10)}`
          const existing = live.get(id)
          const kind = existing === undefined ? "insert" : rand() < 0.25 ? "delete" : "update"
          const fresh = chatbot(id, {
            groupId: pick(["f1", "f2", null]),
            displayOrder: pick([null, 1, 2, 5, 7, 9]),
            createdAt: pick(["2026-01-01 00:00:00", "2026-02-01 00:00:00"]),
            type: rand() < 0.1 ? "GROUP" : "DOCUMENT",
          })
          if (kind === "insert") {
            changes.push(insert("Chatbot", fresh))
            live.set(id, fresh)
          } else if (kind === "delete") {
            changes.push(remove("Chatbot", existing as never))
            live.delete(id)
          } else {
            changes.push(update("Chatbot", existing as never, fresh))
            live.set(id, fresh)
          }
        }
        seq += 1
        const r = engine.applyBatch(batch(schema, "org_1", [txn(seq, changes)]))
        expect(r.ack.status, `round ${round} step ${step}: ${JSON.stringify(r.ack)}`).toBe(
          "applied",
        )
        for (const id of ids) {
          const stored = engine
            .membershipOf(id)
            .map((m) => `${m.table}:${m.key[0]}`)
            .sort()
          const recomputed = engine
            .recompute(id)
            .map((m) => `${m.table}:${m.key[0]}`)
            .sort()
          expect(stored, `round ${round} step ${step} query ${id}`).toEqual(recomputed)
        }
        // Every delta row for a member must equal the cache row (row images are fresh).
        for (const d of deltas(r.events)) {
          for (const row of d.rows as ReadonlyArray<RowUpdate>) {
            if (row.row === null) continue
            const snapRow = (() => {
              const sid = ids.find((s) =>
                engine
                  .membershipOf(s)
                  .some((m) => m.table === row.table && m.key[0] === row.key[0]),
              )
              if (sid === undefined) return undefined
              const snap = engine.snapshot(sid)
              return snap.type === "snapshot"
                ? snap.rows.find((x) => x.table === row.table && x.key[0] === row.key[0])?.row
                : undefined
            })()
            if (snapRow !== undefined) expect(row.row).toEqual(snapRow)
          }
        }
      }
    }
  })
})
