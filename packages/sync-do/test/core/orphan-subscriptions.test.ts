import { Result } from "effect"
import { describe, expect, it, vi } from "vitest"
import type { Query } from "@orbit/protocol"
import { SyncEngine } from "../../src/core/engine.ts"
import {
  batch,
  chatbot,
  fixtureSchema,
  insert,
  makeEngine,
  remove,
  txn,
  update,
} from "./support/fixture.ts"

const schema = fixtureSchema()
const window = (limit: number): Query => ({
  table: "Chatbot",
  orderBy: [{ column: "id", direction: "asc" }],
  limit,
})
const subscribe = (engine: SyncEngine, query: Query) => {
  const result = engine.subscribe(query)
  if (Result.isFailure(result)) throw new Error(result.failure.message)
  return result.success
}
const seed = (engine: SyncEngine, count: number) => {
  const { requests } = engine.ensureScopes(["Chatbot"])
  const id = requests[0]!.fill_id
  engine.applyFillRows(
    id,
    Array.from({ length: count }, (_, i) => chatbot(String(i).padStart(5, "0"))),
  )
  return () =>
    engine.completeFill(id, {
      status: "completed",
      position: "",
      keyspace: "ks",
      shard: "0",
      row_count: count,
      duration_ms: 1,
    })
}

describe("orphan subscription maintenance", () => {
  it("isolates a retained prefix from active descendants through CDC and reactivation", () => {
    const { engine, deps } = makeEngine()
    seed(engine, 200)()
    const small = subscribe(engine, window(50)).subscription
    const middleResult = engine.subscribe(window(100), { basedOn: small })
    if (Result.isFailure(middleResult)) throw middleResult.failure
    const middle = middleResult.success.subscription
    const largeResult = engine.subscribe(window(150), { basedOn: middle })
    if (Result.isFailure(largeResult)) throw largeResult.failure
    const large = largeResult.success.subscription
    const held = engine.membershipOf(small)
    engine.markOrphaned(small, 10)
    engine.applyBatch(
      batch(schema, "org_1", [
        txn(1, [
          remove("Chatbot", chatbot("00000")),
          remove("Chatbot", chatbot("00070")),
          insert("Chatbot", chatbot("00000-new")),
        ]),
      ]),
    )
    expect(engine.membershipOf(small)).toEqual(held)
    for (const id of [middle, large]) expect(engine.membershipOf(id)).toEqual(engine.recompute(id))
    const reopened = new SyncEngine(deps)
    reopened.init()
    subscribe(reopened, window(50))
    for (const id of [small, middle, large])
      expect(reopened.membershipOf(id)).toEqual(reopened.recompute(id))
    reopened.markOrphaned(middle, 20)
    reopened.sweepOrphans(30)
    for (const id of [small, large])
      expect(reopened.membershipOf(id)).toEqual(reopened.recompute(id))
  })

  it("bounds expired membership deletion and safely revives a partially swept view", () => {
    const { engine, driver, deps } = makeEngine()
    seed(engine, 3000)()
    const active = subscribe(engine, window(100)).subscription
    const first = subscribe(engine, window(2500)).subscription
    const second = subscribe(engine, window(2600)).subscription
    engine.markOrphaned(first, 10)
    engine.markOrphaned(second, 11)
    const count = () => Number(driver.query("SELECT COUNT(*) AS n FROM membership")[0]?.["n"])
    const before = count()
    expect(engine.sweepOrphans(20)).toEqual([])
    expect(before - count()).toBe(1000)
    expect(engine.membershipOf(active)).toHaveLength(100)
    expect(
      driver.query("SELECT value FROM meta WHERE key = ?", [`orphan_version:${first}`]),
    ).toEqual([])

    // An eviction or a returning client between cleanup passes must not trust
    // the former current-version proof for the now incomplete membership.
    const reopened = new SyncEngine(deps)
    reopened.init()
    const restored = reopened.subscribe(window(2500), { basedOn: active })
    if (Result.isFailure(restored)) throw restored.failure
    expect(reopened.membershipOf(first)).toEqual(reopened.recompute(first))
    expect(reopened.membershipOf(first)).toHaveLength(2500)
    const dropped: string[] = []
    for (let i = 0; i < 3; i++) {
      const previous = count()
      dropped.push(...reopened.sweepOrphans(20))
      expect(previous - count()).toBeLessThanOrEqual(1000)
    }
    expect(dropped).toEqual([second])
    expect(reopened.membershipOf(active)).toHaveLength(100)
    expect(reopened.membershipOf(first)).toHaveLength(2500)
    expect(reopened.nextOrphanDue(1000)).toBeNull()
    driver.db.close()
  })

  it("also bounds cleanup when abandoned queries have no membership", () => {
    const { engine, driver } = makeEngine()
    seed(engine, 0)()
    for (let i = 1; i <= 40; i++) engine.markOrphaned(subscribe(engine, window(i)).subscription, i)
    expect(engine.sweepOrphans(100)).toHaveLength(16)
    expect(engine.sweepOrphans(100)).toHaveLength(16)
    expect(engine.sweepOrphans(100)).toHaveLength(8)
    expect(engine.nextOrphanDue(1000)).toBeNull()
    driver.db.close()
  })

  it("bounds mutation work by active views after window/filter churn", () => {
    const measure = (orphans: number) => {
      const { engine, driver } = makeEngine()
      seed(engine, 5000)()
      subscribe(engine, window(100))
      for (let i = 0; i < orphans; i++)
        engine.markOrphaned(subscribe(engine, window(101 + i)).subscription, i)
      const query = vi.spyOn(driver, "query")
      const run = vi.spyOn(driver, "run")
      const start = performance.now()
      for (let i = 1; i <= 50; i++) {
        const before = chatbot("00000", { displayOrder: i - 1 })
        const after = chatbot("00000", { displayOrder: i })
        engine.applyBatch(batch(schema, "org_1", [txn(i, [update("Chatbot", before, after)])]))
      }
      const result = {
        orphans,
        queries: query.mock.calls.length,
        writes: run.mock.calls.length,
        ms: performance.now() - start,
      }
      driver.db.close()
      return result
    }
    const results = [0, 10, 50].map(measure)
    console.log("orphan-maintenance", JSON.stringify(results))
    expect(results.map((r) => r.queries)).toEqual(results.map(() => results[0]!.queries))
    expect(results.map((r) => r.writes)).toEqual(results.map(() => results[0]!.writes))
  })

  it("reconciles retained membership after changes, eviction, and reactivation", () => {
    const { engine, driver, deps } = makeEngine()
    seed(engine, 4)()
    const query = window(2)
    const sub = subscribe(engine, query).subscription
    const held = engine.membershipOf(sub)
    engine.markOrphaned(sub, 10)
    const changed = engine.applyBatch(
      batch(schema, "org_1", [
        txn(1, [
          remove("Chatbot", chatbot("00000")),
          insert("Chatbot", chatbot("00000-new")),
          update("Chatbot", chatbot("00001"), chatbot("00001", { displayOrder: 42 })),
        ]),
      ]),
    )
    expect(engine.membershipOf(sub)).toEqual(held)
    expect(changed.events.filter((e) => e.type === "delta").flatMap((e) => e.memberships)).toEqual(
      [],
    )
    const reopened = new SyncEngine(deps)
    reopened.init()
    const snapshot = subscribe(reopened, query).events.find((e) => e.type === "snapshot")
    expect(snapshot?.type === "snapshot" && snapshot.members.map((m) => m.key)).toEqual([
      ["00000-new"],
      ["00001"],
    ])
    expect(
      snapshot?.type === "snapshot" &&
        snapshot.rows.find((r) => r.key[0] === "00001")?.row?.["displayOrder"],
    ).toBe(42)
    expect(reopened.membershipOf(sub)).toEqual(reopened.recompute(sub))
    expect(reopened.sweepOrphans(100)).toEqual([])
    driver.db.close()
  })

  it("does not revive an abandoned pending subscription when its fill completes", () => {
    const { engine, driver } = makeEngine()
    const finish = seed(engine, 4)
    const old = subscribe(engine, window(2)).subscription
    const active = subscribe(engine, window(3)).subscription
    engine.markOrphaned(old, 10)
    expect(
      finish()
        .filter((e) => e.type === "snapshot")
        .map((e) => e.subscription),
    ).toEqual([active])
    expect(subscribe(engine, window(2)).status).toBe("live")
    expect(engine.membershipOf(old)).toEqual(engine.recompute(old))
    driver.db.close()
  })
})

describe("bounded warm retention", () => {
  const policy = { graceMs: 60, warmMs: 36 * 60, maxViews: 8, maxMembers: 1000 }
  it("keeps only eight small views warm, expires large/older ones, and never maintains idle views", () => {
    const { engine, driver, deps } = makeEngine()
    seed(engine, 1500)()
    const ids: string[] = []
    for (let i = 1; i <= 12; i++) {
      const sub = subscribe(engine, window(i * 10)).subscription
      ids.push(sub)
      engine.markOrphaned(sub, i, policy)
    }
    const large = subscribe(engine, window(1500)).subscription
    engine.markOrphaned(large, 13, policy)
    expect(
      driver.query(`SELECT id FROM subscriptions WHERE retire_at > orphaned_at + 60`),
    ).toHaveLength(8)
    expect(engine.nextOrphanDue(60)).toBe(61)
    for (let i = 0; i < 10; i++) engine.sweepOrphans(100, 60)
    expect(engine.subscription(ids[0]!)).toBeNull()
    expect(engine.subscription(large)).toBeNull()
    expect(driver.query(`SELECT id FROM subscriptions`)).toHaveLength(8)
    const retained = engine.membershipOf(ids[11]!)
    // Offline source changes do not maintain retained memberships.
    engine.applyBatch(
      batch(schema, "org_1", [
        txn(1, [remove("Chatbot", chatbot("00000")), insert("Chatbot", chatbot("new"))]),
      ]),
    )
    expect(engine.membershipOf(ids[11]!)).toEqual(retained)
    // Re-open through a fresh engine at a later time, without trusting stale members.
    const reopened = new SyncEngine(deps)
    reopened.init()
    subscribe(reopened, window(120))
    expect(reopened.membershipOf(ids[11]!)).toEqual(reopened.recompute(ids[11]!))
    expect(reopened.membershipOf(ids[11]!).some((m) => m.key[0] === "00000")).toBe(false)
    expect(
      driver.query(`SELECT retire_at FROM subscriptions WHERE id = ?`, [ids[11]!])[0]!["retire_at"],
    ).toBeNull()
    for (let i = 0; i < 10; i++) reopened.sweepOrphans(10_000, 60)
    expect(driver.query(`SELECT id FROM subscriptions`)).toEqual([{ id: ids[11] }])
  })

  it("reconciles a retained query over ten daily reopens without rewriting unchanged members", () => {
    const { engine, driver, deps } = makeEngine()
    seed(engine, 300)()
    const retainedPolicy = { graceMs: 60, warmMs: 2160, maxViews: 8, maxMembers: 1000 }
    const id = subscribe(engine, window(300)).subscription
    let current = engine
    for (let day = 0; day < 10; day++) {
      current.markOrphaned(id, day * 1440, retainedPolicy)
      current.sweepOrphans(day * 1440 + 1000, 60)
      expect(current.subscription(id)).not.toBeNull()
      current = new SyncEngine(deps)
      current.init()
      const writes = vi.spyOn(driver, "run")
      subscribe(current, window(300))
      expect(
        writes.mock.calls.filter(([sql]) =>
          /INSERT.*membership_rows|DELETE FROM membership_rows/.test(sql),
        ),
      ).toEqual([])
      expect(current.membershipOf(id)).toEqual(current.recompute(id))
      writes.mockRestore()
    }
  })

  it("keeps legacy expiry behavior and rolls back retention decisions atomically", () => {
    const { engine, driver } = makeEngine()
    seed(engine, 100)()
    const id = subscribe(engine, window(100)).subscription
    engine.markOrphaned(id, 10)
    expect(engine.nextOrphanDue(60)).toBe(70)
    expect(engine.sweepOrphans(69, 60)).toEqual([])
    const before = driver.query(`SELECT * FROM subscriptions`)
    expect(() =>
      driver.transaction(() => {
        subscribe(engine, window(100))
        engine.markOrphaned(id, 20, policy)
        throw Error("interrupted retention")
      }),
    ).toThrow("interrupted retention")
    expect(driver.query(`SELECT * FROM subscriptions`)).toEqual(before)
    expect(engine.sweepOrphans(70, 60)).toEqual([id])
  })
})
