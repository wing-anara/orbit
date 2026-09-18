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
