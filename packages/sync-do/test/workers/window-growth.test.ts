import { env, runInDurableObject } from "cloudflare:test"
import { Result } from "effect"
import { expect, it } from "vitest"
import { SyncEngine } from "../../src/core/engine.ts"
import type { SqlRecord } from "../../src/core/driver.ts"
import { durableObjectDriver } from "../../src/do-driver.ts"
import { schema } from "../worker/index.ts"

it("bounds growing-window reads in workerd, including the candidate membership probe", async () => {
  const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName("growth-read-budget"))
  const result = await runInDurableObject(stub, async (_object, state) => {
    let rowsRead = 0
    let rowsReturned = 0
    const driver = durableObjectDriver(state.storage)
    const engine = new SyncEngine({
      schema,
      partition: "org_1",
      now: () => Date.now(),
      newId: () => crypto.randomUUID(),
      driver: {
        ...driver,
        query: (sql, params = []) => {
          const cursor = state.storage.sql.exec(sql, ...(params as SqlStorageValue[]))
          const rows = cursor.toArray()
          rowsRead += cursor.rowsRead
          rowsReturned += rows.length
          return rows as SqlRecord[]
        },
      },
    })
    engine.init()
    const fill = engine.ensureScopes(["Chatbot"]).requests[0]!
    const rows = Array.from({ length: 1100 }, (_, i) => ({
      id: `d${String(i).padStart(4, "0")}`,
      organizationId: "org_1",
      groupId: null,
      type: "DOCUMENT",
      displayOrder: i,
      contents: "payload",
      createdAt: "2026-01-01 00:00:00",
      score: null,
      big: i % 2 === 0 ? "9223372036854775807" : "-9223372036854775808",
      price: null,
      blob: null,
      day: null,
      at: null,
    }))
    engine.applyFillRows(fill.fill_id, rows)
    engine.completeFill(fill.fill_id, {
      status: "completed",
      position: "",
      keyspace: "ks",
      shard: "0",
      row_count: rows.length,
      duration_ms: 1,
    })
    const query = { table: "Chatbot", orderBy: [{ column: "id", direction: "asc" as const }] }
    const base = engine.subscribe({ ...query, limit: 1000 })
    if (Result.isFailure(base)) throw base.failure
    rowsRead = 0
    const grown = engine.subscribe(
      { ...query, limit: 1100 },
      { basedOn: base.success.subscription },
    )
    if (Result.isFailure(grown)) throw grown.failure
    const growthReads = rowsRead
    const snapshot = grown.success.events.find((e) => e.type === "snapshot")
    const reused = []
    for (const orphaned of [false, true]) {
      if (orphaned) engine.markOrphaned(grown.success.subscription, Date.now())
      rowsReturned = 0
      const again = engine.subscribe(
        { ...query, limit: 1100 },
        { basedOn: base.success.subscription },
      )
      if (Result.isFailure(again)) throw again.failure
      const extension = again.success.events.find((e) => e.type === "snapshot")
      reused.push({
        orphaned,
        rowsReturned,
        added: extension?.type === "snapshot" ? extension.rows.length : -1,
        basedOn: extension?.type === "snapshot" && extension.basedOn === base.success.subscription,
      })
    }
    engine.markOrphaned(grown.success.subscription, Date.now())
    engine.sweepOrphans(Date.now())
    const afterSweep = engine.membershipOf(grown.success.subscription).length
    const baseAfterSweep = engine.membershipOf(base.success.subscription).length
    const restored = engine.subscribe(
      { ...query, limit: 1100 },
      { basedOn: base.success.subscription },
    )
    if (Result.isFailure(restored)) throw restored.failure
    return {
      afterSweep,
      baseAfterSweep,
      restoredMembers: engine.membershipOf(restored.success.subscription).length,
      reused,
      growthReads,
      bigints: snapshot?.type === "snapshot" ? snapshot.rows.map((r) => r.row?.["big"]) : [],
      added: snapshot?.type === "snapshot" ? snapshot.rows.length : -1,
      basedOn: snapshot?.type === "snapshot" && snapshot.basedOn === base.success.subscription,
      members: engine.membershipOf(grown.success.subscription).length,
    }
  })
  expect(result).toMatchObject({
    added: 100,
    basedOn: true,
    members: 1100,
    afterSweep: 100,
    baseAfterSweep: 1000,
    restoredMembers: 1100,
  })
  // A subscription-first JSON join reads >100,000 rows for this workload in
  // workerd, even though Node SQLite chooses a fast plan for the same query.
  expect(result.growthReads).toBeLessThan(20_000)
  for (const repeat of result.reused) {
    expect(repeat).toMatchObject({ added: 100, basedOn: true })
    expect(repeat.rowsReturned).toBeLessThan(300)
  }
  expect(result.bigints).toEqual(
    Array.from({ length: 100 }, (_, i) =>
      i % 2 === 0 ? "9223372036854775807" : "-9223372036854775808",
    ),
  )
})
