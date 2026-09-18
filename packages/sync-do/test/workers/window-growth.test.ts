import { env, runInDurableObject } from "cloudflare:test"
import { Result } from "effect"
import { expect, it } from "vitest"
import { SyncEngine } from "../../src/core/engine.ts"
import type { SqlValue } from "@orbit/query"
import type { SqlRecord } from "../../src/core/driver.ts"
import { durableObjectDriver } from "../../src/do-driver.ts"
import { schema } from "../worker/index.ts"
import { MembershipStore } from "../../src/core/membership.ts"

it("bounds growing-window reads in workerd, including the candidate membership probe", async () => {
  const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName("growth-read-budget"))
  const result = await runInDurableObject(stub, async (_object, state) => {
    let rowsRead = 0
    let rowsReturned = 0
    let rowsWritten = 0
    const driver = durableObjectDriver(state.storage)
    const engine = new SyncEngine({
      schema,
      partition: "org_1",
      now: () => Date.now(),
      newId: () => crypto.randomUUID(),
      driver: {
        ...driver,
        run: (sql, params = []) => {
          const cursor = state.storage.sql.exec(sql, ...(params as SqlStorageValue[]))
          cursor.toArray()
          rowsWritten += cursor.rowsWritten
        },
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
    rowsWritten = 0
    const grown = engine.subscribe(
      { ...query, limit: 1100 },
      { basedOn: base.success.subscription },
    )
    if (Result.isFailure(grown)) throw grown.failure
    const growthReads = rowsRead
    const growthWrites = rowsWritten
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
      growthWrites,
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
    afterSweep: 0,
    baseAfterSweep: 1000,
    restoredMembers: 1100,
  })
  // A subscription-first JSON join reads >100,000 rows for this workload in
  // workerd, even though Node SQLite chooses a fast plan for the same query.
  expect(result.growthReads).toBeLessThan(20_000)
  expect(result.growthWrites).toBeLessThan(800)
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

it("keeps shared chunks isolated across a workerd eviction and bounded cleanup", async () => {
  const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName("shared-chunk-eviction"))
  await runInDurableObject(stub, async (_object, state) => {
    const driver = durableObjectDriver(state.storage)
    driver.run(`CREATE TABLE subscriptions (id TEXT PRIMARY KEY)`)
    const store = new MembershipStore(driver, () => crypto.randomUUID())
    store.init()
    driver.transaction(() => {
      store.add(
        "base",
        Array.from({ length: 10_000 }, (_, i) => ({
          path: "",
          table: "Chatbot",
          key: JSON.stringify([i]),
        })),
      )
      store.share("peer", "base")
    })
  })
  const result = await runInDurableObject(stub, async (_object, state) => {
    let writes = 0
    const underlying = durableObjectDriver(state.storage)
    const driver = {
      ...underlying,
      run: (sql: string, params: ReadonlyArray<SqlValue> = []) => {
        const cursor = state.storage.sql.exec(sql, ...(params as SqlStorageValue[]))
        cursor.toArray()
        writes += cursor.rowsWritten
      },
    }
    const store = new MembershipStore(driver, () => crypto.randomUUID())
    store.init()
    writes = 0
    driver.transaction(() => store.remove("peer", { path: "", table: "Chatbot", key: "[0]" }))
    const editWrites = writes
    const base = state.storage.sql
      .exec(`SELECT COUNT(*) AS n FROM membership WHERE subscription = 'base'`)
      .one()["n"]
    const peer = state.storage.sql
      .exec(`SELECT COUNT(*) AS n FROM membership WHERE subscription = 'peer'`)
      .one()["n"]
    driver.transaction(() => store.drop("base"))
    const remaining = state.storage.sql.exec(`SELECT COUNT(*) AS n FROM membership_rows`).one()["n"]
    const pass = driver.transaction(() => store.collect("peer", 1000))
    const after = state.storage.sql.exec(`SELECT COUNT(*) AS n FROM membership_rows`).one()["n"]
    return { editWrites, base, peer, remaining, after, pass }
  })
  expect(result).toMatchObject({
    base: 10_000,
    peer: 9999,
    remaining: 9999,
    after: 8999,
    pass: { spent: 1000, complete: false },
  })
  expect(result.editWrites).toBeLessThan(1600)
})
