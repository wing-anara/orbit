import { env, runInDurableObject } from "cloudflare:test"
import { Result } from "effect"
import { expect, it } from "vitest"
import type { SqlValue } from "@orbit/query"
import type { SqlRecord } from "../../src/core/driver.ts"
import { SyncEngine } from "../../src/core/engine.ts"
import { MembershipStore } from "../../src/core/membership.ts"
import { writeCachedRow } from "../../src/core/write-row.ts"
import { durableObjectDriver } from "../../src/do-driver.ts"
import { Sessions } from "../../src/sessions.ts"
import { schema } from "../worker/index.ts"

it("enforces billable write budgets in workerd without changing cached data", async () => {
  const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName("write-cost-budgets"))
  const result = await runInDurableObject(stub, async (_object, state) => {
    let writes = 0
    const execute = (sql: string, params: ReadonlyArray<SqlValue> = []) => {
      const cursor = state.storage.sql.exec(sql, ...(params as SqlStorageValue[]))
      const rows = cursor.toArray()
      writes += cursor.rowsWritten
      return rows as SqlRecord[]
    }
    const driver = {
      ...durableObjectDriver(state.storage),
      query: execute,
      run: (sql: string, params?: ReadonlyArray<SqlValue>) => {
        execute(sql, params)
      },
    }
    const engine = new SyncEngine({
      driver,
      schema,
      partition: "org_1",
      now: Date.now,
      newId: () => crypto.randomUUID(),
    })
    engine.init()
    writes = 0
    engine.init()
    const warm = writes
    const table = schema.tables.find((t) => t.name === "Chatbot")!
    const row = Object.fromEntries(
      table.columns.map((c) => [
        c.name,
        c.name === "id"
          ? "doc"
          : c.name === "type"
            ? "DOCUMENT"
            : c.name === "createdAt"
              ? "2026-09-21 00:00:00"
              : null,
      ]),
    )
    const fill = engine.ensureScopes(["Chatbot"]).requests[0]!
    engine.applyFillRows(fill.fill_id, [row])
    engine.completeFill(fill.fill_id, {
      status: "completed",
      position: "",
      keyspace: "ks",
      shard: "0",
      row_count: 1,
      duration_ms: 1,
    })
    const sub = engine.subscribe({ table: "Chatbot" })
    if (Result.isFailure(sub)) throw sub.failure
    writes = 0
    engine.subscribe({ table: "Chatbot" })
    const resubscribe = writes
    writes = 0
    driver.transaction(() => writeCachedRow(driver, table, { ...row, contents: { edited: true } }))
    const edit = writes
    writes = 0
    driver.transaction(() => writeCachedRow(driver, table, { ...row, contents: { edited: true } }))
    const identical = writes
    const sessions = new Sessions(driver)
    driver.run(`INSERT INTO sessions VALUES ('s','c','u','{}',1,5,123,123)`)
    sessions.setClientSub("s", "q", sub.success.subscription, "live")
    writes = 0
    for (let i = 0; i < 100; i++) {
      sessions.setClientSub("s", "q", sub.success.subscription, "live")
      sessions.markLive(sub.success.subscription)
    }
    const duplicates = writes
    return { warm, resubscribe, edit, identical, duplicates }
  })
  expect(result).toEqual({
    warm: 0,
    resubscribe: 0,
    edit: 1,
    identical: 0,
    duplicates: 0,
  })
})

it("rolls back interrupted format migration in workerd and preserves populated legacy memberships", async () => {
  const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName("membership-format-rollback"))
  await runInDurableObject(stub, async (_object, state) => {
    const driver = durableObjectDriver(state.storage)
    driver.run(`CREATE TABLE subscriptions (id TEXT PRIMARY KEY)`)
    driver.run(
      `CREATE TABLE membership (subscription TEXT NOT NULL, path TEXT NOT NULL, tbl TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY(subscription,path,tbl,key))`,
    )
    driver.run(`INSERT INTO subscriptions VALUES ('old')`)
    driver.run(`INSERT INTO membership VALUES ('old','','Chatbot','[1]')`)
    const store = new MembershipStore(driver, () => crypto.randomUUID())
    store.init()
    store.share("peer", "old")
    store.drop("old")
    store.init()
    expect(driver.query(`SELECT key FROM membership WHERE subscription='peer'`)).toEqual([
      { key: "[1]" },
    ])
    expect(
      String(driver.query(`SELECT sql FROM sqlite_master WHERE name='membership_rows'`)[0]!["sql"]),
    ).not.toContain("WITHOUT ROWID")
    store.drop("peer")
    const fault = new MembershipStore(
      {
        ...driver,
        run: (sql, params) => {
          driver.run(sql, params)
          if (sql.startsWith("DROP TABLE membership_chunks")) throw Error("interrupted")
        },
      },
      () => crypto.randomUUID(),
    )
    expect(() => fault.init()).toThrow("interrupted")
    expect(driver.query(`SELECT * FROM membership`)).toEqual([])
    store.init()
    expect(
      String(driver.query(`SELECT sql FROM sqlite_master WHERE name='membership_rows'`)[0]!["sql"]),
    ).toContain("WITHOUT ROWID")
    store.add("new", [{ path: "", table: "Chatbot", key: "[2]" }])
    expect(driver.query(`SELECT key FROM membership`)).toEqual([{ key: "[2]" }])
  })
})

it("bounds deadline reads with thousands of retained orphans and a mixed legacy cache", async () => {
  const stub = env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName("retention-deadline-budget"))
  const result = await runInDurableObject(stub, async (_object, state) => {
    let reads = 0
    const base = durableObjectDriver(state.storage)
    const driver = {
      ...base,
      run: (sql: string, params: ReadonlyArray<SqlValue> = []) => {
        const c = state.storage.sql.exec(sql, ...(params as SqlStorageValue[]))
        c.toArray()
        reads += c.rowsRead
      },
      query: (sql: string, params: ReadonlyArray<SqlValue> = []) => {
        const c = state.storage.sql.exec(sql, ...(params as SqlStorageValue[]))
        const rows = c.toArray()
        reads += c.rowsRead
        return rows as SqlRecord[]
      },
    }
    const engine = new SyncEngine({
      driver,
      schema,
      partition: "org_1",
      now: Date.now,
      newId: () => crypto.randomUUID(),
    })
    engine.init()
    driver.run(
      `WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<10000) INSERT INTO subscriptions(id,query,tables,live,created_at,orphaned_at,retire_at) SELECT 'q'||x,'{}','[]',0,0,x,100000+x FROM n`,
    )
    driver.run(
      `INSERT INTO subscriptions(id,query,tables,live,created_at,orphaned_at,retire_at) VALUES ('legacy','{}','[]',0,0,42,NULL)`,
    )
    reads = 0
    const next = engine.nextOrphanDue(60)
    const deadlineReads = reads
    reads = 0
    const swept = engine.sweepOrphans(99, 60)
    const idleSweepReads = reads
    const sub = engine.subscribe({ table: "Chatbot" })
    if (Result.isFailure(sub)) throw sub.failure
    reads = 0
    engine.markOrphaned(sub.success.subscription, 200000, {
      graceMs: 60,
      warmMs: 129600000,
      maxViews: 8,
      maxMembers: 1000,
    })
    const retirementReads = reads
    return { next, deadlineReads, idleSweepReads, retirementReads, swept }
  })
  expect(result.next).toBe(102)
  expect(result.swept).toEqual([])
  expect(result.deadlineReads).toBeLessThan(30)
  expect(result.idleSweepReads).toBeLessThan(30)
  expect(result.retirementReads).toBeLessThan(100)
})
