import { readFileSync } from "node:fs"
import { Effect, Result, Schema } from "effect"
import { describe, expect, it } from "vitest"
import { SyncSchema } from "@orbit/protocol"
import { LocalStore } from "../src/store.ts"
import type { Statement } from "../src/driver.ts"
import { nodeAsyncDriver } from "./support/node-driver.ts"

const schema = Schema.decodeUnknownSync(SyncSchema)(
  JSON.parse(
    readFileSync(new URL("../../../schema/fixtures/SyncSchema.json", import.meta.url), "utf8"),
  ),
)

it("rolls back the entire queued burst when a later transaction fails", async () => {
  const driver = nodeAsyncDriver()
  const store = new LocalStore(driver, schema, "org_1")
  try {
    await Effect.runPromise(store.open({ clientId: "burst-rollback" }))
    await Effect.runPromise(
      store.registerSubscription("view", { table: "organization" }, { table: "organization" }),
    )
    const member = { table: "organization", key: ["org_1"] }
    const row = {
      ...member,
      row: { id: "org_1", name: "before", created_at: "2026-01-01 00:00:00", hipaa_enabled: false },
    }
    await Effect.runPromise(store.applySnapshot("view", 1, [row], [member]))
    driver.db.exec(
      "CREATE TEMP TRIGGER reject_bad BEFORE UPDATE ON t_organization WHEN NEW.name = 'bad' BEGIN SELECT RAISE(ABORT, 'test failure'); END",
    )
    await expect(
      Effect.runPromise(
        store.applyDeltas([
          {
            cursor: 2,
            rows: [{ ...row, row: { ...row.row, name: "intermediate" } }],
            memberships: [],
          },
          { cursor: 3, rows: [{ ...row, row: { ...row.row, name: "bad" } }], memberships: [] },
        ]),
      ),
    ).rejects.toThrow("test failure")
    expect(driver.db.prepare("SELECT name FROM t_organization").get()?.["name"]).toBe("before")
    expect(await Effect.runPromise(store.cursor())).toBe(1)
  } finally {
    driver.db.close()
  }
})

describe("subscription garbage collection", () => {
  it("checks only candidate rows, preserving other owners and transferred window memberships", async () => {
    const driver = nodeAsyncDriver()
    const batches: Array<ReadonlyArray<Statement>> = []
    const store = new LocalStore(
      {
        ...driver,
        batch: async (statements) => {
          batches.push(statements)
          await driver.batch(statements)
        },
      },
      schema,
      "org_1",
    )
    try {
      await Effect.runPromise(store.open({ clientId: "gc-test" }))
      for (const id of ["base", "grown", "other"])
        await Effect.runPromise(
          store.registerSubscription(id, { table: "organization" }, { table: "organization" }),
        )
      const rows = ["shared", "base-only", "grown-only", "unrelated"].map((id) => ({
        table: "organization",
        key: [id],
        row: { id, name: id, created_at: "2026-01-01 00:00:00", hipaa_enabled: false },
      }))
      const members = (ids: Array<string>) =>
        ids.map((id) => ({ table: "organization", key: [id] }))
      await Effect.runPromise(
        store.applySnapshot("base", 1, rows.slice(0, 2), members(["shared", "base-only"])),
      )
      await Effect.runPromise(
        store.applySnapshot("other", 1, [rows[0]!, rows[3]!], members(["shared", "unrelated"])),
      )
      await Effect.runPromise(
        store.applySnapshot("grown", 1, [rows[2]!], members(["grown-only"]), "base"),
      )
      await Effect.runPromise(store.removeSubscription("base"))
      expect(
        driver.db
          .prepare("SELECT id FROM t_organization ORDER BY id")
          .all()
          .map((r) => r["id"]),
      ).toEqual(["base-only", "grown-only", "shared", "unrelated"])
      await Effect.runPromise(store.applySnapshot("grown", 2, [], []))
      expect(
        driver.db
          .prepare("SELECT id FROM t_organization ORDER BY id")
          .all()
          .map((r) => r["id"]),
      ).toEqual(["shared", "unrelated"])
      const gc = batches.at(-1)!.find((s) => s.sql.startsWith('DELETE FROM "t_organization"'))!
      const plan = driver.db.prepare(`EXPLAIN QUERY PLAN ${gc.sql}`).all(...gc.params)
      expect(plan.some((r) => String(r["detail"]).includes("tbl=? AND key=?"))).toBe(true)
      await Effect.runPromise(store.removeSubscription("other"))
      expect(driver.db.prepare("SELECT COUNT(*) AS n FROM t_organization").get()?.["n"]).toBe(0)
    } finally {
      driver.db.close()
    }
  })
})

it("replays unchanged snapshots without deleting or rewriting cached row images", async () => {
  const driver = nodeAsyncDriver()
  const store = new LocalStore(driver, schema, "org_1")
  try {
    await Effect.runPromise(store.open({ clientId: "replay-test" }))
    await Effect.runPromise(
      store.registerSubscription("view", { table: "organization" }, { table: "organization" }),
    )
    const row = {
      table: "organization",
      key: ["org_1"],
      row: {
        id: "org_1",
        name: "original",
        created_at: "2026-01-01 00:00:00",
        hipaa_enabled: false,
      },
    }
    const members = [{ table: "organization", key: ["org_1"] }]
    await Effect.runPromise(store.applySnapshot("view", 1, [row], members))
    driver.db.exec(
      `CREATE TEMP TABLE writes (op TEXT); CREATE TEMP TRIGGER track_insert AFTER INSERT ON t_organization BEGIN INSERT INTO writes VALUES ('insert'); END; CREATE TEMP TRIGGER track_update AFTER UPDATE ON t_organization BEGIN INSERT INTO writes VALUES ('update'); END; CREATE TEMP TRIGGER track_delete AFTER DELETE ON t_organization BEGIN INSERT INTO writes VALUES ('delete'); END;`,
    )
    await Effect.runPromise(store.applySnapshot("view", 2, [row], members))
    expect(driver.db.prepare("SELECT * FROM writes").all()).toEqual([])
    const changed = { ...row, row: { ...row.row, name: "changed", hipaa_enabled: true } }
    await Effect.runPromise(store.applySnapshot("view", 3, [changed], members))
    expect(driver.db.prepare("SELECT op FROM writes").all()).toEqual([{ op: "update" }])
    expect(driver.db.prepare("SELECT name, hipaa_enabled FROM t_organization").get()).toEqual({
      name: "changed",
      hipaa_enabled: 1,
    })
    driver.db.exec("DELETE FROM writes")
    await Effect.runPromise(store.applyDelta(4, [changed], []))
    expect(driver.db.prepare("SELECT * FROM writes").all()).toEqual([])
    await Effect.runPromise(store.applySnapshot("view", 5, [], []))
    expect(driver.db.prepare("SELECT op FROM writes").all()).toEqual([{ op: "delete" }])
  } finally {
    driver.db.close()
  }
})

it("keeps window growth linear and restores only live subscriptions while retaining inherited rows", async () => {
  const driver = nodeAsyncDriver()
  const store = new LocalStore(driver, schema, "org_1")
  try {
    await Effect.runPromise(store.open({ clientId: "chain-test" }))
    driver.db.exec(
      "CREATE TEMP TABLE membership_writes (n INTEGER); INSERT INTO membership_writes VALUES (0); CREATE TEMP TRIGGER count_membership AFTER INSERT ON membership BEGIN UPDATE membership_writes SET n = n + 1; END;",
    )
    for (let window = 0; window < 20; window++) {
      const id = String(window)
      const query = { table: "organization", limit: (window + 1) * 10 }
      await Effect.runPromise(store.registerSubscription(id, query, query))
      const rows = Array.from({ length: 10 }, (_, i) => {
        const key = String(window * 10 + i).padStart(4, "0")
        return {
          table: "organization",
          key: [key],
          row: { id: key, name: key, created_at: "2026-01-01 00:00:00", hipaa_enabled: false },
        }
      })
      await Effect.runPromise(
        store.applySnapshot(
          id,
          window,
          rows,
          rows.map((r) => ({ table: r.table, key: r.key })),
          window === 0 ? null : String(window - 1),
        ),
      )
      if (window > 0) await Effect.runPromise(store.removeSubscription(String(window - 1)))
    }
    expect(driver.db.prepare("SELECT n FROM membership_writes").get()?.["n"]).toBe(200)
    expect((await Effect.runPromise(store.subscriptions())).map((s) => s.id)).toEqual(["19"])
    await Effect.runPromise(store.open())
    const planned = store.plan({ table: "organization", limit: 200 })
    if (Result.isFailure(planned)) throw planned.failure
    expect((await Effect.runPromise(store.readSubscription(planned.success, "19"))).length).toBe(
      200,
    )
    // Reactivation must restore the base's subscription without losing its dependents.
    await Effect.runPromise(
      store.registerSubscription(
        "0",
        { table: "organization", limit: 10 },
        { table: "organization", limit: 10 },
      ),
    )
    expect((await Effect.runPromise(store.subscriptions())).map((s) => s.id).sort()).toEqual([
      "0",
      "19",
    ])
    await Effect.runPromise(store.removeSubscription("19"))
    expect(driver.db.prepare("SELECT COUNT(*) AS n FROM t_organization").get()?.["n"]).toBe(10)
    await Effect.runPromise(store.removeSubscription("0"))
    expect(driver.db.prepare("SELECT COUNT(*) AS n FROM subscriptions").get()?.["n"]).toBe(0)
    expect(driver.db.prepare("SELECT COUNT(*) AS n FROM membership").get()?.["n"]).toBe(0)
    expect(driver.db.prepare("SELECT COUNT(*) AS n FROM t_organization").get()?.["n"]).toBe(0)
  } finally {
    driver.db.close()
  }
})

it("adds cache-only retirement bookkeeping without discarding an existing cache", async () => {
  const driver = nodeAsyncDriver()
  const store = new LocalStore(driver, schema, "org_1")
  try {
    await Effect.runPromise(store.open({ clientId: "legacy" }))
    await Effect.runPromise(
      store.registerSubscription("view", { table: "organization" }, { table: "organization" }),
    )
    driver.db.exec("ALTER TABLE subscriptions DROP COLUMN retired")
    expect((await Effect.runPromise(store.open())).action).toBe("none")
    expect((await Effect.runPromise(store.subscriptions())).map((s) => s.id)).toEqual(["view"])
  } finally {
    driver.db.close()
  }
})
