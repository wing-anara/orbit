import { readFileSync } from "node:fs"
import { Effect, Schema } from "effect"
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
