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
