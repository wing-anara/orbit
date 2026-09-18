import { readFileSync } from "node:fs"
import { Effect, Schema } from "effect"
import { expect, it } from "vitest"
import { SyncSchema } from "@orbit/protocol"
import { LocalMutationTx } from "../src/mutations.ts"
import { LocalStore, pruneUndoStatement } from "../src/store.ts"
import { nodeAsyncDriver } from "./support/node-driver.ts"

const schema = Schema.decodeUnknownSync(SyncSchema)(
  JSON.parse(
    readFileSync(new URL("../../../schema/fixtures/SyncSchema.json", import.meta.url), "utf8"),
  ),
)
const row = { id: "org_1", name: "🪐", created_at: "2026-01-01 00:00:00", hipaa_enabled: false }

it("commits undo images atomically and discards them on partition reset", async () => {
  const driver = nodeAsyncDriver()
  const store = new LocalStore(driver, schema, "org_1")
  try {
    await Effect.runPromise(store.open())
    const tx = new LocalMutationTx(driver, store, 1)
    await tx.localUndo.capture("organization", "root", [row])
    await expect(
      driver.batch([...tx.statements, { sql: "INSERT INTO absent_table VALUES (1)", params: [] }]),
    ).rejects.toThrow()
    expect(await driver.query("SELECT * FROM local_undo")).toEqual([])
    await driver.batch(tx.statements)
    const undo = new LocalMutationTx(driver, store, 2)
    await undo.localUndo.restore("organization", "root")
    await driver.batch(undo.statements)
    expect(await undo.get("organization", { id: "org_1" })).toEqual(row)
    await Effect.runPromise(new LocalStore(driver, schema, "different_org").open())
    expect(await driver.query("SELECT * FROM local_undo")).toEqual([])
  } finally {
    await driver.close()
  }
})

it("bounds completed history while preserving all captures needed by pending commands", async () => {
  const driver = nodeAsyncDriver()
  const store = new LocalStore(driver, schema, "org_1")
  try {
    await Effect.runPromise(store.open())
    for (let id = 1; id <= 105; id++) {
      const tx = new LocalMutationTx(driver, store, id)
      await tx.localUndo.capture("organization", "root", [{ ...row, name: String(id) }])
      await driver.batch(tx.statements)
    }
    await driver.batch([
      {
        sql: "INSERT INTO pending_mutations (id, name, args, created_at) VALUES (106, 'undo', '{}', 0)",
        params: [],
      },
      pruneUndoStatement(),
    ])
    expect((await driver.query("SELECT count(*) AS n FROM local_undo"))[0]?.["n"]).toBe(105)
    await driver.batch([{ sql: "DELETE FROM pending_mutations", params: [] }, pruneUndoStatement()])
    expect((await driver.query("SELECT count(*) AS n FROM local_undo"))[0]?.["n"]).toBe(100)
    const undo = new LocalMutationTx(driver, store, 107)
    await undo.localUndo.restore("organization", "root")
    expect(await undo.get("organization", { id: "org_1" })).toEqual({ ...row, name: "105" })
    // A large completed deletion cannot grow local history without bound.
    await driver.batch([
      {
        sql: "UPDATE local_undo SET images = zeroblob(33554433) WHERE mutation_id = 105",
        params: [],
      },
      pruneUndoStatement(),
    ])
    expect(await driver.query("SELECT * FROM local_undo")).toEqual([])
  } finally {
    await driver.close()
  }
})

it("restores missing related rows without overwriting a still-live shared row", async () => {
  const driver = nodeAsyncDriver()
  const store = new LocalStore(driver, schema, "org_1")
  try {
    await Effect.runPromise(store.open())
    const capture = new LocalMutationTx(driver, store, 1)
    await capture.localUndo.capture("organization", "root", [row, { ...row, id: "missing" }])
    await driver.batch(capture.statements)
    const update = new LocalMutationTx(driver, store, 2)
    await update.insert("organization", { ...row, name: "newer shared value" })
    await driver.batch(update.statements)
    const undo = new LocalMutationTx(driver, store, 3)
    await undo.localUndo.restore("organization", "root", { onlyMissing: true })
    await driver.batch(undo.statements)
    expect((await undo.get("organization", { id: "org_1" }))?.name).toBe("newer shared value")
    expect((await undo.get("organization", { id: "missing" }))?.name).toBe("🪐")
  } finally {
    await driver.close()
  }
})
