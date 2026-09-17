import sqlite3InitModule from "@sqlite.org/sqlite-wasm"
import { describe, expect, it, vi } from "vitest"

import { runBatch } from "../src/worker/batch.ts"

const sqlite = await sqlite3InitModule()

describe("SQLite worker batches", () => {
  it("prepares repeated row writes once and clears previous bindings", () => {
    const db = new sqlite.oo1.DB(":memory:")
    try {
      db.exec("CREATE TABLE rows (id INTEGER PRIMARY KEY, value TEXT)")
      const prepare = vi.spyOn(db, "prepare")
      runBatch(
        db,
        Array.from({ length: 1000 }, (_, id) => ({
          sql: "INSERT INTO rows (id, value) VALUES (?, ?)",
          params: id % 2 === 0 ? [id, "value"] : [id],
        })),
      )
      expect(prepare).toHaveBeenCalledTimes(1)
      expect(db.selectValue("SELECT COUNT(*) FROM rows")).toBe(1000)
      expect(db.selectValue("SELECT COUNT(*) FROM rows WHERE value IS NULL")).toBe(500)
    } finally {
      db.close()
    }
  })

  it("rolls back the entire batch on failure and releases statements", () => {
    const db = new sqlite.oo1.DB(":memory:")
    try {
      db.exec("CREATE TABLE rows (id INTEGER PRIMARY KEY)")
      expect(() =>
        runBatch(
          db,
          [1, 2, 1].map((id) => ({
            sql: "INSERT INTO rows VALUES (?)",
            params: [id],
          })),
        ),
      ).toThrow()
      expect(db.selectValue("SELECT COUNT(*) FROM rows")).toBe(0)
      runBatch(db, [{ sql: "INSERT INTO rows VALUES (?)", params: [3] }])
      expect(db.selectValue("SELECT id FROM rows")).toBe(3)
    } finally {
      db.close()
    }
  })

  it("preserves multi-statement SQL, DDL ordering, and RETURNING writes", () => {
    const db = new sqlite.oo1.DB(":memory:")
    try {
      runBatch(db, [
        { sql: "CREATE TABLE rows (id INTEGER PRIMARY KEY)", params: [] },
        { sql: "DELETE FROM rows", params: [] },
        { sql: "INSERT INTO rows VALUES (?) RETURNING id", params: [1] },
        { sql: "DROP TABLE rows; CREATE TABLE rows (id INTEGER PRIMARY KEY)", params: [] },
        { sql: "INSERT INTO rows VALUES (?) RETURNING id", params: [2] },
        { sql: "INSERT INTO rows VALUES (3); INSERT INTO rows VALUES (4)", params: [] },
      ])
      expect(db.selectArray("SELECT SUM(id), COUNT(*) FROM rows")).toEqual([9, 3])
    } finally {
      db.close()
    }
  })
})
