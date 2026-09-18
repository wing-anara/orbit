import { describe, expect, it } from "vitest"
import { MembershipStore } from "../../src/core/membership.ts"
import { makeEngine } from "./support/fixture.ts"

const members = (from: number, to: number) =>
  Array.from({ length: to - from }, (_, i) => ({
    path: "",
    table: "Chatbot",
    key: JSON.stringify([from + i]),
  }))

describe("shared membership chunks", () => {
  it("stores a hundred growing windows once and isolates edits across branches", () => {
    const { driver, deps } = makeEngine()
    const store = new MembershipStore(driver, deps.newId)
    driver.transaction(() => {
      for (let i = 1; i <= 100; i++) {
        if (i > 1) store.share(String(i), String(i - 1))
        store.add(String(i), members((i - 1) * 100, i * 100))
      }
    })
    const physical = () =>
      Number(driver.query(`SELECT COUNT(*) AS n FROM membership_rows`)[0]!["n"])
    const keys = (id: string) =>
      driver.query(`SELECT key FROM membership WHERE subscription = ? ORDER BY key`, [id])
    expect(physical()).toBe(10_000)
    expect(keys("100")).toHaveLength(10_000)
    const before = keys("50")
    driver.transaction(() => {
      store.share("branch", "50")
      store.remove("branch", members(1, 2)[0]!)
      store.add("branch", members(10_000, 10_001))
    })
    expect(keys("50")).toEqual(before)
    expect(keys("100")).toHaveLength(10_000)
    expect(keys("branch")).toHaveLength(5000)
    expect(physical()).toBeLessThanOrEqual(10_512)
    // The previous writer must not mutate the branch after sharing it.
    store.add("50", members(10_001, 10_002))
    expect(keys("branch")).toHaveLength(5000)
    expect(keys("100")).toHaveLength(10_000)
    // Duplicate includes and different paths retain set semantics.
    store.add("branch", [members(2, 3)[0]!, members(2, 3)[0]!])
    expect(keys("branch")).toHaveLength(5000)
    store.add("branch", [{ ...members(2, 3)[0]!, path: "folder" }])
    expect(keys("branch")).toHaveLength(5001)
  })

  it("rolls back chunk allocation, sharing, and copy-on-write and survives eviction", () => {
    const { driver, deps } = makeEngine()
    let store = new MembershipStore(driver, deps.newId)
    store.add("a", members(0, 700))
    store.share("b", "a")
    const dump = () =>
      ["membership_rows", "membership_chunks", "membership_writers"].map((table) =>
        driver.query(`SELECT * FROM ${table} ORDER BY rowid`),
      )
    const before = dump()
    expect(() =>
      driver.transaction(() => {
        store.remove("a", members(5, 6)[0]!)
        store.add("b", members(700, 800))
        store.share("c", "b")
        throw Error("abort")
      }),
    ).toThrow("abort")
    expect(dump()).toEqual(before)
    store = new MembershipStore(driver, deps.newId)
    store.init()
    store.add("a", members(700, 800))
    store.remove("b", members(5, 6)[0]!)
    expect(driver.query(`SELECT * FROM membership WHERE subscription = 'a'`)).toHaveLength(800)
    expect(driver.query(`SELECT * FROM membership WHERE subscription = 'b'`)).toHaveLength(699)
  })

  it("bounds reclamation and keeps shared chunks until the final owner leaves", () => {
    const { driver, deps } = makeEngine()
    const store = new MembershipStore(driver, deps.newId)
    store.add("a", members(0, 2400))
    store.share("b", "a")
    expect(store.collect("a", 2)).toEqual({ spent: 2, complete: false })
    expect(driver.query(`SELECT * FROM membership_rows`)).toHaveLength(2400)
    store.drop("a")
    expect(driver.query(`SELECT * FROM membership WHERE subscription = 'b'`)).toHaveLength(2400)
    expect(store.collect("b", 1000)).toEqual({ spent: 1000, complete: false })
    expect(driver.query(`SELECT * FROM membership_rows`)).toHaveLength(1400)
    store.drop("b")
    expect(driver.query(`SELECT * FROM membership_rows`)).toEqual([])
    expect(driver.query(`SELECT * FROM membership_chunks`)).toEqual([])
    expect(driver.query(`SELECT * FROM membership_writers`)).toEqual([])
  })

  it("adopts a legacy cache without copying rows or losing shared-view isolation", () => {
    const { driver, deps } = makeEngine()
    driver.run(`DROP VIEW membership`)
    driver.run(`DROP TABLE membership_rows`)
    driver.run(`DROP TABLE membership_chunks`)
    driver.run(`DROP TABLE membership_writers`)
    driver.run(
      `CREATE TABLE membership (subscription TEXT NOT NULL, path TEXT NOT NULL, tbl TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY (subscription, path, tbl, key))`,
    )
    driver.run(`CREATE INDEX membership_by_row ON membership (tbl, key, subscription)`)
    driver.run(
      `INSERT INTO subscriptions (id, query, tables, live, created_at) VALUES ('legacy', '{}', '[]', 1, 0)`,
    )
    driver.run(`INSERT INTO membership VALUES ('legacy', '', 'Chatbot', '[1]')`)
    const store = new MembershipStore(driver, deps.newId)
    store.init()
    store.init()
    store.share("grown", "legacy")
    store.remove("grown", { path: "", table: "Chatbot", key: "[1]" })
    expect(driver.query(`SELECT key FROM membership WHERE subscription = 'legacy'`)).toEqual([
      { key: "[1]" },
    ])
    expect(driver.query(`SELECT key FROM membership WHERE subscription = 'grown'`)).toEqual([])
  })
})
