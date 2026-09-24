import { describe, expect, it } from "vitest"
import { rowFromRecord, rowToParams, selectByKeySql, upsertSql } from "@orbit/query"
import { writeCachedRow } from "../../src/core/write-row.ts"
import { SyncEngine } from "../../src/core/engine.ts"
import { batch, chatbot, makeEngine, organization, txn, update } from "./support/fixture.ts"

const changes = (driver: ReturnType<typeof makeEngine>["driver"]) =>
  Number(driver.query(`SELECT total_changes() AS n`)[0]!["n"])

describe("cost-safe cached writes", () => {
  it("matches full-image storage across scalar encodings, nulls, missing rows and retries", () => {
    const actual = makeEngine()
    const reference = makeEngine()
    const table = actual.deps.schema.tables.find((t) => t.name === "Chatbot")!
    const bigs = [
      null,
      "0",
      "9007199254740991",
      "9007199254740993",
      "9223372036854775807",
      "-9223372036854775808",
    ]
    for (let i = 0; i < 600; i++) {
      const row = chatbot(`id${i % 29}`, {
        groupId: i % 3 === 0 ? null : `folder${i % 7}`,
        displayOrder: i % 5 === 0 ? null : -i,
        contents: i % 4 === 0 ? null : { text: "雪 🚀", nested: [false, i, null, { a: "b" }] },
        score: i % 3 === 0 ? null : i / 13,
        big: bigs[i % bigs.length]!,
        price: i % 2 === 0 ? "0000123.4500" : "-99999999.99",
        blob: i % 2 === 0 ? "AAECA/8=" : null,
        day: i % 2 === 0 ? "2026-09-21" : null,
        at: i % 2 === 0 ? "23:59:59.999" : null,
      })
      actual.driver.transaction(() => writeCachedRow(actual.driver, table, row))
      reference.driver.run(upsertSql(table), rowToParams(table, row))
      const key = JSON.stringify([row["id"]])
      expect(actual.driver.query(selectByKeySql(table), [key])).toEqual(
        reference.driver.query(selectByKeySql(table), [key]),
      )
      const before = changes(actual.driver)
      writeCachedRow(actual.driver, table, row)
      expect(changes(actual.driver)).toBe(before)
      if (i % 50 === 0) new SyncEngine(actual.deps).init()
    }
    const org = actual.deps.schema.tables.find((t) => t.name === "organization")!
    for (const flag of [false, true, true, false]) {
      const row = organization("org_1", { hipaa_enabled: flag })
      writeCachedRow(actual.driver, org, row)
      expect(
        rowFromRecord(org, actual.driver.query(selectByKeySql(org), ['["org_1"]'])[0]!),
      ).toEqual(row)
    }
  })

  it("uses the cache rather than a stale before-image, preserves key changes and rolls back SQL failure", () => {
    const { engine, driver, deps } = makeEngine()
    const original = chatbot("old", { contents: { old: true }, big: "9223372036854775807" })
    const fill = engine.ensureScopes(["Chatbot"]).requests[0]!
    engine.applyFillRows(fill.fill_id, [original])
    engine.completeFill(fill.fill_id, {
      status: "completed",
      position: "",
      keyspace: "ks",
      shard: "0",
      row_count: 1,
      duration_ms: 1,
    })
    engine.subscribe({ table: "Chatbot" })
    const after = chatbot("old", { contents: { new: true }, big: "-9223372036854775808" })
    // before claims the change has already happened, although local storage still has original.
    expect(
      engine.applyBatch(batch(deps.schema, "org_1", [txn(1, [update("Chatbot", after, after)])]))
        .ack.status,
    ).toBe("applied")
    const table = deps.schema.tables.find((t) => t.name === "Chatbot")!
    const read = (id: string) => driver.query(selectByKeySql(table), [JSON.stringify([id])])
    expect(rowFromRecord(table, read("old")[0]!)).toEqual(after)
    const renamed = { ...after, id: "new" }
    expect(
      engine.applyBatch(batch(deps.schema, "org_1", [txn(2, [update("Chatbot", after, renamed)])]))
        .ack.status,
    ).toBe("applied")
    expect(read("old")).toEqual([])
    expect(rowFromRecord(table, read("new")[0]!)).toEqual(renamed)
    driver.run(
      `CREATE TRIGGER fail_checkpoint BEFORE INSERT ON seq_log BEGIN SELECT RAISE(ABORT, 'injected checkpoint failure'); END`,
    )
    const failed = batch(deps.schema, "org_1", [
      txn(3, [update("Chatbot", renamed, { ...renamed, contents: "must roll back" })]),
    ])
    expect(engine.applyBatch(failed).ack.status).toBe("rejected")
    expect(engine.appliedSeq).toBe(2)
    expect(rowFromRecord(table, read("new")[0]!)).toEqual(renamed)
    driver.run(`DROP TRIGGER fail_checkpoint`)
    expect(engine.applyBatch(failed).ack.status).toBe("applied")
    expect(engine.applyBatch(failed).ack).toMatchObject({ status: "applied", duplicates: 1 })
  })

  it("keeps composite keys distinct and rolls back an aborted edit", () => {
    const base = makeEngine()
    const table = base.deps.schema.tables.find((t) => t.name === "organization")!
    const composite = { ...table, primary_key: ["id", "name"] }
    const custom = { ...base.deps.schema, schema_hash: "composite-test", tables: [composite] }
    const { driver } = makeEngine("org_1", custom)
    const first = organization("org_1", { name: "one" })
    const second = organization("org_1", { name: "two" })
    writeCachedRow(driver, composite, first)
    writeCachedRow(driver, composite, second)
    writeCachedRow(driver, composite, { ...second, hipaa_enabled: true })
    expect(
      rowFromRecord(composite, driver.query(selectByKeySql(composite), ['["org_1","one"]'])[0]!),
    ).toEqual(first)
    expect(
      rowFromRecord(composite, driver.query(selectByKeySql(composite), ['["org_1","two"]'])[0]!),
    ).toEqual({ ...second, hipaa_enabled: true })
    expect(() =>
      driver.transaction(() => {
        writeCachedRow(driver, composite, { ...first, hipaa_enabled: true })
        throw Error("abort composite edit")
      }),
    ).toThrow("abort composite edit")
    expect(
      rowFromRecord(composite, driver.query(selectByKeySql(composite), ['["org_1","one"]'])[0]!),
    ).toEqual(first)
  })

  it("writes nothing during repeated unchanged initialization", () => {
    const { engine, driver, deps } = makeEngine()
    const before = changes(driver)
    for (let i = 0; i < 100; i++) new SyncEngine(deps).init()
    expect(changes(driver)).toBe(before)
    expect(engine.appliedSeq).toBe(0)
  })
})
