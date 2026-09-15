import { describe, expect, it } from "vitest"

import {
  q,
  compileSelect,
  compileIncludeSelect,
  flattenIncludes,
  mysqlDialect,
  planQuery,
} from "@orbit/query"
import { Result } from "effect"
import { SchemaRuntime } from "@orbit/schema"

import { MutatorError } from "../src/index.ts"
import { createMysqlTx } from "../src/server/mysql-tx.ts"
import { fromMysql, toParam, formatDateTime, base64Encode } from "../src/server/values.ts"
import { compileFixtureSchema, fakeTx, sync, type Sync } from "./support/fixture.ts"

const schema = await compileFixtureSchema()
const rt = new SchemaRuntime(schema)
const itemTable = rt.table("_orbit_mut")!
const colOf = (name: string) => itemTable.columns.find((c) => c.name === name)!

const ITEM_COLUMNS =
  "`id`, `org`, `name`, `flag`, `n`, `big`, `price`, `meta`, `at`, `day`, `tm`, `data`, `score`, `parentId`"

describe("createMysqlTx derived columns", () => {
  const withDerived: typeof schema = {
    ...schema,
    tables: schema.tables.map((t) =>
      t.name !== "_orbit_mut"
        ? t
        : {
            ...t,
            columns: [
              ...t.columns,
              {
                name: "hasMeta",
                kind: "bool",
                nullable: false,
                source_type: "derived",
                derived: { from: "meta", rule: { kind: "not_null" } },
              },
            ],
          },
    ),
  }
  const drt = new SchemaRuntime(withDerived)

  it("drops derived columns from writes and computes them on reads", async () => {
    const { tx, log } = fakeTx(() => [
      {
        id: "i1",
        org: "org_1",
        name: "n",
        flag: 1,
        n: null,
        big: null,
        price: null,
        meta: null,
        at: null,
        day: null,
        tm: null,
        data: null,
        score: null,
        parentId: null,
        hasMeta: 0,
      },
    ])
    const m = createMysqlTx<Sync>(drt, tx, "m")
    await m.insert("_orbit_mut", { id: "i1", org: "org_1", name: "n", hasMeta: true } as never)
    await m.update("_orbit_mut", { id: "i1" }, { hasMeta: false, name: "m" } as never)
    const row = (await m.get("_orbit_mut", { id: "i1" })) as Record<string, unknown> | null
    expect(log[0]?.sql).toBe("INSERT INTO `_orbit_mut` (`id`, `org`, `name`) VALUES (?, ?, ?)")
    expect(log[1]?.sql).toBe("UPDATE `_orbit_mut` SET `name` = ? WHERE `id` = ?")
    expect(log[2]?.sql).toBe(
      `SELECT ${ITEM_COLUMNS}, (\`meta\` IS NOT NULL) AS \`hasMeta\` FROM \`_orbit_mut\` WHERE \`id\` = ?`,
    )
    expect(row?.["hasMeta"]).toBe(false)
  })
})

describe("createMysqlTx writes", () => {
  it("inserts only the given columns with wire values converted per kind", async () => {
    const { tx, log } = fakeTx()
    const m = createMysqlTx<Sync>(rt, tx, "m")
    await m.insert("_orbit_mut", {
      id: "i1",
      org: "org_1",
      name: "first",
      flag: true,
      n: 7,
      big: "9007199254740993",
      price: "12.5000",
      meta: { a: [1, "x", null] },
      at: "2026-09-01 12:00:00.000",
      day: "2026-09-01",
      tm: "12:00:00",
      data: "AQID",
      score: 1.5,
    })
    expect(log).toEqual([
      {
        sql:
          "INSERT INTO `_orbit_mut` (`id`, `org`, `name`, `flag`, `n`, `big`, `price`, `meta`, `at`, `day`, `tm`, `data`, `score`) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FROM_BASE64(?), ?)",
        params: [
          "i1",
          "org_1",
          "first",
          1,
          7,
          "9007199254740993",
          "12.5000",
          '{"a":[1,"x",null]}',
          "2026-09-01 12:00:00.000",
          "2026-09-01",
          "12:00:00",
          "AQID",
          1.5,
        ],
      },
    ])
  })

  it("binds null cells as NULL and skips undefined cells", async () => {
    const { tx, log } = fakeTx()
    // `undefined` is not a wire value; the typed signature rejects it, so go through the untyped one.
    const m = createMysqlTx<Sync>(rt, tx, "m") as unknown as {
      insert: (t: string, r: object) => Promise<void>
    }
    await m.insert("_orbit_mut", { id: "i1", org: "o", n: null, data: null, big: undefined })
    expect(log[0]).toEqual({
      sql: "INSERT INTO `_orbit_mut` (`id`, `org`, `n`, `data`) VALUES (?, ?, ?, ?)",
      params: ["i1", "o", null, null],
    })
  })

  it("updates by primary key with the patch first and the key last", async () => {
    const { tx, log } = fakeTx()
    const m = createMysqlTx<Sync>(rt, tx, "m")
    await m.update("_orbit_mut", { id: "i1" }, { name: "second", flag: false, data: "AQ==" })
    expect(log).toEqual([
      {
        sql: "UPDATE `_orbit_mut` SET `name` = ?, `flag` = ?, `data` = FROM_BASE64(?) WHERE `id` = ?",
        params: ["second", 0, "AQ==", "i1"],
      },
    ])
  })

  it("does nothing for an empty patch", async () => {
    const { tx, log } = fakeTx()
    await createMysqlTx<Sync>(rt, tx, "m").update("_orbit_mut", { id: "i1" }, {})
    expect(log).toEqual([])
  })

  it("deletes by primary key", async () => {
    const { tx, log } = fakeTx()
    await createMysqlTx<Sync>(rt, tx, "m").delete("_orbit_mut_tag", { id: "t1" })
    expect(log).toEqual([{ sql: "DELETE FROM `_orbit_mut_tag` WHERE `id` = ?", params: ["t1"] }])
  })

  it("rejects tables and columns outside the sync schema", async () => {
    const { tx } = fakeTx()
    const m = createMysqlTx<Sync>(rt, tx, "m")
    const loose = m as unknown as {
      insert: (t: string, r: object) => Promise<void>
      get: (t: string, k: object) => Promise<unknown>
    }
    await expect(loose.insert("secrets", { id: 1 })).rejects.toMatchObject({
      _tag: "MutatorError",
      name: "m",
      reason: "apply_failed",
      message: "table secrets is not in the sync schema",
    })
    await expect(loose.insert("_orbit_mut", { id: "x", org: "o", hidden: 1 })).rejects.toThrow(
      "column _orbit_mut.hidden is not in the sync schema",
    )
    await expect(loose.get("_orbit_mut", { org: "o" })).rejects.toThrow(
      "column _orbit_mut.org is not a primary key column",
    )
    await expect(loose.get("_orbit_mut", {})).rejects.toThrow(
      "key for _orbit_mut is missing column id",
    )
    await expect(loose.insert("_orbit_mut", { id: "x", org: "o", flag: "yes" })).rejects.toThrow(
      'column flag (bool): "yes" is not a valid wire value',
    )
  })

  it("rejects an insert of no columns", async () => {
    const { tx } = fakeTx()
    const m = createMysqlTx<Sync>(rt, tx, "m") as unknown as {
      insert: (t: string, r: object) => Promise<void>
    }
    await expect(m.insert("_orbit_mut", {})).rejects.toBeInstanceOf(MutatorError)
  })
})

describe("createMysqlTx reads", () => {
  it("gets a row by key and converts driver values to the wire shape", async () => {
    const { tx, log } = fakeTx(() => [
      {
        id: "i1",
        org: "o",
        name: "n",
        flag: 1,
        n: 3,
        big: 42,
        price: "1.2300",
        meta: '{"k":true}',
        at: new Date(2026, 8, 1, 12, 0, 0, 5),
        day: new Date(2026, 8, 1),
        tm: "12:00:00",
        data: new Uint8Array([1, 2, 3]),
        score: 0.5,
        parentId: null,
        ignored: "extra driver column",
      },
    ])
    const row = await createMysqlTx<Sync>(rt, tx, "m").get("_orbit_mut", { id: "i1" })
    expect(log).toEqual([
      { sql: `SELECT ${ITEM_COLUMNS} FROM \`_orbit_mut\` WHERE \`id\` = ?`, params: ["i1"] },
    ])
    expect(row).toEqual({
      id: "i1",
      org: "o",
      name: "n",
      flag: true,
      n: 3,
      big: "42",
      price: "1.2300",
      meta: { k: true },
      at: "2026-09-01 12:00:00.005",
      day: "2026-09-01",
      tm: "12:00:00",
      data: "AQID",
      score: 0.5,
      parentId: null,
    })
  })

  it("returns null when the row does not exist", async () => {
    const { tx } = fakeTx()
    expect(await createMysqlTx<Sync>(rt, tx, "m").get("_orbit_mut", { id: "nope" })).toBeNull()
  })

  it("fails when a driver row is missing a synced column", async () => {
    const { tx } = fakeTx(() => [{ id: "i1" }])
    await expect(createMysqlTx<Sync>(rt, tx, "m").get("_orbit_mut", { id: "i1" })).rejects.toThrow()
  })

  it("runs a query through the MySQL dialect and attaches includes", async () => {
    const query = q(sync)
      .from("_orbit_mut")
      .where((c) => c.and(c.eq("flag", true), c.isNull("day")))
      .orderBy("name")
      .include("tags")
      .include("parent")
    const planned = planQuery(rt, query.ast)
    if (Result.isFailure(planned)) throw planned.failure
    const primary = compileSelect(planned.success, { dialect: mysqlDialect })
    const includes = flattenIncludes(planned.success).map((inc) => ({
      path: inc.path.join("/"),
      compiled: compileIncludeSelect(planned.success, inc, { dialect: mysqlDialect }),
    }))
    const parentSql = includes.find((i) => i.path === "parent")!.compiled.sql
    const tagsSql = includes.find((i) => i.path === "tags")!.compiled.sql

    const item = (id: string, parentId: string | null) => ({
      id,
      org: "o",
      name: id,
      flag: 1,
      n: null,
      big: null,
      price: null,
      meta: null,
      at: null,
      day: null,
      tm: null,
      data: null,
      score: null,
      parentId,
    })
    const { tx, log } = fakeTx((sql) => {
      if (sql === primary.sql) return [item("child", "root"), item("root", null)]
      if (sql === parentSql) return [item("root", null)]
      if (sql === tagsSql)
        return [
          { id: "t1", org: "o", itemId: "child", label: "a" },
          { id: "t2", org: "o", itemId: "child", label: "b" },
        ]
      throw new Error(`unexpected sql ${sql}`)
    })
    const rows = await createMysqlTx<Sync>(rt, tx, "m").query(query)
    expect(log.map((s) => s.sql)).toEqual([primary.sql, parentSql, tagsSql])
    expect(log[0]!.params).toEqual([1])
    expect(primary.sql).toBe(
      `SELECT ${ITEM_COLUMNS.split(", ")
        .map((c) => `t.${c}`)
        .join(
          ", ",
        )} FROM \`_orbit_mut\` t WHERE ((t.\`flag\` = ?) AND (t.\`day\` IS NULL)) ORDER BY t.\`name\` ASC, t.\`id\` ASC`,
    )
    expect(rows).toHaveLength(2)
    expect(rows[0]!.id).toBe("child")
    expect(rows[0]!.flag).toBe(true)
    expect(rows[0]!.parent?.id).toBe("root")
    expect(rows[0]!.tags.map((t) => t.label)).toEqual(["a", "b"])
    expect(rows[1]!.parent).toBeNull()
    expect(rows[1]!.tags).toEqual([])
  })

  it("rejects a query the planner refuses", async () => {
    const { tx } = fakeTx()
    const bad = q(sync).from("_orbit_mut").orderBy("meta")
    await expect(createMysqlTx<Sync>(rt, tx, "m").query(bad)).rejects.toMatchObject({
      _tag: "MutatorError",
      reason: "apply_failed",
    })
  })
})

describe("value conversion", () => {
  it("accepts every driver shape per kind", () => {
    expect(fromMysql(colOf("flag"), true)).toBe(true)
    expect(fromMysql(colOf("flag"), "0")).toBe(false)
    expect(fromMysql(colOf("flag"), 2n)).toBe(true)
    expect(fromMysql(colOf("n"), "12")).toBe(12)
    expect(fromMysql(colOf("big"), 12345678901234567890n)).toBe("12345678901234567890")
    expect(fromMysql(colOf("big"), 1e21)).toBe("1000000000000000000000")
    expect(fromMysql(colOf("big"), "-1")).toBe("-1")
    expect(fromMysql(colOf("price"), 1.5)).toBe("1.5")
    expect(fromMysql(colOf("at"), "2026-09-01 12:00:00.000")).toBe("2026-09-01 12:00:00.000")
    expect(fromMysql(colOf("meta"), { a: 1 })).toEqual({ a: 1 })
    expect(fromMysql(colOf("meta"), "[1,2]")).toEqual([1, 2])
    expect(fromMysql(colOf("data"), new Uint8Array([255, 0, 1]).buffer)).toBe("/wAB")
    expect(fromMysql(colOf("data"), "AQ==")).toBe("AQ==")
    expect(fromMysql(colOf("name"), 5)).toBe("5")
    expect(fromMysql(colOf("score"), undefined)).toBeNull()
    expect(() => fromMysql(colOf("at"), 5)).toThrow(
      "column at (datetime): cannot convert 5 from MySQL",
    )
    expect(() => fromMysql(colOf("meta"), "{oops")).toThrow()
  })

  it("rejects wire values of the wrong kind", () => {
    expect(() => toParam(colOf("n"), 1.5)).toThrow("column n (int)")
    expect(() => toParam(colOf("big"), 12)).toThrow("column big (bigint)")
    expect(() => toParam(colOf("data"), "not base64!")).toThrow("column data (bytes)")
    expect(() => toParam(colOf("score"), Number.NaN)).toThrow("column score (float)")
    expect(toParam(colOf("meta"), null)).toBeNull()
    expect(toParam(colOf("meta"), "text")).toBe('"text"')
  })

  it("formats dates from local components and encodes bytes", () => {
    expect(formatDateTime(new Date(2026, 0, 2, 3, 4, 5, 6))).toBe("2026-01-02 03:04:05.006")
    const big = new Uint8Array(70000).fill(65)
    expect(base64Encode(big)).toBe(Buffer.from(big).toString("base64"))
  })
})
