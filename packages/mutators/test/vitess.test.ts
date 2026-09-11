/**
 * End-to-end push against the local docker Vitess with mysql2. Guarded by `ORBIT_TEST_VITESS=1`:
 *
 *     ORBIT_TEST_VITESS=1 pnpm --filter @orbit/mutators test
 */

import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest"
import mysql from "mysql2/promise"

import {
  decodePushResponse,
  MUTATION_PROTOCOL_VERSION,
  orbitClientsDdl,
  type PushRequest,
} from "@orbit/protocol"

import { createPushHandler, type PushDb, type SqlTx } from "../src/server/index.ts"
import { compileFixtureSchema, FIXTURE_DDL, mutators, observed } from "./support/fixture.ts"

const enabled = process.env["ORBIT_TEST_VITESS"] === "1"

const mysqlTx = (conn: mysql.Connection): SqlTx => ({
  query: async (sql, params) => {
    const [rows] = await conn.query(sql, [...params])
    return rows as ReadonlyArray<Record<string, unknown>>
  },
  execute: async (sql, params) => {
    await conn.query(sql, [...params])
  },
})

const mysqlDb = (conn: mysql.Connection): PushDb => ({
  transaction: async (f) => {
    await conn.beginTransaction()
    try {
      const out = await f(mysqlTx(conn))
      await conn.commit()
      return out
    } catch (e) {
      await conn.rollback()
      throw e
    }
  },
})

describe.skipIf(!enabled)("push handler against Vitess", () => {
  let conn: mysql.Connection
  let handler: (request: Request) => Promise<Response>

  const push = async (mutations: PushRequest["mutations"], clientId = "it_c1") => {
    const body: PushRequest = {
      protocolVersion: MUTATION_PROTOCOL_VERSION,
      clientId,
      partition: "org_it",
      mutations,
    }
    const response = await handler(
      new Request("http://app.test/push", { method: "POST", body: JSON.stringify(body) }),
    )
    expect(response.status).toBe(200)
    return decodePushResponse(await response.json())
  }

  const lastMutationId = async (clientId = "it_c1"): Promise<number | null> => {
    const [rows] = await conn.query(
      "SELECT last_mutation_id, partition_key FROM orbit_clients WHERE client_id = ?",
      [clientId],
    )
    const first = (rows as Array<{ last_mutation_id: string | number; partition_key: string }>)[0]
    if (first === undefined) return null
    expect(first.partition_key).toBe("org_it")
    return Number(first.last_mutation_id)
  }

  beforeAll(async () => {
    conn = await mysql.createConnection({
      host: "127.0.0.1",
      port: 33577,
      user: "root",
      database: "orbit",
      dateStrings: true,
      supportBigNumbers: true,
      bigNumberStrings: true,
      disableEval: true,
    })
    await conn.query(orbitClientsDdl())
    for (const ddl of FIXTURE_DDL) await conn.query(ddl)
    handler = createPushHandler({
      schema: await compileFixtureSchema(),
      mutators,
      db: mysqlDb(conn),
      authorize: async () => ({ subject: "it_user" }),
    })
  })

  beforeEach(async () => {
    await conn.query("DELETE FROM `_orbit_mut_tag`")
    await conn.query("DELETE FROM `_orbit_mut`")
    await conn.query("DELETE FROM `orbit_clients` WHERE client_id LIKE 'it_%'")
    observed.contexts.length = 0
    observed.rows.length = 0
  })

  afterAll(async () => {
    await conn?.end()
  })

  it("applies, confirms, deduplicates and refuses gaps", async () => {
    expect(
      await push([{ id: 1, name: "createItem", args: { id: "it_1", name: "first" } }]),
    ).toEqual({
      type: "ok",
      outcomes: [{ id: 1, status: "applied" }],
      lastMutationId: 1,
    })
    expect(await lastMutationId()).toBe(1)
    const [rows] = await conn.query(
      "SELECT id, org, name, flag, at FROM `_orbit_mut` WHERE id = ?",
      ["it_1"],
    )
    expect(rows).toEqual([
      { id: "it_1", org: "org_it", name: "first", flag: 1, at: observed.contexts[0]!.now },
    ])

    expect(
      await push([
        { id: 1, name: "createItem", args: { id: "it_1", name: "replay" } },
        { id: 2, name: "rename", args: { id: "it_1", name: "second" } },
      ]),
    ).toEqual({
      type: "ok",
      outcomes: [
        { id: 1, status: "duplicate" },
        { id: 2, status: "applied" },
      ],
      lastMutationId: 2,
    })
    const [renamed] = await conn.query("SELECT name FROM `_orbit_mut` WHERE id = ?", ["it_1"])
    expect(renamed).toEqual([{ name: "second" }])

    expect(await push([{ id: 4, name: "remove", args: { id: "it_1" } }])).toEqual({
      type: "refused",
      reason: "out_of_order",
      message: "mutation 4 does not follow last_mutation_id 2",
      lastMutationId: 2,
    })
    expect(await lastMutationId()).toBe(2)
  })

  it("rolls back a failed mutator and still consumes its id", async () => {
    expect(
      await push([
        { id: 1, name: "writeThenBoom", args: { id: "it_doomed" } },
        { id: 2, name: "createItem", args: { id: "it_2", name: "after" } },
        { id: 3, name: "rename", args: { id: "it_missing", name: "x" } },
      ]),
    ).toEqual({
      type: "ok",
      outcomes: [
        { id: 1, status: "failed", error: "boom" },
        { id: 2, status: "applied" },
        { id: 3, status: "failed", error: "item it_missing not found" },
      ],
      lastMutationId: 3,
    })
    const [rows] = await conn.query("SELECT id FROM `_orbit_mut` ORDER BY id")
    expect(rows).toEqual([{ id: "it_2" }])
    expect(await lastMutationId()).toBe(3)
  })

  it("round-trips every value kind and reads back through get and query", async () => {
    const rich = mutators.definitions.createItem
    expect(rich).toBeDefined()
    const out = await push([
      { id: 1, name: "createItem", args: { id: "it_root", name: "root" } },
      { id: 2, name: "createItem", args: { id: "it_child", name: "child" } },
    ])
    expect(out.type).toBe("ok")
    await conn.query(
      "UPDATE `_orbit_mut` SET n = ?, big = ?, price = ?, meta = ?, day = ?, tm = ?, data = FROM_BASE64(?), score = ?, parentId = ? WHERE id = ?",
      [
        7,
        "9007199254740993",
        "12.5000",
        '{"a":[1,"x",null]}',
        "2026-09-01",
        "12:34:56",
        "AQID",
        1.5,
        "it_root",
        "it_child",
      ],
    )
    await conn.query(
      "INSERT INTO `_orbit_mut_tag` (id, org, itemId, label) VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
      ["it_t1", "org_it", "it_child", "a", "it_t2", "org_it", "it_child", "b"],
    )
    expect(await push([{ id: 3, name: "inspect", args: { flag: true } }])).toEqual({
      type: "ok",
      outcomes: [{ id: 3, status: "applied" }],
      lastMutationId: 3,
    })
    expect(observed.rows).toHaveLength(2)
    const [child, root] = observed.rows as Array<Record<string, unknown>>
    expect(child).toMatchObject({
      id: "it_child",
      org: "org_it",
      name: "child",
      flag: true,
      n: 7,
      big: "9007199254740993",
      price: "12.5000",
      meta: { a: [1, "x", null] },
      day: "2026-09-01",
      tm: "12:34:56",
      data: "AQID",
      score: 1.5,
      parentId: "it_root",
      parent: expect.objectContaining({ id: "it_root", name: "root" }),
      tags: [
        expect.objectContaining({ id: "it_t1", label: "a" }),
        expect.objectContaining({ id: "it_t2", label: "b" }),
      ],
    })
    expect(child!["at"]).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/)
    expect(root).toMatchObject({ id: "it_root", parent: null, tags: [] })
  })
})
