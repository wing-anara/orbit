import { expect, it } from "vitest"
import { Sessions } from "../../src/sessions.ts"
import { nodeDriver } from "./support/node-driver.ts"

it("upgrades old session metadata without rewriting or guessing the original authorization query", () => {
  const driver = nodeDriver()
  driver.run(
    `CREATE TABLE client_subs(session TEXT NOT NULL, client_sub_id TEXT NOT NULL, subscription TEXT NOT NULL, status TEXT NOT NULL, PRIMARY KEY(session,client_sub_id))`,
  )
  driver.run(`INSERT INTO client_subs VALUES ('s','q','normalized-query-key','live')`)
  const sessions = new Sessions(driver)
  expect(sessions.clientSubsOf("s")).toEqual([
    { clientSubId: "q", subscription: "normalized-query-key", status: "live", queryRef: null },
  ])
  const ref = { name: "mine", args: { nested: { values: [null, true, "雪"] } } }
  sessions.setClientSub("s", "q", "normalized-query-key", "live", ref)
  const before = driver.query(`SELECT total_changes() AS n`)
  sessions.setClientSub("s", "q", "normalized-query-key", "live", ref)
  const reopened = new Sessions(driver)
  expect(driver.query(`SELECT total_changes() AS n`)).toEqual(before)
  expect(reopened.clientSubsOf("s")[0]?.queryRef).toEqual(ref)
  sessions.markAllPending()
  sessions.markLive("normalized-query-key")
  expect(sessions.clientSubsOf("s")[0]?.queryRef).toEqual(ref)
})
