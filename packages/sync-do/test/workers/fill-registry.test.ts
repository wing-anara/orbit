import { env, evictDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

async function registry() {
  const stub = env.ORBIT_FILL_REGISTRY.get(env.ORBIT_FILL_REGISTRY.newUniqueId())
  const request = {
    fill_id: "org:fill",
    partition: "org",
    table: "Chatbot",
    schema_hash: "test",
    requested_at_ms: Date.now(),
  }
  await stub.fetch("https://registry/enqueue", { method: "POST", body: JSON.stringify(request) })
  return { stub, request }
}
const poll = (stub: Awaited<ReturnType<typeof registry>>["stub"], suffix = "&ack=1") =>
  stub.fetch(`https://registry/next?wait=0${suffix}`)

describe("fill delivery receipts", () => {
  it("redelivers a lost poll response after eviction without waiting for the active lease", async () => {
    const { stub, request } = await registry()
    const lost = await poll(stub)
    expect(await lost.json()).toEqual({ requests: [request] })
    const staleReceipt = lost.headers.get("x-orbit-fill-lease")!
    expect(await (await poll(stub)).json()).toEqual({ requests: [] })
    await evictDurableObject(stub)
    await sleep(5_100)
    const retry = await poll(stub)
    expect(await retry.json()).toEqual({ requests: [request] })
    expect(retry.headers.get("x-orbit-fill-lease")).not.toBe(staleReceipt)
    // A delayed acknowledgement from the discarded delivery cannot extend the new offer.
    await stub.fetch("https://registry/claim", {
      method: "POST",
      headers: { "x-orbit-fill-lease": staleReceipt },
    })
    await sleep(5_100)
    expect(await (await poll(stub)).json()).toEqual({ requests: [request] })
  }, 15_000)

  it("keeps acknowledged work leased across eviction and accepts duplicate acknowledgements", async () => {
    const { stub } = await registry()
    const response = await poll(stub)
    await response.json()
    const headers = { "x-orbit-fill-lease": response.headers.get("x-orbit-fill-lease")! }
    for (let i = 0; i < 2; i++)
      expect((await stub.fetch("https://registry/claim", { method: "POST", headers })).status).toBe(
        204,
      )
    await evictDurableObject(stub)
    await sleep(5_100)
    expect(await (await poll(stub)).json()).toEqual({ requests: [] })
  }, 10_000)

  it("preserves old pollers and limits offers to available execution capacity", async () => {
    const { stub, request } = await registry()
    await stub.fetch("https://registry/enqueue", {
      method: "POST",
      body: JSON.stringify({ ...request, fill_id: "org:second" }),
    })
    const response = await poll(stub, "&limit=1")
    expect(response.headers.get("x-orbit-fill-lease")).toBeNull()
    expect(((await response.json()) as { requests: unknown[] }).requests).toHaveLength(1)
    expect(((await (await poll(stub)).json()) as { requests: unknown[] }).requests).toHaveLength(1)
    await sleep(5_100)
    const retry = (await (await poll(stub)).json()) as { requests: Array<{ fill_id: string }> }
    expect(retry.requests).toHaveLength(1)
    expect(retry.requests[0]!.fill_id).toBe("org:second")
  }, 10_000)
})
