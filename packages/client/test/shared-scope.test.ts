import { describe, expect, it } from "vitest"
import { sharedScope } from "../src/shared/platform.ts"

describe("shared cache identity", () => {
  const identity = {
    url: "https://sync.example/orbit",
    app: "app",
    partition: "org",
    subject: "user",
  }
  it("normalizes the server URL and isolates every identity dimension", async () => {
    const original = await sharedScope(identity)
    expect(await sharedScope({ ...identity, url: `${identity.url}/` })).toBe(original)
    for (const change of [
      { url: "https://other.example/orbit" },
      { app: "other" },
      { partition: "other" },
      { subject: "other" },
      { databaseName: "other" },
    ])
      expect(await sharedScope({ ...identity, ...change })).not.toBe(original)
  })
  it("never puts credentials in a cache identity", async () => {
    for (const url of [
      "https://user:pass@sync.example/orbit",
      "https://sync.example/orbit?token=secret",
      "https://sync.example/orbit#secret",
    ])
      await expect(sharedScope({ ...identity, url })).rejects.toThrow("must not contain")
  })
})
