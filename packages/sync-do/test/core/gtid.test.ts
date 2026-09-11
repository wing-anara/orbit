import { describe, expect, it } from "vitest"

import { gtidSetContains, parseGtidSet } from "../../src/core/gtid.ts"

const U1 = "a2523813-adbe-11f1-b19c-0a2250a7ed6c"
const U2 = "1d3d5b28-0f4e-11ef-9d5b-0242ac120002"

describe("gtid set containment", () => {
  it("matches the Rust semantics", () => {
    const set = parseGtidSet(`MySQL56/${U1}:1-5:7,${U2}:1-2`)
    expect(gtidSetContains(set, `${U1}:5`)).toBe(true)
    expect(gtidSetContains(set, `${U1}:6`)).toBe(false)
    expect(gtidSetContains(set, `${U1}:7`)).toBe(true)
    expect(gtidSetContains(set, `${U2}:2`)).toBe(true)
    expect(gtidSetContains(set, `${U2}:3`)).toBe(false)
    expect(gtidSetContains(set, `${U1}:3-5`)).toBe(true)
    expect(gtidSetContains(set, `${U1}:4-6`)).toBe(false)
    expect(gtidSetContains(parseGtidSet(""), `${U1}:1`)).toBe(false)
  })

  it("rejects malformed input", () => {
    expect(() => parseGtidSet("MySQL56/garbage")).toThrow(/invalid GTID set/)
    expect(() => parseGtidSet(`${U1}:5-3`)).toThrow()
    expect(() => parseGtidSet(U1)).toThrow()
  })
})
