import { describe, expect, it } from "vitest"
import type { ResultNode } from "@orbit/query"
import { shareResultRows } from "../src/result-sharing.ts"
const row = (id: string, name = id): ResultNode => ({ key: id, row: { id, name }, related: {} })
describe("query result sharing", () => {
  it("retains an unchanged view and retained rows when another row is deleted", () => {
    const original = [row("a"), row("b")]
    expect(shareResultRows(original, [row("a"), row("b")])).toBe(original)
    const deleted = shareResultRows(original, [row("b")])
    expect(deleted).not.toBe(original)
    expect(deleted[0]).toBe(original[1])
  })
  it("does not hide changed fields, reordered rows or relation changes", () => {
    const original = [{ ...row("a"), related: { children: [row("child")] } }, row("b")]
    const changed = shareResultRows(original, [
      { ...row("a"), related: { children: [row("child", "edited")] } },
      row("b"),
    ])
    expect(changed[0]).not.toBe(original[0])
    expect(changed[1]).toBe(original[1])
    expect(shareResultRows(original, [...original].reverse())).toEqual([...original].reverse())
    expect(shareResultRows([row("a")], [row("a", "new")])[0]?.row["name"]).toBe("new")
  })
})
