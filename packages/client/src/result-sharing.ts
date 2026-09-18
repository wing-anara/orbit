import type { ResultNode } from "@orbit/query"

const equalValue = (a: unknown, b: unknown): boolean => {
  if (Object.is(a, b)) return true
  if (a === null || b === null || typeof a !== "object" || typeof b !== "object") return false
  if (Array.isArray(a) !== Array.isArray(b)) return false
  const left = a as Record<string, unknown>,
    right = b as Record<string, unknown>
  const keys = Object.keys(left)
  return (
    keys.length === Object.keys(right).length &&
    keys.every((key) => Object.hasOwn(right, key) && equalValue(left[key], right[key]))
  )
}

/** Keep unchanged query rows stable through fresh SQLite reads, including their relations. */
export const shareResultRows = (
  previous: ReadonlyArray<ResultNode>,
  next: ReadonlyArray<ResultNode>,
): ReadonlyArray<ResultNode> => {
  if (previous === next) return previous
  const byKey = new Map(previous.map((node) => [node.key, node]))
  const shared = next.map((node) => {
    const before = byKey.get(node.key)
    return before !== undefined && equalValue(before, node) ? before : node
  })
  return previous.length === shared.length && shared.every((node, i) => node === previous[i])
    ? previous
    : shared
}
