/**
 * MySQL GTID set containment, the one GTID operation the Durable Object needs:
 * "is transaction `uuid:gno` already reflected in fill position `MySQL56/...`?"
 * Mirrors `orbit-gtid` in Rust; the fixtures in `test/core/gtid.test.ts` pin the semantics.
 */

export interface ParsedGtidSet {
  readonly intervals: ReadonlyMap<string, ReadonlyArray<readonly [number, number]>>
}

export class GtidParseError extends Error {
  constructor(
    readonly input: string,
    reason: string,
  ) {
    super(`invalid GTID set ${JSON.stringify(input)}: ${reason}`)
    this.name = "GtidParseError"
  }
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

export const parseGtidSet = (position: string): ParsedGtidSet => {
  const body = position.startsWith("MySQL56/") ? position.slice("MySQL56/".length) : position
  const intervals = new Map<string, Array<readonly [number, number]>>()
  if (body.trim() === "") return { intervals }
  for (const part of body.split(",")) {
    const [uuidRaw, ...ranges] = part.trim().split(":")
    const uuid = (uuidRaw ?? "").toLowerCase()
    if (!UUID.test(uuid)) throw new GtidParseError(position, `bad uuid ${uuidRaw}`)
    if (ranges.length === 0) throw new GtidParseError(position, "uuid without intervals")
    const list = intervals.get(uuid) ?? []
    for (const r of ranges) {
      const [a, b] = r.includes("-") ? r.split("-") : [r, r]
      const start = Number(a)
      const end = Number(b)
      if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 1 || end < start)
        throw new GtidParseError(position, `bad interval ${r}`)
      list.push([start, end])
    }
    intervals.set(uuid, list)
  }
  return { intervals }
}

/** `gtid` is `uuid:gno` or a rendered set like `uuid:5-7` (merged steps); all ids must be contained. */
export const gtidSetContains = (set: ParsedGtidSet, gtid: string): boolean => {
  const parsed = parseGtidSet(gtid)
  for (const [uuid, ranges] of parsed.intervals) {
    const have = set.intervals.get(uuid)
    if (have === undefined) return false
    for (const [s, e] of ranges) {
      if (!have.some(([hs, he]) => hs <= s && e <= he)) return false
    }
  }
  return true
}
