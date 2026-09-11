/**
 * Incremental maintenance of one subscription's membership after a source transaction.
 *
 * A subscription is a tree of levels: the primary level (path `""`) and one level per include
 * at any depth (path `folder`, `tags/tag`, ...). Each level owns a set of rows in the
 * `membership` table. The invariant maintained here, for every level, is:
 *
 * * primary level: the first `limit` rows (all rows without a limit) of the primary table that
 *   satisfy the predicate, in the query's order;
 * * include level: the rows of its target table that satisfy the include filter and are
 *   referenced through the relation by at least one row of the parent level.
 *
 * The work per transaction depends on the rows the transaction touched, never on the size of
 * the cache: candidates come from the changed rows themselves and from indexed walks along the
 * declared relations. The two exceptions are a limited query whose window lost a row (the next
 * candidate is searched with a bounded `LIMIT`) and a member whose sort key changed (one
 * comparison with the best non-member). Both read the primary table in the query's order.
 *
 * `docs/ivm.md` describes the algorithm; the randomized test in `test/core/engine.test.ts` checks it against a full
 * recomputation over random histories.
 */

import type { Predicate, RelationSchema, RowChange, TableSchema } from "@orbit/protocol"
import type { MemberRef, RowImage } from "@orbit/protocol/client"
import {
  compileChainCandidates,
  compileRowsByColumns,
  compileWhere,
  flattenIncludes,
  keyOfRecord,
  paramFor,
  parseKey,
  placeholderFor,
  predicateChains,
  primaryOrderSql,
  primarySelectList,
  rowFromRecord,
  selectByKeySql,
  type PlannedQuery,
  type SqlValue,
} from "@orbit/query"
import { KEY_COLUMN, localTableName, quoteIdent, SchemaRuntime } from "@orbit/schema"

import type { SqlDriver, SqlRecord } from "./driver.ts"

/** Bound parameters per statement the engine allows itself (Durable Objects permit 100). */
export const MAX_BOUND_PARAMS = 90

export interface Level {
  readonly path: string
  readonly table: TableSchema
  readonly parent: Level | null
  /** Relation from the parent level's table to this level's table (null for the primary). */
  readonly relation: RelationSchema | null
  readonly where: Predicate | undefined
  readonly children: Array<Level>
}

export const pathOf = (path: ReadonlyArray<string>): string => path.join("/")

/** The level tree of a planned query, parents before children in `all`. */
export const levelsOf = (
  planned: PlannedQuery,
): { readonly root: Level; readonly all: ReadonlyArray<Level> } => {
  const root: Level = {
    path: "",
    table: planned.table,
    parent: null,
    relation: null,
    where: planned.query.where,
    children: [],
  }
  const byPath = new Map<string, Level>([["", root]])
  const all: Array<Level> = [root]
  for (const inc of flattenIncludes(planned)) {
    const parent = byPath.get(pathOf(inc.path.slice(0, -1)))
    if (parent === undefined)
      throw new Error(`planner invariant violated: include path ${inc.path.join("/")}`)
    const level: Level = {
      path: pathOf(inc.path),
      table: inc.target,
      parent,
      relation: inc.relation,
      where: inc.where,
      children: [],
    }
    parent.children.push(level)
    byPath.set(level.path, level)
    all.push(level)
  }
  return { root, all }
}

export interface MaintenanceOutcome {
  /** Rows that entered the subscription (no path held them before, at least one does now). */
  readonly added: ReadonlyArray<MemberRef>
  /** Rows that left the subscription (some path held them before, none does now). */
  readonly removed: ReadonlyArray<MemberRef>
}

const asString = (v: SqlValue | undefined): string =>
  typeof v === "string" ? v : v === null || v === undefined ? "" : String(v)

const sameValues = (columns: ReadonlyArray<string>, a: RowImage, b: RowImage): boolean =>
  columns.every((c) => JSON.stringify(a[c] ?? null) === JSON.stringify(b[c] ?? null))

/** Table and key joined with NUL, which cannot occur unescaped in JSON text. */
const NUL = String.fromCharCode(0)
const refOf = (table: string, key: string): string => [table, key].join(NUL)
const splitRef = (ref: string): MemberRef => {
  const i = ref.indexOf(NUL)
  return { table: ref.slice(0, i), key: parseKey(ref.slice(i + 1)) }
}

const t = (table: TableSchema): string => quoteIdent(localTableName(table.name))
const membersOf = `(SELECT key FROM membership WHERE subscription = ? AND path = ? AND tbl = ?)`

/** Maintains one subscription for one transaction. Not reusable across transactions. */
export class SubscriptionMaintainer {
  private readonly root: Level
  private readonly levels: ReadonlyArray<Level>
  private readonly chains: ReadonlyArray<ReadonlyArray<RelationSchema>>
  /** Membership (any path) of every row this transaction touched, as it was before. */
  private readonly before = new Map<string, boolean>()
  private rootCount: number | null = null

  constructor(
    private readonly db: SqlDriver,
    private readonly rt: SchemaRuntime,
    private readonly id: string,
    private readonly planned: PlannedQuery,
  ) {
    const levels = levelsOf(planned)
    this.root = levels.root
    this.levels = levels.all
    this.chains = predicateChains(planned)
  }

  apply(touched: ReadonlyMap<string, ReadonlyArray<RowChange>>): MaintenanceOutcome {
    this.maintainPrimary(touched)
    for (const level of this.levels) {
      if (level.parent === null) continue
      for (const change of touched.get(level.table.name) ?? []) this.directChange(level, change)
    }
    const added: Array<MemberRef> = []
    const removed: Array<MemberRef> = []
    for (const [ref, was] of this.before) {
      const { table, key } = splitRef(ref)
      const now = this.hasAnyPath(table, SchemaRuntime.keyString(key))
      if (now && !was) added.push({ table, key })
      if (was && !now) removed.push({ table, key })
    }
    return { added, removed }
  }

  // ---------------------------------------------------------------------------------------------
  // Primary level
  // ---------------------------------------------------------------------------------------------

  private maintainPrimary(touched: ReadonlyMap<string, ReadonlyArray<RowChange>>): void {
    const root = this.root
    const limit = this.planned.limit
    interface Candidate {
      readonly image: RowImage | null
      readonly before: RowImage | null
      readonly direct: boolean
    }
    const candidates = new Map<string, Candidate>()
    for (const c of touched.get(root.table.name) ?? []) {
      const key = SchemaRuntime.keyString(c.key)
      // A row changed several times in one transaction collapses to one candidate whose
      // `before` is the image at the start of the transaction: that is the image the include
      // levels were derived from the last time this subscription was maintained.
      const prev = candidates.get(key)
      const before = prev !== undefined && prev.direct ? prev.before : (c.before ?? null)
      const after = c.op === "delete" ? null : (c.after ?? null)
      candidates.set(key, { image: after, before, direct: true })
      if (c.op === "update" && c.before !== undefined && c.before !== null) {
        // A primary key update: the old identity disappears.
        const oldKey = SchemaRuntime.keyString(this.rt.keyOf(root.table, c.before))
        if (oldKey !== key) candidates.set(oldKey, { image: null, before, direct: true })
      }
    }
    for (const chain of this.chains) {
      const last = chain[chain.length - 1]
      if (last === undefined) continue
      const changes = touched.get(last.target_table)
      if (changes === undefined) continue
      for (const c of changes) {
        for (const image of [c.before, c.after]) {
          if (image === undefined || image === null) continue
          const q = compileChainCandidates(this.planned, chain, image)
          for (const record of this.db.query(q.sql, q.params)) {
            const key = keyOfRecord(record)
            if (!candidates.has(key))
              candidates.set(key, {
                image: rowFromRecord(root.table, record),
                before: null,
                direct: false,
              })
          }
        }
      }
    }
    if (candidates.size === 0) return
    const keys = [...candidates.keys()]
    const members = this.memberKeys(root, keys)
    const matching = this.matchingKeys(
      root,
      keys.filter((k) => candidates.get(k)?.image !== null),
    )
    const toAdd: Array<{ key: string; image: RowImage }> = []
    const toRemove: Array<{ key: string; image: RowImage | null }> = []
    const updated: Array<{ before: RowImage; after: RowImage }> = []
    for (const [key, c] of candidates) {
      const matches = c.image !== null && matching.has(key)
      const isMember = members.has(key)
      if (matches && !isMember && c.image !== null) toAdd.push({ key, image: c.image })
      // Children of a leaving row were derived from its image at the start of the transaction.
      else if (!matches && isMember) toRemove.push({ key, image: c.direct ? c.before : c.image })
      else if (matches && isMember && c.direct && c.before !== null && c.image !== null)
        updated.push({ before: c.before, after: c.image })
    }
    // Order matters: every reference check consults the current membership, so rows that leave
    // go first, then the children of rows that stayed are re-derived, then rows enter. Free
    // places are filled from every non-member in order (rows that started to match in this
    // transaction compete there too); remaining candidates then compete with the last member.
    for (const r of toRemove) this.remove(root, r.key, r.image)
    if (limit !== undefined && toRemove.length > 0 && this.count() < limit) this.refill(limit)
    let orderChanged = false
    const orderColumns = this.planned.orderBy.map((o) => o.column)
    for (const u of updated) {
      this.refreshChildren(root, u.before, u.after)
      if (limit !== undefined && !sameValues(orderColumns, u.before, u.after)) orderChanged = true
    }
    for (const a of toAdd) this.addPrimary(a.key, a.image)
    if (limit !== undefined && orderChanged && this.count() >= limit) this.rebalance()
  }

  /** Adds a matching primary row; with a full window, only when it precedes the last member. */
  private addPrimary(key: string, image: RowImage): void {
    const limit = this.planned.limit
    if (this.hasPath(this.root, key)) return
    if (limit === undefined || this.count() < limit) {
      this.insert(this.root, key)
      this.cascadeAdd(this.root, image)
      return
    }
    const last = this.db.query(
      `SELECT t.${quoteIdent(KEY_COLUMN)} AS k FROM ${t(this.root.table)} t WHERE (t.${quoteIdent(KEY_COLUMN)} IN ${membersOf} OR t.${quoteIdent(KEY_COLUMN)} = ?) ORDER BY ${primaryOrderSql(this.planned, true)} LIMIT 1`,
      [this.id, "", this.root.table.name, key],
    )[0]
    const lastKey = asString(last?.["k"])
    if (lastKey === key) return
    this.insert(this.root, key)
    this.cascadeAdd(this.root, image)
    this.remove(this.root, lastKey, this.imageOf(this.root.table, lastKey))
  }

  /** After removals from a full window: the next rows in order fill the free places. */
  private refill(limit: number): void {
    const where = compileWhere(this.planned, this.root.table, this.root.where, "t")
    const rows = this.db.query(
      `SELECT ${primarySelectList(this.planned)} FROM ${t(this.root.table)} t WHERE ${where.sql} AND t.${quoteIdent(KEY_COLUMN)} NOT IN ${membersOf} ORDER BY ${primaryOrderSql(this.planned)} LIMIT ?`,
      [...where.params, this.id, "", this.root.table.name, limit - this.count()],
    )
    for (const record of rows) {
      this.insert(this.root, keyOfRecord(record))
      this.cascadeAdd(this.root, rowFromRecord(this.root.table, record))
    }
  }

  /**
   * A member's sort key changed while the window is full: it may now sort after the best
   * non-member, which then takes its place. Repeats until the window is the true prefix again.
   */
  private rebalance(): void {
    const where = compileWhere(this.planned, this.root.table, this.root.where, "t")
    const k = quoteIdent(KEY_COLUMN)
    for (let i = 0; i < (this.planned.limit ?? 0) + 1; i++) {
      const best = this.db.query(
        `SELECT ${primarySelectList(this.planned)} FROM ${t(this.root.table)} t WHERE ${where.sql} AND t.${k} NOT IN ${membersOf} ORDER BY ${primaryOrderSql(this.planned)} LIMIT 1`,
        [...where.params, this.id, "", this.root.table.name],
      )[0]
      if (best === undefined) return
      const bestKey = keyOfRecord(best)
      const last = asString(
        this.db.query(
          `SELECT t.${k} AS k FROM ${t(this.root.table)} t WHERE t.${k} IN ${membersOf} ORDER BY ${primaryOrderSql(this.planned, true)} LIMIT 1`,
          [this.id, "", this.root.table.name],
        )[0]?.["k"],
      )
      const first = asString(
        this.db.query(
          `SELECT t.${k} AS k FROM ${t(this.root.table)} t WHERE t.${k} IN (?, ?) ORDER BY ${primaryOrderSql(this.planned)} LIMIT 1`,
          [bestKey, last],
        )[0]?.["k"],
      )
      if (first !== bestKey) return
      this.remove(this.root, last, this.imageOf(this.root.table, last))
      this.insert(this.root, bestKey)
      this.cascadeAdd(this.root, rowFromRecord(this.root.table, best))
    }
  }

  private count(): number {
    if (this.rootCount === null) {
      const n = this.db.query(
        `SELECT COUNT(*) AS n FROM membership WHERE subscription = ? AND path = ? AND tbl = ?`,
        [this.id, "", this.root.table.name],
      )[0]?.["n"]
      this.rootCount = typeof n === "number" ? n : Number(n ?? 0)
    }
    return this.rootCount
  }

  // ---------------------------------------------------------------------------------------------
  // Include levels
  // ---------------------------------------------------------------------------------------------

  /** A change to a row of an include level's table, independent of parent-level changes. */
  private directChange(level: Level, change: RowChange): void {
    const key = SchemaRuntime.keyString(change.key)
    const before = change.before ?? null
    const after = change.op === "delete" ? null : (change.after ?? null)
    if (change.op === "update" && before !== null) {
      const oldKey = SchemaRuntime.keyString(this.rt.keyOf(level.table, before))
      if (oldKey !== key && this.hasPath(level, oldKey)) {
        this.delete(level, oldKey)
        this.cascadeRemove(level, before)
      }
    }
    const inLevel = this.hasPath(level, key)
    const shouldBe =
      after !== null &&
      this.matchingKeys(level, [key]).has(key) &&
      this.parentReferences(level, after)
    if (shouldBe && !inLevel && after !== null) {
      this.insert(level, key)
      this.cascadeAdd(level, after)
    } else if (!shouldBe && inLevel) {
      this.delete(level, key)
      const image = before ?? after
      if (image !== null) this.cascadeRemove(level, image)
    } else if (shouldBe && inLevel && before !== null && after !== null) {
      this.refreshChildren(level, before, after)
    }
  }

  /** Does a row of the parent level reference `image` through the level's relation? */
  private parentReferences(level: Level, image: RowImage): boolean {
    const parent = level.parent
    const relation = level.relation
    if (parent === null || relation === null) return true
    const conds = relation.from_columns.map(
      (from) => `p.${quoteIdent(from)} = ${placeholderFor(parent.table, from)}`,
    )
    const params = relation.from_columns.map((from, i) =>
      paramFor(parent.table, from, image[relation.to_columns[i] ?? ""]),
    )
    // The parent table is the outer loop (index on its relation columns, bounded by the fan-in of
    // the value) and the membership is probed by primary key. A join would let the planner scan
    // the whole membership of a large subscription instead.
    const row = this.db.query(
      `SELECT 1 AS x FROM ${t(parent.table)} p WHERE ${conds.join(" AND ")} AND EXISTS (SELECT 1 FROM membership m WHERE m.subscription = ? AND m.path = ? AND m.tbl = ? AND m.key = p.${quoteIdent(KEY_COLUMN)}) LIMIT 1`,
      [...params, this.id, parent.path, parent.table.name],
    )[0]
    return row !== undefined
  }

  /** Rows of `child` referenced by a parent image, filtered by the include's `where`. */
  private childRows(child: Level, parentImage: RowImage): ReadonlyArray<SqlRecord> {
    const relation = child.relation
    if (relation === null) return []
    const values = relation.from_columns.map((from) => parentImage[from] ?? null)
    const q = compileRowsByColumns(
      this.planned,
      child.table,
      relation.to_columns,
      values,
      child.where,
    )
    return this.db.query(q.sql, q.params)
  }

  /** The parent row `image` entered `level`: its referenced rows enter the child levels. */
  private cascadeAdd(level: Level, image: RowImage): void {
    for (const child of level.children) {
      for (const record of this.childRows(child, image)) {
        const key = keyOfRecord(record)
        if (this.hasPath(child, key)) continue
        this.insert(child, key)
        this.cascadeAdd(child, rowFromRecord(child.table, record))
      }
    }
  }

  /** The parent row `image` left `level`: rows no other parent references leave the children. */
  private cascadeRemove(level: Level, image: RowImage): void {
    for (const child of level.children) {
      for (const record of this.childRows(child, image)) {
        const key = keyOfRecord(record)
        if (!this.hasPath(child, key)) continue
        const childImage = rowFromRecord(child.table, record)
        if (this.parentReferences(child, childImage)) continue
        this.delete(child, key)
        this.cascadeRemove(child, childImage)
      }
    }
  }

  /** A member row changed: children whose join columns moved are re-derived. */
  private refreshChildren(level: Level, before: RowImage, after: RowImage): void {
    for (const child of level.children) {
      const relation = child.relation
      if (relation === null || sameValues(relation.from_columns, before, after)) continue
      const next = new Set(this.childRows(child, after).map(keyOfRecord))
      for (const record of this.childRows(child, before)) {
        const key = keyOfRecord(record)
        if (next.has(key) || !this.hasPath(child, key)) continue
        const childImage = rowFromRecord(child.table, record)
        if (this.parentReferences(child, childImage)) continue
        this.delete(child, key)
        this.cascadeRemove(child, childImage)
      }
      for (const record of this.childRows(child, after)) {
        const key = keyOfRecord(record)
        if (this.hasPath(child, key)) continue
        this.insert(child, key)
        this.cascadeAdd(child, rowFromRecord(child.table, record))
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Membership rows
  // ---------------------------------------------------------------------------------------------

  private remove(level: Level, key: string, image: RowImage | null): void {
    this.delete(level, key)
    if (image !== null) this.cascadeRemove(level, image)
  }

  /** Adds the row to the level; callers check `hasPath` first so every insert is a real write. */
  private insert(level: Level, key: string): void {
    this.noteBefore(level.table.name, key)
    this.db.run(`INSERT INTO membership (subscription, path, tbl, key) VALUES (?, ?, ?, ?)`, [
      this.id,
      level.path,
      level.table.name,
      key,
    ])
    if (level === this.root && this.rootCount !== null) this.rootCount += 1
  }

  /** Removes the row from the level; callers check `hasPath` first so every delete is real. */
  private delete(level: Level, key: string): void {
    this.noteBefore(level.table.name, key)
    this.db.run(
      `DELETE FROM membership WHERE subscription = ? AND path = ? AND tbl = ? AND key = ?`,
      [this.id, level.path, level.table.name, key],
    )
    if (level === this.root && this.rootCount !== null) this.rootCount -= 1
  }

  /** Records whether the row was a member (any path) before its first modification. */
  private noteBefore(table: string, key: string): void {
    const ref = refOf(table, key)
    if (this.before.has(ref)) return
    this.before.set(ref, this.hasAnyPath(table, key))
  }

  /** Any path of this subscription holds the row: a probe of the `(tbl, key, subscription)` index. */
  private hasAnyPath(table: string, key: string): boolean {
    return (
      this.db.query(
        `SELECT 1 AS x FROM membership WHERE tbl = ? AND key = ? AND subscription = ? LIMIT 1`,
        [table, key, this.id],
      ).length > 0
    )
  }

  private hasPath(level: Level, key: string): boolean {
    return (
      this.db.query(
        `SELECT 1 AS x FROM membership WHERE subscription = ? AND path = ? AND tbl = ? AND key = ?`,
        [this.id, level.path, level.table.name, key],
      ).length > 0
    )
  }

  /** Keys among `keys` that the level holds. */
  private memberKeys(level: Level, keys: ReadonlyArray<string>): Set<string> {
    const out = new Set<string>()
    const chunk = MAX_BOUND_PARAMS - 3
    for (let i = 0; i < keys.length; i += chunk) {
      const part = keys.slice(i, i + chunk)
      for (const row of this.db.query(
        `SELECT key FROM membership WHERE subscription = ? AND path = ? AND tbl = ? AND key IN (${part.map(() => "?").join(", ")})`,
        [this.id, level.path, level.table.name, ...part],
      ))
        out.add(asString(row["key"]))
    }
    return out
  }

  /** Keys among `keys` whose current cache row satisfies the level's predicate. */
  private matchingKeys(level: Level, keys: ReadonlyArray<string>): Set<string> {
    const out = new Set<string>()
    if (keys.length === 0) return out
    const where = compileWhere(this.planned, level.table, level.where, "t")
    const chunk = Math.max(1, MAX_BOUND_PARAMS - where.params.length)
    for (let i = 0; i < keys.length; i += chunk) {
      const part = keys.slice(i, i + chunk)
      for (const row of this.db.query(
        `SELECT t.${quoteIdent(KEY_COLUMN)} AS k FROM ${t(level.table)} t WHERE t.${quoteIdent(KEY_COLUMN)} IN (${part.map(() => "?").join(", ")}) AND ${where.sql}`,
        [...part, ...where.params],
      ))
        out.add(asString(row["k"]))
    }
    return out
  }

  private imageOf(table: TableSchema, key: string): RowImage | null {
    const record = this.db.query(selectByKeySql(table), [key])[0]
    return record === undefined ? null : rowFromRecord(table, record)
  }
}
