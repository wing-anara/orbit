/**
 * SQL compilation of planned queries and of predicates over literal row images.
 *
 * Two dialects share one compiler:
 * * `sqlite`: the local caches. The Durable Object and the browser store rows in identical
 *   local tables (see `@orbit/schema` `ddl.ts`), addressed by `__key`.
 * * `mysql`: the authoritative database, used by server-side mutators through the application's
 *   own connection. Rows are addressed by their primary key columns.
 *
 * Every statement is parameterized. Relation predicates (`exists`) and nested includes compile to
 * correlated subqueries over the declared relation columns.
 */

import type {
  ColumnSchema,
  ComparisonOp,
  Predicate,
  Scalar,
  TableSchema,
  ValueKind,
  RelationSchema,
} from "@orbit/protocol"
import type { RowImage } from "@orbit/protocol/client"
import { KEY_COLUMN, localTableName, quoteIdent } from "@orbit/schema"

import type { PlannedInclude, PlannedQuery } from "./plan.ts"

export type SqlParam = string | number | null

const COMPARISON_SQL: Record<ComparisonOp, string> = {
  eq: "=",
  ne: "<>",
  lt: "<",
  lte: "<=",
  gt: ">",
  gte: ">=",
}

export interface CompiledSql {
  readonly sql: string
  readonly params: ReadonlyArray<SqlParam>
}

/** How a predicate literal is bound for a column kind (booleans become 0/1). */
export const bindScalar = (kind: ValueKind, value: Scalar): SqlParam => {
  if (kind === "bool") return value === true ? 1 : 0
  if (kind === "bigint" && typeof value === "string") {
    const n = Number(value)
    return Number.isSafeInteger(n) ? n : value
  }
  return typeof value === "boolean" ? (value ? 1 : 0) : value
}

export interface Dialect {
  readonly name: "sqlite" | "mysql"
  readonly ident: (name: string) => string
  /** Full reference to a synced table. */
  readonly table: (table: string) => string
  /**
   * Placeholder with the column's affinity applied. Bare parameters have no affinity in SQLite,
   * so a bigint literal bound as text would compare as TEXT against an INTEGER. Casting reproduces
   * column-affinity semantics for literals and for row images evaluated without a table.
   */
  readonly placeholder: (kind: ValueKind) => string
  /** Whether tables carry the engine's `__key` column. */
  readonly hasKeyColumn: boolean
}

const sqlitePlaceholder = (kind: ValueKind): string => {
  switch (kind) {
    case "bool":
    case "int":
    case "bigint":
      return "CAST(? AS INTEGER)"
    case "float":
      return "CAST(? AS REAL)"
    default:
      return "?"
  }
}

/** The local cache dialect. `tableRef` lets a caller substitute views for the cache tables. */
export const sqliteDialect = (tableRef?: (table: string) => string): Dialect => ({
  name: "sqlite",
  ident: quoteIdent,
  table: tableRef ?? ((table) => quoteIdent(localTableName(table))),
  placeholder: sqlitePlaceholder,
  hasKeyColumn: true,
})

const mysqlIdent = (name: string): string => `\`${name.replaceAll("`", "``")}\``

export const mysqlDialect: Dialect = {
  name: "mysql",
  ident: mysqlIdent,
  table: mysqlIdent,
  placeholder: () => "?",
  hasKeyColumn: false,
}

export const SQLITE = sqliteDialect()

const kindOf = (table: TableSchema, column: string): ValueKind => {
  const col = table.columns.find((c) => c.name === column)
  if (col === undefined)
    throw new Error(`planner invariant violated: unknown column ${table.name}.${column}`)
  return col.kind
}

interface Ctx {
  readonly planned: PlannedQuery
  readonly dialect: Dialect
  readonly params: Array<SqlParam>
  aliases: number
}

const targetOf = (ctx: Ctx, table: TableSchema, relationName: string): RelationSchema => {
  const relation = table.relations.find((r) => r.name === relationName)
  if (relation === undefined)
    throw new Error(`planner invariant violated: unknown relation ${table.name}.${relationName}`)
  return relation
}

const tableOf = (ctx: Ctx, name: string): TableSchema => {
  const table = ctx.planned.rt.table(name)
  if (table === undefined) throw new Error(`planner invariant violated: unknown table ${name}`)
  return table
}

/** `parent.from = child.to AND ...` for a relation; `parentRef` renders the parent side. */
const joinCondition = (
  ctx: Ctx,
  relation: RelationSchema,
  parentRef: (column: string) => string,
  childAlias: string,
): string =>
  relation.from_columns
    .map(
      (from, i) =>
        `${parentRef(from)} = ${childAlias}.${ctx.dialect.ident(relation.to_columns[i] ?? "")}`,
    )
    .join(" AND ")

/** Compiles a predicate where `columnRef` renders a column reference of `table`. */
const compilePredicate = (
  ctx: Ctx,
  table: TableSchema,
  p: Predicate,
  columnRef: (column: string) => string,
): string => {
  const { dialect, params } = ctx
  switch (p.op) {
    case "and":
      return p.args.length === 0
        ? "1"
        : `(${p.args.map((a) => compilePredicate(ctx, table, a, columnRef)).join(" AND ")})`
    case "or":
      return p.args.length === 0
        ? "0"
        : `(${p.args.map((a) => compilePredicate(ctx, table, a, columnRef)).join(" OR ")})`
    case "not":
      return `(NOT ${compilePredicate(ctx, table, p.arg, columnRef)})`
    case "isNull":
      return `(${columnRef(p.column)} IS NULL)`
    case "isNotNull":
      return `(${columnRef(p.column)} IS NOT NULL)`
    case "exists": {
      const relation = targetOf(ctx, table, p.relation)
      const target = tableOf(ctx, relation.target_table)
      const alias = `r${ctx.aliases++}`
      // The join renders the outer columns first: with predicate-over-images the outer
      // reference is itself a parameter, and parameter order must follow the text.
      const join = joinCondition(ctx, relation, columnRef, alias)
      const nested =
        p.where === undefined
          ? ""
          : ` AND ${compilePredicate(ctx, target, p.where, (c) => `${alias}.${dialect.ident(c)}`)}`
      return `EXISTS (SELECT 1 FROM ${dialect.table(target.name)} ${alias} WHERE ${join}${nested})`
    }
    // `columnRef` may itself push a parameter (predicate-over-images), so it must run before the
    // literal is pushed: parameter order has to follow placeholder order in the text.
    case "in": {
      const kind = kindOf(table, p.column)
      const ref = columnRef(p.column)
      for (const v of p.values) params.push(bindScalar(kind, v))
      return `(${ref} IN (${p.values.map(() => dialect.placeholder(kind)).join(", ")}))`
    }
    case "like": {
      const ref = columnRef(p.column)
      params.push(p.pattern)
      return `(${ref} LIKE ?)`
    }
    default: {
      const kind = kindOf(table, p.column)
      const ref = columnRef(p.column)
      params.push(bindScalar(kind, p.value))
      return `(${ref} ${COMPARISON_SQL[p.op]} ${dialect.placeholder(kind)})`
    }
  }
}

const mysqlString = (text: string): string =>
  `'${text.replaceAll("\\", "\\\\").replaceAll("'", "''")}'`

/**
 * The expression that computes a derived column from its source column in MySQL, where only the
 * source column exists. Null for plain columns and for the local cache, which stores the value.
 */
export const derivedColumnSql = (
  dialect: Dialect,
  column: ColumnSchema,
  alias: string | null,
): string | null => {
  const derived = column.derived
  if (derived === undefined || derived === null || dialect.name !== "mysql") return null
  const src =
    alias === null ? dialect.ident(derived.from) : `${alias}.${dialect.ident(derived.from)}`
  switch (derived.rule.kind) {
    case "not_null":
      return `(${src} IS NOT NULL)`
    case "starts_with": {
      const prefix = derived.rule.prefix
      return `(${src} IS NOT NULL AND LEFT(${src}, ${Array.from(prefix).length}) = ${mysqlString(prefix)})`
    }
  }
}

/** Select list that returns wire-shaped values: bigint as text, everything else raw. */
const selectList = (dialect: Dialect, table: TableSchema, alias: string): string => {
  const cols = table.columns.map((c) => {
    const derived = derivedColumnSql(dialect, c, alias)
    if (derived !== null) return `${derived} AS ${dialect.ident(c.name)}`
    return c.kind === "bigint" && dialect.name === "sqlite"
      ? `CAST(${alias}.${dialect.ident(c.name)} AS TEXT) AS ${dialect.ident(c.name)}`
      : `${alias}.${dialect.ident(c.name)}`
  })
  if (dialect.hasKeyColumn)
    cols.unshift(`${alias}.${dialect.ident(KEY_COLUMN)} AS ${dialect.ident(KEY_COLUMN)}`)
  return cols.join(", ")
}

/**
 * Text columns order case-insensitively in the cache, as MySQL's default collation does, so a
 * window and the server agree on which rows are first. `NOCASE` folds ASCII only; accents
 * still order by code point.
 */
const orderClause = (dialect: Dialect, planned: PlannedQuery, alias: string): string =>
  planned.orderBy
    .map((o) => {
      const collate =
        dialect.name === "sqlite" && kindOf(planned.table, o.column) === "string"
          ? " COLLATE NOCASE"
          : ""
      return `${alias}.${dialect.ident(o.column)}${collate} ${o.direction === "asc" ? "ASC" : "DESC"}`
    })
    .join(", ")

const keyOrder = (dialect: Dialect, table: TableSchema, alias: string): string =>
  dialect.hasKeyColumn
    ? `${alias}.${dialect.ident(KEY_COLUMN)}`
    : table.primary_key.map((c) => `${alias}.${dialect.ident(c)}`).join(", ")

export interface SelectOptions {
  /** Return only local primary keys, preserving the exact predicate, ordering and limit. */
  readonly keysOnly?: boolean
  /** Restrict to rows that are members of the given subscription in the local `membership` table. */
  readonly membershipOf?: string
  /**
   * Extra admission condition, ORed with the membership check. The primary alias is `t`. Used by
   * the browser to admit rows written optimistically by pending mutations.
   */
  readonly alsoAdmit?: string
  readonly dialect?: Dialect
}

const makeCtx = (planned: PlannedQuery, options: SelectOptions): Ctx => ({
  planned,
  dialect: options.dialect ?? SQLITE,
  params: [],
  aliases: 0,
})

/** Primary rows of a planned query, ordered, with `__key` first (SQLite). */
export const compileSelect = (planned: PlannedQuery, options: SelectOptions = {}): CompiledSql => {
  const ctx = makeCtx(planned, options)
  const { dialect } = ctx
  const t = "t"
  const where: Array<string> = []
  if (planned.query.where !== undefined)
    where.push(
      compilePredicate(ctx, planned.table, planned.query.where, (c) => `${t}.${dialect.ident(c)}`),
    )
  if (options.membershipOf !== undefined) {
    if (!dialect.hasKeyColumn) throw new Error("membership requires the local cache dialect")
    // A subscription that extends another (`subscriptions.based_on`) reads the base's rows too.
    ctx.params.push(planned.table.name, options.membershipOf)
    const member = `${t}."__key" IN (SELECT m."key" FROM "membership" m WHERE m."tbl" = ? AND m."subscription" IN (WITH RECURSIVE chain(id) AS (SELECT ? UNION SELECT s."based_on" FROM "subscriptions" s JOIN chain ON s."id" = chain.id WHERE s."based_on" IS NOT NULL) SELECT id FROM chain))`
    where.push(options.alsoAdmit === undefined ? member : `(${member} OR ${options.alsoAdmit})`)
  }
  if (options.keysOnly && !dialect.hasKeyColumn)
    throw new Error("key projection requires the local cache dialect")
  const projection = options.keysOnly
    ? `${t}.${dialect.ident(KEY_COLUMN)}`
    : selectList(dialect, planned.table, t)
  let sql = `SELECT ${projection} FROM ${dialect.table(planned.table.name)} ${t}`
  if (where.length > 0) sql += ` WHERE ${where.join(" AND ")}`
  sql += ` ORDER BY ${orderClause(dialect, planned, t)}`
  if (planned.limit !== undefined) sql += ` LIMIT ${Math.trunc(planned.limit)}`
  return { sql, params: ctx.params }
}

const parentOf = (planned: PlannedQuery, include: PlannedInclude): PlannedInclude | null => {
  let list = planned.includes
  let parent: PlannedInclude | null = null
  for (const name of include.path.slice(0, -1)) {
    const next = list.find((i) => i.relation.name === name)
    if (next === undefined) throw new Error(`planner invariant violated: include path ${name}`)
    parent = next
    list = next.includes
  }
  return parent
}

/** The select whose rows an include hangs off: the primary select or the parent include's. */
const parentSelect = (
  planned: PlannedQuery,
  include: PlannedInclude,
  options: SelectOptions,
): { readonly sql: CompiledSql; readonly table: TableSchema } => {
  const parent = parentOf(planned, include)
  return parent === null
    ? { sql: compileSelect(planned, options), table: planned.table }
    : { sql: compileIncludeSelect(planned, parent, options), table: parent.target }
}

/** Rows of an included relation for the rows of its parent level, at any depth. */
export const compileIncludeSelect = (
  planned: PlannedQuery,
  include: PlannedInclude,
  options: SelectOptions = {},
  parentKeys?: ReadonlyArray<string>,
): CompiledSql => {
  const ctx = makeCtx(planned, options)
  const { dialect } = ctx
  // Cache readers already evaluated the parent level. Reuse exactly those rows rather than
  // repeating its permission predicates, ordering and limit for every included relation.
  // Fetch relation columns from the table so composite keys, NULLs and affinities retain
  // SQLite's normal semantics. One JSON parameter avoids the bind-variable limit.
  if (parentKeys !== undefined && !dialect.hasKeyColumn)
    throw new Error("parent keys require the local cache dialect")
  const parentTable = parentOf(planned, include)?.target ?? planned.table
  const parent =
    parentKeys === undefined
      ? parentSelect(planned, include, options)
      : {
          sql: {
            sql: `SELECT * FROM ${dialect.table(parentTable.name)} WHERE ${dialect.ident(KEY_COLUMN)} IN (SELECT value FROM json_each(?))`,
            params: [JSON.stringify(parentKeys)],
          },
          table: parentTable,
        }
  ctx.params.push(...parent.sql.params)
  const r = "r"
  // A non-correlated `IN (SELECT ...)`: the parent level is evaluated once into an ephemeral
  // index and every candidate row probes it. A correlated `EXISTS` over the same derived table
  // re-runs the parent select for every candidate row, which is quadratic in the cache size.
  const childCols = include.relation.to_columns.map((c) => `${r}.${dialect.ident(c)}`)
  const parentCols = include.relation.from_columns.map((c) => `p.${dialect.ident(c)}`)
  const lhs = childCols.length === 1 ? (childCols[0] ?? "") : `(${childCols.join(", ")})`
  const where = [`${lhs} IN (SELECT ${parentCols.join(", ")} FROM (${parent.sql.sql}) p)`]
  if (include.where !== undefined)
    where.push(
      compilePredicate(ctx, include.target, include.where, (c) => `${r}.${dialect.ident(c)}`),
    )
  const sql = `SELECT ${selectList(dialect, include.target, r)} FROM ${dialect.table(include.target.name)} ${r} WHERE ${where.join(" AND ")} ORDER BY ${keyOrder(dialect, include.target, r)}`
  return { sql, params: ctx.params }
}

/**
 * A predicate of `table` rendered over `alias` (`1` when there is none), with its parameters.
 * Relation predicates (`exists`) read the related cache tables. The engine composes its own
 * maintenance statements around it.
 */
export const compileWhere = (
  planned: PlannedQuery,
  table: TableSchema,
  where: Predicate | undefined,
  alias: string,
): CompiledSql => {
  const ctx = makeCtx(planned, {})
  const sql =
    where === undefined
      ? "1"
      : compilePredicate(ctx, table, where, (c) => `${alias}.${ctx.dialect.ident(c)}`)
  return { sql, params: ctx.params }
}

/** `SELECT <columns> FROM <table> r WHERE ...`, the building block of incremental maintenance. */
const selectFrom = (
  ctx: Ctx,
  table: TableSchema,
  alias: string,
  where: ReadonlyArray<string>,
): string => {
  const { dialect } = ctx
  let sql = `SELECT ${selectList(dialect, table, alias)} FROM ${dialect.table(table.name)} ${alias}`
  if (where.length > 0) sql += ` WHERE ${where.join(" AND ")}`
  return sql
}

/**
 * Rows of `table` whose `columns` equal `values` (SQL `=`, so a NULL never matches), optionally
 * filtered by `where`. Used to find the rows an include level references for one parent row.
 */
export const compileRowsByColumns = (
  planned: PlannedQuery,
  table: TableSchema,
  columns: ReadonlyArray<string>,
  values: ReadonlyArray<RowImage[string] | null | undefined>,
  where: Predicate | undefined,
): CompiledSql => {
  const ctx = makeCtx(planned, {})
  const r = "r"
  const conds = columns.map((column, i) => {
    const kind = kindOf(table, column)
    ctx.params.push(literalParam(kind, values[i] ?? null))
    return `${r}.${ctx.dialect.ident(column)} = ${sqlitePlaceholder(kind)}`
  })
  if (where !== undefined)
    conds.push(compilePredicate(ctx, table, where, (c) => `${r}.${ctx.dialect.ident(c)}`))
  return {
    sql: `${selectFrom(ctx, table, r, conds)} ORDER BY ${keyOrder(ctx.dialect, table, r)}`,
    params: ctx.params,
  }
}

/**
 * Primary rows that reach a changed row of the last relation's target through a chain of
 * `exists` relations: `t -> a0 -> a1 -> <image>`. The innermost hop compares the previous hop's
 * `from_columns` with the image's `to_columns` values, so the changed table is never read.
 */
export const compileChainCandidates = (
  planned: PlannedQuery,
  chain: ReadonlyArray<RelationSchema>,
  image: RowImage,
): CompiledSql => {
  const ctx = makeCtx(planned, {})
  const { dialect } = ctx
  const t = "t"
  const last = chain[chain.length - 1]
  if (last === undefined) throw new Error("planner invariant violated: empty relation chain")
  // Build from the innermost hop outwards.
  const hopTable = (i: number): TableSchema =>
    i === 0 ? planned.table : tableOf(ctx, chain[i - 1]?.target_table ?? "")
  const build = (i: number, outer: string): string => {
    const rel = chain[i]
    if (rel === undefined) return "1"
    const source = hopTable(i)
    if (i === chain.length - 1) {
      return rel.from_columns
        .map((from, j) => {
          const kind = kindOf(source, from)
          ctx.params.push(literalParam(kind, image[rel.to_columns[j] ?? ""] ?? null))
          return `${outer}.${dialect.ident(from)} = ${sqlitePlaceholder(kind)}`
        })
        .join(" AND ")
    }
    const alias = `a${i}`
    const target = tableOf(ctx, rel.target_table)
    const join = joinCondition(ctx, rel, (c) => `${outer}.${dialect.ident(c)}`, alias)
    return `EXISTS (SELECT 1 FROM ${dialect.table(target.name)} ${alias} WHERE ${join} AND ${build(i + 1, alias)})`
  }
  const cond = build(0, t)
  return {
    sql: `${selectFrom(ctx, planned.table, t, [cond])} ORDER BY ${keyOrder(dialect, planned.table, t)}`,
    params: ctx.params,
  }
}

/** `ORDER BY` of the primary rows over alias `t`, optionally reversed (last row first). */
export const primaryOrderSql = (planned: PlannedQuery, reverse = false): string =>
  planned.orderBy
    .map((o) => {
      const asc = (o.direction === "asc") !== reverse
      return `t.${quoteIdent(o.column)} ${asc ? "ASC" : "DESC"}`
    })
    .join(", ")

/** Select list of the primary table over alias `t` (SQLite, `__key` first). */
export const primarySelectList = (planned: PlannedQuery): string =>
  selectList(SQLITE, planned.table, "t")

/** Placeholder with the column's affinity, for statements the engine composes itself. */
export const placeholderFor = (table: TableSchema, column: string): string =>
  sqlitePlaceholder(kindOf(table, column))

/** Parameter for a column value, for statements the engine composes itself. */
export const paramFor = (
  table: TableSchema,
  column: string,
  value: RowImage[string] | null | undefined,
): SqlParam => literalParam(kindOf(table, column), value ?? null)

export class UnstorableValueError extends Error {
  constructor(
    readonly kind: ValueKind,
    readonly value: unknown,
    reason: string,
  ) {
    super(`value ${JSON.stringify(value)} of kind ${kind} cannot be stored: ${reason}`)
    this.name = "UnstorableValueError"
  }
}

const I64_MIN = -(2n ** 63n)
const I64_MAX = 2n ** 63n - 1n

/**
 * Converts a wire cell to a SQLite parameter for the given kind. Throws `UnstorableValueError`
 * for bigints outside the signed 64-bit range, which SQLite would silently store as a lossy REAL.
 */
export const literalParam = (kind: ValueKind, value: RowImage[string] | null): SqlParam => {
  if (value === null || value === undefined) return null
  switch (kind) {
    case "bool":
      return value === true ? 1 : 0
    case "int":
    case "float":
      return typeof value === "number" ? value : null
    case "bigint": {
      if (typeof value !== "string") return null
      const n = Number(value)
      if (Number.isSafeInteger(n)) return n
      const big = BigInt(value)
      if (big < I64_MIN || big > I64_MAX)
        throw new UnstorableValueError(kind, value, "outside the signed 64-bit range")
      return value
    }
    case "json":
      return JSON.stringify(value)
    default:
      return typeof value === "string" ? value : JSON.stringify(value)
  }
}

/** A result row with its included relations attached, recursively. */
export interface ResultNode {
  readonly key: string
  readonly row: RowImage
  readonly related: Record<string, ResultNode | null | ReadonlyArray<ResultNode>>
}

const joinKeyOf = (columns: ReadonlyArray<string>, row: RowImage): string =>
  JSON.stringify(columns.map((c) => row[c] ?? null))

/**
 * Nests included rows under their parents. `rowsByPath` maps an include's `path.join("/")` to
 * the rows its compiled select returned (`key` and `row` per record).
 */
export const attachIncludes = (
  planned: PlannedQuery,
  primary: ReadonlyArray<{ readonly key: string; readonly row: RowImage }>,
  rowsByPath: ReadonlyMap<string, ReadonlyArray<{ readonly key: string; readonly row: RowImage }>>,
): ReadonlyArray<ResultNode> => {
  const build = (
    rows: ReadonlyArray<{ readonly key: string; readonly row: RowImage }>,
    includes: ReadonlyArray<PlannedInclude>,
  ): ReadonlyArray<ResultNode> => {
    const nodes: Array<ResultNode> = rows.map((r) => ({ key: r.key, row: r.row, related: {} }))
    for (const include of includes) {
      const children = build(rowsByPath.get(include.path.join("/")) ?? [], include.includes)
      const byJoin = new Map<string, Array<ResultNode>>()
      for (const child of children) {
        const jk = joinKeyOf(include.relation.to_columns, child.row)
        const list = byJoin.get(jk) ?? []
        list.push(child)
        byJoin.set(jk, list)
      }
      for (const node of nodes) {
        const matches = byJoin.get(joinKeyOf(include.relation.from_columns, node.row)) ?? []
        node.related[include.relation.name] =
          include.relation.kind === "one" ? (matches[0] ?? null) : matches
      }
    }
    return nodes
  }
  return build(primary, planned.includes)
}

/** Flattens a result node into the object shape the typed query promises. */
export const flattenNode = (node: ResultNode): Record<string, unknown> => {
  const out: Record<string, unknown> = { ...node.row }
  for (const [name, rel] of Object.entries(node.related)) {
    if (rel === null) out[name] = null
    else if ("key" in rel) out[name] = flattenNode(rel)
    else out[name] = rel.map(flattenNode)
  }
  return out
}
