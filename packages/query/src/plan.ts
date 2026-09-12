/**
 * Query validation and planning against a compiled sync schema.
 *
 * Planning is where unsupported query forms are rejected. The output is a `PlannedQuery` that
 * the Durable Object (for incremental maintenance), the browser (for local execution) and the
 * application's server (for mutators) consume. Routing is not a planning concern: by the time a
 * query reaches a Sync Durable Object it is already scoped to one logical partition.
 */

import { Data, Result } from "effect"
import type {
  Include,
  OrderBy,
  Predicate,
  Query,
  RelationSchema,
  Scalar,
  TableSchema,
  ValueKind,
} from "@orbit/protocol"
import { canonicalJson, type SchemaRuntime } from "@orbit/schema"

export const MAX_LIMIT = 10_000
export const MAX_IN_VALUES = 40
export const MAX_PREDICATE_NODES = 60
/** Levels of nested includes and of `exists` predicates. */
export const MAX_RELATION_DEPTH = 3
/**
 * Bound parameters a predicate may need. Durable Object SQLite allows 100 bound parameters per
 * statement; the compiled select adds a handful (limit, membership) on top of this budget.
 */
export const MAX_PREDICATE_PARAMS = 80

export type QueryProblem =
  | { readonly reason: "unknown_table"; readonly table: string }
  | { readonly reason: "unknown_column"; readonly table: string; readonly column: string }
  | { readonly reason: "unknown_relation"; readonly table: string; readonly relation: string }
  | {
      readonly reason: "type_mismatch"
      readonly column: string
      readonly kind: ValueKind
      readonly value: Scalar
    }
  | {
      readonly reason: "not_comparable"
      readonly column: string
      readonly kind: ValueKind
      readonly op: string
    }
  | { readonly reason: "not_orderable"; readonly column: string; readonly kind: ValueKind }
  | { readonly reason: "limit_too_large"; readonly limit: number }
  | { readonly reason: "too_many_values"; readonly column: string; readonly count: number }
  | { readonly reason: "predicate_too_large"; readonly nodes: number }
  | { readonly reason: "too_many_parameters"; readonly params: number }
  | { readonly reason: "empty_in"; readonly column: string }
  | { readonly reason: "too_deep"; readonly depth: number }

export class QueryValidationError extends Data.TaggedError("QueryValidationError")<{
  readonly problem: QueryProblem
}> {
  override get message(): string {
    return `unsupported query: ${JSON.stringify(this.problem)}`
  }
}

export interface PlannedInclude {
  readonly relation: RelationSchema
  readonly target: TableSchema
  readonly where: Predicate | undefined
  /** Nested includes of the target, sorted by relation name. */
  readonly includes: ReadonlyArray<PlannedInclude>
  /** Relation names from the primary table down to this include. */
  readonly path: ReadonlyArray<string>
  /** 1 for includes of the primary table. */
  readonly depth: number
}

export interface PlannedQuery {
  /** Normalized query: `orderBy` always ends with the primary key, includes are objects, sorted. */
  readonly query: Query
  readonly table: TableSchema
  readonly orderBy: ReadonlyArray<OrderBy>
  readonly limit: number | undefined
  readonly includes: ReadonlyArray<PlannedInclude>
  /** Every table whose changes can affect the result (predicate relations and includes). */
  readonly tables: ReadonlySet<string>
  /** Tables referenced by `exists` predicates anywhere in the query. */
  readonly predicateTables: ReadonlySet<string>
  /** Canonical identity; equal queries share one materialization. */
  readonly key: string
  /** The schema the query was planned against (relation targets are looked up through it). */
  readonly rt: SchemaRuntime
}

const valueMatchesKind = (kind: ValueKind, value: Scalar): boolean => {
  switch (kind) {
    case "bool":
      return typeof value === "boolean"
    case "int":
      return typeof value === "number" && Number.isInteger(value)
    case "float":
      return typeof value === "number"
    case "bigint":
      return typeof value === "string" && /^-?\d+$/.test(value)
    case "decimal":
    case "string":
    case "bytes":
    case "datetime":
    case "date":
    case "time":
      return typeof value === "string"
    case "json":
      return false
  }
}

/** Ops allowed per kind. Range comparisons need a total order that SQLite reproduces. */
const allowedOps = (kind: ValueKind): ReadonlySet<string> => {
  switch (kind) {
    // JSON travels and is stored as text, so LIKE over that text is the one search it allows.
    case "json":
      return new Set(["isNull", "isNotNull", "like"])
    case "bytes":
    case "decimal":
      return new Set(["eq", "ne", "in", "isNull", "isNotNull"])
    case "string":
      return new Set(["eq", "ne", "lt", "lte", "gt", "gte", "in", "isNull", "isNotNull", "like"])
    default:
      return new Set(["eq", "ne", "lt", "lte", "gt", "gte", "in", "isNull", "isNotNull"])
  }
}

const isOrderable = (kind: ValueKind): boolean =>
  kind !== "json" && kind !== "bytes" && kind !== "decimal"

interface Budget {
  nodes: number
  params: number
}

interface Walk {
  readonly rt: SchemaRuntime
  readonly budget: Budget
  readonly predicateTables: Set<string>
}

const relationOf = (
  rt: SchemaRuntime,
  table: TableSchema,
  name: string,
): Result.Result<{ relation: RelationSchema; target: TableSchema }, QueryProblem> => {
  const relation = table.relations.find((r) => r.name === name)
  if (relation === undefined)
    return Result.fail({ reason: "unknown_relation", table: table.name, relation: name })
  const target = rt.table(relation.target_table)
  if (target === undefined)
    return Result.fail({ reason: "unknown_table", table: relation.target_table })
  return Result.succeed({ relation, target })
}

const validatePredicate = (
  walk: Walk,
  table: TableSchema,
  p: Predicate,
  depth: number,
): Result.Result<void, QueryProblem> => {
  const { budget, rt } = walk
  budget.nodes += 1
  if (budget.nodes > MAX_PREDICATE_NODES)
    return Result.fail({ reason: "predicate_too_large", nodes: budget.nodes })
  switch (p.op) {
    case "and":
    case "or": {
      for (const a of p.args) {
        const r = validatePredicate(walk, table, a, depth)
        if (Result.isFailure(r)) return r
      }
      return Result.succeed(undefined)
    }
    case "not":
      return validatePredicate(walk, table, p.arg, depth)
    case "exists": {
      if (depth >= MAX_RELATION_DEPTH) return Result.fail({ reason: "too_deep", depth: depth + 1 })
      const rel = relationOf(rt, table, p.relation)
      if (Result.isFailure(rel)) return Result.fail(rel.failure)
      walk.predicateTables.add(rel.success.target.name)
      return p.where === undefined
        ? Result.succeed(undefined)
        : validatePredicate(walk, rel.success.target, p.where, depth + 1)
    }
    default: {
      const col = rt.column(table.name, p.column)
      if (col === undefined)
        return Result.fail({ reason: "unknown_column", table: table.name, column: p.column })
      if (!allowedOps(col.kind).has(p.op))
        return Result.fail({ reason: "not_comparable", column: p.column, kind: col.kind, op: p.op })
      if (p.op === "in") {
        if (p.values.length === 0) return Result.fail({ reason: "empty_in", column: p.column })
        if (p.values.length > MAX_IN_VALUES)
          return Result.fail({
            reason: "too_many_values",
            column: p.column,
            count: p.values.length,
          })
        for (const v of p.values)
          if (!valueMatchesKind(col.kind, v))
            return Result.fail({
              reason: "type_mismatch",
              column: p.column,
              kind: col.kind,
              value: v,
            })
        budget.params += p.values.length
      } else if (p.op === "like") {
        budget.params += 1
      } else if (p.op !== "isNull" && p.op !== "isNotNull") {
        if (!valueMatchesKind(col.kind, p.value))
          return Result.fail({
            reason: "type_mismatch",
            column: p.column,
            kind: col.kind,
            value: p.value,
          })
        budget.params += 1
      }
      if (budget.params > MAX_PREDICATE_PARAMS)
        return Result.fail({ reason: "too_many_parameters", params: budget.params })
      return Result.succeed(undefined)
    }
  }
}

const normalizeInclude = (spec: string | Include): Include =>
  typeof spec === "string" ? { relation: spec } : spec

const planIncludes = (
  walk: Walk,
  table: TableSchema,
  specs: ReadonlyArray<string | Include> | undefined,
  path: ReadonlyArray<string>,
  depth: number,
  tables: Set<string>,
): Result.Result<ReadonlyArray<PlannedInclude>, QueryProblem> => {
  if (specs === undefined || specs.length === 0) return Result.succeed([])
  if (depth > MAX_RELATION_DEPTH) return Result.fail({ reason: "too_deep", depth })
  const byName = new Map<string, Include>()
  for (const spec of specs) {
    const inc = normalizeInclude(spec)
    // The same relation listed twice keeps the last spec; a query cannot include a relation
    // under two different filters.
    byName.set(inc.relation, inc)
  }
  const out: Array<PlannedInclude> = []
  for (const name of [...byName.keys()].toSorted()) {
    const inc = byName.get(name)
    if (inc === undefined) continue
    const rel = relationOf(walk.rt, table, name)
    if (Result.isFailure(rel)) return Result.fail(rel.failure)
    const { relation, target } = rel.success
    if (inc.where !== undefined) {
      const r = validatePredicate(walk, target, inc.where, depth)
      if (Result.isFailure(r)) return Result.fail(r.failure)
    }
    tables.add(target.name)
    const nested = planIncludes(walk, target, inc.include, [...path, name], depth + 1, tables)
    if (Result.isFailure(nested)) return nested
    out.push({
      relation,
      target,
      where: inc.where,
      includes: nested.success,
      path: [...path, name],
      depth,
    })
  }
  return Result.succeed(out)
}

const normalizedInclude = (inc: PlannedInclude): Include => ({
  relation: inc.relation.name,
  ...(inc.where === undefined ? {} : { where: inc.where }),
  ...(inc.includes.length === 0 ? {} : { include: inc.includes.map(normalizedInclude) }),
})

export const planQuery = (
  rt: SchemaRuntime,
  query: Query,
): Result.Result<PlannedQuery, QueryValidationError> => {
  const fail = (problem: QueryProblem): Result.Result<PlannedQuery, QueryValidationError> =>
    Result.fail(new QueryValidationError({ problem }))
  const table = rt.table(query.table)
  if (table === undefined) return fail({ reason: "unknown_table", table: query.table })
  const walk: Walk = { rt, budget: { nodes: 0, params: 0 }, predicateTables: new Set() }
  if (query.where !== undefined) {
    const r = validatePredicate(walk, table, query.where, 0)
    if (Result.isFailure(r)) return fail(r.failure)
  }
  const orderBy: Array<OrderBy> = []
  for (const o of query.orderBy ?? []) {
    const col = rt.column(table.name, o.column)
    if (col === undefined)
      return fail({ reason: "unknown_column", table: table.name, column: o.column })
    if (!isOrderable(col.kind))
      return fail({ reason: "not_orderable", column: o.column, kind: col.kind })
    orderBy.push({ column: o.column, direction: o.direction })
  }
  for (const pk of table.primary_key) {
    if (!orderBy.some((o) => o.column === pk)) orderBy.push({ column: pk, direction: "asc" })
  }
  if (query.limit !== undefined && query.limit > MAX_LIMIT)
    return fail({ reason: "limit_too_large", limit: query.limit })
  const tables = new Set<string>([table.name, ...walk.predicateTables])
  const includes = planIncludes(walk, table, query.include, [], 1, tables)
  if (Result.isFailure(includes)) return fail(includes.failure)
  for (const t of walk.predicateTables) tables.add(t)
  const normalized: Query = {
    table: table.name,
    ...(query.where === undefined ? {} : { where: query.where }),
    orderBy,
    ...(query.limit === undefined ? {} : { limit: query.limit }),
    ...(includes.success.length === 0 ? {} : { include: includes.success.map(normalizedInclude) }),
  }
  return Result.succeed({
    query: normalized,
    table,
    orderBy,
    limit: query.limit,
    includes: includes.success,
    tables,
    predicateTables: walk.predicateTables,
    key: canonicalJson(normalized),
    rt,
  })
}

/**
 * Every chain of relations an `exists` predicate walks, from the primary table outwards: one
 * chain per `exists` node, so a nested node yields a chain that extends its parent's. The last
 * relation's target is the table whose changes can flip primary membership through that chain.
 */
export const predicateChains = (
  planned: PlannedQuery,
): ReadonlyArray<ReadonlyArray<RelationSchema>> => {
  const out: Array<ReadonlyArray<RelationSchema>> = []
  const walk = (table: TableSchema, p: Predicate, chain: ReadonlyArray<RelationSchema>): void => {
    switch (p.op) {
      case "and":
      case "or":
        for (const a of p.args) walk(table, a, chain)
        return
      case "not":
        walk(table, p.arg, chain)
        return
      case "exists": {
        const rel = relationOf(planned.rt, table, p.relation)
        if (Result.isFailure(rel)) return
        const next = [...chain, rel.success.relation]
        out.push(next)
        if (p.where !== undefined) walk(rel.success.target, p.where, next)
        return
      }
      default:
        return
    }
  }
  if (planned.query.where !== undefined) walk(planned.table, planned.query.where, [])
  return out
}

/** Every include at every depth, parents before children. */
export const flattenIncludes = (planned: PlannedQuery): ReadonlyArray<PlannedInclude> => {
  const out: Array<PlannedInclude> = []
  const visit = (list: ReadonlyArray<PlannedInclude>): void => {
    for (const inc of list) {
      out.push(inc)
      visit(inc.includes)
    }
  }
  visit(planned.includes)
  return out
}
