/**
 * Typed query builder. Column names, value types and relation names are checked against the
 * sync schema definition at compile time; the output is the plain serializable `Query`.
 *
 * ```ts
 * const documents = q(sync)
 *   .from("Chatbot")
 *   .where((c) =>
 *     c.and(
 *       c.eq("type", "DOCUMENT"),
 *       c.exists("tags", (t) => t.eq("entityId", tagId)),
 *     ),
 *   )
 *   .orderBy("displayOrder", "asc")
 *   .limit(50)
 *   .include("folder")
 *   .include("tags", (t) => t.include("tag"))
 * ```
 */

import type { Include, Predicate, Query, Scalar } from "@orbit/protocol"
import type {
  ColumnsOf,
  RelationKindOf,
  RelationNamesOf,
  RelationTarget,
  RowOf,
  SyncedTables,
  SyncSchemaDefinition,
} from "@orbit/schema"

type ScalarOf<V> = Extract<NonNullable<V>, Scalar>

export interface PredicateBuilder<D, N extends string> {
  readonly and: (...args: ReadonlyArray<Predicate>) => Predicate
  readonly or: (...args: ReadonlyArray<Predicate>) => Predicate
  readonly not: (arg: Predicate) => Predicate
  readonly eq: <C extends ColumnsOf<D, N>>(column: C, value: ScalarOf<RowOf<D, N>[C]>) => Predicate
  readonly ne: <C extends ColumnsOf<D, N>>(column: C, value: ScalarOf<RowOf<D, N>[C]>) => Predicate
  readonly lt: <C extends ColumnsOf<D, N>>(column: C, value: ScalarOf<RowOf<D, N>[C]>) => Predicate
  readonly lte: <C extends ColumnsOf<D, N>>(column: C, value: ScalarOf<RowOf<D, N>[C]>) => Predicate
  readonly gt: <C extends ColumnsOf<D, N>>(column: C, value: ScalarOf<RowOf<D, N>[C]>) => Predicate
  readonly gte: <C extends ColumnsOf<D, N>>(column: C, value: ScalarOf<RowOf<D, N>[C]>) => Predicate
  readonly in: <C extends ColumnsOf<D, N>>(
    column: C,
    values: ReadonlyArray<ScalarOf<RowOf<D, N>[C]>>,
  ) => Predicate
  readonly isNull: (column: ColumnsOf<D, N>) => Predicate
  readonly isNotNull: (column: ColumnsOf<D, N>) => Predicate
  readonly like: <C extends ColumnsOf<D, N>>(
    column: RowOf<D, N>[C] extends string | null ? C : never,
    pattern: string,
  ) => Predicate
  /** At least one related row exists (and satisfies `where`, when given). */
  readonly exists: <R extends RelationNamesOf<D, N>>(
    relation: R,
    where?: (r: PredicateBuilder<D, RelationTarget<D, N, R>>) => Predicate,
  ) => Predicate
  /** No related row exists (that satisfies `where`, when given). */
  readonly notExists: <R extends RelationNamesOf<D, N>>(
    relation: R,
    where?: (r: PredicateBuilder<D, RelationTarget<D, N, R>>) => Predicate,
  ) => Predicate
}

const predicateBuilder = <D, N extends string>(): PredicateBuilder<D, N> => ({
  and: (...args) => ({ op: "and", args }),
  or: (...args) => ({ op: "or", args }),
  not: (arg) => ({ op: "not", arg }),
  eq: (column, value) => ({ op: "eq", column, value }),
  ne: (column, value) => ({ op: "ne", column, value }),
  lt: (column, value) => ({ op: "lt", column, value }),
  lte: (column, value) => ({ op: "lte", column, value }),
  gt: (column, value) => ({ op: "gt", column, value }),
  gte: (column, value) => ({ op: "gte", column, value }),
  in: (column, values) => ({ op: "in", column, values }),
  isNull: (column) => ({ op: "isNull", column }),
  isNotNull: (column) => ({ op: "isNotNull", column }),
  like: (column, pattern) => ({ op: "like", column, pattern }),
  exists: (relation, where) => ({
    op: "exists",
    relation,
    ...(where === undefined ? {} : { where: where(predicateBuilder()) }),
  }),
  notExists: (relation, where) => ({
    op: "not",
    arg: {
      op: "exists",
      relation,
      ...(where === undefined ? {} : { where: where(predicateBuilder()) }),
    },
  }),
})

/** Include shape: relation name to the nested include shape of its target. */
export type IncludeShape = { readonly [relation: string]: IncludeShape }

/** Result row type: the primary row plus included relations, nested. */
export type ResultRow<D, N extends string, I extends IncludeShape = {}> = RowOf<D, N> & {
  readonly [R in Extract<keyof I, string>]: RelationKindOf<D, N, R> extends "one"
    ? ResultRow<D, RelationTarget<D, N, R>, I[R]> | null
    : ReadonlyArray<ResultRow<D, RelationTarget<D, N, R>, I[R]>>
}

const withWhere = (spec: Include, where: Predicate): Include => ({
  relation: spec.relation,
  where,
  ...(spec.include === undefined ? {} : { include: spec.include }),
})

const withInclude = (spec: Include, nested: Include): Include => ({
  relation: spec.relation,
  ...(spec.where === undefined ? {} : { where: spec.where }),
  include: [...(spec.include ?? []), nested],
})

/** Builds the spec of one included relation: an optional filter and nested includes. */
export class IncludeBuilder<D, N extends string, I extends IncludeShape = {}> {
  declare readonly _Shape: I

  constructor(readonly spec: Include) {}

  where(build: (c: PredicateBuilder<D, N>) => Predicate): IncludeBuilder<D, N, I> {
    return new IncludeBuilder(withWhere(this.spec, build(predicateBuilder<D, N>())))
  }

  include<R extends RelationNamesOf<D, N>, S extends IncludeShape = {}>(
    relation: R,
    build?: (
      i: IncludeBuilder<D, RelationTarget<D, N, R>>,
    ) => IncludeBuilder<D, RelationTarget<D, N, R>, S>,
  ): IncludeBuilder<D, N, I & { readonly [K in R]: S }> {
    const nested = build === undefined ? { relation } : build(new IncludeBuilder({ relation })).spec
    return new IncludeBuilder(withInclude(this.spec, nested))
  }
}

export class TypedQuery<D, N extends string, I extends IncludeShape = {}> {
  /** Phantom marker so the result type can be recovered from a query value. */
  declare readonly _Row: ResultRow<D, N, I>

  constructor(readonly ast: Query) {}

  where(build: (c: PredicateBuilder<D, N>) => Predicate): TypedQuery<D, N, I> {
    return new TypedQuery({ ...this.ast, where: build(predicateBuilder<D, N>()) })
  }

  orderBy(column: ColumnsOf<D, N>, direction: "asc" | "desc" = "asc"): TypedQuery<D, N, I> {
    return new TypedQuery({
      ...this.ast,
      orderBy: [...(this.ast.orderBy ?? []), { column, direction }],
    })
  }

  limit(n: number): TypedQuery<D, N, I> {
    return new TypedQuery({ ...this.ast, limit: n })
  }

  /** Includes a relation; `build` filters it and includes relations of its target. */
  include<R extends RelationNamesOf<D, N>, S extends IncludeShape = {}>(
    relation: R,
    build?: (
      i: IncludeBuilder<D, RelationTarget<D, N, R>>,
    ) => IncludeBuilder<D, RelationTarget<D, N, R>, S>,
  ): TypedQuery<D, N, I & { readonly [K in R]: S }> {
    const nested = build === undefined ? { relation } : build(new IncludeBuilder({ relation })).spec
    return new TypedQuery({ ...this.ast, include: [...(this.ast.include ?? []), nested] })
  }
}

export type RowOfQuery<Q> =
  Q extends TypedQuery<infer D, infer N, infer I> ? ResultRow<D, N, I> : never

export interface QueryRoot<D> {
  readonly from: <N extends SyncedTables<D>>(table: N) => TypedQuery<D, N>
}

/** Entry point; the definition value is only used for its type. */
export const q = <
  D extends SyncSchemaDefinition<never, unknown> | { readonly _tag: "SyncSchemaDefinition" },
>(
  _definition: D,
): QueryRoot<D> => ({
  from: (table) => new TypedQuery({ table }),
})
