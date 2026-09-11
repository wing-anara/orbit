/**
 * The serializable query representation.
 *
 * A query names one table, an optional predicate over that table's columns and relations, an
 * ordering, a limit and the relations to include. Includes nest: an included relation can
 * filter its rows and include relations of its own. There is no opaque code anywhere: every part
 * is data, so the same query can be validated on the client, sent over the wire, planned inside a
 * Sync Durable Object and executed against SQLite on both sides, or against MySQL on the
 * application's server (see `docs/queries.md`).
 *
 * Deliberately unsupported (rejected explicitly by the planner): aggregates, arbitrary
 * subqueries, joins other than declared relations, offsets, ordering or limits inside includes,
 * and comparisons on `decimal`, `json` or `bytes` columns.
 */

import { Schema } from "effect"

import { JsonValue } from "./generated/protocol.gen.ts"

/** Scalar literals allowed in predicates. SQL NULL is expressed with `isNull` / `isNotNull`. */
export const Scalar = Schema.Union([Schema.String, Schema.Finite, Schema.Boolean])
export type Scalar = typeof Scalar.Type

export const ComparisonOp = Schema.Literals(["eq", "ne", "lt", "lte", "gt", "gte"])
export type ComparisonOp = typeof ComparisonOp.Type

export type Predicate =
  | { readonly op: "and"; readonly args: ReadonlyArray<Predicate> }
  | { readonly op: "or"; readonly args: ReadonlyArray<Predicate> }
  | { readonly op: "not"; readonly arg: Predicate }
  | { readonly op: ComparisonOp; readonly column: string; readonly value: Scalar }
  | { readonly op: "in"; readonly column: string; readonly values: ReadonlyArray<Scalar> }
  | { readonly op: "isNull"; readonly column: string }
  | { readonly op: "isNotNull"; readonly column: string }
  | { readonly op: "like"; readonly column: string; readonly pattern: string }
  /**
   * True when at least one row of the declared relation satisfies `where` (or exists at all).
   * `where` is a predicate over the relation's target table. Negate with `not`.
   */
  | { readonly op: "exists"; readonly relation: string; readonly where?: Predicate }

const Suspended = Schema.suspend((): Schema.Codec<Predicate, Predicate> => Predicate)

export const Predicate: Schema.Codec<Predicate, Predicate> = Schema.Union([
  Schema.Struct({ op: Schema.Literal("and"), args: Schema.Array(Suspended) }),
  Schema.Struct({ op: Schema.Literal("or"), args: Schema.Array(Suspended) }),
  Schema.Struct({ op: Schema.Literal("not"), arg: Suspended }),
  Schema.Struct({ op: ComparisonOp, column: Schema.String, value: Scalar }),
  Schema.Struct({ op: Schema.Literal("in"), column: Schema.String, values: Schema.Array(Scalar) }),
  Schema.Struct({ op: Schema.Literal("isNull"), column: Schema.String }),
  Schema.Struct({ op: Schema.Literal("isNotNull"), column: Schema.String }),
  Schema.Struct({ op: Schema.Literal("like"), column: Schema.String, pattern: Schema.String }),
  Schema.Struct({
    op: Schema.Literal("exists"),
    relation: Schema.String,
    where: Schema.optionalKey(Suspended),
  }),
])

export const OrderBy = Schema.Struct({
  column: Schema.String,
  direction: Schema.Literals(["asc", "desc"]),
})
export type OrderBy = typeof OrderBy.Type

/** An included relation: optionally filtered, optionally including relations of the target. */
export interface Include {
  readonly relation: string
  readonly where?: Predicate
  readonly include?: ReadonlyArray<string | Include>
}

export const Include: Schema.Codec<Include, Include> = Schema.Struct({
  relation: Schema.String,
  where: Schema.optionalKey(Predicate),
  include: Schema.optionalKey(
    Schema.Array(
      Schema.Union([Schema.String, Schema.suspend((): Schema.Codec<Include, Include> => Include)]),
    ),
  ),
})

export const Query = Schema.Struct({
  table: Schema.String,
  where: Schema.optionalKey(Predicate),
  /** Ordering; the primary key is always appended by the planner to make results deterministic. */
  orderBy: Schema.optionalKey(Schema.Array(OrderBy)),
  limit: Schema.optionalKey(Schema.Finite.check(Schema.isInt(), Schema.isGreaterThan(0))),
  /** Relations (declared in the sync schema) whose rows are delivered with the result. */
  include: Schema.optionalKey(Schema.Array(Schema.Union([Schema.String, Include]))),
})
export type Query = typeof Query.Type

/**
 * A reference to a named query defined by the application (see `defineQueries` in
 * `@orbit/query`). The server resolves it to a `Query` with the caller's identity as context,
 * which is how authorization is expressed without row-level rules on the client.
 */
export const NamedQueryRef = Schema.Struct({
  name: Schema.String,
  args: Schema.Record(Schema.String, JsonValue),
})
export type NamedQueryRef = typeof NamedQueryRef.Type

/** What a client may subscribe to: a named query, or (when the server allows it) a raw query. */
export const QueryRef = Schema.Union([NamedQueryRef, Query])
export type QueryRef = typeof QueryRef.Type

export const isNamedQueryRef = (ref: QueryRef): ref is NamedQueryRef => "name" in ref
