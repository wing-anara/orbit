# Queries

Orbit queries are data, not code. A query names one synced table, a predicate, an ordering, a limit, and the relations to include. The same query value is validated in the browser, sent over the WebSocket, planned inside the Sync Durable Object, and executed against SQLite on both sides. Mutators run the same query against MySQL on the application's server. This document describes named queries, the query AST, the typed builder, the planner limits, the SQL compiler, and the forms Orbit does not support.

## Named queries

An application defines every query a client may run. A client refers to a query by name and arguments. The server resolves the name with the caller's identity. This is the authorization model of Orbit: the resolver decides what a caller sees, so no row-level rules exist on the client (see [auth.md](auth.md)). Raw queries from the client are refused when named queries are configured.

Definitions live in shared code, so the browser resolves the same query for an immediate local result:

```ts
import { defineQueries, defineQuery, q } from "@orbit/query"

const define = defineQuery(sync)
export const queries = defineQueries(sync, {
  documents: define(
    Schema.Struct({ folderId: Schema.NullOr(Schema.String) }),
    (ctx, { folderId }) =>
      q(sync)
        .from("Chatbot")
        .where((c) =>
          c.and(
            c.eq("organizationId", ctx.partition),
            c.eq("type", "DOCUMENT"),
            folderId === null ? c.isNull("groupId") : c.eq("groupId", folderId),
          ),
        )
        .orderBy("displayOrder", "asc")
        .include("folder"),
  ),
})
```

`ctx` is a `QueryContext` with `partition`, `subject`, and `clientId`. The client calls `queries.documents({ folderId })` and passes the result to `liveQuery`. The wire form is `{ name, args }`. The Durable Object decodes the arguments with the codec, runs the resolver with the session's subject, and answers `subscribed` with the resolved query. The client adopts that query when it differs from its own resolution.

The engine defines one query of its own, `$orbit.client`. It resolves to the caller's row in `orbit_clients` and confirms mutations (see [mutations.md](mutations.md)).

Related documents: [ivm.md](ivm.md) (incremental maintenance), [protocol.md](protocol.md) (the `subscribe` message), [type-safety.md](type-safety.md) (row types), [client-persistence.md](client-persistence.md) (local execution).

## The query AST

The AST lives in `packages/protocol/src/query-ast.ts`. A `Query` has these fields:

```ts
Query {
  table: string
  where?: Predicate
  orderBy?: Array<{ column: string; direction: "asc" | "desc" }>
  limit?: number      // integer, greater than 0
  include?: Array<string | Include>
}

Include {
  relation: string
  where?: Predicate                 // over the target table
  include?: Array<string | Include> // relations of the target
}
```

A predicate is a tree of these nodes:

- `and` with `args: Predicate[]`
- `or` with `args: Predicate[]`
- `not` with `arg: Predicate`
- `eq`, `ne`, `lt`, `lte`, `gt`, `gte` with `column` and `value`
- `in` with `column` and `values: Scalar[]`
- `isNull` and `isNotNull` with `column`
- `like` with `column` and `pattern: string`
- `exists` with `relation` and an optional `where` over the relation's target table

A `Scalar` is a string, a finite number, or a boolean. SQL NULL is not a scalar. Use `isNull` or `isNotNull` to test for NULL. An empty `and` compiles to `1` (true). An empty `or` compiles to `0` (false).

`exists` is true when at least one related row exists and satisfies `where`. Negate it with `not`. Relation predicates nest: a `where` inside `exists` can contain another `exists` over a relation of the target. The depth limit is three.

## The typed builder

The builder in `packages/query/src/builder.ts` produces a plain `Query`. It checks column names, value types, and relation names against the sync schema definition at compile time. The definition value is used only for its type.

```ts
import { q } from "@orbit/query"
import { sync } from "../../orbit/sync.config.ts"

const documents = q(sync)
  .from("Chatbot")
  .where((c) =>
    c.and(
      c.eq("type", "DOCUMENT"),
      c.eq("deleted", false),
      selectedFolder === null ? c.isNull("groupId") : c.eq("groupId", selectedFolder),
    ),
  )
  .orderBy("displayOrder", "asc")
  .limit(200)
  .include("folder")
```

A relation predicate and a nested include look like this:

```ts
const documentsWithTag = q(sync)
  .from("Chatbot")
  .where((c) =>
    c.and(
      c.eq("type", "DOCUMENT"),
      c.exists("tags", (t) => t.eq("entityId", tagId)),
    ),
  )
  .include("tags", (i) => i.include("tag"))
```

The builder methods are:

- `from(table)` starts a `TypedQuery` for a synced table.
- `where(build)` sets the predicate. `build` receives a `PredicateBuilder` with one method per node type.
- `orderBy(column, direction)` appends one ordering. The default direction is `asc`.
- `limit(n)` sets the limit.
- `include(relation, build?)` appends one relation declared in the sync schema. `build` receives an `IncludeBuilder` with `where` (a filter over the target) and `include` (relations of the target).
- `c.exists(relation, where?)` and `c.notExists(relation, where?)` test related rows. `where` receives a `PredicateBuilder` typed for the target table.

The value type of `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, and `in` is derived from the row type of the column. The `like` method accepts only columns whose row type is `string | null`.

The result row type is `ResultRow<D, N, I>`. It is the row of the primary table plus one property per included relation, nested. A `one` relation gives `Row | null`. A `many` relation gives `ReadonlyArray<Row>`. `I` is the include shape, an object type that mirrors the nesting. In the example schema, `d.folder` is a `Chatbot` row or `null`, because `folder` is declared as `kind: "one"`:

```ts
Chatbot: {
  partitionBy: "organizationId",
  columns: ["id", "name", "type", "groupId", "organizationId", /* ... */],
  relations: {
    folder: { kind: "one", to: "Chatbot", from: ["groupId"], toColumns: ["id"] },
    documents: { kind: "many", to: "Chatbot", from: ["id"], toColumns: ["groupId"] },
    organization: { kind: "one", to: "organization", from: ["organizationId"], toColumns: ["id"] },
  },
},
```

## The planner

The planner in `packages/query/src/plan.ts` turns a `Query` into a `PlannedQuery`. The Durable Object and the browser both use it. The planner rejects unsupported queries with a `QueryValidationError` that carries a `QueryProblem`. The planner does these steps:

1. Resolve the table in the compiled schema. Unknown tables fail with `unknown_table`.
2. Validate the predicate. Each node must name a known column. Each value must match the column kind. Each operator must be allowed for the kind. An `exists` node must name a relation of the current table; its `where` is validated against the target.
3. Validate the ordering. Each column must be a column of the primary table. Each column must be orderable.
4. Append every primary key column to the ordering when it is not present. This makes results deterministic.
5. Check the limit.
6. Resolve the includes, recursively. Each level is deduplicated by relation name and sorted. Each name must be a relation declared on the table of that level. Include filters are validated against the target.
7. Compute the canonical key: `canonicalJson(normalizedQuery)`.

The key is the identity of the query. Two clients that send equal queries share one materialization in the Durable Object. The browser also uses the key as the subscription id.

The planner enforces these limits:

| Constant               | Value | Problem reason        |
| ---------------------- | ----- | --------------------- |
| `MAX_LIMIT`            | 10000 | `limit_too_large`     |
| `MAX_IN_VALUES`        | 40    | `too_many_values`     |
| `MAX_PREDICATE_NODES`  | 60    | `predicate_too_large` |
| `MAX_PREDICATE_PARAMS` | 80    | `too_many_parameters` |
| `MAX_RELATION_DEPTH`   | 3     | `too_deep`            |

`MAX_PREDICATE_PARAMS` exists because Durable Object SQLite allows 100 bound parameters per statement. The compiled select adds a few parameters for the limit and the membership filter. An `in` node uses one parameter per value. A comparison or `like` node uses one parameter.

The operators allowed per value kind are:

| Kind                                                         | Allowed operators                       |
| ------------------------------------------------------------ | --------------------------------------- |
| `json`                                                       | `isNull`, `isNotNull`                   |
| `bytes`, `decimal`                                           | `eq`, `ne`, `in`, `isNull`, `isNotNull` |
| `string`                                                     | all operators, including `like`         |
| `bool`, `int`, `bigint`, `float`, `datetime`, `date`, `time` | all operators except `like`             |

Values must match the kind. A `bigint` value is a string of decimal digits. An `int` value is an integer number. An `in` node with no values fails with `empty_in`. Columns of kind `json`, `bytes`, and `decimal` are not orderable.

## SQL compilation

The compiler in `packages/query/src/sql.ts` produces parameterized SQL in two dialects. The `sqlite` dialect serves the local caches. Both sides store rows in identical tables named `t_<table>` (see `packages/schema/src/ddl.ts`). Every local table has a `__key` column that holds the JSON-encoded primary key. The `mysql` dialect serves server-side mutators. It uses backtick identifiers, plain `?` placeholders, and the primary key columns instead of `__key`.

An `exists` node compiles to a correlated `EXISTS (SELECT 1 FROM <target> r WHERE <join> AND <where>)` subquery over the relation columns. A NULL relation column never matches.

`compileSelect` produces the primary rows. The select list returns `__key` first, then every synced column. Columns of kind `bigint` are returned as `CAST(... AS TEXT)` so the wire shape stays a decimal string. The `membershipOf` option restricts the rows to the members of one subscription in the local `membership` table.

Placeholders carry the column affinity. A bare `?` has no affinity in SQLite, so a `bigint` literal bound as text would compare as TEXT against an INTEGER column. The compiler emits:

- `CAST(? AS INTEGER)` for `bool`, `int`, and `bigint`
- `CAST(? AS REAL)` for `float`
- `?` for every other kind

`bindScalar` converts a predicate literal to a parameter. Booleans become `0` or `1`. A `bigint` string becomes a number when it is a safe integer. Otherwise it stays a string.

`literalParam` converts a wire cell to a storage parameter. It throws `UnstorableValueError` for a `bigint` outside the signed 64-bit range. SQLite would store such a value as a lossy REAL. A `json` cell is stored as JSON text.

`compileIncludeSelect` produces the rows of one included relation at any depth. It selects from the target table where an `EXISTS` subquery matches the parent level's select on the relation columns, and applies the include filter. The parent level is the primary select or the parent include's select. Included rows are ordered by `__key`. `flattenIncludes` lists every include of a plan, parents first. `attachIncludes` nests the rows of every level under their parents and `flattenNode` produces the result object.

`compileWhere`, `compileRowsByColumns` and `compileChainCandidates` are the building blocks of incremental maintenance in the Durable Object: a predicate over one alias, the rows of a relation target for one parent row, and the primary rows that reach a changed row through a chain of `exists` relations. See [ivm.md](ivm.md).

## Local execution in the browser

The browser store (`packages/client/src/store.ts`) runs the same compiled statements. `readSubscription` reads the members of one subscription and attaches the included rows in memory. `readLocal` reads everything cached, without a membership filter. The `read` method on the client uses `readLocal`; its result is complete only for rows that live queries cover.

## What is not supported

The planner or the AST rejects these forms:

- Aggregates (`count`, `sum`, and so on).
- Subqueries other than `exists` over a declared relation.
- Joins other than declared relations. Includes and `exists` are the only joins.
- Offsets. Use a predicate on the ordered columns instead.
- Comparisons on `json` columns, other than NULL checks.
- Range comparisons and `like` on `bytes` and `decimal` columns.
- Ordering by `json`, `bytes`, or `decimal` columns.
- Ordering by columns of an included table. `orderBy` accepts only columns of the primary table.
- Ordering and limits inside includes. Included rows arrive ordered by `__key`.
- Relation depth above three, for both includes and `exists`.
- More than 40 values in `in`, more than 60 predicate nodes, or a limit above 10000.

A rejected query reaches the application as a `subscription_error` with code `unsupported_query`, or as a `StoreError` when the browser planner rejects it first. See [unsupported-behavior.md](unsupported-behavior.md) for the wider list.
