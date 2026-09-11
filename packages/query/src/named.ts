/**
 * Named queries: the application defines every query the client may run, as a function from
 * validated arguments and the caller's identity to a typed query. The client refers to a query
 * by name and arguments; the server resolves it with the authoritative identity. This is the
 * authorization model: the resolver decides what a caller sees, so no row-level rules are
 * needed on the client (see `docs/auth.md` and `docs/queries.md`).
 *
 * The same definitions run on the client (for an immediate local result) and on the server (for
 * the materialized subscription). The client adopts the server's resolved query when it differs.
 *
 * ```ts
 * export const queries = defineQueries(sync, {
 *   documents: {
 *     args: Schema.Struct({ folderId: Schema.NullOr(Schema.String) }),
 *     query: (ctx, { folderId }) =>
 *       q(sync).from("Chatbot").where((c) => c.and(c.eq("organizationId", ctx.partition), ...)),
 *   },
 * })
 * client.liveQuery(queries.documents({ folderId }))
 * ```
 */

import { Result, Schema } from "effect"
import { JsonValue, type NamedQueryRef, type Query } from "@orbit/protocol"

import type { IncludeShape, TypedQuery } from "./builder.ts"

/** Who is asking. `subject` is null when the server did not authenticate the caller. */
export interface QueryContext {
  readonly partition: string
  readonly subject: string | null
  readonly clientId: string | null
}

/** Any codec that decodes without services; `Codec` is covariant, so specific codecs fit. */
export type ArgsSchema = Schema.Codec<unknown, unknown>

export interface QueryDefinition<
  D,
  S extends ArgsSchema,
  N extends string,
  I extends IncludeShape,
> {
  /** Argument codec; the server decodes the wire arguments with it before resolving. */
  readonly args: S
  // A method signature keeps `args` bivariant so definitions with specific argument types fit
  // the `QueryDefinitions` record.
  query(ctx: QueryContext, args: S["Type"]): TypedQuery<D, N, I>
}

/** A named query with bound arguments, as produced by a caller. */
export class NamedQueryCall<D, N extends string, I extends IncludeShape = {}> {
  declare readonly _Row: TypedQuery<D, N, I>["_Row"]

  constructor(
    readonly ref: NamedQueryRef,
    /** Resolves locally with the caller's own view of its identity. */
    readonly resolve: (ctx: QueryContext) => TypedQuery<D, N, I>,
  ) {}
}

export type QueryDefinitions<D> = Record<
  string,
  QueryDefinition<D, ArgsSchema, string, IncludeShape>
>

/**
 * Builds one definition with the argument type inferred from the codec:
 * `const define = defineQuery(sync); define(Schema.Struct({ id: Schema.String }), (ctx, { id }) => ...)`.
 */
export const defineQuery =
  <D extends { readonly _tag: "SyncSchemaDefinition" }>(_definition: D) =>
  <S extends ArgsSchema, N extends string, I extends IncludeShape>(
    args: S,
    query: (ctx: QueryContext, args: S["Type"]) => TypedQuery<D, N, I>,
  ): QueryDefinition<D, S, N, I> => ({ args, query })

export type QueryCallers<D, Q extends QueryDefinitions<D>> = {
  readonly [K in keyof Q]: Q[K] extends QueryDefinition<D, infer S, infer N, infer I>
    ? (args: S["Type"]) => NamedQueryCall<D, N, I>
    : never
}

/** The callers by name, plus the definitions the server resolves against. */
export type DefinedQueries<D, Q extends QueryDefinitions<D>> = QueryCallers<D, Q> & {
  readonly _tag: "DefinedQueries"
  readonly definitions: Q
}

/**
 * Definition-erased views for code that resolves queries without knowing the schema type (the
 * Durable Object). `args: unknown` keeps the method assignable from any concrete definition.
 */
export interface AnyQueryDefinition {
  readonly args: ArgsSchema
  query(ctx: QueryContext, args: unknown): { readonly ast: Query }
}

export interface AnyDefinedQueries {
  readonly _tag: "DefinedQueries"
  readonly definitions: Record<string, AnyQueryDefinition>
}

export type ResolveProblem =
  | { readonly reason: "unknown_query"; readonly name: string }
  | { readonly reason: "invalid_args"; readonly name: string; readonly message: string }
  | { readonly reason: "resolver_failed"; readonly name: string; readonly message: string }

/** Resolves a wire reference to a query using the given identity. */
export const resolveNamedQuery = (
  queries: AnyDefinedQueries,
  ref: NamedQueryRef,
  ctx: QueryContext,
): Result.Result<Query, ResolveProblem> => {
  const def = Object.hasOwn(queries.definitions, ref.name)
    ? queries.definitions[ref.name]
    : undefined
  if (def === undefined) return Result.fail({ reason: "unknown_query", name: ref.name })
  const decoded = Schema.decodeUnknownResult(def.args)(ref.args)
  if (Result.isFailure(decoded))
    return Result.fail({
      reason: "invalid_args",
      name: ref.name,
      message: decoded.failure.message.slice(0, 500),
    })
  try {
    return Result.succeed(def.query(ctx, decoded.success).ast)
  } catch (e) {
    return Result.fail({
      reason: "resolver_failed",
      name: ref.name,
      message: e instanceof Error ? e.message : String(e),
    })
  }
}

const isJsonObject = (v: JsonValue): v is { readonly [key: string]: JsonValue } =>
  typeof v === "object" && v !== null && !Array.isArray(v)

/** Wire form of encoded arguments: a JSON object, or `{ value }` for a scalar or array codec. */
const wireArgs = (encoded: unknown): Record<string, JsonValue> => {
  const json: JsonValue = Schema.decodeUnknownSync(JsonValue)(encoded)
  return isJsonObject(json) ? { ...json } : { value: json }
}

export const defineQueries = <
  D extends { readonly _tag: "SyncSchemaDefinition" },
  const Q extends QueryDefinitions<D>,
>(
  _definition: D,
  definitions: Q,
): DefinedQueries<D, Q> => {
  const callers: Record<string, (args: unknown) => NamedQueryCall<D, string, IncludeShape>> = {}
  for (const [name, def] of Object.entries(definitions)) {
    callers[name] = (args) => {
      // Encoding validates the arguments and produces the wire form in one step.
      const encoded = Schema.encodeUnknownSync(def.args)(args)
      const wire: Record<string, JsonValue> = wireArgs(encoded)
      const decodedArgs: unknown = Schema.decodeUnknownSync(def.args)(encoded)
      return new NamedQueryCall({ name, args: wire }, (ctx) => def.query(ctx, decodedArgs))
    }
  }
  // The caller map is built from the definition keys, so it has exactly the typed shape.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion
  return { ...(callers as QueryCallers<D, Q>), _tag: "DefinedQueries", definitions }
}
