/**
 * Custom mutators (the write side of the "custom queries" model).
 *
 * A mutator is a named function from validated arguments to writes, expressed against the
 * `MutationTx` interface. The same definition runs twice:
 * * in the browser, against the local cache, immediately (optimistic);
 * * on the application's server, against the authoritative database, inside one transaction
 *   together with the bookkeeping row in `orbit_clients` (see `@orbit/mutators/server`).
 *
 * The server is authoritative. Its effects come back through the sync protocol, and the
 * confirmation is the synced `orbit_clients` row: when `last_mutation_id` reaches the mutation's
 * id, the optimistic overlay for that mutation is dropped in the same local transaction that
 * applies the confirmed rows, so the UI never flickers (see `docs/mutations.md`).
 *
 * Authorization lives in the mutator: `ctx.subject` and `ctx.partition` come from the server's
 * session, never from the client, and a mutator throws to refuse a write.
 *
 * ```ts
 * const define = defineMutator(sync)
 * export const mutators = defineMutators(sync, {
 *   renameDocument: define(
 *     Schema.Struct({ id: Schema.String, name: Schema.String }),
 *     async (tx, { id, name }, ctx) => {
 *       const doc = await tx.get("Chatbot", { id })
 *       if (doc === null || doc.organizationId !== ctx.partition) throw new Error("not found")
 *       await tx.update("Chatbot", { id }, { name, updatedAt: ctx.now })
 *     },
 *   ),
 * })
 * ```
 */

import { Data, Result, Schema } from "effect"
import { JsonValue } from "@orbit/protocol"
import type { IncludeShape, ResultRow, TypedQuery } from "@orbit/query"
import type { KeyOf, RowOf, SyncedTables } from "@orbit/schema"

export interface MutationContext {
  readonly partition: string
  /** The authenticated caller on the server; the client's own view of its identity locally. */
  readonly subject: string | null
  readonly clientId: string
  readonly mutationId: number
  /** Wall-clock time in the wire datetime format (`YYYY-MM-DD HH:MM:SS.mmm`). */
  readonly now: string
  readonly side: "client" | "server"
}

/** Writes and reads a mutator may do. Every call is scoped to the mutation's partition. */
export interface MutationTx<D> {
  /** Inserts a full row (every synced column; columns the server defaults may be omitted). */
  readonly insert: <N extends SyncedTables<D>>(
    table: N,
    row: Partial<RowOf<D, N>> & KeyOf<D, N>,
  ) => Promise<void>
  /** Inserts many rows; the server sends one statement per chunk instead of one per row. */
  readonly insertMany: <N extends SyncedTables<D>>(
    table: N,
    rows: ReadonlyArray<Partial<RowOf<D, N>> & KeyOf<D, N>>,
  ) => Promise<void>
  readonly update: <N extends SyncedTables<D>>(
    table: N,
    key: KeyOf<D, N>,
    patch: Partial<RowOf<D, N>>,
  ) => Promise<void>
  readonly delete: <N extends SyncedTables<D>>(table: N, key: KeyOf<D, N>) => Promise<void>
  readonly get: <N extends SyncedTables<D>>(
    table: N,
    key: KeyOf<D, N>,
  ) => Promise<RowOf<D, N> | null>
  readonly query: <N extends string, I extends IncludeShape>(
    query: TypedQuery<D, N, I>,
  ) => Promise<ReadonlyArray<ResultRow<D, N, I>>>
}

/** Any codec that decodes without services; `Codec` is covariant, so specific codecs fit. */
export type ArgsSchema = Schema.Codec<unknown, unknown>

export interface MutatorDefinition<D, S extends ArgsSchema> {
  readonly args: S
  // A method signature keeps `args` bivariant so definitions with specific argument types fit
  // the `MutatorDefinitions` record.
  apply(tx: MutationTx<D>, args: S["Type"], ctx: MutationContext): Promise<void>
}

export type MutatorDefinitions<D> = Record<string, MutatorDefinition<D, ArgsSchema>>

export interface DefinedMutators<D, M extends MutatorDefinitions<D>> {
  readonly _tag: "DefinedMutators"
  readonly definitions: M
}

/** Argument type of one mutator, for typed client and server callers. */
export type MutatorArgs<M, K extends keyof M> =
  M[K] extends MutatorDefinition<infer _D, infer S> ? S["Type"] : never

/** Wire arguments of one mutator. */
export type MutatorWireArgs<M, K extends keyof M> =
  M[K] extends MutatorDefinition<infer _D, infer S> ? S["Encoded"] : never

/**
 * Builds one definition with the argument type inferred from the codec:
 * `const define = defineMutator(sync); define(Schema.Struct({...}), async (tx, args, ctx) => ...)`.
 */
export const defineMutator =
  <D extends { readonly _tag: "SyncSchemaDefinition" }>(_definition: D) =>
  <S extends ArgsSchema>(
    args: S,
    apply: (tx: MutationTx<D>, args: S["Type"], ctx: MutationContext) => Promise<void>,
  ): MutatorDefinition<D, S> => ({ args, apply })

export const defineMutators = <
  D extends { readonly _tag: "SyncSchemaDefinition" },
  const M extends MutatorDefinitions<D>,
>(
  _definition: D,
  definitions: M,
): DefinedMutators<D, M> => ({ _tag: "DefinedMutators", definitions })

export class MutatorError extends Data.TaggedError("MutatorError")<{
  readonly name: string
  readonly reason: "unknown_mutator" | "invalid_args" | "apply_failed"
  readonly message: string
}> {}

const isJsonObject = (v: JsonValue): v is { readonly [key: string]: JsonValue } =>
  typeof v === "object" && v !== null && !Array.isArray(v)

/** `undefined` properties are absent on the wire, as `JSON.stringify` treats them. */
const dropUndefined = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(dropUndefined)
  if (value !== null && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .filter(([, v]) => v !== undefined)
        .map(([k, v]) => [k, dropUndefined(v)]),
    )
  return value
}

/** Wire form of encoded arguments: a JSON object, or `{ value }` for a scalar or array codec. */
export const wireArgs = (encoded: unknown): Record<string, JsonValue> => {
  const json: JsonValue = Schema.decodeUnknownSync(JsonValue)(dropUndefined(encoded))
  return isJsonObject(json) ? { ...json } : { value: json }
}

/** Decodes wire arguments back to the mutator's argument type; `{ value }` unwraps. */
export const decodeArgs = <D>(
  mutators: DefinedMutators<D, MutatorDefinitions<D>>,
  name: string,
  args: Record<string, JsonValue>,
): { readonly definition: MutatorDefinition<D, ArgsSchema>; readonly args: unknown } => {
  const definition = Object.hasOwn(mutators.definitions, name)
    ? mutators.definitions[name]
    : undefined
  if (definition === undefined)
    throw new MutatorError({ name, reason: "unknown_mutator", message: `unknown mutator ${name}` })
  const direct = Schema.decodeUnknownResult(definition.args)(args)
  if (Result.isSuccess(direct)) return { definition, args: direct.success }
  const unwrapped =
    "value" in args && Object.keys(args).length === 1
      ? Schema.decodeUnknownResult(definition.args)(args["value"])
      : direct
  if (Result.isSuccess(unwrapped)) return { definition, args: unwrapped.success }
  throw new MutatorError({
    name,
    reason: "invalid_args",
    message: unwrapped.failure.message.slice(0, 500),
  })
}

/** Encodes arguments for the wire, validating them. */
export const encodeArgs = <D>(
  mutators: DefinedMutators<D, MutatorDefinitions<D>>,
  name: string,
  args: unknown,
): Record<string, JsonValue> => {
  const definition = Object.hasOwn(mutators.definitions, name)
    ? mutators.definitions[name]
    : undefined
  if (definition === undefined)
    throw new MutatorError({ name, reason: "unknown_mutator", message: `unknown mutator ${name}` })
  return wireArgs(Schema.encodeUnknownSync(definition.args)(args))
}

/** Wire datetime for `MutationContext.now`. */
export const nowWire = (date: Date = new Date()): string =>
  date.toISOString().replace("T", " ").replace("Z", "")
