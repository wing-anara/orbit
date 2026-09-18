/**
 * The application's mutation endpoint: `POST` a `PushRequest`, get a `PushResponse`.
 *
 * Each mutation runs in its own database transaction that also writes the client's bookkeeping
 * row in `orbit_clients`. The row is written in the same transaction as the mutator's writes,
 * so the CDC transaction that carries the effects also carries the confirmation: a client that
 * syncs `orbit_clients` sees `last_mutation_id` reach the mutation's id in the very delta that
 * applies the mutation's rows, and drops its optimistic overlay atomically with them.
 *
 * Per mutation, in order:
 * 1. `SELECT last_mutation_id ... FOR UPDATE` locks the client's row (a missing row counts as 0).
 * 2. `id <= last`: `duplicate`, nothing is written.
 * 3. `id > last + 1`: the push stops and is `refused` as `out_of_order` with the server's
 *    `last_mutation_id`; the client resynchronizes from there.
 * 4. Otherwise the arguments are decoded, the mutator runs against a `MutationTx` over the
 *    transaction, the bookkeeping row is upserted, and the transaction commits: `applied`.
 * 5. When the arguments are invalid or the mutator throws, the transaction rolls back, a second
 *    transaction records `last_mutation_id = id` so the client drops the mutation, and the outcome
 *    is `failed` with the error message. RetryableMutationError instead leaves the id unconsumed.
 *
 * Runs on the Web standard `Request`/`Response`, so it fits Cloudflare Workers and Node alike.
 */

import {
  decodePushRequest,
  encodePushResponse,
  MUTATION_PROTOCOL_VERSION,
  ORBIT_CLIENTS_TABLE,
  type MutationOutcome,
  type PushRequest,
  type PushResponse,
  type SyncSchema,
} from "@orbit/protocol"
import { SchemaRuntime } from "@orbit/schema"

import {
  decodeArgs,
  nowWire,
  type DefinedMutators,
  type MutationContext,
  type MutatorDefinitions,
} from "../index.ts"
import type { PushDb, SqlTx } from "./db.ts"
import { createMysqlTx } from "./mysql-tx.ts"

export interface PushAuthorization {
  /** The authenticated caller; becomes `ctx.subject` for every mutator in the push. */
  readonly subject: string
}

export interface PushHandlerOptions<D, M extends MutatorDefinitions<D>> {
  readonly schema: SyncSchema
  readonly mutators: DefinedMutators<D, M>
  readonly db: PushDb
  /**
   * Authenticates the request and decides whether the caller may write to `body.partition`.
   * `null` refuses the whole push as `unauthorized`.
   */
  readonly authorize: (request: Request, body: PushRequest) => Promise<PushAuthorization | null>
}

export type PushHandler = (request: Request) => Promise<Response>

export const SELECT_LAST_MUTATION_SQL = `SELECT last_mutation_id FROM ${ORBIT_CLIENTS_TABLE} WHERE client_id = ? FOR UPDATE`

export const UPSERT_CLIENT_SQL =
  `INSERT INTO ${ORBIT_CLIENTS_TABLE} (client_id, partition_key, last_mutation_id, updated_at) VALUES (?, ?, ?, ?) ` +
  `ON DUPLICATE KEY UPDATE last_mutation_id = VALUES(last_mutation_id), partition_key = VALUES(partition_key), updated_at = VALUES(updated_at)`

/** Longest error text reported in a `failed` outcome. */
export const MAX_ERROR_LENGTH = 500

const JSON_HEADERS = { "content-type": "application/json" } as const

const json = (status: number, body: unknown): Response =>
  new Response(JSON.stringify(body), { status, headers: JSON_HEADERS })

const respond = (response: PushResponse): Response => json(200, encodePushResponse(response))

const errorText = (e: unknown): string =>
  (e instanceof Error ? e.message : String(e)).slice(0, MAX_ERROR_LENGTH)

/** Drivers return `bigint` columns as number, string or `bigint`; a missing row is 0. */
const toMutationId = (value: unknown): number => {
  if (typeof value === "number") return value
  if (typeof value === "string" || typeof value === "bigint") return Number(value)
  return 0
}

const readLast = async (tx: SqlTx, clientId: string): Promise<number> => {
  const rows = await tx.query(SELECT_LAST_MUTATION_SQL, [clientId])
  const first = rows[0]
  return first === undefined ? 0 : toMutationId(first["last_mutation_id"])
}

const recordLast = (tx: SqlTx, body: PushRequest, id: number): Promise<void> =>
  tx.execute(UPSERT_CLIENT_SQL, [body.clientId, body.partition, id, nowWire()])

/** Transient application failures leave the durable mutation queued for another push. */
export class RetryableMutationError extends Error {}

/** Marks an error raised by the mutator (or argument decoding), as opposed to the database. */
class ApplyFailure {
  constructor(readonly cause: unknown) {}
}

type Step =
  | { readonly kind: "applied" }
  | { readonly kind: "duplicate"; readonly last: number }
  | { readonly kind: "out_of_order"; readonly last: number }
  | { readonly kind: "failed"; readonly error: string }

export const createPushHandler = <D, M extends MutatorDefinitions<D>>(
  options: PushHandlerOptions<D, M>,
): PushHandler => {
  const rt = new SchemaRuntime(options.schema)
  const { db, mutators } = options

  const runOne = async (
    body: PushRequest,
    subject: string,
    mutation: PushRequest["mutations"][number],
  ): Promise<Step> => {
    const attempt = db.transaction(async (tx): Promise<Step> => {
      const last = await readLast(tx, body.clientId)
      if (mutation.id <= last) return { kind: "duplicate", last }
      if (mutation.id > last + 1) return { kind: "out_of_order", last }
      const ctx: MutationContext = {
        partition: body.partition,
        subject,
        clientId: body.clientId,
        mutationId: mutation.id,
        now: nowWire(),
        side: "server",
      }
      try {
        const { definition, args } = decodeArgs(mutators, mutation.name, mutation.args)
        await definition.apply(createMysqlTx<D>(rt, tx, mutation.name), args, ctx)
      } catch (e) {
        if (e instanceof RetryableMutationError) throw e
        throw new ApplyFailure(e)
      }
      await recordLast(tx, body, mutation.id)
      return { kind: "applied" }
    })
    const step = await attempt.catch((e: unknown): Step => {
      if (e instanceof ApplyFailure) return { kind: "failed", error: errorText(e.cause) }
      throw e
    })
    if (step.kind === "failed") await db.transaction((tx) => recordLast(tx, body, mutation.id))
    return step
  }

  const handle = async (request: Request): Promise<Response> => {
    let body: PushRequest
    try {
      body = decodePushRequest(await request.json())
    } catch (e) {
      return json(400, { error: `invalid push request: ${errorText(e)}` })
    }
    if (body.protocolVersion !== MUTATION_PROTOCOL_VERSION)
      return respond({
        type: "refused",
        reason: "protocol_version_mismatch",
        message: `server speaks mutation protocol ${MUTATION_PROTOCOL_VERSION}, client sent ${body.protocolVersion}`,
      })
    const auth = await options.authorize(request, body)
    if (auth === null)
      return respond({
        type: "refused",
        reason: "unauthorized",
        message: `not allowed to write to partition ${body.partition}`,
      })

    const outcomes: Array<MutationOutcome> = []
    let lastMutationId =
      body.mutations.length === 0 ? await db.transaction((tx) => readLast(tx, body.clientId)) : 0
    for (const mutation of body.mutations) {
      const step = await runOne(body, auth.subject, mutation)
      switch (step.kind) {
        case "applied":
          outcomes.push({ id: mutation.id, status: "applied" })
          lastMutationId = mutation.id
          break
        case "failed":
          outcomes.push({ id: mutation.id, status: "failed", error: step.error })
          lastMutationId = mutation.id
          break
        case "duplicate":
          outcomes.push({ id: mutation.id, status: "duplicate" })
          lastMutationId = Math.max(lastMutationId, step.last)
          break
        case "out_of_order":
          return respond({
            type: "refused",
            reason: "out_of_order",
            message: `mutation ${mutation.id} does not follow last_mutation_id ${step.last}`,
            lastMutationId: step.last,
          })
      }
    }
    return respond({ type: "ok", outcomes, lastMutationId })
  }

  return async (request) => {
    try {
      return await handle(request)
    } catch (e) {
      return json(500, { error: errorText(e) })
    }
  }
}
