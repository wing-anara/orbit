/**
 * Public client API.
 *
 * ```ts
 * const client = await createOrbitClient({
 *   definition: sync,                       // from defineSyncSchema (types only)
 *   schema: artifact,                       // compiled orbit.schema.json
 *   url: "https://app.example.com/orbit",
 *   partition: organizationId,
 *   getToken: () => fetchSyncToken(),
 *   mutators,                               // from defineMutators (optional)
 *   pushUrl: "https://app.example.com/orbit/push",
 * })
 * const documents = client.liveQuery(queries.documents({ folderId }))
 * documents.subscribe(() => render(documents.getSnapshot().rows))
 * const { local, server } = client.mutate.renameDocument({ id, name })
 * ```
 *
 * Applications never see Durable Object ids, VStream positions or fills; they see typed rows,
 * a status object, and typed errors.
 */

import { Effect } from "effect"
import type { SyncSchema } from "@orbit/protocol"
import { WS_SUBPROTOCOL, WS_TOKEN_PREFIX } from "@orbit/protocol/client"
import type { DefinedMutators, MutatorArgs, MutatorDefinitions } from "@orbit/mutators"
import { NamedQueryCall, type IncludeShape, type ResultRow, type TypedQuery } from "@orbit/query"
import type { SyncSchemaDefinition } from "@orbit/schema"
import { flattenNode, type ResultNode } from "@orbit/query"

import { SqlDriverError, type AsyncSqlDriver, type StorageMode } from "./driver.ts"
import {
  ClientEngine,
  type LiveQueryHandle,
  type LiveQuerySnapshot,
  type LiveQueryStatus,
  type SyncStatus,
} from "./engine.ts"
import type { MutationEvent, MutationHandle } from "./mutations.ts"
import { openWorkerDriver } from "./worker/proxy.ts"

export interface OrbitClientConfig<D, M extends MutatorDefinitions<D> = Record<never, never>> {
  /** The sync schema definition; used for types only. */
  readonly definition: D
  /** The compiled artifact (must match the server's). */
  readonly schema: SyncSchema
  /** Base URL of the sync router, for example `https://app.example.com/orbit`. */
  readonly url: string
  readonly partition: string
  /** Produces a fresh token for every connection attempt. */
  readonly getToken: () => Promise<string>
  /** The client's own view of its identity, used only for local query resolution. */
  readonly subject?: string
  /** Custom mutators (see `defineMutators` in `@orbit/mutators`). */
  readonly mutators?: DefinedMutators<D, M>
  /** The application's mutation endpoint (`POST`, see `@orbit/protocol` `PushRequest`). */
  readonly pushUrl?: string
  /** `fetch` used by the push loop; defaults to the global one. */
  readonly fetch?: typeof fetch
  /** `opfs` (default, persistent, needs `worker`) or `memory`. */
  readonly storage?: StorageMode
  /** Factory for the SQLite worker; required unless `driver` is given. */
  readonly worker?: () => Worker
  /** A ready driver (tests and non-browser hosts). */
  readonly driver?: AsyncSqlDriver
  readonly databaseName?: string
  /** Overrides the client id persisted in the local database. */
  readonly clientId?: string
  readonly onLog?: (event: string, data: Record<string, unknown>) => void
  readonly makeWebSocket?: (url: string, protocols: ReadonlyArray<string>) => WebSocket
  readonly backoffMinMs?: number
  readonly backoffMaxMs?: number
  /** How long a released query stays cached and current (default five minutes). */
  readonly queryTtlMs?: number
  /** Heartbeat: ping interval and the pong timeout that closes a dead socket. */
  readonly pingIntervalMs?: number
  readonly pongTimeoutMs?: number
}

export interface LiveQueryResult<Row> {
  readonly status: LiveQueryStatus
  readonly rows: ReadonlyArray<Row>
  readonly error: LiveQuerySnapshot["error"]
  readonly cursor: number | null
}

export interface LiveQuery<Row> {
  readonly getSnapshot: () => LiveQueryResult<Row>
  readonly subscribe: (listener: () => void) => () => void
  readonly release: () => Promise<void>
}

/** Typed callers for the configured mutators: `client.mutate.rename({ id, name })`. */
export type Mutate<M> = {
  readonly [K in Extract<keyof M, string>]: (args: MutatorArgs<M, K>) => MutationHandle
}

export interface OrbitClient<D, M extends MutatorDefinitions<D> = Record<never, never>> {
  /** A live, locally served query that stays current through the sync protocol. */
  readonly liveQuery: <N extends string, I extends IncludeShape = {}>(
    query: TypedQuery<D, N, I> | NamedQueryCall<D, N, I>,
  ) => LiveQuery<ResultRow<D, N, I>>
  /** One-shot read of locally cached rows; complete only for rows covered by live queries. */
  readonly read: <N extends string, I extends IncludeShape = {}>(
    query: TypedQuery<D, N, I> | NamedQueryCall<D, N, I>,
  ) => Promise<ReadonlyArray<ResultRow<D, N, I>>>
  readonly mutate: Mutate<M>
  readonly onMutation: (listener: (event: MutationEvent) => void) => () => void
  readonly getStatus: () => SyncStatus
  readonly onStatus: (listener: () => void) => () => void
  readonly close: () => Promise<void>
  /** The persisted client id. */
  readonly clientId: string
}

/**
 * Flattens locally served rows (primary row plus included relations) into the result shape the
 * query promises. The row type comes from the query's phantom `_Row`; the store only holds the
 * columns the compiled schema declares, so the flattened object has that shape by construction.
 */
function toRows<D, N extends string, I extends IncludeShape>(
  query: TypedQuery<D, N, I> | NamedQueryCall<D, N, I>,
  rows: ReadonlyArray<ResultNode>,
): ReadonlyArray<ResultRow<D, N, I>>
function toRows(_query: unknown, rows: ReadonlyArray<ResultNode>): ReadonlyArray<object> {
  return rows.map(flattenNode)
}

export const isNamedQueryCall = <D, N extends string, I extends IncludeShape>(
  query: TypedQuery<D, N, I> | NamedQueryCall<D, N, I>,
): query is NamedQueryCall<D, N, I> => query instanceof NamedQueryCall

export class OrbitClientError extends Error {
  constructor(
    message: string,
    override readonly cause?: unknown,
  ) {
    super(message)
    this.name = "OrbitClientError"
  }
}

export const createOrbitClient = async <
  D extends SyncSchemaDefinition<never, unknown> | { readonly _tag: "SyncSchemaDefinition" },
  M extends MutatorDefinitions<D> = Record<never, never>,
>(
  config: OrbitClientConfig<D, M>,
): Promise<OrbitClient<D, M>> => {
  const requested = config.storage ?? "opfs"
  let mode: StorageMode = requested
  let driver = config.driver
  if (driver === undefined) {
    if (config.worker === undefined)
      throw new OrbitClientError("createOrbitClient needs either `driver` or `worker`")
    const name = config.databaseName ?? `orbit-${config.schema.app}-${config.partition}`
    try {
      driver = await openWorkerDriver(config.worker(), name, requested)
    } catch (e) {
      // OPFS is exclusive to one context per origin (another tab holds the pool) or unavailable
      // (private mode, unsupported browser). Fall back to an in-memory database so the tab still
      // works; persistence resumes once it can hold the pool. Any other error is reported.
      const recoverable =
        e instanceof SqlDriverError && (e.code === "locked" || e.code === "unsupported")
      if (!recoverable || requested === "memory")
        throw new OrbitClientError(
          `could not open the local database: ${e instanceof Error ? e.message : String(e)}`,
          e,
        )
      config.onLog?.("store.fallback", { from: requested, to: "memory", reason: e.message })
      mode = "memory"
      driver = await openWorkerDriver(config.worker(), name, "memory")
    }
  }
  const engine = new ClientEngine({
    schema: config.schema,
    partition: config.partition,
    driver,
    storageMode: mode,
    target: async () => {
      const token = await config.getToken()
      const base = config.url.replace(/^http/, "ws").replace(/\/$/, "")
      // The token travels as a subprotocol entry, not in the URL, so request logs never see it.
      return {
        url: `${base}/ws?partition=${encodeURIComponent(config.partition)}`,
        protocols: [WS_SUBPROTOCOL, `${WS_TOKEN_PREFIX}${encodeURIComponent(token)}`],
      }
    },
    ...(config.clientId === undefined ? {} : { clientId: config.clientId }),
    ...(config.subject === undefined ? {} : { subject: config.subject }),
    ...(config.mutators === undefined ? {} : { mutators: config.mutators }),
    ...(config.pushUrl === undefined ? {} : { pushUrl: config.pushUrl }),
    ...(config.fetch === undefined ? {} : { fetch: config.fetch }),
    ...(config.makeWebSocket === undefined ? {} : { makeWebSocket: config.makeWebSocket }),
    ...(config.onLog === undefined ? {} : { onLog: config.onLog }),
    ...(config.backoffMinMs === undefined ? {} : { backoffMinMs: config.backoffMinMs }),
    ...(config.backoffMaxMs === undefined ? {} : { backoffMaxMs: config.backoffMaxMs }),
    ...(config.queryTtlMs === undefined ? {} : { queryTtlMs: config.queryTtlMs }),
    ...(config.pingIntervalMs === undefined ? {} : { pingIntervalMs: config.pingIntervalMs }),
    ...(config.pongTimeoutMs === undefined ? {} : { pongTimeoutMs: config.pongTimeoutMs }),
  })
  try {
    await Effect.runPromise(engine.open())
  } catch (e) {
    throw new OrbitClientError(
      `could not open the local store: ${e instanceof Error ? e.message : String(e)}`,
      e,
    )
  }

  const subscribe = <N extends string, I extends IncludeShape>(
    query: TypedQuery<D, N, I> | NamedQueryCall<D, N, I>,
  ): Effect.Effect<LiveQueryHandle, Error> =>
    isNamedQueryCall(query)
      ? Effect.try(() => query.resolve(engine.queryContext).ast).pipe(
          Effect.flatMap((local) => engine.subscribeNamed(query.ref, local)),
        )
      : engine.subscribe(query.ast)

  const liveQuery = <N extends string, I extends IncludeShape = {}>(
    query: TypedQuery<D, N, I> | NamedQueryCall<D, N, I>,
  ): LiveQuery<ResultRow<D, N, I>> => {
    const handlePromise = Effect.runPromise(subscribe(query))
    let handle: LiveQueryHandle | null = null
    let cached: LiveQueryResult<ResultRow<D, N, I>> = {
      status: "pending",
      rows: [],
      error: null,
      cursor: null,
    }
    let lastSnapshot: LiveQuerySnapshot | null = null
    const listeners = new Set<() => void>()
    let unsubscribe: (() => void) | null = null
    void handlePromise.then(
      (h) => {
        handle = h
        unsubscribe = h.subscribe(() => {
          for (const l of listeners) l()
        })
        for (const l of listeners) l()
      },
      (e: unknown) => {
        cached = {
          status: "error",
          rows: [],
          error: { code: "unsupported_query", message: e instanceof Error ? e.message : String(e) },
          cursor: null,
        }
        for (const l of listeners) l()
      },
    )
    return {
      getSnapshot: () => {
        if (handle === null) return cached
        const snap = handle.getSnapshot()
        if (snap !== lastSnapshot) {
          lastSnapshot = snap
          cached = {
            status: snap.status,
            rows: toRows(query, snap.rows),
            error: snap.error,
            cursor: snap.cursor,
          }
        }
        return cached
      },
      subscribe: (listener) => {
        listeners.add(listener)
        return () => listeners.delete(listener)
      },
      release: async () => {
        unsubscribe?.()
        const h = handle ?? (await handlePromise.catch(() => null))
        if (h !== null) await Effect.runPromise(h.release())
      },
    }
  }

  const read = async <N extends string, I extends IncludeShape = {}>(
    query: TypedQuery<D, N, I> | NamedQueryCall<D, N, I>,
  ): Promise<ReadonlyArray<ResultRow<D, N, I>>> => {
    const ast = isNamedQueryCall(query) ? query.resolve(engine.queryContext).ast : query.ast
    return toRows(query, await Effect.runPromise(engine.readLocal(ast)))
  }

  const mutate = mutateMap<M>(config.mutators, (name, args) => engine.mutate(name, args))

  return {
    liveQuery,
    read,
    mutate,
    onMutation: (listener) => engine.onMutation(listener),
    getStatus: () => engine.getStatus(),
    onStatus: (listener) => engine.onStatus(listener),
    close: () => Effect.runPromise(engine.close()),
    clientId: engine.clientId,
  }
}

/** Builds the typed `mutate` map from the definition keys. */
function mutateMap<M>(
  mutators: DefinedMutators<unknown, MutatorDefinitions<unknown>> | undefined,
  call: (name: string, args: unknown) => MutationHandle,
): Mutate<M>
function mutateMap(
  mutators: DefinedMutators<unknown, MutatorDefinitions<unknown>> | undefined,
  call: (name: string, args: unknown) => MutationHandle,
): Record<string, (args: unknown) => MutationHandle> {
  const out: Record<string, (args: unknown) => MutationHandle> = {}
  if (mutators === undefined) return out
  for (const name of Object.keys(mutators.definitions)) out[name] = (args) => call(name, args)
  return out
}
