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
import type { MutationOutcome, SyncSchema } from "@orbit/protocol"
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
  /** OPFS pool namespace; callers sharing a database must also coordinate its owner. */
  readonly pool?: string
  /** Disable the memory fallback when durable ownership is required. Default true. */
  readonly storageFallback?: boolean
  readonly signal?: AbortSignal
  /** Recover pending writes from this many pre-shared OPFS slots, without importing their views. */
  readonly legacySlots?: number
  /** Test seam for legacy slot recovery. */
  readonly openLegacySlot?: (slot: number) => Promise<AsyncSqlDriver | null>
  /**
   * How many tabs per origin keep a persistent database. Each tab holds its own OPFS pool
   * (slot), so an edit queued offline survives the tab that made it; the first tab pushes the
   * pending mutations that closed tabs left behind. Beyond this many tabs, a tab falls back to
   * memory. Default 4.
   */
  readonly tabs?: number
  /** Test seam: opens the database of a slot, or null when a tab holds it. */
  readonly openSlot?: (slot: number) => Promise<AsyncSqlDriver | null>
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
  /** Maximum socket upgrade and welcome wait; defaults to ten seconds. */
  readonly connectTimeoutMs?: number
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
  readonly awaitMutation: (id: number) => Promise<MutationOutcome>
  readonly mutate: Mutate<M>
  readonly onMutation: (listener: (event: MutationEvent) => void) => () => void
  readonly getStatus: () => SyncStatus
  readonly onStatus: (listener: () => void) => () => void
  readonly close: () => Promise<void>
  /** The persisted client id. */
  readonly clientId: string
}

type ResultCache = {
  readonly nodes: WeakMap<ResultNode, object>
  readonly arrays: WeakMap<ReadonlyArray<ResultNode>, ReadonlyArray<object>>
}

/**
 * Flattens locally served rows (primary row plus included relations) into the result shape the
 * query promises. The row type comes from the query's phantom `_Row`; the store only holds the
 * columns the compiled schema declares, so the flattened object has that shape by construction.
 */
function toRows<D, N extends string, I extends IncludeShape>(
  query: TypedQuery<D, N, I> | NamedQueryCall<D, N, I>,
  rows: ReadonlyArray<ResultNode>,
  cache: ResultCache,
): ReadonlyArray<ResultRow<D, N, I>>
function toRows(
  _query: unknown,
  rows: ReadonlyArray<ResultNode>,
  cache: ResultCache,
): ReadonlyArray<object> {
  const previous = cache.arrays.get(rows)
  if (previous !== undefined) return previous
  const result = rows.map((node) => {
    const existing = cache.nodes.get(node)
    if (existing !== undefined) return existing
    const row = flattenNode(node)
    cache.nodes.set(node, row)
    return row
  })
  cache.arrays.set(rows, result)
  return result
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

/** The OPFS pool a tab slot holds; slot 0 is the pool name of single-tab databases. */
export const slotPool = (slot: number): string =>
  slot === 0 ? "orbit-sahpool" : `orbit-sahpool-${slot}`

const DEFAULT_TABS = 4
const DRAIN_TIMEOUT_MS = 60_000

const isLocked = (e: unknown): boolean => e instanceof SqlDriverError && e.code === "locked"

const pendingCount = async (driver: AsyncSqlDriver): Promise<number> => {
  try {
    const rows = await driver.query(`SELECT count(*) AS n FROM pending_mutations`)
    const n = rows[0]?.["n"]
    return typeof n === "number" ? n : 0
  } catch {
    return 0
  }
}

/** Resolves when the drain engine's pending log is confirmed, or after the drain timeout. */
const drained = (drain: ClientEngine, signal?: AbortSignal): Promise<void> =>
  new Promise<void>((resolve) => {
    let off: (() => void) | null = null
    const done = (): void => {
      clearTimeout(timer)
      signal?.removeEventListener("abort", done)
      off?.()
      resolve()
    }
    const timer = setTimeout(done, DRAIN_TIMEOUT_MS)
    off = drain.onStatus(() => {
      signal?.addEventListener("abort", done, { once: true })
      if (signal?.aborted || drain.getStatus().pendingMutations === 0) done()
    })
    signal?.addEventListener("abort", done, { once: true })
    if (signal?.aborted || drain.getStatus().pendingMutations === 0) done()
  })

export const createOrbitClient = async <
  D extends SyncSchemaDefinition<never, unknown> | { readonly _tag: "SyncSchemaDefinition" },
  M extends MutatorDefinitions<D> = Record<never, never>,
>(
  config: OrbitClientConfig<D, M>,
): Promise<OrbitClient<D, M>> => {
  const recovery = new AbortController()
  const stopRecovery = (): void => recovery.abort()
  config.signal?.addEventListener("abort", stopRecovery, { once: true })
  const requested = config.storage ?? "opfs"
  const tabs = Math.max(1, config.tabs ?? DEFAULT_TABS)
  const poolForSlot = (s: number): string =>
    config.pool === undefined ? slotPool(s) : s === 0 ? config.pool : `${config.pool}-${s}`
  const databaseName = config.databaseName ?? `orbit-${config.schema.app}-${config.partition}`
  let mode: StorageMode = requested
  let driver = config.driver
  // The slot this tab holds; the first slot also pushes what closed tabs left behind.
  let slot = 0
  /** The database of another slot, or null while a tab holds it. */
  const openSlot =
    config.openSlot ??
    (async (s: number): Promise<AsyncSqlDriver | null> => {
      if (config.worker === undefined) return null
      try {
        return await openWorkerDriver(
          config.worker(),
          databaseName,
          "opfs",
          poolForSlot(s),
          config.signal,
        )
      } catch (e) {
        if (isLocked(e)) return null
        throw e
      }
    })
  if (driver === undefined) {
    if (config.worker === undefined)
      throw new OrbitClientError("createOrbitClient needs either `driver` or `worker`")
    try {
      if (requested === "memory")
        driver = await openWorkerDriver(
          config.worker(),
          databaseName,
          "memory",
          undefined,
          config.signal,
        )
      else {
        // An OPFS pool is exclusive to one context per origin. Each tab holds its own slot, so
        // its database (and the mutations it queued offline) outlives the tab.
        for (; slot < tabs && driver === undefined; slot++) {
          const opened = await openSlot(slot)
          if (opened !== null) {
            driver = opened
            break
          }
        }
        if (driver === undefined)
          throw new SqlDriverError({ code: "locked", message: `${tabs} tabs hold every slot` })
        if (slot > 0) config.onLog?.("store.slot", { slot, pool: poolForSlot(slot) })
      }
    } catch (e) {
      // Every slot is held by another tab, or OPFS is unavailable (private mode, unsupported
      // browser). Fall back to an in-memory database so the tab still works. Any other error is
      // reported.
      const recoverable =
        e instanceof SqlDriverError && (e.code === "locked" || e.code === "unsupported")
      if (!recoverable || requested === "memory" || config.storageFallback === false)
        throw new OrbitClientError(
          `could not open the local database: ${e instanceof Error ? e.message : String(e)}`,
          e,
        )
      config.onLog?.("store.fallback", { from: requested, to: "memory", reason: e.message })
      mode = "memory"
      driver = await openWorkerDriver(
        config.worker(),
        databaseName,
        "memory",
        undefined,
        config.signal,
      )
    }
  }
  const engineConfig = (d: AsyncSqlDriver, drainOnly: boolean) => ({
    schema: config.schema,
    partition: config.partition,
    driver: d,
    storageMode: mode,
    ...(drainOnly ? { drainOnly } : {}),
    target: async () => {
      const token = await config.getToken()
      const base = config.url.replace(/^http/, "ws").replace(/\/$/, "")
      // The token travels as a subprotocol entry, not in the URL, so request logs never see it.
      return {
        url: `${base}/ws?partition=${encodeURIComponent(config.partition)}`,
        protocols: [WS_SUBPROTOCOL, `${WS_TOKEN_PREFIX}${encodeURIComponent(token)}`],
      }
    },
    // A drained database keeps the client id of the tab that wrote it.
    ...(config.clientId === undefined || drainOnly ? {} : { clientId: config.clientId }),
    ...(config.subject === undefined ? {} : { subject: config.subject }),
    ...(config.mutators === undefined ? {} : { mutators: config.mutators }),
    ...(config.pushUrl === undefined ? {} : { pushUrl: config.pushUrl }),
    ...(config.fetch === undefined ? {} : { fetch: config.fetch }),
    ...(config.makeWebSocket === undefined ? {} : { makeWebSocket: config.makeWebSocket }),
    ...(config.onLog === undefined ? {} : { onLog: config.onLog }),
    ...(config.backoffMinMs === undefined ? {} : { backoffMinMs: config.backoffMinMs }),
    ...(config.backoffMaxMs === undefined ? {} : { backoffMaxMs: config.backoffMaxMs }),
    ...(config.queryTtlMs === undefined ? {} : { queryTtlMs: config.queryTtlMs }),
    ...(config.connectTimeoutMs === undefined ? {} : { connectTimeoutMs: config.connectTimeoutMs }),
    ...(config.pingIntervalMs === undefined ? {} : { pingIntervalMs: config.pingIntervalMs }),
    ...(config.pongTimeoutMs === undefined ? {} : { pongTimeoutMs: config.pongTimeoutMs }),
  })
  const engine = new ClientEngine(engineConfig(driver, false))
  try {
    await Effect.runPromise(engine.open())
  } catch (e) {
    throw new OrbitClientError(
      `could not open the local store: ${e instanceof Error ? e.message : String(e)}`,
      e,
    )
  }

  const abort = (): void => {
    void Effect.runPromise(engine.close()).catch(() => undefined)
  }
  config.signal?.addEventListener("abort", abort, { once: true })
  if (config.signal?.aborted) {
    await Effect.runPromise(engine.close())
    throw new OrbitClientError("Client creation aborted")
  }

  /**
   * Pushes the mutations that closed tabs queued offline in their own slots. Each orphaned
   * database is opened as the client that wrote it, connected until its pending log is
   * confirmed (or a timeout passes), and closed again.
   */
  const drainOrphans = async (): Promise<void> => {
    const legacy = Math.max(0, config.legacySlots ?? 0)
    const slots = [
      ...Array.from({ length: tabs - 1 }, (_, i) => ({ slot: i + 1, legacy: false })),
      ...Array.from({ length: legacy }, (_, i) => ({ slot: i, legacy: true })),
    ]
    for (const entry of slots) {
      if (recovery.signal.aborted) return
      const s = entry.slot
      let drain: ClientEngine | null = null
      let orphan: AsyncSqlDriver | null = null
      try {
        orphan = entry.legacy
          ? config.openLegacySlot !== undefined
            ? await config.openLegacySlot(s)
            : config.worker === undefined
              ? null
              : await openWorkerDriver(
                  config.worker(),
                  databaseName,
                  "opfs",
                  slotPool(s),
                  recovery.signal,
                )
          : await openSlot(s)
        if (orphan === null) continue
        const pending = await pendingCount(orphan)
        if (pending === 0) {
          await orphan.close()
          continue
        }
        if (entry.legacy) {
          const meta = await orphan.query(
            "SELECT key, value FROM meta WHERE key IN ('partition', 'schema_hash')",
          )
          const stored = new Map(meta.map((row) => [row["key"], row["value"]]))
          if (
            stored.get("partition") !== config.partition ||
            stored.get("schema_hash") !== config.schema.schema_hash
          ) {
            config.onLog?.("store.recovery_skipped", {
              slot: s,
              reason: "legacy identity or schema differs",
              pending,
            })
            continue
          }
        }
        config.onLog?.("store.drain", { slot: s, legacy: entry.legacy, pending })
        drain = new ClientEngine(engineConfig(orphan, true))
        await Effect.runPromise(drain.open())
        await drained(drain, recovery.signal)
        const remaining = await pendingCount(orphan)
        config.onLog?.("store.drained", { slot: s, pending, remaining })
      } catch (e) {
        config.onLog?.("store.drain_failed", {
          slot: s,
          error: e instanceof Error ? e.message : String(e),
        })
      } finally {
        if (drain !== null) await Effect.runPromise(drain.close()).catch(() => undefined)
        else await orphan?.close().catch(() => undefined)
      }
    }
  }
  if (mode === "opfs" && slot === 0 && config.mutators !== undefined) void drainOrphans()

  const subscribe = <N extends string, I extends IncludeShape>(
    query: TypedQuery<D, N, I> | NamedQueryCall<D, N, I>,
  ): Effect.Effect<LiveQueryHandle, Error> =>
    isNamedQueryCall(query)
      ? Effect.try(() => query.resolve(engine.queryContext).ast).pipe(
          Effect.flatMap((local) => engine.subscribeNamed(query.ref, local)),
        )
      : engine.subscribe(query.ast)

  // Published engine nodes are immutable; growing windows share their unchanged prefix.
  const resultCache: ResultCache = { nodes: new WeakMap(), arrays: new WeakMap() }
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
            rows: toRows(query, snap.rows, resultCache),
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
    return toRows(query, await Effect.runPromise(engine.readLocal(ast)), resultCache)
  }

  const mutate = mutateMap<M>(config.mutators, (name, args) => engine.mutate(name, args))

  return {
    liveQuery,
    read,
    mutate,
    awaitMutation: (id) => engine.awaitMutation(id),
    onMutation: (listener) => engine.onMutation(listener),
    getStatus: () => engine.getStatus(),
    onStatus: (listener) => engine.onStatus(listener),
    close: () => {
      recovery.abort()
      config.signal?.removeEventListener("abort", stopRecovery)
      config.signal?.removeEventListener("abort", abort)
      return Effect.runPromise(engine.close())
    },
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
