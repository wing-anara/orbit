import { encodeArgs, type MutatorArgs, type MutatorDefinitions } from "@orbit/mutators"
import type { MutationOutcome } from "@orbit/protocol"
import type { IncludeShape, NamedQueryCall, ResultRow, TypedQuery } from "@orbit/query"
import {
  isNamedQueryCall,
  type LiveQuery,
  type OrbitClient,
  type OrbitClientConfig,
} from "../client.ts"
import type { SyncStatus } from "../engine.ts"
import type { MutationEvent } from "../mutations.ts"
import { SharedCoordinator } from "./coordinator.ts"
import { openSharedOwner, type Definition } from "./owner.ts"
import { browserPlatform, deferred, sharedScope, type SharedPlatform } from "./platform.ts"
import {
  queryKey,
  SHARED_PROTOCOL_VERSION,
  type Command,
  type Event,
  type Snapshot,
  type WireQuery,
} from "./protocol.ts"

export interface SharedMutationHandle {
  /** Assigned by the shared owner; dense across all tabs. */
  readonly id: Promise<number>
  readonly local: Promise<void>
  readonly server: Promise<MutationOutcome>
}
export type SharedMutate<M> = {
  readonly [K in Extract<keyof M, string>]: (args: MutatorArgs<M, K>) => SharedMutationHandle
}
export type SharedOrbitClient<D, M extends MutatorDefinitions<D> = Record<never, never>> = Omit<
  OrbitClient<D, M>,
  "mutate"
> & { readonly mutate: SharedMutate<M> }
export type SharedOrbitClientConfig<
  D,
  M extends MutatorDefinitions<D> = Record<never, never>,
> = Omit<OrbitClientConfig<D, M>, "tabs" | "openSlot" | "pool" | "storageFallback"> & {
  /** Required even for anonymous clients: use an explicit, stable anonymous identity. */
  readonly subject: string
  /** Test/non-window host seam. */
  readonly platform?: SharedPlatform
}

export class SharedOwnerChangedError extends Error {
  constructor(readonly locallyCommitted: boolean) {
    super(
      locallyCommitted
        ? "Orbit owner changed; the durable mutation will resume, but its server outcome is not yet known"
        : "Orbit owner changed before acknowledging the mutation; check its state before retrying",
    )
    this.name = "SharedOwnerChangedError"
  }
}

export const createSharedOrbitClient = async <
  D extends Definition,
  M extends MutatorDefinitions<D> = Record<never, never>,
>(
  config: SharedOrbitClientConfig<D, M>,
): Promise<SharedOrbitClient<D, M>> => {
  if (config.subject.length === 0)
    throw new Error("Shared Orbit clients require a non-empty subject")
  const scope = await sharedScope({
    url: config.url,
    app: config.schema.app,
    partition: config.partition,
    subject: config.subject,
    ...(config.databaseName === undefined ? {} : { databaseName: config.databaseName }),
  })
  const platform = config.platform ?? browserPlatform()
  const initial = deferred<void>()
  const statusListeners = new Set<() => void>()
  const mutationListeners = new Set<(event: MutationEvent) => void>()
  const queries = new Map<
    string,
    {
      id: string
      refs: number
      wire: WireQuery
      key: string
      snapshot: Snapshot
      listeners: Set<() => void>
    }
  >()
  const queryIds = new Map<string, string>()
  const requests = new Map<
    string,
    {
      command: Command
      sent: boolean
      committed: boolean
      mutationId?: number | undefined
      receive: (event: Event) => void
      reject: (error: Error) => void
    }
  >()
  let clientId = ""
  let ready = false
  let closed = false
  let sequence = 0
  let status: SyncStatus = {
    connection: { status: "connecting", attempt: 0 },
    cursor: null,
    partition: config.partition,
    pendingSubscriptions: 0,
    pendingMutations: 0,
    lastDeltaAt: null,
    lastCommitTimestamp: null,
    storage: null,
    storageMode: config.storage ?? "opfs",
    fatalError: null,
  }
  const notify = (): void => {
    for (const listener of statusListeners) listener()
  }
  const flush = (): void => {
    if (!ready) return
    for (const request of requests.values())
      if (!request.sent) request.sent = coordinator.command(request.command)
  }
  const fail = (error: Error): void => {
    initial.reject(error)
    for (const request of requests.values()) request.reject(error)
    requests.clear()
    status = {
      ...status,
      connection: { status: "closed", reason: error.message, fatal: true, code: null },
    }
    notify()
  }
  const receive = (event: Event): void => {
    if (closed) return
    switch (event.type) {
      case "ready":
        if (clientId !== "" && clientId !== event.clientId) {
          for (const [id, request] of requests) {
            if (request.mutationId === undefined) continue
            request.reject(
              new Error(
                "Orbit durable client identity changed; mutation outcome cannot be recovered",
              ),
            )
            requests.delete(id)
          }
        }
        clientId = event.clientId
        status = event.status
        if (!ready) {
          ready = true
          for (const [id, entry] of queries)
            coordinator.command({ type: "subscribe", id, query: entry.wire })
          flush()
        }
        initial.resolve()
        notify()
        break
      case "status":
        status = event.status
        notify()
        break
      case "mutation":
        for (const listener of mutationListeners) listener(event.event)
        break
      case "snapshot": {
        const id = queryIds.get(event.id)
        const entry = id === undefined ? undefined : queries.get(id)
        if (entry === undefined) return
        entry.snapshot = event.snapshot
        for (const listener of entry.listeners) listener()
        break
      }
      default: {
        if (event.type === "error") {
          const query = queries.get(event.id)
          if (query !== undefined) {
            query.snapshot = {
              ...query.snapshot,
              status: "error",
              error: { code: "unsupported_query", message: event.message },
            }
            for (const listener of query.listeners) listener()
          }
        }
        requests.get(event.id)?.receive(event)
      }
    }
  }
  const changed = (): void => {
    ready = false
    for (const [id, request] of requests) {
      if (!request.sent) continue
      if (request.command.type === "read") {
        request.sent = false
        continue
      }
      if (request.committed && request.mutationId !== undefined) {
        request.command = { type: "awaitMutation", id, mutationId: request.mutationId }
        request.sent = false
        continue
      }
      request.reject(new SharedOwnerChangedError(request.committed))
      requests.delete(id)
    }
    for (const entry of queries.values()) {
      entry.snapshot = {
        ...entry.snapshot,
        status: entry.snapshot.status === "live" ? "stale" : entry.snapshot.status,
      }
      for (const listener of entry.listeners) listener()
    }
    status = {
      ...status,
      connection: {
        status: "reconnecting",
        attempt: 0,
        retryInMs: 0,
        lastError: "Shared owner changed",
      },
    }
    notify()
  }
  const makeCoordinator = (): SharedCoordinator =>
    new SharedCoordinator({
      scope,
      schema: JSON.stringify([config.schema.schema_hash, SHARED_PROTOCOL_VERSION]),
      platform,
      event: receive,
      failure: fail,
      open: async (send, signal) => {
        config.onLog?.("shared.owner", { scope })
        return openSharedOwner(
          { ...config, pool: scope, tabs: 1, storageFallback: false, signal },
          send,
        )
      },
      change: changed,
    })
  let coordinator = makeCoordinator()
  let suspended = false
  let suspension = Promise.resolve()
  const offLifecycle = platform.lifecycle?.(
    () => {
      if (closed || suspended) return
      suspended = true
      changed()
      suspension = coordinator.close(true)
    },
    () => {
      if (closed || !suspended) return
      suspended = false
      void suspension
        .then(async () => {
          if (closed || suspended) return
          coordinator = makeCoordinator()
          await coordinator.start()
        })
        .catch(fail)
    },
  )

  try {
    await coordinator.start()
    await initial.promise
  } catch (error) {
    offLifecycle?.()
    await coordinator.close()
    throw error
  }
  const checkOpen = (): void => {
    if (closed) throw new Error("Orbit client is closed")
  }
  const wire = <N extends string, I extends IncludeShape>(
    query: TypedQuery<D, N, I> | NamedQueryCall<D, N, I>,
  ): WireQuery =>
    isNamedQueryCall(query)
      ? {
          ref: query.ref,
          ast: query.resolve({ partition: config.partition, subject: config.subject, clientId })
            .ast,
        }
      : { ast: query.ast }
  function liveQuery<N extends string, I extends IncludeShape = {}>(
    query: TypedQuery<D, N, I> | NamedQueryCall<D, N, I>,
  ): LiveQuery<ResultRow<D, N, I>>
  function liveQuery(
    query: TypedQuery<D, string, IncludeShape> | NamedQueryCall<D, string, IncludeShape>,
  ): LiveQuery<object> {
    checkOpen()
    const resolved = wire(query)
    const key = queryKey(resolved)
    const existingId = queryIds.get(key)
    const existing = existingId === undefined ? undefined : queries.get(existingId)
    const entry = existing ?? {
      id: String(++sequence),
      refs: 0,
      wire: resolved,
      key,
      snapshot: { status: "pending", rows: [], error: null, cursor: null },
      listeners: new Set<() => void>(),
    }
    entry.refs++
    if (existing === undefined) {
      queries.set(entry.id, entry)
      queryIds.set(key, entry.id)
      if (ready) coordinator.command({ type: "subscribe", id: entry.id, query: entry.wire })
    }
    const listeners = new Set<() => void>()
    let released = false
    return {
      getSnapshot: () => entry.snapshot,
      subscribe: (listener: () => void) => {
        if (released) return () => undefined
        const wrapped = (): void => listener()
        listeners.add(wrapped)
        entry.listeners.add(wrapped)
        return () => {
          listeners.delete(wrapped)
          entry.listeners.delete(wrapped)
        }
      },
      release: async () => {
        if (released) return
        released = true
        for (const listener of listeners) entry.listeners.delete(listener)
        listeners.clear()
        if (--entry.refs > 0) return
        // React cleans up a prefetch before acquiring the visible query in the
        // same passive-effect flush. Preserve its ready snapshot through that
        // handoff, without retaining unused views beyond this microtask.
        await Promise.resolve()
        if (entry.refs > 0 || queries.get(entry.id) !== entry) return
        queries.delete(entry.id)
        queryIds.delete(key)
        if (ready) coordinator.command({ type: "release", id: entry.id })
      },
    }
  }

  function read<N extends string, I extends IncludeShape = {}>(
    query: TypedQuery<D, N, I> | NamedQueryCall<D, N, I>,
  ): Promise<ReadonlyArray<ResultRow<D, N, I>>>
  async function read(
    query: TypedQuery<D, string, IncludeShape> | NamedQueryCall<D, string, IncludeShape>,
  ): Promise<ReadonlyArray<object>> {
    checkOpen()
    const id = String(++sequence)
    const result = deferred<ReadonlyArray<object>>()
    requests.set(id, {
      command: { type: "read", id, query: wire(query) },
      sent: false,
      committed: false,
      reject: result.reject,
      receive: (event) => {
        if (event.type === "read") {
          requests.delete(id)
          result.resolve(event.rows)
        }
        if (event.type === "error") {
          requests.delete(id)
          result.reject(new Error(event.message))
        }
      },
    })
    flush()
    return result.promise
  }
  const awaitMutation = (mutationId: number): Promise<MutationOutcome> => {
    checkOpen()
    const id = String(++sequence),
      outcome = deferred<MutationOutcome>()
    requests.set(id, {
      command: { type: "awaitMutation", id, mutationId },
      sent: false,
      committed: true,
      mutationId,
      reject: outcome.reject,
      receive: (event) => {
        if (event.type === "outcome") {
          outcome.resolve(event.outcome)
          requests.delete(id)
        }
        if (event.type === "error") {
          outcome.reject(new Error(event.message))
          requests.delete(id)
        }
      },
    })
    flush()
    return outcome.promise
  }
  const mutate: Record<string, (args: unknown) => SharedMutationHandle> = {}
  for (const name of Object.keys(config.mutators?.definitions ?? {}))
    mutate[name] = (args) => {
      checkOpen()
      if (config.mutators === undefined) throw new Error("Mutators not configured")
      const encoded = encodeArgs(config.mutators, name, args)
      const id = String(++sequence)
      const allocated = deferred<number>(),
        local = deferred<void>(),
        server = deferred<MutationOutcome>()
      const request = {
        command: { type: "mutate", id, name, args: encoded } as Command,
        sent: false,
        committed: false,
        mutationId: undefined as number | undefined,
        reject: (error: Error) => {
          allocated.reject(error)
          local.reject(error)
          server.reject(error)
        },
        receive: (event: Event) => {
          switch (event.type) {
            case "allocated":
              request.mutationId = event.mutationId
              allocated.resolve(event.mutationId)
              break
            case "local":
              request.committed = true
              local.resolve()
              break
            case "outcome":
              server.resolve(event.outcome)
              requests.delete(id)
              break
            case "error":
              request.reject(new Error(event.message))
              requests.delete(id)
              break
            case "mutation":
            case "read":
            case "ready":
            case "snapshot":
            case "status":
              break
          }
        },
      }
      requests.set(id, request)
      flush()
      return { id: allocated.promise, local: local.promise, server: server.promise }
    }
  return {
    liveQuery,
    read,
    awaitMutation,
    mutate: typedMutators<M>(mutate),
    get clientId() {
      return clientId
    },
    getStatus: () => status,
    onStatus: (listener) => {
      statusListeners.add(listener)
      return () => statusListeners.delete(listener)
    },
    onMutation: (listener) => {
      mutationListeners.add(listener)
      return () => mutationListeners.delete(listener)
    },
    close: async () => {
      if (closed) return
      closed = true
      offLifecycle?.()
      await suspension
      const error = new Error("Orbit client is closed")
      for (const request of requests.values()) request.reject(error)
      requests.clear()
      queries.clear()
      queryIds.clear()
      await coordinator.close()
      statusListeners.clear()
      mutationListeners.clear()
    },
  }
}

function typedMutators<M>(
  value: Record<string, (args: unknown) => SharedMutationHandle>,
): SharedMutate<M>
function typedMutators(value: Record<string, (args: unknown) => SharedMutationHandle>): object {
  return value
}
