/**
 * The client engine: owns the local store, the connection, the live queries and the mutations.
 *
 * Flow:
 * 1. `open()` migrates the local schema, loads persisted subscriptions, rebuilds the optimistic
 *    overlay from the pending mutation log, and connects.
 * 2. On every (re)connect the engine sends `hello` with its cursor and all active subscriptions
 *    (named query references or raw queries). The server answers with `subscribed`, carrying the
 *    query it resolved, and a fresh snapshot per subscription; the store applies each snapshot
 *    atomically as a diff against what it already has.
 * 3. Deltas are applied atomically, then every live query whose tables were touched is re-run
 *    against the local store and its listeners are notified.
 * 4. After every delta or snapshot the engine checks the synced `orbit_clients` row: pending
 *    mutations it covers are confirmed and the overlay is rebased (see `mutations.ts`).
 *
 * Live queries read the local store only. Their status tells the application whether the
 * result is `stale` (from a previous session, not yet confirmed), `live`, or `pending`.
 */

import { Effect, Exit, Result, Scope, Stream } from "effect"
import {
  isNamedQueryRef,
  ORBIT_CLIENTS_TABLE,
  type NamedQueryRef,
  type Query,
  type QueryRef,
  type SyncSchema,
} from "@orbit/protocol"
import {
  CLIENT_PROTOCOL_VERSION,
  type ClientMessage,
  type ServerMessage,
  type SyncError,
  type RowUpdate,
  type MemberRef,
} from "@orbit/protocol/client"
import type { DefinedMutators, MutatorDefinitions } from "@orbit/mutators"
import { canonicalJson } from "@orbit/schema"
import type { PlannedQuery, QueryContext, ResultNode } from "@orbit/query"

import { ConnectionError, makeConnection, type ConnectionState } from "./connection.ts"
import type { AsyncSqlDriver } from "./driver.ts"
import { AsyncLock, MutationManager, type MutationEvent, type MutationHandle } from "./mutations.ts"
import { LocalStore, StoreError } from "./store.ts"

export type LiveQueryStatus = "pending" | "stale" | "live" | "error"

export interface LiveQuerySnapshot {
  readonly status: LiveQueryStatus
  readonly rows: ReadonlyArray<ResultNode>
  readonly error: SyncError | null
  /** Cursor of the local state the rows were read at. */
  readonly cursor: number | null
}

export interface SyncStatus {
  readonly connection: ConnectionState
  readonly cursor: number | null
  readonly partition: string
  readonly pendingSubscriptions: number
  /** Mutations applied locally and not yet confirmed by the server through sync. */
  readonly pendingMutations: number
  readonly lastDeltaAt: number | null
  /** Server commit time of the last applied delta, unix seconds. */
  readonly lastCommitTimestamp: number | null
  readonly storage: {
    readonly usage: number | null
    readonly quota: number | null
    readonly databaseBytes: number | null
  } | null
  /** Which storage the local database uses; `memory` after a fallback (see `createOrbitClient`). */
  readonly storageMode: "opfs" | "memory"
  readonly fatalError: SyncError | null
}

interface Subscription {
  readonly id: string
  /** What the client sends on the wire. */
  readonly ref: QueryRef
  /** The current local plan; replaced when the server resolves the query differently. */
  planned: PlannedQuery
  status: LiveQueryStatus
  error: SyncError | null
  refs: number
  listeners: Set<() => void>
  snapshot: LiveQuerySnapshot
  pendingChunks: { rows: Array<RowUpdate>; members: Array<MemberRef> } | null
  /** Set while the subscription has no references and waits out `queryTtlMs`. */
  retention: ReturnType<typeof setTimeout> | null
}

export interface EngineConfig {
  readonly schema: SyncSchema
  readonly partition: string
  readonly driver: AsyncSqlDriver
  /** Resolves the WebSocket URL (with token) for each connection attempt. */
  readonly url: () => Promise<string>
  /** Overrides the client id persisted in the local database. */
  readonly clientId?: string
  /** The client's own view of its identity, for local resolution only; the server decides. */
  readonly subject?: string
  readonly mutators?: DefinedMutators<unknown, MutatorDefinitions<unknown>>
  readonly pushUrl?: string
  readonly fetch?: typeof fetch
  readonly storageMode?: "opfs" | "memory"
  readonly makeWebSocket?: (url: string) => WebSocket
  readonly backoffMinMs?: number
  readonly backoffMaxMs?: number
  /**
   * How long a query stays subscribed (and its rows cached and current) after its last
   * reference is released. Returning to a recently used query is then instant and offline.
   * Zero calls this the query TTL. Default five minutes; `0` retires queries at once.
   */
  readonly queryTtlMs?: number
  readonly pushBackoffMinMs?: number
  readonly pushBackoffMaxMs?: number
  readonly pingIntervalMs?: number
  readonly pongTimeoutMs?: number
  readonly onLog?: (event: string, data: Record<string, unknown>) => void
  readonly now?: () => number
}

/** The built-in named query that syncs this client's `orbit_clients` row. */
const DEFAULT_QUERY_TTL_MS = 5 * 60_000

export const clientRowQueryRef = (clientId: string): NamedQueryRef => ({
  name: "$orbit.client",
  args: { clientId },
})

const clientRowQuery = (clientId: string): Query => ({
  table: ORBIT_CLIENTS_TABLE,
  where: { op: "eq", column: "client_id", value: clientId },
})

/** Subscription id: the canonical reference for a named query, the canonical AST for a raw one. */
export const subscriptionIdOf = (ref: QueryRef, planned: PlannedQuery): string =>
  isNamedQueryRef(ref) ? canonicalJson({ name: ref.name, args: ref.args }) : planned.key

export class ClientEngine {
  readonly store: LocalStore
  /** Serializes store writes: message application, mutation apply, rebase, and local reads. */
  readonly lock = new AsyncLock()
  private readonly subscriptions = new Map<string, Subscription>()
  private status: SyncStatus
  private readonly statusListeners = new Set<() => void>()
  private scope: Scope.Closeable | null = null
  private send: ((m: ClientMessage) => Effect.Effect<boolean>) | null = null
  private cursor: number | null = null
  private opened = false
  private clientIdValue: string
  private mutations: MutationManager | null = null
  private onlineHandler: (() => void) | null = null

  constructor(private readonly config: EngineConfig) {
    this.store = new LocalStore(config.driver, config.schema, config.partition)
    this.clientIdValue = config.clientId ?? ""
    this.status = {
      connection: { status: "connecting", attempt: 0 },
      cursor: null,
      partition: config.partition,
      pendingSubscriptions: 0,
      pendingMutations: 0,
      lastDeltaAt: null,
      lastCommitTimestamp: null,
      storage: null,
      storageMode: config.storageMode ?? "memory",
      fatalError: null,
    }
  }

  /** The persisted client id (known after `open`). */
  get clientId(): string {
    return this.clientIdValue
  }

  /** The identity used for local named query resolution. */
  get queryContext(): QueryContext {
    return {
      partition: this.config.partition,
      subject: this.config.subject ?? null,
      clientId: this.clientIdValue === "" ? null : this.clientIdValue,
    }
  }

  private now(): number {
    return this.config.now?.() ?? Date.now()
  }

  private log(event: string, data: Record<string, unknown> = {}): void {
    this.config.onLog?.(event, data)
  }

  // ---------------------------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------------------------

  /** Opens the store, restores persisted subscriptions and mutations, and starts the connection. */
  open(): Effect.Effect<void, StoreError> {
    return Effect.gen({ self: this }, function* () {
      if (this.opened) return
      this.opened = true
      const opened = yield* this.store.open(
        this.config.clientId === undefined ? {} : { clientId: this.config.clientId },
      )
      this.cursor = opened.cursor
      this.clientIdValue = opened.clientId
      this.log("store.opened", {
        action: opened.action,
        cursor: opened.cursor,
        clientId: opened.clientId,
      })
      if (this.config.mutators !== undefined) {
        const manager = new MutationManager({
          store: this.store,
          mutators: this.config.mutators,
          clientId: opened.clientId,
          partition: this.config.partition,
          subject: this.config.subject ?? null,
          pushUrl: this.config.pushUrl,
          fetch: this.config.fetch,
          lock: this.lock,
          onLog: (event, data) => this.log(event, data),
          onChanged: (tables) => Effect.runPromise(this.refreshTables(tables, true)),
          onPendingCount: (n) => this.updateStatus({ pendingMutations: n }),
          now: () => this.now(),
          ...(this.config.pushBackoffMinMs === undefined
            ? {}
            : { backoffMinMs: this.config.pushBackoffMinMs }),
          ...(this.config.pushBackoffMaxMs === undefined
            ? {}
            : { backoffMaxMs: this.config.pushBackoffMaxMs }),
        })
        this.mutations = manager
        yield* Effect.tryPromise({
          try: () => manager.init(),
          catch: (e) =>
            new StoreError({
              message: `could not restore pending mutations: ${e instanceof Error ? e.message : String(e)}`,
              cause: e,
            }),
        })
      }
      for (const persisted of yield* this.store.subscriptions()) {
        const planned = this.store.plan(persisted.query)
        if (Result.isFailure(planned)) continue
        const sub = this.register(persisted.id, persisted.ref, planned.success, 0)
        sub.status = persisted.complete ? "stale" : "pending"
        yield* this.refresh(sub)
      }
      if (this.mutations !== null) yield* this.subscribeClientRow()
      this.updateStatus({ cursor: this.cursor })
      if (typeof window !== "undefined" && typeof window.addEventListener === "function") {
        this.onlineHandler = () => this.mutations?.kick()
        window.addEventListener("online", this.onlineHandler)
      }
      yield* this.connect()
      this.mutations?.kick()
    })
  }

  /** Subscribes to this client's `orbit_clients` row, which confirms its mutations. */
  private subscribeClientRow(): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      const query = clientRowQuery(this.clientIdValue)
      const planned = this.store.plan(query)
      if (Result.isFailure(planned)) {
        this.log("mutations.unconfirmable", {
          reason: `table ${ORBIT_CLIENTS_TABLE} is not in the sync schema; mutations are never confirmed`,
        })
        return
      }
      const ref = clientRowQueryRef(this.clientIdValue)
      const id = subscriptionIdOf(ref, planned.success)
      const fresh = !this.subscriptions.has(id)
      this.register(id, ref, planned.success, 1)
      if (fresh)
        yield* this.store
          .registerSubscription(id, ref, planned.success.query)
          .pipe(
            Effect.catch((e) => Effect.sync(() => this.log("store.failed", { error: e.message }))),
          )
    })
  }

  private connect(): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      const scope = yield* Scope.make()
      this.scope = scope
      const connection = yield* makeConnection({
        url: () =>
          Effect.tryPromise({
            try: () => this.config.url(),
            catch: (e) =>
              new ConnectionError({
                message: e instanceof Error ? e.message : String(e),
                code: null,
                fatal: false,
              }),
          }),
        onOpen: (send) => Effect.sync(() => this.sendHello(send)),
        ...(this.config.makeWebSocket === undefined
          ? {}
          : { makeWebSocket: this.config.makeWebSocket }),
        ...(this.config.backoffMinMs === undefined
          ? {}
          : { backoffMinMs: this.config.backoffMinMs }),
        ...(this.config.backoffMaxMs === undefined
          ? {}
          : { backoffMaxMs: this.config.backoffMaxMs }),
        ...(this.config.pingIntervalMs === undefined
          ? {}
          : { pingIntervalMs: this.config.pingIntervalMs }),
        ...(this.config.pongTimeoutMs === undefined
          ? {}
          : { pongTimeoutMs: this.config.pongTimeoutMs }),
      }).pipe(Scope.provide(scope))
      this.send = connection.send
      const consumer = connection.events.pipe(
        Stream.runForEach((event) =>
          event.type === "state"
            ? Effect.sync(() => this.onConnectionState(event.state))
            : this.onMessage(event.message).pipe(
                Effect.catch((e) =>
                  Effect.sync(() => this.log("message.failed", { error: e.message })),
                ),
              ),
        ),
      )
      yield* Effect.forkIn(consumer, scope)
    })
  }

  close(): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      for (const sub of this.subscriptions.values()) {
        if (sub.retention !== null) clearTimeout(sub.retention)
        sub.retention = null
      }
      const scope = this.scope
      this.scope = null
      this.mutations?.close()
      if (this.onlineHandler !== null) {
        window.removeEventListener("online", this.onlineHandler)
        this.onlineHandler = null
      }
      if (scope !== null) yield* Scope.close(scope, Exit.void)
      yield* this.store.close().pipe(Effect.ignore)
    })
  }

  private sendHello(send: (m: ClientMessage) => void): void {
    const summary = this.store.rt.summary()
    send({
      type: "hello",
      protocolVersion: CLIENT_PROTOCOL_VERSION,
      clientId: this.clientIdValue,
      token: "",
      partition: this.config.partition,
      schema: summary,
      cursor: this.cursor,
      subscriptions: [...this.subscriptions.values()].map((s) => ({
        type: "subscribe" as const,
        id: s.id,
        query: s.ref,
      })),
    })
    for (const s of this.subscriptions.values()) if (s.status === "live") s.status = "stale"
    this.notifyAll()
  }

  private onConnectionState(state: ConnectionState): void {
    this.log("connection", { status: state.status })
    if (state.status === "closed" && state.fatal) {
      const error: SyncError = {
        code:
          state.code === 4409
            ? "schema_mismatch"
            : state.code === 4403
              ? "partition_denied"
              : state.code === 4401
                ? "unauthorized"
                : state.code === 4400
                  ? "protocol_version_mismatch"
                  : "internal",
        message: state.reason,
      }
      this.updateStatus({ connection: state, fatalError: error })
      return
    }
    this.updateStatus({ connection: state })
    if (state.status === "open") this.mutations?.kick()
  }

  // ---------------------------------------------------------------------------------------------
  // Subscriptions and live queries
  // ---------------------------------------------------------------------------------------------

  private register(id: string, ref: QueryRef, planned: PlannedQuery, refs: number): Subscription {
    const existing = this.subscriptions.get(id)
    if (existing !== undefined) {
      existing.refs += refs
      if (existing.retention !== null && existing.refs > 0) {
        clearTimeout(existing.retention)
        existing.retention = null
      }
      return existing
    }
    const sub: Subscription = {
      id,
      ref,
      planned,
      status: "pending",
      error: null,
      refs,
      listeners: new Set(),
      snapshot: { status: "pending", rows: [], error: null, cursor: null },
      pendingChunks: null,
      retention: null,
    }
    this.subscriptions.set(id, sub)
    return sub
  }

  /** Subscribes to a raw query. Identical queries share one subscription. Returns the handle. */
  subscribe(query: Query): Effect.Effect<LiveQueryHandle, StoreError> {
    return this.subscribeWith(query, query)
  }

  /**
   * Subscribes to a named query. `local` is the query the client resolved itself; it renders at
   * once and is replaced by the server's resolution when `subscribed` arrives.
   */
  subscribeNamed(ref: NamedQueryRef, local: Query): Effect.Effect<LiveQueryHandle, StoreError> {
    return this.subscribeWith(ref, local)
  }

  private subscribeWith(ref: QueryRef, query: Query): Effect.Effect<LiveQueryHandle, StoreError> {
    return Effect.gen({ self: this }, function* () {
      const planned = this.store.plan(query)
      if (Result.isFailure(planned)) return yield* planned.failure
      const wire: QueryRef = isNamedQueryRef(ref) ? ref : planned.success.query
      const id = subscriptionIdOf(wire, planned.success)
      const fresh = !this.subscriptions.has(id)
      const sub = this.register(id, wire, planned.success, 1)
      if (fresh) {
        yield* this.store.registerSubscription(id, wire, planned.success.query)
        yield* this.refresh(sub)
        if (this.send !== null) yield* this.send({ type: "subscribe", id, query: wire })
        this.updateStatus({ pendingSubscriptions: this.pendingCount() })
      }
      return this.handle(sub)
    })
  }

  private handle(sub: Subscription): LiveQueryHandle {
    let released = false
    return {
      id: sub.id,
      getSnapshot: () => sub.snapshot,
      subscribe: (listener) => {
        sub.listeners.add(listener)
        return () => sub.listeners.delete(listener)
      },
      release: () => {
        if (released) return Effect.void
        released = true
        return this.release(sub)
      },
    }
  }

  private release(sub: Subscription): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      sub.refs -= 1
      if (sub.refs > 0) return
      const ttl = this.config.queryTtlMs ?? DEFAULT_QUERY_TTL_MS
      if (ttl <= 0) {
        yield* this.retire(sub)
        return
      }
      // Keep the subscription (and its rows) alive for the TTL; a new reference cancels this.
      const timer = setTimeout(() => {
        sub.retention = null
        if (sub.refs > 0 || this.subscriptions.get(sub.id) !== sub) return
        void Effect.runPromise(this.retire(sub))
      }, ttl)
      // Node timers must not keep a test process alive; browsers return a number here.
      if (typeof timer === "object" && "unref" in timer) timer.unref()
      sub.retention = timer
    })
  }

  /** Drops a subscription: tells the server, and garbage-collects rows only it referenced. */
  private retire(sub: Subscription): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      this.subscriptions.delete(sub.id)
      if (this.send !== null) yield* this.send({ type: "unsubscribe", id: sub.id })
      yield* this.lock.runEffect(this.store.removeSubscription(sub.id)).pipe(Effect.ignore)
      this.updateStatus({ pendingSubscriptions: this.pendingCount() })
    })
  }

  private pendingCount(): number {
    let n = 0
    for (const s of this.subscriptions.values()) if (s.status !== "live") n += 1
    return n
  }

  private refresh(sub: Subscription): Effect.Effect<void, StoreError> {
    return Effect.gen({ self: this }, function* () {
      // A subscription that has no snapshot yet has no membership to read through. Answer it
      // from the local cache at once (rows other subscriptions and pending mutations already
      // hold); the server's snapshot replaces the set when it arrives and the status turns live.
      const rows =
        sub.status === "pending"
          ? yield* this.store.readLocal(sub.planned)
          : yield* this.store.readSubscription(sub.planned, sub.id)
      sub.snapshot = { status: sub.status, rows, error: sub.error, cursor: this.cursor }
      for (const l of sub.listeners) l()
    })
  }

  /** Re-runs the queries over `tables` (every table when null); `any` includes non-live queries. */
  private refreshTables(
    tables: ReadonlySet<string> | null,
    any: boolean,
  ): Effect.Effect<void, StoreError> {
    return Effect.gen({ self: this }, function* () {
      for (const sub of this.subscriptions.values()) {
        if (!any && sub.status !== "live") continue
        if (sub.status === "error") continue
        if (tables !== null && ![...sub.planned.tables].some((t) => tables.has(t))) continue
        yield* this.refresh(sub)
      }
    })
  }

  private notifyAll(): void {
    for (const sub of this.subscriptions.values()) {
      sub.snapshot = { ...sub.snapshot, status: sub.status, error: sub.error }
      for (const l of sub.listeners) l()
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Protocol handling
  // ---------------------------------------------------------------------------------------------

  private onMessage(message: ServerMessage): Effect.Effect<void, StoreError> {
    return Effect.gen({ self: this }, function* () {
      switch (message.type) {
        case "welcome":
          this.log("welcome", { cursor: message.cursor, sessionId: message.sessionId })
          return
        case "subscribed": {
          const sub = this.subscriptions.get(message.id)
          if (sub === undefined) return
          yield* this.lock.runEffect(this.adoptServerQuery(sub, message.query))
          if (message.status === "pending") {
            sub.status =
              sub.snapshot.rows.length > 0 || sub.status === "stale" ? "stale" : "pending"
            sub.snapshot = { ...sub.snapshot, status: sub.status }
            for (const l of sub.listeners) l()
          }
          this.updateStatus({ pendingSubscriptions: this.pendingCount() })
          return
        }
        case "snapshot": {
          const sub = this.subscriptions.get(message.subscriptionId)
          if (sub === undefined) return
          const chunks = sub.pendingChunks ?? { rows: [], members: [] }
          chunks.rows.push(...message.rows)
          chunks.members.push(...message.members)
          if (!message.complete) {
            sub.pendingChunks = chunks
            return
          }
          sub.pendingChunks = null
          yield* this.lock.runEffect(
            Effect.gen({ self: this }, function* () {
              yield* this.store.applySnapshot(sub.id, message.cursor, chunks.rows, chunks.members)
              this.cursor = message.cursor
              sub.status = "live"
              sub.error = null
              const rebased = yield* this.confirmMutations()
              if (rebased) yield* this.refreshTables(null, true)
              else yield* this.refresh(sub)
            }),
          )
          this.updateStatus({ cursor: this.cursor, pendingSubscriptions: this.pendingCount() })
          this.log("snapshot.applied", {
            subscription: sub.id,
            rows: chunks.rows.length,
            cursor: message.cursor,
          })
          return
        }
        case "delta": {
          const started = this.now()
          yield* this.lock.runEffect(
            Effect.gen({ self: this }, function* () {
              yield* this.store.applyDelta(message.cursor, message.rows, message.memberships)
              this.cursor = message.cursor
              const rebased = yield* this.confirmMutations()
              if (rebased) {
                yield* this.refreshTables(null, true)
                return
              }
              const touched = new Set<string>(message.rows.map((r) => r.table))
              const touchedSubs = new Set<string>(message.memberships.map((m) => m.subscriptionId))
              for (const sub of this.subscriptions.values()) {
                if (sub.status !== "live") continue
                if (
                  !touchedSubs.has(sub.id) &&
                  ![...sub.planned.tables].some((t) => touched.has(t))
                )
                  continue
                yield* this.refresh(sub)
              }
            }),
          )
          if (this.send !== null) yield* this.send({ type: "ack", cursor: message.cursor })
          this.updateStatus({
            cursor: this.cursor,
            lastDeltaAt: this.now(),
            lastCommitTimestamp: message.origin.commitTimestamp,
          })
          this.log("delta.applied", {
            cursor: message.cursor,
            gtid: message.origin.gtid,
            rows: message.rows.length,
            applyMs: this.now() - started,
            appliedAtDo: message.origin.appliedAt,
          })
          return
        }
        case "subscription_error": {
          const sub = this.subscriptions.get(message.id)
          if (sub === undefined) return
          sub.status = "error"
          sub.error = message.error
          sub.snapshot = { ...sub.snapshot, status: "error", error: message.error }
          for (const l of sub.listeners) l()
          this.log("subscription.error", { subscription: sub.id, code: message.error.code })
          this.updateStatus({ pendingSubscriptions: this.pendingCount() })
          return
        }
        case "error":
          this.log("server.error", { code: message.error.code, fatal: message.fatal })
          if (message.fatal) this.updateStatus({ fatalError: message.error })
          return
        case "unsubscribed":
        case "pong":
          return
      }
    })
  }

  /**
   * The server's resolution of a subscription is authoritative: when it differs from the local
   * plan (for example it added a filter), the plan is replaced, persisted, and re-read.
   */
  private adoptServerQuery(sub: Subscription, query: Query): Effect.Effect<void, StoreError> {
    return Effect.gen({ self: this }, function* () {
      const planned = this.store.plan(query)
      if (Result.isFailure(planned)) {
        this.log("subscription.unplannable", {
          subscription: sub.id,
          error: planned.failure.message,
        })
        return
      }
      if (planned.success.key === sub.planned.key) return
      sub.planned = planned.success
      yield* this.store.updateSubscriptionQuery(sub.id, planned.success.query)
      this.log("subscription.resolved", { subscription: sub.id })
      yield* this.refresh(sub)
    })
  }

  /** Confirms and rebases pending mutations (lock held). Returns whether the overlay changed. */
  private confirmMutations(): Effect.Effect<boolean, StoreError> {
    const manager = this.mutations
    if (manager === null) return Effect.succeed(false)
    return Effect.tryPromise({
      try: () => manager.confirm(),
      catch: (e) =>
        new StoreError({
          message: `rebase failed: ${e instanceof Error ? e.message : String(e)}`,
          cause: e,
        }),
    })
  }

  // ---------------------------------------------------------------------------------------------
  // Mutations
  // ---------------------------------------------------------------------------------------------

  /** Applies a mutator locally and queues it for the push loop. */
  mutate(name: string, args: unknown): MutationHandle {
    const manager = this.mutations
    if (manager === null) throw new Error("createOrbitClient was called without `mutators`")
    return manager.mutate(name, args)
  }

  onMutation(listener: (event: MutationEvent) => void): () => void {
    return this.mutations?.onEvent(listener) ?? (() => undefined)
  }

  // ---------------------------------------------------------------------------------------------
  // Status
  // ---------------------------------------------------------------------------------------------

  getStatus(): SyncStatus {
    return this.status
  }

  onStatus(listener: () => void): () => void {
    this.statusListeners.add(listener)
    return () => this.statusListeners.delete(listener)
  }

  private updateStatus(patch: Partial<SyncStatus>): void {
    this.status = { ...this.status, ...patch }
    for (const l of this.statusListeners) l()
  }

  /** One-shot local read over everything cached (no server round trip). */
  readLocal(query: Query): Effect.Effect<ReadonlyArray<ResultNode>, StoreError> {
    return Effect.gen({ self: this }, function* () {
      const planned = this.store.plan(query)
      if (Result.isFailure(planned)) return yield* planned.failure
      return yield* this.lock.runEffect(this.store.readLocal(planned.success))
    })
  }

  /** For tests: waits until a subscription is live. */
  awaitLive(id: string, timeoutMs = 5000): Effect.Effect<void, StoreError> {
    return Effect.gen({ self: this }, function* () {
      const deadline = this.now() + timeoutMs
      while (this.subscriptions.get(id)?.status !== "live") {
        if (this.now() > deadline)
          return yield* new StoreError({ message: `subscription ${id} did not become live` })
        yield* Effect.sleep("10 millis")
      }
    })
  }

  /** For tests: the current plan of a subscription. */
  plannedQueryOf(id: string): Query | null {
    return this.subscriptions.get(id)?.planned.query ?? null
  }
}

export interface LiveQueryHandle {
  readonly id: string
  readonly getSnapshot: () => LiveQuerySnapshot
  readonly subscribe: (listener: () => void) => () => void
  readonly release: () => Effect.Effect<void>
}
