/**
 * Client-side mutations: optimistic, persistent, confirmed through sync.
 *
 * A call to `mutate.<name>(args)`:
 * 1. allocates the next dense mutation id for this client;
 * 2. runs the mutator against a `MutationTx` whose writes go to the overlay tables (`o_<table>`)
 *    and whose reads see the effective rows (`v_<table>` views);
 * 3. writes the pending log row and the overlay rows in one batch;
 * 4. pushes the log to the application's mutation endpoint in id order.
 *
 * Confirmation does not come from the push response. The application's server records
 * `last_mutation_id` in the `orbit_clients` table inside the same transaction as the mutation's
 * writes, and that row syncs to the client through the built-in `$orbit.client` subscription.
 * When it advances, every pending mutation with a lower or equal id is confirmed and the overlay
 * is rebased: cleared and rebuilt by re-running the remaining pending mutations in id order
 * (the Replicache and Zero model, see `docs/mutations.md`).
 *
 * Every operation that touches the pending log or the overlay runs under one lock, shared with
 * the engine's message handling, so a rebase never interleaves with a new mutation or a delta.
 */

import { Effect, Schema } from "effect"
import {
  decodePushResponse,
  JsonValue,
  MUTATION_PROTOCOL_VERSION,
  ORBIT_CLIENTS_TABLE,
  MutationOutcome,
  type PushRequest,
  type PushResponse,
} from "@orbit/protocol"
import type { RowImage } from "@orbit/protocol/client"
import {
  decodeArgs,
  encodeArgs,
  nowWire,
  type DefinedMutators,
  type MutationContext,
  type MutationTx,
  type MutatorDefinitions,
} from "@orbit/mutators"
import {
  flattenNode,
  literalParam,
  planQuery,
  rowFromRecord,
  type IncludeShape,
  type SqlParam,
  type TypedQuery,
} from "@orbit/query"
import type { TableSchema } from "@orbit/protocol"
import { KEY_COLUMN, quoteIdent, SchemaRuntime } from "@orbit/schema"

import type { AsyncSqlDriver, Statement } from "./driver.ts"
import {
  pruneUndoStatement,
  readNodes,
  readPendingMutations,
  overlayViewName,
  type LocalStore,
} from "./store.ts"

export type MutationEventStatus = "applied_locally" | "pushed" | "confirmed" | "failed"

export interface MutationEvent {
  readonly id: number
  readonly name: string
  readonly status: MutationEventStatus
  readonly error?: string
}

export interface MutationHandle {
  readonly id: number
  /** Settles when the mutation is applied to the local overlay (rejects when the mutator throws). */
  readonly local: Promise<void>
  /** Settles with the server's outcome for this mutation. */
  readonly server: Promise<MutationOutcome>
}

/** A local refusal after the mutation was durably queued; the server still decides. */
export class PersistedMutationError extends Error {}

/** A FIFO mutual exclusion lock usable from Promise and Effect code alike. */
export class AsyncLock {
  private tail: Promise<void> = Promise.resolve()

  run<T>(fn: () => Promise<T>): Promise<T> {
    const result = this.tail.then(fn)
    this.tail = result.then(
      () => undefined,
      () => undefined,
    )
    return result
  }

  /** Runs an Effect while holding the lock (the Effect runs in its own runtime). */
  runEffect<A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E> {
    return Effect.promise(() => this.run(() => Effect.runPromiseExit(effect))).pipe(
      Effect.flatMap((exit) => exit),
    )
  }
}

const decodeCell = Schema.decodeUnknownSync(JsonValue)

/** Converts a typed cell value to a SQLite parameter for the column kind. */
const cellParam = (table: TableSchema, column: string, value: unknown): SqlParam => {
  const col = table.columns.find((c) => c.name === column)
  if (col === undefined) throw new Error(`unknown column ${table.name}.${column}`)
  return literalParam(col.kind, value === undefined ? null : decodeCell(value))
}

const isRecord = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null && !Array.isArray(v)

/**
 * The client `MutationTx`: collects overlay statements; reads go to the views. Statements are
 * written in one batch after the mutator returns, so a throwing mutator writes nothing.
 */
export class LocalMutationTx {
  readonly statements: Array<Statement> = []
  readonly touched = new Set<string>()
  /** Rows written by this transaction, so a later `get` in the same mutator sees them. */
  private readonly written = new Map<string, RowImage | null>()

  constructor(
    private readonly driver: AsyncSqlDriver,
    private readonly store: LocalStore,
    private readonly mutationId: number,
  ) {}

  deletedKeys(): ReadonlyMap<string, ReadonlySet<string>> | undefined {
    const removed = new Map<string, Set<string>>()
    for (const [identity, image] of this.written) {
      if (image !== null) return undefined
      const separator = identity.indexOf("\u0000")
      const table = identity.slice(0, separator),
        key = identity.slice(separator + 1)
      const keys = removed.get(table) ?? new Set<string>()
      keys.add(key)
      removed.set(table, keys)
    }
    return removed.size === 0 ? undefined : removed
  }

  private table(name: string): TableSchema {
    const table = this.store.rt.table(name)
    if (table === undefined) throw new Error(`table ${name} is not in the sync schema`)
    return table
  }

  private keyString(table: TableSchema, key: unknown): string {
    if (!isRecord(key)) throw new Error(`key of ${table.name} must be an object`)
    return SchemaRuntime.keyString(table.primary_key.map((c) => decodeCell(key[c] ?? null)))
  }

  private image(table: TableSchema, values: Record<string, unknown>): RowImage {
    const out: Record<string, JsonValue> = {}
    for (const c of table.columns) out[c.name] = decodeCell(values[c.name] ?? null)
    return out
  }

  private write(table: TableSchema, key: string, image: RowImage | null): void {
    const params: ReadonlyArray<SqlParam> =
      image === null
        ? [key, ...table.columns.map(() => null)]
        : [key, ...table.columns.map((c) => cellParam(table, c.name, image[c.name]))]
    this.statements.push(
      this.store.overlayStatement(
        table,
        params,
        image === null ? "delete" : "upsert",
        this.mutationId,
      ),
    )
    this.written.set(`${table.name}\u0000${key}`, image)
    this.touched.add(table.name)
  }

  private async readByKey(table: TableSchema, key: string): Promise<RowImage | null> {
    const local = this.written.get(`${table.name}\u0000${key}`)
    if (local !== undefined) return local
    const cols = table.columns
      .map((c) =>
        c.kind === "bigint"
          ? `CAST(${quoteIdent(c.name)} AS TEXT) AS ${quoteIdent(c.name)}`
          : quoteIdent(c.name),
      )
      .join(", ")
    const rows = await this.driver.query(
      `SELECT ${quoteIdent(KEY_COLUMN)}, ${cols} FROM ${quoteIdent(overlayViewName(table.name))} WHERE ${quoteIdent(KEY_COLUMN)} = ?`,
      [key],
    )
    const record = rows[0]
    if (record === undefined) return null
    return rowFromRecord(table, record)
  }

  readonly localUndo = {
    capture: async (name: string, group: string, rows: ReadonlyArray<unknown>): Promise<void> => {
      const table = this.table(name)
      const images = rows.map((row) => {
        if (!isRecord(row)) throw new Error(`row of ${name} must be an object`)
        return this.image(table, row)
      })
      // INSERT OR IGNORE is essential: replay after CDC removes the base rows must
      // not replace the original before-images with an empty/incomplete selection.
      this.statements.push({
        sql: `INSERT OR IGNORE INTO local_undo (tbl, undo_group, mutation_id, images) VALUES (?, ?, ?, ?)`,
        params: [name, group, this.mutationId, JSON.stringify(images)],
      })
    },
    restore: async (name: string, group: string): Promise<void> => {
      const table = this.table(name)
      const records = await this.driver.query(
        `SELECT images FROM local_undo WHERE tbl = ? AND undo_group = ? AND mutation_id < ? ORDER BY mutation_id DESC LIMIT 1`,
        [name, group, this.mutationId],
      )
      const value = records[0]?.["images"]
      if (typeof value !== "string") return
      const images: unknown = JSON.parse(value)
      if (!Array.isArray(images)) throw new Error("invalid local undo images")
      for (const row of images) {
        if (!isRecord(row)) throw new Error("invalid local undo row")
        const image = this.image(table, row)
        this.write(table, SchemaRuntime.keyString(this.store.rt.keyOf(table, image)), image)
      }
    },
  }

  async insert(name: string, row: unknown): Promise<void> {
    const table = this.table(name)
    if (!isRecord(row)) throw new Error(`row of ${name} must be an object`)
    // Columns the mutator omits default to null; the server fills its own defaults.
    const image = this.image(table, row)
    this.write(table, SchemaRuntime.keyString(this.store.rt.keyOf(table, image)), image)
  }

  async insertMany(name: string, rows: ReadonlyArray<unknown>): Promise<void> {
    for (const row of rows) await this.insert(name, row)
  }

  async update(name: string, key: unknown, patch: unknown): Promise<void> {
    const table = this.table(name)
    if (!isRecord(patch)) throw new Error(`patch of ${name} must be an object`)
    const keyString = this.keyString(table, key)
    const current = await this.readByKey(table, keyString)
    if (current === null) throw new Error(`${name} row ${keyString} does not exist`)
    const merged = this.image(table, { ...current, ...patch })
    const newKey = SchemaRuntime.keyString(this.store.rt.keyOf(table, merged))
    if (newKey !== keyString) this.write(table, keyString, null)
    this.write(table, newKey, merged)
  }

  async delete(name: string, key: unknown): Promise<void> {
    const table = this.table(name)
    this.write(table, this.keyString(table, key), null)
  }

  async get(name: string, key: unknown): Promise<RowImage | null> {
    const table = this.table(name)
    return this.readByKey(table, this.keyString(table, key))
  }

  async query(
    query: TypedQuery<unknown, string, IncludeShape>,
  ): Promise<ReadonlyArray<Record<string, unknown>>> {
    const planned = planQuery(this.store.rt, query.ast)
    if (planned._tag === "Failure") throw new Error(planned.failure.message)
    const nodes = await readNodes(this.driver, planned.success, {})
    return nodes.map(flattenNode)
  }

  /** The tx as the typed interface mutators are written against; the shapes come from the schema. */
  typed<D>(): MutationTx<D> {
    // The local tx is written against the compiled schema; the definition's types describe the
    // same tables, so the untyped implementation satisfies the typed interface by construction.
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion
    return this as unknown as MutationTx<D>
  }
}

export interface MutationManagerConfig {
  readonly store: LocalStore
  readonly mutators: DefinedMutators<unknown, MutatorDefinitions<unknown>>
  readonly clientId: string
  readonly partition: string
  readonly subject: string | null
  readonly pushUrl: string | undefined
  readonly fetch: typeof fetch | undefined
  readonly lock: AsyncLock
  readonly onLog: (event: string, data: Record<string, unknown>) => void
  /** Called (under the lock) after the overlay changed; `null` means every table may have changed. */
  readonly onChanged: (
    tables: ReadonlySet<string> | null,
    deleted?: ReadonlyMap<string, ReadonlySet<string>>,
  ) => Promise<void>
  readonly onPendingCount: (n: number) => void
  readonly now: () => number
  readonly backoffMinMs?: number
  readonly backoffMaxMs?: number
}

const PUSH_BACKOFF_MIN = 250
const PUSH_BACKOFF_MAX = 30_000

export class MutationManager {
  private lastId = 0
  private pendingCount = 0
  private closed = false
  private readonly listeners = new Set<(event: MutationEvent) => void>()
  private readonly serverWaiters = new Map<number, (outcome: MutationOutcome) => void>()
  private readonly serverRejecters = new Map<number, (error: unknown) => void>()
  /** Mutation ids whose local replay failed, so the failure is reported once. */
  private readonly replayFailed = new Set<number>()
  private readonly observers = new Map<
    number,
    Set<{ resolve: (outcome: MutationOutcome) => void; reject: (error: Error) => void }>
  >()
  private pushing = false
  private pushAgain = false
  private wake: (() => void) | null = null

  constructor(private readonly config: MutationManagerConfig) {}

  private get driver(): AsyncSqlDriver {
    return this.config.store.driver
  }

  onEvent(listener: (event: MutationEvent) => void): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  private emit(event: MutationEvent): void {
    for (const l of this.listeners) l(event)
  }

  private setPending(n: number): void {
    this.pendingCount = n
    this.config.onPendingCount(n)
  }

  get pending(): number {
    return this.pendingCount
  }

  /** Loads the log, allocates ids after it, and rebuilds the overlay. Runs under the lock. */
  init(): Promise<void> {
    return this.config.lock.run(async () => {
      const pending = await readPendingMutations(this.driver)
      const last = await Effect.runPromise(
        this.config.store.lastMutationIdOf(ORBIT_CLIENTS_TABLE, this.config.clientId),
      )
      const receipts = await this.driver.query(`SELECT MAX(id) AS id FROM mutation_outcomes`)
      this.lastId = Math.max(pending.at(-1)?.id ?? 0, last ?? 0, Number(receipts[0]?.["id"] ?? 0))
      await this.rebase([])
    })
  }

  // ---------------------------------------------------------------------------------------------
  // Apply
  // ---------------------------------------------------------------------------------------------

  mutate(name: string, args: unknown): MutationHandle {
    // Invalid arguments never consume an id: encoding happens before allocation.
    const wire = encodeArgs(this.config.mutators, name, args)
    const id = ++this.lastId
    const server = new Promise<MutationOutcome>((resolve, reject) => {
      this.serverWaiters.set(id, resolve)
      this.serverRejecters.set(id, reject)
    })
    // A caller may only await `local`; the server promise must not surface as unhandled.
    void server.catch(() => undefined)
    const local = this.config.lock.run(() => this.apply(id, name, wire))
    // A mutator that failed locally is still in the log (ids stay dense); the server decides its
    // outcome, so the push loop runs either way.
    void local.then(
      () => this.kick(),
      () => this.kick(),
    )
    return { id, local, server }
  }

  observe(id: number): Promise<MutationOutcome> {
    if (!Number.isSafeInteger(id) || id < 1 || id > this.lastId || this.closed)
      return Promise.reject(new Error("Mutation is not available in this client"))
    return new Promise((resolve, reject) => {
      const waiter = { resolve, reject }
      const waiting = this.observers.get(id) ?? new Set()
      waiting.add(waiter)
      this.observers.set(id, waiting)
      void this.config.lock
        .run(async () => {
          const rows = await this.driver.query(
            `SELECT outcome FROM mutation_outcomes WHERE id = ?`,
            [id],
          )
          const value = rows[0]?.["outcome"]
          if (typeof value === "string") {
            this.settle(id, Schema.decodeUnknownSync(MutationOutcome)(JSON.parse(value)))
          } else {
            const pending = await this.driver.query(
              `SELECT id FROM pending_mutations WHERE id = ?`,
              [id],
            )
            if (pending.length === 0) {
              waiting.delete(waiter)
              if (waiting.size === 0) this.observers.delete(id)
              reject(new Error("Mutation outcome is no longer retained"))
            }
          }
        })
        .catch((error: unknown) => {
          waiting.delete(waiter)
          if (waiting.size === 0) this.observers.delete(id)
          reject(error instanceof Error ? error : new Error(String(error)))
        })
    })
  }

  private async remember(
    outcomes: ReadonlyArray<MutationOutcome>,
    overwrite = true,
  ): Promise<void> {
    if (outcomes.length === 0) return
    await this.driver.batch([
      ...outcomes.map((outcome) => ({
        sql: `INSERT OR ${overwrite ? "REPLACE" : "IGNORE"} INTO mutation_outcomes (id, outcome) VALUES (?, ?)`,
        params: [outcome.id, JSON.stringify(outcome)],
      })),
      {
        sql: `DELETE FROM mutation_outcomes WHERE id < (SELECT MAX(id) - 1023 FROM mutation_outcomes)`,
        params: [],
      },
    ])
  }

  private context(id: number): MutationContext {
    return {
      partition: this.config.partition,
      subject: this.config.subject,
      clientId: this.config.clientId,
      mutationId: id,
      now: nowWire(new Date(this.config.now())),
      side: "client",
    }
  }

  /** Runs a mutator against a fresh tx; returns its statements or the error it threw. */
  private async runMutator(
    id: number,
    name: string,
    wire: Record<string, JsonValue>,
  ): Promise<{ readonly tx: LocalMutationTx; readonly error: string | null }> {
    const tx = new LocalMutationTx(this.driver, this.config.store, id)
    try {
      const decoded = decodeArgs(this.config.mutators, name, wire)
      await decoded.definition.apply(tx.typed<unknown>(), decoded.args, this.context(id))
      return { tx, error: null }
    } catch (e) {
      return { tx, error: e instanceof Error ? e.message : String(e) }
    }
  }

  private async apply(id: number, name: string, wire: Record<string, JsonValue>): Promise<void> {
    const { tx, error } = await this.runMutator(id, name, wire)
    // The log row is written even when the mutator threw: ids must stay dense for the push
    // protocol, and the server's outcome (`failed`) removes the mutation. The overlay gets nothing.
    await this.driver.batch([
      this.config.store.insertPendingStatement({
        id,
        name,
        args: wire,
        createdAt: this.config.now(),
      }),
      ...(error === null ? tx.statements : []),
    ])
    this.setPending(this.pendingCount + 1)
    if (error !== null) {
      this.replayFailed.add(id)
      this.emit({ id, name, status: "failed", error })
      this.config.onLog("mutation.failed_locally", { id, name, error })
      throw new PersistedMutationError(`mutator ${name} failed: ${error}`)
    }
    this.emit({ id, name, status: "applied_locally" })
    await this.config.onChanged(tx.touched, tx.deletedKeys())
  }

  // ---------------------------------------------------------------------------------------------
  // Rebase and confirmation (lock held by the caller)
  // ---------------------------------------------------------------------------------------------

  /**
   * Removes `dropIds` from the log, clears the overlay and re-runs every remaining mutation in id
   * order. A mutation whose replay fails contributes nothing to the overlay but stays in the log
   * (its id is already allocated; the server decides its outcome).
   */
  private async rebase(dropIds: ReadonlyArray<number>): Promise<void> {
    await this.driver.batch([
      ...(dropIds.length === 0 ? [] : [this.config.store.deletePendingStatement(dropIds)]),
      ...this.config.store.clearOverlayStatements(),
    ])
    const pending = await readPendingMutations(this.driver)
    for (const m of pending) {
      const { tx, error } = await this.runMutator(m.id, m.name, m.args)
      if (error === null) {
        if (tx.statements.length > 0) await this.driver.batch(tx.statements)
        this.replayFailed.delete(m.id)
        continue
      }
      if (this.replayFailed.has(m.id)) continue
      this.replayFailed.add(m.id)
      this.config.onLog("mutation.replay_failed", { id: m.id, name: m.name, error })
      this.emit({ id: m.id, name: m.name, status: "failed", error })
    }
    this.setPending(pending.length)
    if (pending.length === 0) await this.driver.batch([pruneUndoStatement()])
  }

  /**
   * Called under the lock after every applied delta or snapshot. Confirms every pending mutation
   * the synced `orbit_clients` row covers after HTTP establishes success, and rebases the rest.
   * A consumed id alone cannot distinguish an applied mutation from a refusal.
   */
  async confirm(): Promise<boolean> {
    const pending = await readPendingMutations(this.driver)
    if (pending.length === 0) return false
    const last = await Effect.runPromise(
      this.config.store.lastMutationIdOf(ORBIT_CLIENTS_TABLE, this.config.clientId),
    )
    if (last !== null && last > this.lastId) this.lastId = last
    const confirmed = last === null ? [] : pending.filter((m) => m.pushed && m.id <= last)
    await this.remember(
      confirmed.map((m) => ({ id: m.id, status: "applied" as const })),
      false,
    )
    await this.rebase(confirmed.map((m) => m.id))
    for (const m of confirmed) {
      this.settle(m.id, { id: m.id, status: "applied" })
      this.emit({ id: m.id, name: m.name, status: "confirmed" })
    }
    if (confirmed.length > 0)
      this.config.onLog("mutation.confirmed", { ids: confirmed.map((m) => m.id) })
    return true
  }

  private settle(id: number, outcome: MutationOutcome): void {
    const resolve = this.serverWaiters.get(id)
    this.serverWaiters.delete(id)
    this.serverRejecters.delete(id)
    resolve?.(outcome)
    for (const waiter of this.observers.get(id) ?? []) waiter.resolve(outcome)
    this.observers.delete(id)
  }

  // ---------------------------------------------------------------------------------------------
  // Push loop
  // ---------------------------------------------------------------------------------------------

  /** Starts the push loop, or wakes it from its backoff sleep. */
  kick(): void {
    if (this.config.pushUrl === undefined || this.closed) return
    if (this.pushing) {
      this.pushAgain = true
      this.wake?.()
      return
    }
    void this.pushLoop()
  }

  close(): void {
    this.closed = true
    const error = new Error("Orbit mutation manager closed")
    for (const reject of this.serverRejecters.values()) reject(error)
    this.serverRejecters.clear()
    this.serverWaiters.clear()
    for (const waiting of this.observers.values())
      for (const waiter of waiting) waiter.reject(error)
    this.observers.clear()
    this.wake?.()
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.wake = null
        resolve()
      }, ms)
      this.wake = () => {
        clearTimeout(timer)
        this.wake = null
        resolve()
      }
    })
  }

  private async pushLoop(): Promise<void> {
    this.pushing = true
    const min = this.config.backoffMinMs ?? PUSH_BACKOFF_MIN
    const max = this.config.backoffMaxMs ?? PUSH_BACKOFF_MAX
    let backoff = min
    try {
      while (!this.closed) {
        this.pushAgain = false
        const batch = (await readPendingMutations(this.driver)).filter((m) => !m.pushed)
        if (batch.length === 0) {
          if (!this.pushAgain) return
          continue
        }
        const response = await this.post(batch)
        if (this.closed) return
        if (response === null) {
          this.config.onLog("push.retry", { inMs: backoff, mutations: batch.length })
          await this.sleep(backoff)
          backoff = Math.min(backoff * 2, max)
          continue
        }
        backoff = min
        const retryNow = await this.config.lock.run(() => this.handleResponse(batch, response))
        if (retryNow) continue
        if (response.type === "refused") {
          await this.sleep(max)
          continue
        }
      }
    } finally {
      this.pushing = false
    }
  }

  /** One HTTP push; null on a network error or an unreadable response (both retried). */
  private async post(
    batch: ReadonlyArray<{ id: number; name: string; args: Record<string, JsonValue> }>,
  ): Promise<PushResponse | null> {
    const url = this.config.pushUrl
    if (url === undefined) return null
    const doFetch = this.config.fetch ?? globalThis.fetch
    const request: PushRequest = {
      protocolVersion: MUTATION_PROTOCOL_VERSION,
      clientId: this.config.clientId,
      partition: this.config.partition,
      mutations: batch.map((m) => ({ id: m.id, name: m.name, args: m.args })),
    }
    try {
      const response = await doFetch(url, {
        method: "POST",
        credentials: "include",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(request),
      })
      const body: unknown = await response.json()
      return decodePushResponse(body)
    } catch (e) {
      this.config.onLog("push.failed", { error: e instanceof Error ? e.message : String(e) })
      return null
    }
  }

  /** Applies a push response under the lock. Returns true when the loop should retry at once. */
  private async handleResponse(
    batch: ReadonlyArray<{ id: number; name: string }>,
    response: PushResponse,
  ): Promise<boolean> {
    const names = new Map(batch.map((m) => [m.id, m.name] as const))
    if (response.type === "ok") {
      await this.remember(response.outcomes)
      const pushed: Array<number> = []
      const failed: Array<MutationOutcome & { status: "failed" }> = []
      for (const outcome of response.outcomes) {
        if (outcome.status === "failed") failed.push(outcome)
        else pushed.push(outcome.id)
      }
      if (pushed.length > 0)
        await this.driver.batch([this.config.store.markPushedStatement(pushed)])
      if (failed.length > 0) {
        await this.rebase(failed.map((f) => f.id))
        await this.config.onChanged(null)
      }
      for (const outcome of response.outcomes) {
        this.settle(outcome.id, outcome)
        const name = names.get(outcome.id) ?? ""
        if (outcome.status === "failed") {
          this.config.onLog("mutation.rejected", { id: outcome.id, name, error: outcome.error })
          this.emit({ id: outcome.id, name, status: "failed", error: outcome.error })
        } else this.emit({ id: outcome.id, name, status: "pushed" })
      }
      // CDC may beat the HTTP response. Its consumed id does not distinguish
      // success from refusal. Only retire successful mutations after recording
      // their actual outcome, and confirm here if CDC has already arrived.
      if (pushed.length > 0 && (await this.confirm())) await this.config.onChanged(null)
      if (response.lastMutationId > this.lastId) this.lastId = response.lastMutationId
      return false
    }
    this.config.onLog("push.refused", { reason: response.reason, message: response.message })
    if (response.reason === "out_of_order" && response.lastMutationId !== undefined) {
      // Everything up to the server's id was applied by an earlier push whose response was lost.
      const last = response.lastMutationId
      if (last > this.lastId) this.lastId = last
      const applied = batch.filter((m) => m.id <= last)
      if (applied.length > 0) {
        await this.remember(applied.map((m) => ({ id: m.id, status: "duplicate" as const })))
        await this.rebase(applied.map((m) => m.id))
        await this.config.onChanged(null)
        for (const m of applied) {
          this.settle(m.id, { id: m.id, status: "duplicate" })
          this.emit({ id: m.id, name: m.name, status: "pushed" })
        }
      }
      return true
    }
    return false
  }
}
