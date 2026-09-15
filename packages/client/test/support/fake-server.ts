/**
 * In-process sync server for client tests: the real Durable Object core (`SyncEngine`) behind a
 * fake WebSocket, with session logic equivalent to the Durable Object's. Lets the client engine
 * be exercised end to end (snapshots, deltas, reconnects, named queries) without workerd.
 *
 * `FakePushServer` is the application's mutation endpoint: it applies each pushed mutation as a
 * source transaction (rows plus the `orbit_clients` bookkeeping row) through the sync server, so
 * confirmation reaches the client the same way it does in production.
 */

import { DatabaseSync } from "node:sqlite"

import { Result, Schema } from "effect"
import {
  CdcBatch,
  decodePushRequest,
  INTERNAL_PROTOCOL_VERSION,
  isNamedQueryRef,
  ORBIT_CLIENTS_TABLE,
  type MutationOutcome,
  type MutationRef,
  type NamedQueryRef,
  type PartitionTransaction,
  type PushRequest,
  type PushResponse,
  type Query,
  type QueryRef,
  type RowChange,
  type SyncSchema,
} from "@orbit/protocol"
import {
  CLIENT_PROTOCOL_VERSION,
  ClientMessage,
  ServerMessage,
  type RowImage,
  type ServerMessage as ServerMessageType,
  type SyncError,
} from "@orbit/protocol/client"
import { resolveNamedQuery, type AnyDefinedQueries, type QueryContext } from "@orbit/query"
import { SyncEngine, type EngineEvent } from "@orbit/sync-do/core"
import { compatibility } from "@orbit/schema"

const decodeClient = Schema.decodeUnknownSync(ClientMessage)
const encodeServer = Schema.encodeSync(ServerMessage)

/** Node-side sync driver for the engine (mirrors the sync-do test driver). */
const engineDriver = (db: DatabaseSync) => {
  let depth = 0
  return {
    query: (sql: string, params: ReadonlyArray<string | number | null | Uint8Array> = []) =>
      db.prepare(sql).all(...params) as Array<Record<string, string | number | null | Uint8Array>>,
    run: (sql: string, params: ReadonlyArray<string | number | null | Uint8Array> = []) => {
      db.prepare(sql).run(...params)
    },
    transaction: <T>(f: () => T): T => {
      const name = `sp${depth}`
      db.exec(depth === 0 ? "BEGIN" : `SAVEPOINT ${name}`)
      depth += 1
      try {
        const out = f()
        depth -= 1
        db.exec(depth === 0 ? "COMMIT" : `RELEASE ${name}`)
        return out
      } catch (e) {
        depth -= 1
        db.exec(depth === 0 ? "ROLLBACK" : `ROLLBACK TO ${name}; RELEASE ${name}`)
        throw e
      }
    },
  }
}

/** Node has no global `CloseEvent`; a plain `Event` that carries the close fields serves the client. */
const closeEvent = (code: number, reason: string, wasClean: boolean): Event =>
  Object.assign(new Event("close"), { code, reason, wasClean })

export class FakeSocket
  extends EventTarget
  implements Pick<WebSocket, "readyState" | "send" | "close">
{
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSING = 2
  static readonly CLOSED = 3
  readyState: 0 | 1 | 2 | 3 = 0
  constructor(
    readonly server: FakeSyncServer,
    readonly url: string,
  ) {
    super()
  }
  send(data: string): void {
    if (this.readyState !== 1) throw new Error("socket not open")
    queueMicrotask(() => this.server.receive(this, data))
  }
  close(code = 1000, reason = ""): void {
    if (this.readyState === 3) return
    if (this.server.hangClose) {
      // A browser that is offline starts the closing handshake and never finishes it.
      this.readyState = 2
      return
    }
    this.readyState = 3
    this.server.detach(this)
    queueMicrotask(() => this.dispatchEvent(closeEvent(code, reason, true)))
  }
  /** Server side: deliver a message to the client. */
  deliver(message: ServerMessageType): void {
    if (this.readyState !== 1) return
    const text = JSON.stringify(encodeServer(message))
    queueMicrotask(() => this.dispatchEvent(new MessageEvent("message", { data: text })))
  }
  /** Server side: drop the connection (simulates network loss). */
  dropFromServer(code = 1006): void {
    if (this.readyState === 3) return
    this.readyState = 3
    this.server.detach(this)
    queueMicrotask(() => this.dispatchEvent(closeEvent(code, "dropped", false)))
  }
}

interface Session {
  readonly socket: FakeSocket
  readonly clientSubs: Map<string, { subscription: string; status: "pending" | "live" }>
  helloDone: boolean
  identical: boolean
  clientId: string
  columns: Map<string, Set<string>>
}

export interface FakeSyncServerOptions {
  /** Application named queries, resolved like the Durable Object does. */
  readonly queries?: AnyDefinedQueries
  /** Custom resolver for named queries; takes precedence over `queries`. Null means unknown. */
  readonly resolve?: (ref: NamedQueryRef, ctx: QueryContext) => Query | null
  /** The subject the server attributes to every session. */
  readonly subject?: string
}

export class FakeSyncServer {
  readonly engine: SyncEngine
  readonly sessions = new Map<FakeSocket, Session>()
  readonly pendingFills: Array<{ fill_id: string; table: string }> = []
  /** Every reference the server received, in order (for assertions on the wire form). */
  readonly receivedRefs: Array<QueryRef> = []
  /** The `basedOn` of every subscribe, null when absent, in the order received. */
  readonly receivedBases: Array<string | null> = []
  /** When false, new sockets never open (simulates the server being unreachable). */
  reachable = true
  /** When true, `close()` leaves the socket in CLOSING forever (a browser that is offline). */
  hangClose = false
  /** When false, pings get no pong (a link that is silently dead). */
  answerPings = true
  private seq = 0
  private readonly U1 = "a2523813-adbe-11f1-b19c-0a2250a7ed6c"

  constructor(
    readonly schema: SyncSchema,
    readonly partition: string,
    readonly options: FakeSyncServerOptions = {},
  ) {
    let ids = 0
    this.engine = new SyncEngine({
      driver: engineDriver(new DatabaseSync(":memory:")),
      schema,
      partition,
      now: () => Date.now(),
      newId: () => `f${++ids}`,
    })
    this.engine.init()
  }

  /** `makeWebSocket` for the client engine. */
  connect = (url: string): WebSocket => {
    const socket = new FakeSocket(this, url)
    queueMicrotask(() => {
      if (!this.reachable) {
        socket.readyState = 3
        socket.dispatchEvent(closeEvent(1006, "unreachable", false))
        return
      }
      socket.readyState = 1
      this.sessions.set(socket, {
        socket,
        clientSubs: new Map(),
        helloDone: false,
        identical: true,
        clientId: "",
        columns: new Map(),
      })
      socket.dispatchEvent(new Event("open"))
    })
    return socket as unknown as WebSocket
  }

  detach(socket: FakeSocket): void {
    const session = this.sessions.get(socket)
    if (session === undefined) return
    this.sessions.delete(socket)
    for (const cs of session.clientSubs.values()) {
      if (
        ![...this.sessions.values()].some((s) =>
          [...s.clientSubs.values()].some((x) => x.subscription === cs.subscription),
        )
      )
        this.engine.unsubscribe(cs.subscription)
    }
  }

  dropAll(): void {
    for (const s of [...this.sessions.keys()]) s.dropFromServer()
  }

  receive(socket: FakeSocket, text: string): void {
    const session = this.sessions.get(socket)
    if (session === undefined) return
    const msg = decodeClient(JSON.parse(text))
    if (msg.type === "hello") {
      if (msg.protocolVersion !== CLIENT_PROTOCOL_VERSION) {
        socket.deliver({
          type: "error",
          error: { code: "protocol_version_mismatch", message: "bad version" },
          fatal: true,
        })
        socket.close(4400, "protocol_version_mismatch")
        return
      }
      const compat = compatibility(this.schema, msg.schema)
      if (!compat.compatible) {
        socket.deliver({
          type: "error",
          error: { code: "schema_mismatch", message: compat.reason },
          fatal: true,
        })
        socket.close(4409, "schema_mismatch")
        return
      }
      session.helloDone = true
      session.identical = compat.identical
      session.clientId = msg.clientId
      session.columns = new Map(msg.schema.tables.map((t) => [t.name, new Set(t.columns)] as const))
      socket.deliver({
        type: "welcome",
        protocolVersion: CLIENT_PROTOCOL_VERSION,
        sessionId: "s",
        partition: this.partition,
        schemaHash: this.schema.schema_hash,
        cursor: this.engine.appliedSeq,
        serverTime: Date.now(),
      })
      for (const sub of msg.subscriptions) this.subscribe(session, sub.id, sub.query)
      return
    }
    if (!session.helloDone) return
    switch (msg.type) {
      case "subscribe":
        this.subscribe(session, msg.id, msg.query, msg.basedOn)
        return
      case "unsubscribe": {
        const cs = session.clientSubs.get(msg.id)
        session.clientSubs.delete(msg.id)
        if (
          cs !== undefined &&
          ![...this.sessions.values()].some((s) =>
            [...s.clientSubs.values()].some((x) => x.subscription === cs.subscription),
          )
        )
          this.engine.unsubscribe(cs.subscription)
        socket.deliver({ type: "unsubscribed", id: msg.id })
        return
      }
      case "ping":
        if (this.answerPings)
          socket.deliver({ type: "pong", sentAt: msg.sentAt, serverTime: Date.now() })
        return
      case "ack":
        return
    }
  }

  /** Resolves a wire reference the way the Durable Object does. */
  private resolve(ref: QueryRef, session: Session): Result.Result<Query, SyncError> {
    if (!isNamedQueryRef(ref)) return Result.succeed(ref)
    if (ref.name === "$orbit.client") {
      if (!this.schema.tables.some((t) => t.name === ORBIT_CLIENTS_TABLE))
        return Result.fail({
          code: "unsupported_query",
          message: `table ${ORBIT_CLIENTS_TABLE} is not in the sync schema`,
        })
      return Result.succeed({
        table: ORBIT_CLIENTS_TABLE,
        where: { op: "eq", column: "client_id", value: session.clientId },
      })
    }
    const ctx: QueryContext = {
      partition: this.partition,
      subject: this.options.subject ?? null,
      clientId: session.clientId,
    }
    if (this.options.resolve !== undefined) {
      const query = this.options.resolve(ref, ctx)
      return query === null
        ? Result.fail({ code: "unknown_query", message: `unknown query ${ref.name}` })
        : Result.succeed(query)
    }
    if (this.options.queries === undefined)
      return Result.fail({ code: "unknown_query", message: "no named queries are configured" })
    const resolved = resolveNamedQuery(this.options.queries, ref, ctx)
    if (Result.isSuccess(resolved)) return Result.succeed(resolved.success)
    return Result.fail({
      code: resolved.failure.reason === "unknown_query" ? "unknown_query" : "unsupported_query",
      message: JSON.stringify(resolved.failure),
    })
  }

  private subscribe(session: Session, clientSubId: string, ref: QueryRef, basedOn?: string): void {
    this.receivedRefs.push(ref)
    this.receivedBases.push(basedOn ?? null)
    const query = this.resolve(ref, session)
    if (Result.isFailure(query)) {
      session.socket.deliver({ type: "subscription_error", id: clientSubId, error: query.failure })
      return
    }
    const base = basedOn === undefined ? undefined : session.clientSubs.get(basedOn)
    const outcome = this.engine.subscribe(query.success, {
      ...(base === undefined || base.status !== "live" ? {} : { basedOn: base.subscription }),
    })
    if (Result.isFailure(outcome)) {
      session.socket.deliver({
        type: "subscription_error",
        id: clientSubId,
        error: outcome.failure,
      })
      return
    }
    session.clientSubs.set(clientSubId, {
      subscription: outcome.success.subscription,
      status: outcome.success.status,
    })
    session.socket.deliver({
      type: "subscribed",
      id: clientSubId,
      status: outcome.success.status,
      query: outcome.success.query,
    })
    // As in the Durable Object: the snapshot of the new subscription goes to this session only,
    // reduced to the members its base does not hold when the engine extended it.
    const others: Array<EngineEvent> = []
    for (const event of outcome.success.events) {
      if (event.type !== "snapshot" || event.subscription !== outcome.success.subscription) {
        others.push(event)
        continue
      }
      const cs = session.clientSubs.get(clientSubId)
      if (cs !== undefined) cs.status = "live"
      session.socket.deliver({
        type: "snapshot",
        subscriptionId: clientSubId,
        cursor: event.cursor,
        rows: event.rows,
        members: event.members,
        complete: true,
        ...(event.basedOn !== undefined &&
        event.basedOn === base?.subscription &&
        basedOn !== undefined
          ? { basedOn }
          : {}),
      })
    }
    this.dispatch(others)
  }

  private subscribed(
    engineSub: string,
    clientSubId: string,
    status: "pending" | "live",
  ): ServerMessageType | null {
    const query = this.engine.subscription(engineSub)?.planned.query
    return query === undefined ? null : { type: "subscribed", id: clientSubId, status, query }
  }

  private dispatch(events: ReadonlyArray<EngineEvent>): void {
    for (const event of events) {
      switch (event.type) {
        case "fill_needed":
          this.pendingFills.push({ fill_id: event.request.fill_id, table: event.request.table })
          break
        case "snapshot":
          for (const session of this.sessions.values()) {
            for (const [clientSubId, cs] of session.clientSubs) {
              if (cs.subscription !== event.subscription) continue
              cs.status = "live"
              const live = this.subscribed(event.subscription, clientSubId, "live")
              if (live !== null) session.socket.deliver(live)
              session.socket.deliver({
                type: "snapshot",
                subscriptionId: clientSubId,
                cursor: event.cursor,
                rows: event.rows,
                members: event.members,
                complete: true,
              })
            }
          }
          break
        case "subscription_failed":
          for (const session of this.sessions.values()) {
            for (const [clientSubId, cs] of session.clientSubs)
              if (cs.subscription === event.subscription)
                session.socket.deliver({
                  type: "subscription_error",
                  id: clientSubId,
                  error: event.error,
                })
          }
          break
        case "scopes_reset":
          break
        case "delta":
          for (const session of this.sessions.values()) {
            const live = [...session.clientSubs.entries()].filter(([, cs]) => cs.status === "live")
            if (live.length === 0) continue
            const memberships = event.memberships.flatMap((m) =>
              live
                .filter(([, cs]) => cs.subscription === m.subscriptionId)
                .map(([id]) => ({ ...m, subscriptionId: id })),
            )
            const engineSubs = new Set(live.map(([, cs]) => cs.subscription))
            const rows = event.rows.filter(
              (r) =>
                r.row === null ||
                [...engineSubs].some((id) =>
                  this.engine
                    .membershipOf(id)
                    .some(
                      (m) => m.table === r.table && JSON.stringify(m.key) === JSON.stringify(r.key),
                    ),
                ),
            )
            if (memberships.length === 0 && rows.length === 0) continue
            session.socket.deliver({
              type: "delta",
              cursor: event.cursor,
              origin: event.origin,
              rows,
              memberships,
            })
          }
          break
      }
    }
  }

  /** Completes a pending fill with the given rows at the current stream position. */
  completeFill(table: string, rows: ReadonlyArray<Record<string, unknown>>): void {
    const i = this.pendingFills.findIndex((f) => f.table === table)
    if (i < 0) throw new Error(`no pending fill for ${table}`)
    const fill = this.pendingFills.splice(i, 1)[0]!
    const r = this.engine.applyFillRows(fill.fill_id, rows as never)
    if (Result.isFailure(r)) throw new Error(JSON.stringify(r.failure))
    this.dispatch(
      this.engine.completeFill(fill.fill_id, {
        status: "completed",
        position: this.seq === 0 ? "" : `MySQL56/${this.U1}:1-${this.seq}`,
        keyspace: "ks",
        shard: "0",
        row_count: rows.length,
        duration_ms: 1,
      }),
    )
  }

  /** Applies a source transaction as the distributor would. */
  commit(changes: ReadonlyArray<RowChange>): void {
    this.seq += 1
    const txn: PartitionTransaction = {
      seq: this.seq,
      keyspace: "ks",
      shard: "0",
      gtid: `${this.U1}:${this.seq}`,
      position: `MySQL56/${this.U1}:1-${this.seq}`,
      commit_timestamp: 1_789_000_000 + this.seq,
      changes: [...changes],
      trace: {},
    }
    const batch: CdcBatch = {
      protocol_version: INTERNAL_PROTOCOL_VERSION,
      schema_hash: this.schema.schema_hash,
      stream_epoch: 0,
      partition: this.partition,
      transactions: [txn],
      delivery_id: `d${this.seq}`,
    }
    const { ack, events } = this.engine.applyBatch(Schema.encodeSync(CdcBatch)(batch))
    if (ack.status !== "applied") throw new Error(JSON.stringify(ack))
    this.dispatch(events)
  }
}

/** What the fake application does with one pushed mutation: the row changes it commits. */
export type FakeMutationApply = (
  mutation: MutationRef,
  ctx: { readonly clientId: string; readonly partition: string },
) => ReadonlyArray<RowChange>

export interface FakePushServerOptions {
  /** When false, `fetch` rejects like a network failure. */
  reachable?: boolean
  /** When true, mutations are recorded but no CDC transaction is committed (confirm later). */
  deferCommit?: boolean
}

/**
 * The application's push endpoint. Each mutation becomes one source transaction: the mutator's
 * row changes plus the `orbit_clients` row with the new `last_mutation_id`, committed through
 * the sync server. A mutator that throws yields a `failed` outcome and still consumes the id.
 */
export class FakePushServer {
  readonly requests: Array<PushRequest> = []
  readonly lastMutationId = new Map<string, number>()
  reachable: boolean
  deferCommit: boolean
  /** Transactions held back by `deferCommit`, in order. */
  readonly deferred: Array<ReadonlyArray<RowChange>> = []

  constructor(
    readonly server: FakeSyncServer,
    readonly apply: FakeMutationApply,
    options: FakePushServerOptions = {},
  ) {
    this.reachable = options.reachable ?? true
    this.deferCommit = options.deferCommit ?? false
  }

  private clientRow(clientId: string, last: number): RowImage {
    return {
      client_id: clientId,
      partition_key: this.server.partition,
      last_mutation_id: String(last),
      updated_at: "2026-01-01 00:00:00.000",
    }
  }

  /** Commits (or defers) the transaction that applies one mutation and bumps the client row. */
  private commitMutation(
    clientId: string,
    previous: number,
    next: number,
    changes: ReadonlyArray<RowChange>,
  ): void {
    const before = this.clientRow(clientId, previous)
    const after = this.clientRow(clientId, next)
    const bookkeeping: RowChange =
      previous === 0
        ? { table: ORBIT_CLIENTS_TABLE, op: "insert", key: [clientId], after }
        : { table: ORBIT_CLIENTS_TABLE, op: "update", key: [clientId], before, after }
    const txn = [...changes, bookkeeping]
    if (this.deferCommit) this.deferred.push(txn)
    else this.server.commit(txn)
  }

  /** Commits every deferred transaction in order. */
  flush(): void {
    for (const txn of this.deferred.splice(0)) this.server.commit(txn)
  }

  handle(request: PushRequest): PushResponse {
    this.requests.push(request)
    let last = this.lastMutationId.get(request.clientId) ?? 0
    const first = request.mutations[0]
    if (first !== undefined && first.id !== last + 1) {
      const already = first.id <= last
      return {
        type: "refused",
        reason: "out_of_order",
        message: already ? "already applied" : "gap",
        lastMutationId: last,
      }
    }
    const outcomes: Array<MutationOutcome> = []
    for (const m of request.mutations) {
      if (m.id !== last + 1) break
      let changes: ReadonlyArray<RowChange>
      try {
        changes = this.apply(m, { clientId: request.clientId, partition: request.partition })
      } catch (e) {
        changes = []
        outcomes.push({
          id: m.id,
          status: "failed",
          error: e instanceof Error ? e.message : String(e),
        })
        this.commitMutation(request.clientId, last, m.id, changes)
        last = m.id
        continue
      }
      this.commitMutation(request.clientId, last, m.id, changes)
      last = m.id
      outcomes.push({ id: m.id, status: "applied" })
    }
    this.lastMutationId.set(request.clientId, last)
    return { type: "ok", outcomes, lastMutationId: last }
  }

  /** `fetch` for the client. */
  fetch = async (_input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    if (!this.reachable) throw new TypeError("fetch failed")
    const body = typeof init?.body === "string" ? init.body : ""
    const request = decodePushRequest(JSON.parse(body))
    return Response.json(this.handle(request))
  }
}
