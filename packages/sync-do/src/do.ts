/**
 * The Sync Durable Object: one per logical partition (see `placement.ts`).
 *
 * Responsibilities:
 * * own the relational cache and subscription state through `SyncEngine`
 * * accept CDC batches from the distributor (`POST /cdc`) and fill uploads (`POST /fill/:id`)
 * * serve clients over hibernating WebSockets using the client protocol
 * * request fills from the registry and retry them from alarms
 *
 * Everything the engine does happens synchronously inside SQLite transactions; this class adds
 * transport, session bookkeeping and Effect-based orchestration around it.
 */

import { DurableObject } from "cloudflare:workers"
import { Cause, Effect, Exit, Result, Schema } from "effect"
import {
  CdcBatchAck,
  FillChunk,
  FillRequest,
  isNamedQueryRef,
  ORBIT_CLIENTS_TABLE,
  type NamedQueryRef,
  type Query,
  type SyncSchema,
} from "@orbit/protocol"
import {
  CLIENT_PROTOCOL_VERSION,
  ClientMessage,
  CloseCode,
  ServerMessage,
  type MemberRef,
  type RowUpdate,
  type SchemaSummary,
  type SubscribeMessage,
  type SyncError,
  WS_SUBPROTOCOL,
} from "@orbit/protocol/client"
import { resolveNamedQuery, type AnyDefinedQueries } from "@orbit/query"
import { compatibility, projectRow } from "@orbit/schema"

import { SyncEngine, type EngineEvent } from "./core/engine.ts"
import { durableObjectDriver } from "./do-driver.ts"
import { offeredSubprotocols } from "./worker.ts"
import { log } from "./log.ts"
import {
  encodeAttachment,
  readAttachment,
  Sessions,
  type SessionRow,
  type SocketAttachment,
} from "./sessions.ts"

export interface SyncDurableObjectEnv {
  readonly ORBIT_FILL_REGISTRY: DurableObjectNamespace
}

/** Application-provided configuration for the Durable Object class. */
export interface SyncDurableObjectConfig {
  readonly schema: SyncSchema
  /**
   * Named queries clients may subscribe to (see `defineQueries` in `@orbit/query`). Resolved
   * with the session's subject and partition, so authorization lives in the resolver.
   */
  readonly queries?: AnyDefinedQueries
  /**
   * Whether clients may send raw queries. Defaults to `true` only when no named queries are
   * configured; with named queries, raw queries are refused as `unauthorized`.
   */
  readonly allowAdHocQueries?: boolean
  /** Fill timeout before a retry, milliseconds. */
  readonly fillTimeoutMs?: number
  /** Maximum fill attempts before pending subscriptions are failed. */
  readonly maxFillAttempts?: number
  /** Rows per snapshot message. */
  readonly snapshotChunkRows?: number
  /**
   * How long a subscription stays materialized after its last session left, milliseconds.
   * A reload, a redeploy or a client's query TTL within this period reuses the membership
   * instead of rewriting it. Maintenance of an orphaned subscription costs a few indexed
   * operations per change that touches its tables. Default: one hour.
   */
  readonly subscriptionGraceMs?: number
}

const SNAPSHOT_CHUNK_DEFAULT = 500
const SUBSCRIPTION_GRACE_DEFAULT = 60 * 60_000

const decodeClient = Schema.decodeUnknownEffect(ClientMessage)
const encodeServer = Schema.encodeSync(ServerMessage)
const decodeFillChunk = Schema.decodeUnknownSync(Schema.fromJsonString(FillChunk))
const encodeAck = Schema.encodeSync(CdcBatchAck)
const encodeFillRequest = Schema.encodeSync(FillRequest)

/**
 * Creates the Durable Object class bound to one sync schema. Applications export the result
 * under the class name referenced in their `wrangler.jsonc`.
 */
export const makeSyncDurableObject = (config: SyncDurableObjectConfig) => {
  const fillTimeoutMs = config.fillTimeoutMs ?? 120_000
  const maxFillAttempts = config.maxFillAttempts ?? 5
  const snapshotChunkRows = config.snapshotChunkRows ?? SNAPSHOT_CHUNK_DEFAULT
  const subscriptionGraceMs = config.subscriptionGraceMs ?? SUBSCRIPTION_GRACE_DEFAULT
  const allowAdHoc = config.allowAdHocQueries ?? config.queries === undefined
  const hasClientsTable = config.schema.tables.some((t) => t.name === ORBIT_CLIENTS_TABLE)

  /**
   * The engine's own named query: a client's bookkeeping row (`orbit_clients`), which confirms
   * its mutations. Always scoped to the session's client id, whatever the arguments say.
   */
  const builtinQuery = (ref: NamedQueryRef, clientId: string): Result.Result<Query, SyncError> => {
    if (ref.name !== "$orbit.client")
      return Result.fail({ code: "unknown_query", message: `unknown query ${ref.name}` })
    if (!hasClientsTable)
      return Result.fail({
        code: "unsupported_query",
        message: `table ${ORBIT_CLIENTS_TABLE} is not in the sync schema`,
      })
    return Result.succeed({
      table: ORBIT_CLIENTS_TABLE,
      where: { op: "eq", column: "client_id", value: clientId },
    })
  }

  const resolveQuery = (
    ref: SubscribeMessage["query"],
    ctx: { readonly partition: string; readonly subject: string; readonly clientId: string },
  ): Result.Result<Query, SyncError> => {
    if (!isNamedQueryRef(ref)) {
      if (allowAdHoc) return Result.succeed(ref)
      return Result.fail({ code: "unauthorized", message: "raw queries are not allowed" })
    }
    if (ref.name.startsWith("$orbit.")) return builtinQuery(ref, ctx.clientId)
    if (config.queries === undefined)
      return Result.fail({ code: "unknown_query", message: "no named queries are configured" })
    const resolved = resolveNamedQuery(config.queries, ref, ctx)
    if (Result.isSuccess(resolved)) return Result.succeed(resolved.success)
    const problem = resolved.failure
    return Result.fail({
      code: problem.reason === "unknown_query" ? "unknown_query" : "unsupported_query",
      message:
        problem.reason === "unknown_query"
          ? `unknown query ${problem.name}`
          : `${problem.name}: ${problem.message}`,
      details: { reason: problem.reason },
    })
  }

  return class SyncDurableObject extends DurableObject<SyncDurableObjectEnv> {
    private engine: SyncEngine | null = null
    private sessions: Sessions | null = null
    private partition: string | null = null

    constructor(ctx: DurableObjectState, env: SyncDurableObjectEnv) {
      super(ctx, env)
      // Partition and engine are bound lazily: the first request carries the partition name.
      const stored = ctx.storage.sql
        .exec(
          `CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL); SELECT value FROM meta WHERE key = 'partition'`,
        )
        .toArray()[0]
      if (stored !== undefined && typeof stored["value"] === "string") this.bind(stored["value"])
    }

    private bind(partition: string): { readonly engine: SyncEngine; readonly sessions: Sessions } {
      if (this.engine !== null && this.sessions !== null && this.partition === partition)
        return { engine: this.engine, sessions: this.sessions }
      if (this.partition !== null && this.partition !== partition)
        throw new Error(`durable object bound to ${this.partition}, got ${partition}`)
      const driver = durableObjectDriver(this.ctx.storage)
      const engine = new SyncEngine({
        driver,
        schema: config.schema,
        partition,
        now: () => Date.now(),
        newId: () => crypto.randomUUID(),
      })
      const events = engine.init()
      const sessions = new Sessions(driver)
      this.engine = engine
      this.sessions = sessions
      this.partition = partition
      if (events.length > 0) this.dispatch(events)
      return { engine, sessions }
    }

    // -------------------------------------------------------------------------------------------
    // HTTP entry points (called only by the Worker router)
    // -------------------------------------------------------------------------------------------

    override async fetch(request: Request): Promise<Response> {
      const url = new URL(request.url)
      const partition = request.headers.get("x-orbit-partition")
      if (partition === null)
        return Response.json({ error: "missing partition header" }, { status: 400 })
      let bound: { engine: SyncEngine; sessions: Sessions }
      try {
        bound = this.bind(partition)
      } catch (e) {
        return Response.json({ error: e instanceof Error ? e.message : String(e) }, { status: 409 })
      }
      const { engine, sessions } = bound
      try {
        if (url.pathname === "/ws") return this.openSocket(request, partition)
        if (url.pathname === "/cdc" && request.method === "POST") {
          const body: unknown = await request.json()
          const started = Date.now()
          const { ack, events } = engine.applyBatch(body)
          this.dispatch(events)
          const deltas = events.filter((e) => e.type === "delta")
          // One line per batch, no row contents.
          log({
            event: "orbit.cdc.batch",
            partition,
            status: ack.status,
            ...(ack.status === "applied"
              ? {
                  applied_seq: ack.applied_seq,
                  duplicates: ack.duplicates,
                  apply_ms: ack.apply_ms,
                }
              : { reason: ack.reason.kind }),
            transactions: deltas.length,
            first_gtid: deltas[0]?.type === "delta" ? deltas[0].origin.gtid : null,
            sessions: sessions.count(),
            total_ms: Date.now() - started,
          })
          return Response.json(encodeAck(ack), { status: 200 })
        }
        if (url.pathname.startsWith("/fill/") && request.method === "POST") {
          return await this.acceptFill(
            request,
            decodeURIComponent(url.pathname.slice("/fill/".length)),
          )
        }
        if (url.pathname === "/status" && request.method === "GET") {
          return Response.json({
            ...engine.status(),
            sessions: sessions.count(),
            sockets: this.ctx.getWebSockets().length,
            outstandingFills: engine.outstandingFills().length,
          })
        }
        if (url.pathname === "/admin/reset" && request.method === "POST") {
          for (const ws of this.ctx.getWebSockets()) ws.close(CloseCode.internal, "partition reset")
          await this.ctx.storage.deleteAll()
          this.engine = null
          this.sessions = null
          this.partition = null
          return new Response(null, { status: 204 })
        }
        return Response.json({ error: "not found" }, { status: 404 })
      } catch (e) {
        const message = e instanceof Error ? e.message : String(e)
        return Response.json({ error: message }, { status: 500 })
      }
    }

    private async acceptFill(request: Request, fillId: string): Promise<Response> {
      const engine = this.engine
      if (engine === null) return Response.json({ error: "unbound" }, { status: 409 })
      const scope = engine.status().scopes.some((s) => s.state === "filling")
      if (!scope) {
        // A stale lease re-issued a fill this object already completed: drop it from the registry.
        this.dropFromRegistry(fillId)
        return Response.json({ error: "no fill in progress" }, { status: 409 })
      }
      const text = new TextDecoder().decode(await request.arrayBuffer())
      let done = false
      let rows = 0
      for (const line of text.split("\n")) {
        if (line.trim() === "") continue
        let chunk: FillChunk
        try {
          chunk = decodeFillChunk(line)
        } catch (e) {
          return Response.json(
            { error: `invalid fill chunk: ${e instanceof Error ? e.message : String(e)}` },
            { status: 400 },
          )
        }
        if (chunk.fill_id !== fillId)
          return Response.json({ error: "fill id mismatch" }, { status: 400 })
        if (chunk.type === "rows") {
          const r = engine.applyFillRows(fillId, chunk.rows)
          if (Result.isFailure(r)) {
            if (r.failure.code === "internal") {
              this.dropFromRegistry(fillId)
              return Response.json({ error: "fill is not active" }, { status: 409 })
            }
            // A row the source produced that the schema rejects: fail the fill explicitly.
            const events = engine.completeFill(fillId, { status: "failed", error: r.failure })
            this.dispatch(events)
            return Response.json({ error: "fill rows rejected", cause: r.failure }, { status: 422 })
          }
          rows += r.success
        } else {
          const events = engine.completeFill(fillId, chunk.result)
          this.dispatch(events)
          done = true
          log({
            event: "orbit.fill.completed",
            partition: this.partition,
            fill_id: fillId,
            status: chunk.result.status,
            rows,
            ...(chunk.result.status === "completed"
              ? { position: chunk.result.position, duration_ms: chunk.result.duration_ms }
              : { error: chunk.result.error.code }),
          })
          this.dropFromRegistry(fillId)
        }
      }
      return Response.json({ ok: true, rows, done })
    }

    private dropFromRegistry(fillId: string): void {
      void this.registry().then((stub) =>
        stub.fetch(
          new Request(`https://registry/fills/${encodeURIComponent(fillId)}`, {
            method: "DELETE",
          }),
        ),
      )
    }

    // -------------------------------------------------------------------------------------------
    // WebSocket sessions (Hibernation API)
    // -------------------------------------------------------------------------------------------

    private openSocket(request: Request, partition: string): Response {
      if (request.headers.get("upgrade")?.toLowerCase() !== "websocket")
        return Response.json({ error: "expected websocket" }, { status: 426 })
      const subject = request.headers.get("x-orbit-subject") ?? ""
      const expires = Number(request.headers.get("x-orbit-expires") ?? "")
      const expiresAt = Number.isFinite(expires) && expires > 0 ? expires : undefined
      const now = Date.now()
      if (expiresAt !== undefined && expiresAt <= now)
        return Response.json({ error: "unauthorized", reason: "expired" }, { status: 401 })
      const pair = new WebSocketPair()
      const client = pair[0]
      const server = pair[1]
      const session = crypto.randomUUID()
      const attachment: SocketAttachment = {
        session,
        subject,
        partition,
        hello: false,
        ...(expiresAt === undefined ? {} : { expiresAt }),
      }
      server.serializeAttachment(encodeAttachment(attachment))
      this.ctx.acceptWebSocket(server)
      // The alarm closes the socket when the grant expires; the client reconnects with a new token.
      if (expiresAt !== undefined) void this.scheduleAlarm(expiresAt)
      // A client that offered the Orbit subprotocol (the token travels in it) expects the echo.
      const headers = new Headers()
      if (offeredSubprotocols(request).includes(WS_SUBPROTOCOL))
        headers.set("sec-websocket-protocol", WS_SUBPROTOCOL)
      return new Response(null, { status: 101, webSocket: client, headers })
    }

    /** Closes the socket when the grant behind it has expired. Returns true when it did. */
    private expireIfDue(ws: WebSocket, attachment: SocketAttachment, now: number): boolean {
      if (attachment.expiresAt === undefined || attachment.expiresAt > now) return false
      this.send(ws, {
        type: "error",
        error: { code: "session_expired", message: "the token behind this session expired" },
        fatal: false,
      })
      try {
        ws.close(CloseCode.sessionExpired, "session_expired")
      } catch {
        // already closed
      }
      return true
    }

    /**
     * Closes every socket whose grant expired and returns the next expiry to wake up for, or
     * null when no live socket expires.
     */
    private sweepExpiredSockets(now: number): number | null {
      let next: number | null = null
      for (const ws of this.ctx.getWebSockets()) {
        const attachment = readAttachment(ws)
        if (attachment === null || attachment.expiresAt === undefined) continue
        if (this.expireIfDue(ws, attachment, now)) continue
        next = next === null ? attachment.expiresAt : Math.min(next, attachment.expiresAt)
      }
      return next
    }

    override async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
      const attachment = readAttachment(ws)
      if (attachment === null) {
        ws.close(CloseCode.internal, "missing session")
        return
      }
      if (this.expireIfDue(ws, attachment, Date.now())) return
      const text = typeof message === "string" ? message : new TextDecoder().decode(message)
      let parsed: unknown
      try {
        parsed = JSON.parse(text)
      } catch {
        this.fail(
          ws,
          { code: "invalid_message", message: "message is not JSON" },
          CloseCode.invalidMessage,
        )
        return
      }
      const decoded = await Effect.runPromiseExit(decodeClient(parsed))
      if (Exit.isFailure(decoded)) {
        this.fail(
          ws,
          { code: "invalid_message", message: Cause.pretty(decoded.cause).slice(0, 500) },
          CloseCode.invalidMessage,
        )
        return
      }
      const msg = decoded.value
      const { engine, sessions } = this.bind(attachment.partition)
      const now = Date.now()
      if (msg.type === "hello") {
        if (msg.protocolVersion !== CLIENT_PROTOCOL_VERSION) {
          this.fail(
            ws,
            {
              code: "protocol_version_mismatch",
              message: `server speaks protocol ${CLIENT_PROTOCOL_VERSION}`,
              details: { serverProtocolVersion: CLIENT_PROTOCOL_VERSION },
            },
            CloseCode.protocolVersionMismatch,
          )
          return
        }
        if (msg.partition !== attachment.partition) {
          this.fail(
            ws,
            {
              code: "partition_denied",
              message: "hello partition does not match the authorized partition",
            },
            CloseCode.partitionDenied,
          )
          return
        }
        const compat = compatibility(config.schema, msg.schema)
        if (!compat.compatible) {
          this.fail(
            ws,
            {
              code: "schema_mismatch",
              message: compat.reason,
              details: { serverSchemaHash: config.schema.schema_hash },
            },
            CloseCode.schemaMismatch,
          )
          return
        }
        sessions.create({
          session: attachment.session,
          clientId: msg.clientId,
          subject: attachment.subject,
          schema: msg.schema,
          identicalSchema: compat.identical,
          now,
        })
        ws.serializeAttachment(encodeAttachment({ ...attachment, hello: true }))
        this.send(ws, {
          type: "welcome",
          protocolVersion: CLIENT_PROTOCOL_VERSION,
          sessionId: attachment.session,
          partition: attachment.partition,
          schemaHash: config.schema.schema_hash,
          cursor: engine.appliedSeq,
          serverTime: now,
        })
        for (const sub of msg.subscriptions) this.subscribe(ws, attachment.session, sub)
        return
      }
      if (!attachment.hello || sessions.get(attachment.session) === null) {
        this.fail(
          ws,
          { code: "invalid_message", message: "hello must be the first message" },
          CloseCode.invalidMessage,
        )
        return
      }
      switch (msg.type) {
        case "subscribe":
          this.subscribe(ws, attachment.session, msg)
          return
        case "unsubscribe": {
          const subscription = sessions.removeClientSub(attachment.session, msg.id)
          if (subscription !== null && sessions.referenceCount(subscription) === 0)
            this.orphan(subscription)
          this.send(ws, { type: "unsubscribed", id: msg.id })
          return
        }
        case "ack":
          sessions.touch(attachment.session, now, msg.cursor)
          return
        case "ping":
          sessions.touch(attachment.session, now)
          this.send(ws, { type: "pong", sentAt: msg.sentAt, serverTime: now })
          return
      }
    }

    override async webSocketClose(
      ws: WebSocket,
      _code: number,
      _reason: string,
      _wasClean: boolean,
    ): Promise<void> {
      this.dropSession(ws)
    }

    override async webSocketError(ws: WebSocket, _error: unknown): Promise<void> {
      this.dropSession(ws)
    }

    private dropSession(ws: WebSocket): void {
      const attachment = readAttachment(ws)
      if (attachment === null) return
      this.dropSessionById(attachment.session)
    }

    private dropSessionById(session: string): void {
      if (this.sessions === null || this.engine === null) return
      const subs = this.sessions.clientSubsOf(session)
      this.sessions.remove(session)
      for (const s of subs) {
        if (this.sessions.referenceCount(s.subscription) === 0) this.orphan(s.subscription)
      }
    }

    /**
     * The last session left a subscription. It stays materialized for the grace period (see
     * `subscriptionGraceMs`); the alarm drops it afterwards.
     */
    private orphan(subscription: string): void {
      const engine = this.engine
      if (engine === null) return
      const now = Date.now()
      engine.markOrphaned(subscription, now)
      void this.scheduleAlarm(now + subscriptionGraceMs)
    }

    /** Sets the alarm to `at` unless an earlier one is already set. */
    private async scheduleAlarm(at: number): Promise<void> {
      try {
        const current = await this.ctx.storage.getAlarm()
        if (current === null || current > at) await this.ctx.storage.setAlarm(at)
      } catch {
        // The next event that sets an alarm retries; an orphan that lingers costs little.
      }
    }

    /**
     * Sessions whose socket is gone (a close the runtime never reported, an eviction) would
     * otherwise pile up and every delta would be prepared for them. Reconciles against the live
     * sockets and returns the sessions that are still connected.
     */
    private liveSessions(sockets: ReadonlyMap<string, WebSocket>): ReadonlyArray<SessionRow> {
      const sessions = this.sessions
      if (sessions === null) return []
      const live: Array<SessionRow> = []
      for (const row of sessions.all()) {
        if (sockets.has(row.session)) live.push(row)
        else this.dropSessionById(row.session)
      }
      return live
    }

    private subscribe(ws: WebSocket, session: string, sub: SubscribeMessage): void {
      const engine = this.engine
      const sessions = this.sessions
      const partition = this.partition
      if (engine === null || sessions === null || partition === null) return
      const row = sessions.get(session)
      if (row === null) return
      const query = resolveQuery(sub.query, {
        partition,
        subject: row.subject,
        clientId: row.clientId,
      })
      if (Result.isFailure(query)) {
        this.send(ws, { type: "subscription_error", id: sub.id, error: query.failure })
        return
      }
      // A base is honoured only when this session holds it live: its members are then known to
      // the client at every cursor the snapshot can be taken at.
      const base =
        sub.basedOn === undefined
          ? null
          : (sessions
              .clientSubsOf(session)
              .find((s) => s.clientSubId === sub.basedOn && s.status === "live") ?? null)
      const outcome = engine.subscribe(
        query.success,
        base === null ? {} : { basedOn: base.subscription },
      )
      if (Result.isFailure(outcome)) {
        this.send(ws, { type: "subscription_error", id: sub.id, error: outcome.failure })
        return
      }
      sessions.setClientSub(session, sub.id, outcome.success.subscription, outcome.success.status)
      const snapshot = outcome.success.events.find(
        (e) => e.type === "snapshot" && e.subscription === outcome.success.subscription,
      )
      // One line per subscribe, no row contents: what the first snapshot carries. (No duration:
      // the Workers clock does not advance during synchronous work.)
      log({
        event: "orbit.subscription.subscribed",
        partition,
        status: outcome.success.status,
        rows: snapshot?.type === "snapshot" ? snapshot.rows.length : 0,
        members: snapshot?.type === "snapshot" ? snapshot.members.length : 0,
        based_on: base !== null,
      })
      this.send(ws, {
        type: "subscribed",
        id: sub.id,
        status: outcome.success.status,
        query: outcome.success.query,
      })
      // The snapshot of an already-live subscription is for this session only: the sessions
      // that hold it already have the rows, and a large preload must not be resent to everyone
      // each time a client joins. Other events (fills, a fresh materialization) go through
      // `dispatch`, which addresses every session of the subscription.
      const others: Array<EngineEvent> = []
      for (const event of outcome.success.events) {
        if (event.type !== "snapshot" || event.subscription !== outcome.success.subscription) {
          others.push(event)
          continue
        }
        sessions.markLive(event.subscription)
        this.sendSnapshot(
          ws,
          row,
          sub.id,
          event.cursor,
          event.rows,
          event.members,
          event.basedOn !== undefined && event.basedOn === base?.subscription
            ? sub.basedOn
            : undefined,
        )
      }
      this.dispatch(others)
    }

    /** `subscribed` for an engine subscription, carrying its normalized query. */
    private subscribed(
      engineSub: string,
      clientSubId: string,
      status: "pending" | "live",
    ): ServerMessage | null {
      const query = this.engine?.subscription(engineSub)?.planned.query
      return query === undefined ? null : { type: "subscribed", id: clientSubId, status, query }
    }

    // -------------------------------------------------------------------------------------------
    // Event delivery
    // -------------------------------------------------------------------------------------------

    /** Routes engine events to sessions, the fill registry and the alarm. Never throws. */
    private dispatch(events: ReadonlyArray<EngineEvent>): void {
      const sessions = this.sessions
      if (sessions === null) return
      const sockets = new Map<string, WebSocket>()
      for (const ws of this.ctx.getWebSockets()) {
        const a = readAttachment(ws)
        if (a !== null) sockets.set(a.session, ws)
      }
      for (const event of events) {
        switch (event.type) {
          case "fill_needed":
            void this.enqueueFill(event.request)
            break
          case "snapshot": {
            sessions.markLive(event.subscription)
            for (const { session, clientSubId } of sessions.sessionsOf(event.subscription)) {
              const ws = sockets.get(session)
              const row = sessions.get(session)
              if (ws === undefined || row === null) continue
              const live = this.subscribed(event.subscription, clientSubId, "live")
              if (live !== null) this.send(ws, live)
              this.sendSnapshot(ws, row, clientSubId, event.cursor, event.rows, event.members)
            }
            break
          }
          case "subscription_failed":
            for (const { session, clientSubId } of sessions.sessionsOf(event.subscription)) {
              const ws = sockets.get(session)
              if (ws !== undefined)
                this.send(ws, { type: "subscription_error", id: clientSubId, error: event.error })
            }
            break
          case "scopes_reset": {
            sessions.markAllPending()
            const engine = this.engine
            for (const row of sessions.all()) {
              const ws = sockets.get(row.session)
              if (ws === undefined) continue
              for (const cs of sessions.clientSubsOf(row.session)) {
                const pending = this.subscribed(cs.subscription, cs.clientSubId, "pending")
                if (pending !== null) this.send(ws, pending)
                if (engine !== null) {
                  const sub = engine.subscription(cs.subscription)
                  if (sub !== null) {
                    const again = engine.subscribe(sub.planned.query)
                    if (Result.isSuccess(again)) this.dispatch(again.success.events)
                  }
                }
              }
            }
            break
          }
          case "delta": {
            const engine = this.engine
            if (engine === null) break
            // One indexed lookup for every row in the delta, shared by all sessions.
            const index = engine.membershipIndex(
              event.rows.filter((r) => r.row !== null).map((r) => ({ table: r.table, key: r.key })),
            )
            for (const row of this.liveSessions(sockets)) {
              const ws = sockets.get(row.session)
              if (ws === undefined) continue
              const subs = sessions.clientSubsOf(row.session).filter((s) => s.status === "live")
              if (subs.length === 0) continue
              const memberships = event.memberships.flatMap((m) =>
                subs
                  .filter((s) => s.subscription === m.subscriptionId)
                  .map((s) => ({ ...m, subscriptionId: s.clientSubId })),
              )
              const engineSubs = subs.map((s) => s.subscription)
              const rows = event.rows.filter((r) => {
                if (r.row === null) return true
                const owners = index.get(`${r.table}\u0000${JSON.stringify(r.key)}`)
                return owners !== undefined && engineSubs.some((s) => owners.has(s))
              })
              if (memberships.length === 0 && rows.length === 0) continue
              this.send(ws, {
                type: "delta",
                cursor: event.cursor,
                origin: event.origin,
                rows: this.projectRows(row, rows),
                memberships,
              })
            }
            break
          }
        }
      }
    }

    private projectRows(
      session: { readonly identicalSchema: boolean; readonly schema: SchemaSummary },
      rows: ReadonlyArray<RowUpdate>,
    ): ReadonlyArray<RowUpdate> {
      if (session.identicalSchema) return rows
      const columns = new Map(
        session.schema.tables.map((t) => [t.name, new Set(t.columns)] as const),
      )
      return rows.map((r) => {
        const cols = columns.get(r.table)
        return r.row === null || cols === undefined ? r : { ...r, row: projectRow(r.row, cols) }
      })
    }

    private sendSnapshot(
      ws: WebSocket,
      session: { readonly identicalSchema: boolean; readonly schema: SchemaSummary },
      clientSubId: string,
      cursor: number,
      rows: ReadonlyArray<RowUpdate>,
      members: ReadonlyArray<MemberRef>,
      basedOn?: string,
    ): void {
      const projected = this.projectRows(session, rows)
      const extension = basedOn === undefined ? {} : { basedOn }
      if (projected.length <= snapshotChunkRows) {
        this.send(ws, {
          type: "snapshot",
          subscriptionId: clientSubId,
          cursor,
          rows: projected,
          members,
          complete: true,
          ...extension,
        })
        return
      }
      for (let i = 0; i < projected.length; i += snapshotChunkRows) {
        const last = i + snapshotChunkRows >= projected.length
        this.send(ws, {
          type: "snapshot",
          ...extension,
          subscriptionId: clientSubId,
          cursor,
          rows: projected.slice(i, i + snapshotChunkRows),
          members: last ? members : [],
          complete: last,
        })
      }
    }

    private send(ws: WebSocket, message: ServerMessage): void {
      try {
        ws.send(JSON.stringify(encodeServer(message)))
      } catch {
        // The socket is gone; close bookkeeping happens in webSocketClose/webSocketError.
      }
    }

    private fail(ws: WebSocket, error: SyncError, code: number): void {
      this.send(ws, { type: "error", error, fatal: true })
      try {
        ws.close(code, error.code)
      } catch {
        // already closed
      }
    }

    // -------------------------------------------------------------------------------------------
    // Fills and alarms
    // -------------------------------------------------------------------------------------------

    private async registry(): Promise<DurableObjectStub> {
      return this.env.ORBIT_FILL_REGISTRY.get(this.env.ORBIT_FILL_REGISTRY.idFromName("registry"))
    }

    private async enqueueFill(request: FillRequest): Promise<void> {
      try {
        const stub = await this.registry()
        await stub.fetch(
          new Request("https://registry/enqueue", {
            method: "POST",
            body: JSON.stringify(encodeFillRequest(request)),
            headers: { "content-type": "application/json" },
          }),
        )
      } catch {
        // The alarm retries outstanding fills.
      }
      await this.scheduleAlarm(Date.now() + fillTimeoutMs)
    }

    /**
     * Retries fills that did not complete within the timeout (gives up after `maxFillAttempts`)
     * and drops subscriptions whose grace period passed.
     */
    override async alarm(): Promise<void> {
      const now = Date.now()
      const nextExpiry = this.sweepExpiredSockets(now)
      if (nextExpiry !== null) await this.scheduleAlarm(nextExpiry)
      const engine = this.engine
      if (engine === null) return
      const dropped = engine.sweepOrphans(now - subscriptionGraceMs)
      if (dropped.length > 0)
        log({
          event: "orbit.subscriptions.swept",
          partition: this.partition,
          count: dropped.length,
        })
      const nextOrphan = engine.nextOrphanDue(subscriptionGraceMs)
      if (nextOrphan !== null) await this.scheduleAlarm(Math.max(nextOrphan, now + 1000))
      let pending = 0
      for (const fill of engine.outstandingFills()) {
        if (now - fill.requested_at_ms < fillTimeoutMs) {
          pending += 1
          continue
        }
        const attempts = this.ctx.storage.sql
          .exec(`SELECT attempts FROM fills WHERE fill_id = ?`, fill.fill_id)
          .toArray()[0]?.["attempts"]
        if (typeof attempts === "number" && attempts >= maxFillAttempts) {
          const events = engine.completeFill(fill.fill_id, {
            status: "failed",
            error: { code: "timeout", after_ms: fillTimeoutMs * attempts },
          })
          this.dispatch(events)
          continue
        }
        const next = engine.retryFill(fill.fill_id)
        if (next !== null) {
          pending += 1
          await this.enqueueFill(next)
        }
      }
      if (pending > 0) await this.scheduleAlarm(now + fillTimeoutMs)
    }
  }
}

export type SyncDurableObjectClass = ReturnType<typeof makeSyncDurableObject>
