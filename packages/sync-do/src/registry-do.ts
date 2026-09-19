/**
 * Fill registry: one Durable Object per deployment that queues demand-fill requests and serves
 * them to the Rust server through long polling. Requests are stored in SQLite so a restart
 * loses nothing; delivery is at-least-once (the Sync Durable Object ignores results for fills it
 * no longer expects).
 */

import { DurableObject } from "cloudflare:workers"
import { Schema } from "effect"
import { FillPollResponse, FillRequest } from "@orbit/protocol"

const MAX_PER_POLL = 16
const OFFER_LEASE_MS = 5_000
const ACTIVE_LEASE_MS = 180_000

export class FillRegistryDurableObject extends DurableObject<Record<string, unknown>> {
  private waiters = new Set<() => void>()

  constructor(ctx: DurableObjectState, env: Record<string, unknown>) {
    super(ctx, env)
    ctx.storage.sql.exec(
      `CREATE TABLE IF NOT EXISTS fills (fill_id TEXT PRIMARY KEY, request TEXT NOT NULL, enqueued_at INTEGER NOT NULL, leased_until INTEGER NOT NULL DEFAULT 0)`,
    )
    const columns = ctx.storage.sql.exec(`PRAGMA table_info(fills)`).toArray()
    if (!columns.some((c) => c["name"] === "lease_id"))
      ctx.storage.sql.exec(`ALTER TABLE fills ADD COLUMN lease_id TEXT`)
    ctx.storage.sql.exec(
      `CREATE INDEX IF NOT EXISTS fills_lease ON fills(leased_until, enqueued_at)`,
    )
    ctx.storage.sql.exec(`CREATE INDEX IF NOT EXISTS fills_receipt ON fills(lease_id)`)
  }

  /** Called by Sync Durable Objects. Idempotent per fill id. */
  enqueue(request: FillRequest): void {
    const encoded = JSON.stringify(Schema.encodeSync(FillRequest)(request))
    this.ctx.storage.sql.exec(
      `INSERT OR IGNORE INTO fills (fill_id, request, enqueued_at) VALUES (?, ?, ?)`,
      request.fill_id,
      encoded,
      Date.now(),
    )
    const waiters = this.waiters
    this.waiters = new Set()
    for (const w of waiters) w()
  }

  /** Called when a fill is no longer wanted (completed or superseded). */
  dequeue(fillId: string): void {
    this.ctx.storage.sql.exec(`DELETE FROM fills WHERE fill_id = ?`, fillId)
  }

  private takeReady(now: number, leaseId: string | null, limit: number): FillPollResponse {
    const rows = this.ctx.storage.sql
      .exec(
        `SELECT fill_id, request FROM fills WHERE leased_until < ? ORDER BY enqueued_at LIMIT ?`,
        now,
        limit,
      )
      .toArray()
    const requests: Array<FillRequest> = []
    for (const r of rows) {
      const parsed = Schema.decodeUnknownSync(Schema.fromJsonString(FillRequest))(r["request"])
      requests.push(parsed)
      // A lease keeps two pollers from running the same fill; it expires so a crashed server's
      // fills are handed out again. Completion removes the row.
      this.ctx.storage.sql.exec(
        `UPDATE fills SET leased_until = ?, lease_id = ? WHERE fill_id = ?`,
        now + (leaseId === null ? ACTIVE_LEASE_MS : OFFER_LEASE_MS),
        leaseId,
        parsed.fill_id,
      )
    }
    return { requests }
  }

  /** Long poll, also waking when a lost delivery's provisional lease expires. */
  async next(
    waitMs: number,
    leaseId: string | null = null,
    limit = MAX_PER_POLL,
  ): Promise<FillPollResponse> {
    const first = this.takeReady(Date.now(), leaseId, limit)
    if (first.requests.length > 0) return first
    const nextLease = this.ctx.storage.sql
      .exec(`SELECT MIN(leased_until) AS expiry FROM fills`)
      .one()["expiry"]
    const untilLease =
      typeof nextLease === "number" ? Math.max(1, nextLease - Date.now() + 1) : 25_000
    await new Promise<void>((resolve) => {
      const wake = () => {
        clearTimeout(timer)
        this.waiters.delete(wake)
        resolve()
      }
      const timer = setTimeout(wake, Math.min(Math.max(waitMs, 0), 25_000, untilLease))
      this.waiters.add(wake)
    })
    return this.takeReady(Date.now(), leaseId, limit)
  }

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    if (request.method === "GET" && url.pathname === "/next") {
      const wait = Number(url.searchParams.get("wait") ?? "20") * 1000
      // Opt-in preserves old engines: only receipt-aware pollers get short offers.
      const leaseId = url.searchParams.get("ack") === "1" ? crypto.randomUUID() : null
      const requestedLimit = Number(url.searchParams.get("limit") ?? MAX_PER_POLL)
      const limit = Number.isFinite(requestedLimit)
        ? Math.max(1, Math.min(MAX_PER_POLL, Math.floor(requestedLimit)))
        : MAX_PER_POLL
      const resp = await this.next(wait, leaseId, limit)
      return Response.json(Schema.encodeSync(FillPollResponse)(resp), {
        headers: leaseId === null ? {} : { "x-orbit-fill-lease": leaseId },
      })
    }
    if (request.method === "POST" && url.pathname === "/claim") {
      const leaseId = request.headers.get("x-orbit-fill-lease")
      if (leaseId === null) return new Response("missing lease", { status: 400 })
      // A stale receipt cannot claim requests already offered to another poller.
      this.ctx.storage.sql.exec(
        `UPDATE fills SET leased_until = ? WHERE lease_id = ?`,
        Date.now() + ACTIVE_LEASE_MS,
        leaseId,
      )
      return new Response(null, { status: 204 })
    }
    if (request.method === "POST" && url.pathname === "/enqueue") {
      const body: unknown = await request.json()
      this.enqueue(Schema.decodeUnknownSync(FillRequest)(body))
      return new Response(null, { status: 204 })
    }
    if (request.method === "DELETE" && url.pathname.startsWith("/fills/")) {
      this.dequeue(decodeURIComponent(url.pathname.slice("/fills/".length)))
      return new Response(null, { status: 204 })
    }
    if (request.method === "GET" && url.pathname === "/status") {
      const n = this.ctx.storage.sql.exec(`SELECT COUNT(*) AS n FROM fills`).one()["n"]
      return Response.json({ queued: n })
    }
    return new Response("not found", { status: 404 })
  }
}
