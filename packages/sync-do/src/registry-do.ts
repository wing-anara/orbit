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

export class FillRegistryDurableObject extends DurableObject<Record<string, unknown>> {
  private waiters: Array<() => void> = []

  constructor(ctx: DurableObjectState, env: Record<string, unknown>) {
    super(ctx, env)
    ctx.storage.sql.exec(
      `CREATE TABLE IF NOT EXISTS fills (fill_id TEXT PRIMARY KEY, request TEXT NOT NULL, enqueued_at INTEGER NOT NULL, leased_until INTEGER NOT NULL DEFAULT 0)`,
    )
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
    this.waiters = []
    for (const w of waiters) w()
  }

  /** Called when a fill is no longer wanted (completed or superseded). */
  dequeue(fillId: string): void {
    this.ctx.storage.sql.exec(`DELETE FROM fills WHERE fill_id = ?`, fillId)
  }

  private takeReady(now: number): FillPollResponse {
    const rows = this.ctx.storage.sql
      .exec(
        `SELECT fill_id, request FROM fills WHERE leased_until < ? ORDER BY enqueued_at LIMIT ?`,
        now,
        MAX_PER_POLL,
      )
      .toArray()
    const requests: Array<FillRequest> = []
    for (const r of rows) {
      const parsed = Schema.decodeUnknownSync(Schema.fromJsonString(FillRequest))(r["request"])
      requests.push(parsed)
      // A lease keeps two pollers from running the same fill; it expires so a crashed server's
      // fills are handed out again. Completion removes the row.
      this.ctx.storage.sql.exec(
        `UPDATE fills SET leased_until = ? WHERE fill_id = ?`,
        now + 180_000,
        parsed.fill_id,
      )
    }
    return { requests }
  }

  /** Long poll: waits up to `waitMs` for at least one request. */
  async next(waitMs: number): Promise<FillPollResponse> {
    const first = this.takeReady(Date.now())
    if (first.requests.length > 0) return first
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, Math.min(Math.max(waitMs, 0), 25_000))
      this.waiters.push(() => {
        clearTimeout(timer)
        resolve()
      })
    })
    return this.takeReady(Date.now())
  }

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    if (request.method === "GET" && url.pathname === "/next") {
      const wait = Number(url.searchParams.get("wait") ?? "20") * 1000
      const resp = await this.next(wait)
      return Response.json(Schema.encodeSync(FillPollResponse)(resp))
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
