/**
 * Dedicated worker that owns the SQLite database.
 *
 * Persistence uses the official `@sqlite.org/sqlite-wasm` build with the `opfs-sahpool` VFS:
 * it needs no COOP/COEP headers, runs in a dedicated worker, and gives the best write
 * throughput. Only one context may hold the pool at a time: a second tab gets a `locked` error
 * and `createOrbitClient` falls back to an in-memory database (see `client.ts`).
 *
 * Errors are classified explicitly: quota exhaustion, lock contention (another tab holds the
 * pool), corruption and SQL errors all reach the main thread as typed `error` responses.
 */

import sqlite3InitModule, {
  type Database,
  type SAHPoolUtil,
  type Sqlite3Static,
} from "@sqlite.org/sqlite-wasm"
import { Schema } from "effect"

import { WorkerRequest, type WorkerResponse } from "../driver.ts"
import { runBatch } from "./batch.ts"

let sqlite3: Sqlite3Static | null = null
let db: Database | null = null
let pool: SAHPoolUtil | null = null
/** Releases the Web Lock that marks this context's slot (see `holdSlot`). */
let releaseSlot: (() => void) | null = null

/**
 * Marks the pool as held by this context with a Web Lock, released when the context ends. A
 * pool another tab holds is refused here, before OPFS reports the conflict handle by handle.
 * Without the Web Locks API the OPFS access handles alone decide.
 */
const holdSlot = async (poolName: string): Promise<boolean> => {
  if (typeof navigator === "undefined" || navigator.locks === undefined) return true
  return new Promise<boolean>((resolve) => {
    void navigator.locks.request(`orbit-slot:${poolName}`, { ifAvailable: true }, (lock) => {
      if (lock === null) {
        resolve(false)
        return
      }
      resolve(true)
      return new Promise<void>((release) => {
        releaseSlot = release
      })
    })
  })
}

const decodeRequest = Schema.decodeUnknownSync(WorkerRequest)

const post = (message: WorkerResponse): void => {
  self.postMessage(message)
}

const classify = (
  e: unknown,
): {
  code: "quota_exceeded" | "locked" | "corrupt" | "unsupported" | "sql" | "worker"
  message: string
} => {
  const message = e instanceof Error ? e.message : String(e)
  const lower = message.toLowerCase()
  if (
    lower.includes("quota") ||
    lower.includes("no space") ||
    lower.includes("sqlite_full") ||
    lower.includes("database or disk is full")
  )
    return { code: "quota_exceeded", message }
  if (
    lower.includes("nomodificationallowederror") ||
    lower.includes("access handle") ||
    lower.includes("locked") ||
    lower.includes("sqlite_busy")
  )
    return { code: "locked", message }
  if (lower.includes("malformed") || lower.includes("corrupt") || lower.includes("sqlite_corrupt"))
    return { code: "corrupt", message }
  if (
    lower.includes("not supported") ||
    lower.includes("unsupported") ||
    lower.includes("opfs") ||
    lower.includes("createsyncaccesshandle")
  )
    return { code: "unsupported", message }
  return { code: "sql", message }
}

const open = async (
  name: string,
  mode: "opfs" | "memory",
  poolName = "orbit-sahpool",
): Promise<void> => {
  if (sqlite3 === null) sqlite3 = await sqlite3InitModule()
  if (db !== null) return
  if (mode === "memory") {
    db = new sqlite3.oo1.DB(":memory:", "c")
  } else {
    if (!(await holdSlot(poolName))) throw new Error(`pool ${poolName} is locked by another tab`)
    pool = await sqlite3.installOpfsSAHPoolVfs({ name: poolName, initialCapacity: 6 })
    db = new pool.OpfsSAHPoolDb(`/${name}.sqlite3`)
  }
  db.exec("PRAGMA foreign_keys = OFF; PRAGMA synchronous = NORMAL;")
}

type Cell = string | number | null | Uint8Array<ArrayBuffer>

/**
 * Maps a SQLite cell to a structured-clone-safe value. SQLite only yields null, string, number,
 * bigint and blob cells; the remaining branches keep the function total over `unknown`.
 */
const normalizeCell = (v: unknown): Cell => {
  if (v === null || typeof v === "string" || typeof v === "number") return v
  if (typeof v === "bigint") return Number(v)
  if (v instanceof Uint8Array) return new Uint8Array(v)
  if (typeof v === "boolean") return v ? 1 : 0
  if (typeof v === "object") return JSON.stringify(v)
  return null
}

const rowsOf = (
  sql: string,
  params: ReadonlyArray<string | number | null | Uint8Array>,
): Array<Record<string, Cell>> => {
  if (db === null) throw new Error("database is not open")
  const rows: Array<Record<string, Cell>> = []
  db.exec({
    sql,
    bind: params,
    rowMode: "object",
    callback: (row) => {
      const out: Record<string, Cell> = {}
      for (const [k, v] of Object.entries(row)) out[k] = normalizeCell(v)
      rows.push(out)
    },
  })
  return rows
}

const handleRequest = async (event: MessageEvent<unknown>): Promise<void> => {
  let request: WorkerRequest
  try {
    request = decodeRequest(event.data)
  } catch (e) {
    post({
      type: "error",
      id: -1,
      code: "worker",
      message: `invalid worker request: ${e instanceof Error ? e.message : String(e)}`,
    })
    return
  }
  try {
    switch (request.type) {
      case "open":
        await open(request.name, request.mode, request.pool)
        post({ type: "ok", id: request.id })
        return
      case "query":
        post({ type: "ok", id: request.id, rows: rowsOf(request.sql, request.params) })
        return
      case "batch": {
        if (db === null) throw new Error("database is not open")
        runBatch(db, request.statements)
        post({ type: "ok", id: request.id })
        return
      }
      case "estimate": {
        const estimate =
          typeof navigator !== "undefined" &&
          "storage" in navigator &&
          typeof navigator.storage.estimate === "function"
            ? await navigator.storage.estimate()
            : null
        const size =
          db === null
            ? null
            : (rowsOf(
                "SELECT page_count * page_size AS bytes FROM pragma_page_count(), pragma_page_size()",
                [],
              )[0]?.["bytes"] ?? null)
        post({
          type: "estimate_result",
          id: request.id,
          usage: estimate?.usage ?? null,
          quota: estimate?.quota ?? null,
          databaseBytes: typeof size === "number" ? size : null,
        })
        return
      }
      case "close":
        db?.close()
        db = null
        if (pool !== null) {
          pool.pauseVfs()
          pool = null
        }
        releaseSlot?.()
        releaseSlot = null
        post({ type: "ok", id: request.id })
        return
    }
  } catch (e) {
    const { code, message } = classify(e)
    post({ type: "error", id: request.id, code, message })
  }
}

// `handleRequest` reports every outcome through `post`, so the returned promise never rejects.
self.addEventListener("message", (event: MessageEvent<unknown>) => {
  void handleRequest(event)
})
