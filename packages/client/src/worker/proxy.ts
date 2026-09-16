/** Main-thread proxy that turns the SQLite worker's message protocol into an `AsyncSqlDriver`. */

import { Schema } from "effect"
import type { SqlValue } from "@orbit/query"

import {
  SqlDriverError,
  WorkerResponse,
  type AsyncSqlDriver,
  type Statement,
  type WorkerRequest,
} from "../driver.ts"

const decodeResponse = Schema.decodeUnknownSync(WorkerResponse)

// Structured clone needs plain ArrayBuffer-backed views; copy any shared-buffer views.
const bind = (
  params: ReadonlyArray<SqlValue>,
): Array<string | number | null | Uint8Array<ArrayBuffer>> =>
  params.map((p) => (p instanceof Uint8Array ? new Uint8Array(p) : p))

interface Pending {
  readonly resolve: (value: WorkerResponse) => void
  readonly reject: (error: SqlDriverError) => void
}

export interface WorkerDriver extends AsyncSqlDriver {
  readonly estimate: () => Promise<{
    readonly usage: number | null
    readonly quota: number | null
    readonly databaseBytes: number | null
  }>
}

export const openWorkerDriver = async (
  worker: Worker,
  name: string,
  mode: "opfs" | "memory",
  pool?: string,
  signal?: AbortSignal,
): Promise<WorkerDriver> => {
  let nextId = 1
  const pending = new Map<number, Pending>()
  let closed = false

  worker.addEventListener("message", (event: MessageEvent<unknown>) => {
    let response: WorkerResponse
    try {
      response = decodeResponse(event.data)
    } catch {
      return
    }
    const p = pending.get(response.id)
    if (p === undefined) return
    pending.delete(response.id)
    if (response.type === "error")
      p.reject(new SqlDriverError({ code: response.code, message: response.message }))
    else p.resolve(response)
  })
  worker.addEventListener("error", (event: ErrorEvent) => {
    const error = new SqlDriverError({ code: "worker", message: event.message })
    for (const p of pending.values()) p.reject(error)
    pending.clear()
  })

  const abort = (): void => {
    if (closed) return
    closed = true
    worker.terminate()
    const error = new SqlDriverError({ code: "closed", message: "SQLite worker aborted" })
    for (const request of pending.values()) request.reject(error)
    pending.clear()
  }
  signal?.addEventListener("abort", abort, { once: true })
  if (signal?.aborted) abort()

  type DistributiveOmit<T, K extends keyof T> = T extends unknown ? Omit<T, K> : never
  const call = (request: DistributiveOmit<WorkerRequest, "id">): Promise<WorkerResponse> => {
    if (closed)
      return Promise.reject(new SqlDriverError({ code: "closed", message: "driver is closed" }))
    const id = nextId++
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject })
      worker.postMessage({ ...request, id })
    })
  }

  try {
    await call({ type: "open", name, mode, ...(pool === undefined ? {} : { pool }) })
  } catch (e) {
    // A worker whose open failed (the pool is held by another tab) has nothing to keep.
    signal?.removeEventListener("abort", abort)
    worker.terminate()
    throw e
  }

  return {
    query: async (sql, params = []) => {
      const r = await call({ type: "query", sql, params: bind(params) })
      return r.type === "ok" ? (r.rows ?? []) : []
    },
    batch: async (statements: ReadonlyArray<Statement>) => {
      await call({
        type: "batch",
        statements: statements.map((s) => ({ sql: s.sql, params: bind(s.params) })),
      })
    },
    estimate: async () => {
      const r = await call({ type: "estimate" })
      return r.type === "estimate_result"
        ? { usage: r.usage, quota: r.quota, databaseBytes: r.databaseBytes }
        : { usage: null, quota: null, databaseBytes: null }
    },
    close: async () => {
      if (closed) return
      await call({ type: "close" })
      closed = true
      signal?.removeEventListener("abort", abort)
      worker.terminate()
    },
  }
}
