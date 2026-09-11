import type { SqlValue } from "@orbit/query"

import type { SqlDriver, SqlRecord } from "./core/driver.ts"

/** Durable Object SQLite binds `ArrayBuffer`, not typed arrays; copy the viewed bytes. */
const toArrayBuffer = (bytes: Uint8Array): ArrayBuffer => {
  const copy = new ArrayBuffer(bytes.byteLength)
  new Uint8Array(copy).set(bytes)
  return copy
}

const bind = (params: ReadonlyArray<SqlValue>): Array<SqlStorageValue> =>
  params.map((p) => (p instanceof Uint8Array ? toArrayBuffer(p) : p))

const fromStorage = (value: SqlStorageValue): SqlValue =>
  value instanceof ArrayBuffer ? new Uint8Array(value) : value

/** Cursor rows carry `ArrayBuffer` blobs; the engine's `SqlRecord` carries `Uint8Array`. */
const toRecord = (row: Readonly<Record<string, SqlStorageValue>>): SqlRecord => {
  const out: Record<string, SqlValue> = {}
  for (const [column, value] of Object.entries(row)) out[column] = fromStorage(value)
  return out
}

/**
 * `SqlDriver` over Durable Object SQLite storage. `transactionSync` cannot nest, so nested calls
 * from the engine join the outer transaction by running inline.
 */
export const durableObjectDriver = (storage: DurableObjectStorage): SqlDriver => {
  let depth = 0
  return {
    query: (sql, params = []) =>
      storage.sql
        .exec<Record<string, SqlStorageValue>>(sql, ...bind(params))
        .toArray()
        .map(toRecord),
    run: (sql, params = []) => {
      storage.sql.exec(sql, ...bind(params))
    },
    transaction: <T>(f: () => T): T => {
      if (depth > 0) return f()
      depth += 1
      try {
        return storage.transactionSync(f)
      } finally {
        depth -= 1
      }
    },
  }
}
