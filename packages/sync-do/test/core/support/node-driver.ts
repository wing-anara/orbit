import { DatabaseSync } from "node:sqlite"

import type { SqlDriver, SqlRecord } from "../../../src/core/driver.ts"

/** Durable Object SQLite binds at most 100 parameters per statement; mirror that here. */
const DO_MAX_BOUND_PARAMS = 100

const guardParams = (sql: string, params: ReadonlyArray<unknown>): void => {
  if (params.length > DO_MAX_BOUND_PARAMS)
    throw new Error(`too many SQL variables (${params.length}) in: ${sql.slice(0, 80)}`)
}

/** `SqlDriver` over Node's built-in SQLite, with nested-transaction support via savepoints. */
export const nodeDriver = (
  db: DatabaseSync = new DatabaseSync(":memory:"),
): SqlDriver & { readonly db: DatabaseSync } => {
  let depth = 0
  return {
    db,
    query: (sql, params = []) => {
      guardParams(sql, params)
      return db
        .prepare(sql)
        .all(...(params as Array<string | number | null | Uint8Array>)) as Array<SqlRecord>
    },
    run: (sql, params = []) => {
      guardParams(sql, params)
      db.prepare(sql).run(...(params as Array<string | number | null | Uint8Array>))
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
