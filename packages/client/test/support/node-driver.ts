import { DatabaseSync } from "node:sqlite"

import type { SqlValue } from "@orbit/query"

import type { AsyncSqlDriver, SqlRecord, Statement } from "../../src/driver.ts"

/** Async driver over Node's SQLite; `batch` runs inside one transaction like the worker does. */
export const nodeAsyncDriver = (
  db: DatabaseSync = new DatabaseSync(":memory:"),
): AsyncSqlDriver & { readonly db: DatabaseSync } => ({
  db,
  query: async (sql, params = []) =>
    db
      .prepare(sql)
      .all(...(params as Array<string | number | null | Uint8Array>)) as Array<SqlRecord>,
  batch: async (statements: ReadonlyArray<Statement>) => {
    db.exec("BEGIN")
    try {
      for (const s of statements)
        db.prepare(s.sql).run(
          ...(s.params as Array<SqlValue> as Array<string | number | null | Uint8Array>),
        )
      db.exec("COMMIT")
    } catch (e) {
      db.exec("ROLLBACK")
      throw e
    }
  },
  close: async () => {
    db.close()
  },
})
