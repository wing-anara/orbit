import type { Database, PreparedStatement } from "@sqlite.org/sqlite-wasm"

import type { Statement } from "../driver.ts"

/** Reuse generated writes within a transaction, without retaining statements across batches. */
export const runBatch = (database: Database, statements: ReadonlyArray<Statement>): void => {
  const prepared = new Map<string, PreparedStatement>()
  const finalize = (): void => {
    for (const statement of prepared.values()) statement.finalize()
    prepared.clear()
  }
  database.transaction(() => {
    try {
      for (const statement of statements) {
        // Keep exec's multi-statement and DDL semantics. Generated row writes have no
        // semicolons; unusual SQL uses the existing path and invalidates prepared handles.
        if (!/^(INSERT|UPDATE|DELETE)\b/i.test(statement.sql) || statement.sql.includes(";")) {
          finalize()
          database.exec({ sql: statement.sql, bind: statement.params })
          continue
        }
        let compiled = prepared.get(statement.sql)
        if (compiled === undefined) {
          if (prepared.size >= 64) finalize()
          compiled = database.prepare(statement.sql)
          prepared.set(statement.sql, compiled)
        }
        if (compiled.parameterCount > 0) compiled.bind(statement.params)
        while (compiled.step()) {
          /* Drain RETURNING rows, as exec without a callback does. */
        }
        compiled.reset(true)
      }
    } finally {
      finalize()
    }
  })
}
