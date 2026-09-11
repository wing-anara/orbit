/**
 * The synchronous SQL driver the engine core runs on.
 *
 * Durable Object SQLite is synchronous and transactional (`ctx.storage.transactionSync`), and
 * so is Node's built-in `node:sqlite` used in tests. Keeping the core synchronous is deliberate:
 * a source transaction is applied, its subscriptions are maintained and the cursor is advanced
 * inside one SQLite transaction with no interleaving, which is what makes the consistency
 * invariants hold.
 */

import type { SqlValue } from "@orbit/query"

export type SqlRecord = Readonly<Record<string, SqlValue>>

export interface SqlDriver {
  /** Runs a statement and returns its rows as objects. */
  readonly query: (sql: string, params?: ReadonlyArray<SqlValue>) => ReadonlyArray<SqlRecord>
  /** Runs a statement without reading results. */
  readonly run: (sql: string, params?: ReadonlyArray<SqlValue>) => void
  /** Runs `f` atomically. Nested calls join the outer transaction. */
  readonly transaction: <T>(f: () => T) => T
}

/** Names and columns of existing user tables, for migrations. */
export const listTables = (
  driver: SqlDriver,
): ReadonlyArray<{ readonly name: string; readonly columns: ReadonlyArray<string> }> => {
  const tables = driver.query(
    `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%'`,
  )
  return tables.map((t) => {
    const name = String(t["name"])
    const cols = driver
      .query(`SELECT name FROM pragma_table_info(?)`, [name])
      .map((c) => String(c["name"]))
    return { name, columns: cols }
  })
}
