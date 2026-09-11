/**
 * The database surface the push handler needs from the application. It is deliberately small so
 * any MySQL driver fits: `mysql2` and HTTP drivers of hosted Vitess both implement it
 * with a few lines.
 */

/** Parameters are plain JSON scalars; binary columns travel as base64 text (see `mysql-tx.ts`). */
export type SqlParam = string | number | boolean | null

/** Statements inside one transaction. Rows come back as the driver returns them, untyped. */
export interface SqlTx {
  readonly query: (
    sql: string,
    params: ReadonlyArray<SqlParam>,
  ) => Promise<ReadonlyArray<Record<string, unknown>>>
  readonly execute: (sql: string, params: ReadonlyArray<SqlParam>) => Promise<void>
}

/**
 * Runs `f` inside one transaction: commit when it resolves, roll back when it rejects, and
 * rethrow the rejection.
 */
export interface PushDb {
  readonly transaction: <T>(f: (tx: SqlTx) => Promise<T>) => Promise<T>
}
