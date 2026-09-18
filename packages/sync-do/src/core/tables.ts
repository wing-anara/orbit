/**
 * Engine tables inside the Durable Object's SQLite database.
 *
 * | table         | purpose                                                              |
 * |---------------|----------------------------------------------------------------------|
 * | meta          | schema hash, partition, epoch, applied_seq                           |
 * | scopes        | per synced table: absent (no row) / filling / live, fill position    |
 * | held          | row changes received while a scope was filling                       |
 * | seq_log       | recent (seq, gtid) pairs for duplicate verification                  |
 * | subscriptions | materialized queries by canonical key; `orphaned_at` when no session holds them |
 * | membership    | (subscription, path, table, key): rows each level of a subscription contains |
 * | fills         | outstanding fill requests                                            |
 * | t_<table>     | the relational cache itself (`@orbit/schema` DDL)                    |
 *
 * `path` is `""` for the primary rows and the include path (`folder`, `tags/tag`) for included
 * rows. A row reached through two paths has two membership rows; the client-facing membership
 * is the distinct set of (table, key). Keeping the path makes incremental maintenance exact for
 * self relations, where the same table appears at several levels.
 */

export const ENGINE_DDL: ReadonlyArray<string> = [
  `CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS scopes (tbl TEXT PRIMARY KEY, state TEXT NOT NULL, fill_id TEXT, fill_position TEXT, hold_from_seq INTEGER NOT NULL DEFAULT 0, rows_filled INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS held (seq INTEGER NOT NULL, ord INTEGER NOT NULL, tbl TEXT NOT NULL, gtid TEXT NOT NULL, change TEXT NOT NULL, PRIMARY KEY (seq, ord))`,
  `CREATE INDEX IF NOT EXISTS held_tbl ON held (tbl, seq, ord)`,
  `CREATE TABLE IF NOT EXISTS seq_log (seq INTEGER PRIMARY KEY, gtid TEXT NOT NULL, applied_at INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS subscriptions (id TEXT PRIMARY KEY, query TEXT NOT NULL, tables TEXT NOT NULL, live INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, orphaned_at INTEGER)`,
  `CREATE TABLE IF NOT EXISTS membership (subscription TEXT NOT NULL, path TEXT NOT NULL, tbl TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY (subscription, path, tbl, key))`,
  // Covering index for "which subscriptions hold this row": the delta fan-out and the
  // any-path check. The old two-column index made the planner scan a whole subscription.
  `DROP INDEX IF EXISTS membership_row`,
  `CREATE TABLE IF NOT EXISTS fills (fill_id TEXT PRIMARY KEY, tbl TEXT NOT NULL, requested_at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 1)`,
]

/**
 * In-place upgrades of engine tables created by earlier versions. Each entry is applied when the
 * named column is missing. A membership table without `path` is dropped and rebuilt from the
 * cache by re-materializing every live subscription (see `SyncEngine.init`).
 */
export const ENGINE_UPGRADES: ReadonlyArray<{
  readonly table: string
  readonly column: string
  readonly statements: ReadonlyArray<string>
}> = [
  {
    table: "subscriptions",
    column: "orphaned_at",
    statements: [`ALTER TABLE subscriptions ADD COLUMN orphaned_at INTEGER`],
  },
  {
    table: "membership",
    column: "path",
    statements: [
      `DROP TABLE membership`,
      `CREATE TABLE membership (subscription TEXT NOT NULL, path TEXT NOT NULL, tbl TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY (subscription, path, tbl, key))`,
    ],
  },
]

export const SEQ_LOG_RETENTION = 2000
