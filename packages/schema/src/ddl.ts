/**
 * SQLite DDL for the local relational cache, shared by the Durable Object and the browser.
 *
 * Storage mapping per value kind:
 * | kind                                   | SQLite    | note                                    |
 * |----------------------------------------|-----------|-----------------------------------------|
 * | bool                                   | INTEGER   | 0 / 1                                   |
 * | int, bigint                            | INTEGER   | bigint beyond 64-bit signed is rejected |
 * | float                                  | REAL      |                                         |
 * | decimal, string, datetime, date, time  | TEXT      | canonical text sorts correctly          |
 * | bytes                                  | TEXT      | base64                                  |
 * | json                                   | TEXT      | JSON text; not comparable in queries    |
 */

import type { SyncSchema, TableSchema, ValueKind } from "@orbit/protocol"

export const quoteIdent = (name: string): string => `"${name.replaceAll('"', '""')}"`

export const sqliteType = (kind: ValueKind): "INTEGER" | "REAL" | "TEXT" => {
  switch (kind) {
    case "bool":
    case "int":
    case "bigint":
      return "INTEGER"
    case "float":
      return "REAL"
    case "decimal":
    case "string":
    case "bytes":
    case "datetime":
    case "date":
    case "time":
    case "json":
      return "TEXT"
  }
}

/** Name of the local table for a synced table. Prefixed to avoid clashes with engine tables. */
export const localTableName = (table: string): string => `t_${table}`

/**
 * Every local table carries `__key`: the JSON-encoded primary key (see `SchemaRuntime.keyString`).
 * The engine addresses rows by it; the real primary key columns keep a unique index so queries on
 * them stay fast.
 */
export const KEY_COLUMN = "__key"

export interface StorageOptions {
  /** Rowid tables keep large payloads in leaves instead of internal B-tree nodes. */
  readonly rowid?: boolean
}

export const createTableSql = (table: TableSchema, options: StorageOptions = {}): string => {
  const cols = table.columns.map(
    (c) => `${quoteIdent(c.name)} ${sqliteType(c.kind)}${c.nullable ? "" : " NOT NULL"}`,
  )
  const unique = `UNIQUE (${table.primary_key.map(quoteIdent).join(", ")})`
  return `CREATE TABLE IF NOT EXISTS ${quoteIdent(localTableName(table.name))} (${[`${quoteIdent(KEY_COLUMN)} TEXT NOT NULL PRIMARY KEY`, ...cols, unique].join(", ")})${options.rowid ? "" : " WITHOUT ROWID"}`
}

const indexSql = (table: string, columns: ReadonlyArray<string>): string =>
  `CREATE INDEX IF NOT EXISTS ${quoteIdent(`ix_${table}_${columns.join("_")}`)} ON ${quoteIdent(localTableName(table))} (${columns.map(quoteIdent).join(", ")})`

/**
 * Indexes that make every relation walk an index probe in both directions: the source side
 * (`from_columns`) and the target side (`to_columns`) of every declared relation. Columns that
 * are a prefix of the primary key are already covered by its unique index. Incremental
 * maintenance in the Durable Object and includes in every cache depend on these.
 */
export const createIndexesSql = (schema: SyncSchema): ReadonlyArray<string> => {
  const out: Array<string> = []
  const seen = new Set<string>()
  const byName = new Map(schema.tables.map((t) => [t.name, t] as const))
  const add = (table: TableSchema, columns: ReadonlyArray<string>): void => {
    const key = `${table.name}:${columns.join(",")}`
    if (seen.has(key) || columns.every((c, i) => table.primary_key[i] === c)) return
    seen.add(key)
    out.push(indexSql(table.name, columns))
  }
  for (const t of schema.tables) {
    for (const r of t.relations) {
      add(t, r.from_columns)
      const target = byName.get(r.target_table)
      if (target !== undefined) add(target, r.to_columns)
    }
  }
  return out
}

export const allDdl = (schema: SyncSchema, options: StorageOptions = {}): ReadonlyArray<string> => [
  ...schema.tables.map((table) => createTableSql(table, options)),
  ...createIndexesSql(schema),
]

export interface ExistingTable {
  readonly name: string
  readonly columns: ReadonlyArray<string>
}

export type MigrationPlan =
  | { readonly action: "create"; readonly statements: ReadonlyArray<string> }
  | {
      readonly action: "additive"
      readonly statements: ReadonlyArray<string>
      readonly addedColumns: ReadonlyArray<string>
    }
  | {
      readonly action: "reset"
      readonly statements: ReadonlyArray<string>
      readonly reason: string
    }
  | { readonly action: "none" }

/**
 * Plans the local schema migration from what exists to what the artifact needs.
 *
 * * Missing tables and added nullable columns are applied in place.
 * * A removed column, a required column with no default, or a changed primary key cannot be
 *   migrated in place: the plan is a reset (drop everything and resync). Callers must treat a
 *   reset as "all cached data is gone" and re-bootstrap.
 */
export const planMigration = (
  schema: SyncSchema,
  existing: ReadonlyArray<ExistingTable>,
  storedHash: string | null,
  options: StorageOptions = {},
): MigrationPlan => {
  if (storedHash === schema.schema_hash && existing.length > 0) return { action: "none" }
  const byName = new Map(
    existing.map((t) => [t.name, new Set(t.columns.filter((c) => c !== KEY_COLUMN))] as const),
  )
  if (byName.size === 0) return { action: "create", statements: allDdl(schema, options) }
  const statements: Array<string> = []
  const added: Array<string> = []
  for (const t of schema.tables) {
    const cols = byName.get(localTableName(t.name))
    if (cols === undefined) {
      statements.push(createTableSql(t, options))
      continue
    }
    for (const c of t.columns) {
      if (cols.has(c.name)) continue
      if (!c.nullable) {
        return {
          action: "reset",
          statements: resetStatements(schema, existing, options),
          reason: `column ${t.name}.${c.name} is required and cannot be added in place`,
        }
      }
      statements.push(
        `ALTER TABLE ${quoteIdent(localTableName(t.name))} ADD COLUMN ${quoteIdent(c.name)} ${sqliteType(c.kind)}`,
      )
      added.push(`${t.name}.${c.name}`)
    }
    for (const existingCol of cols) {
      if (!t.columns.some((c) => c.name === existingCol)) {
        return {
          action: "reset",
          statements: resetStatements(schema, existing, options),
          reason: `column ${t.name}.${existingCol} was removed from the sync schema`,
        }
      }
    }
  }
  for (const t of existing) {
    if (t.name.startsWith("t_") && !schema.tables.some((s) => localTableName(s.name) === t.name)) {
      statements.push(`DROP TABLE IF EXISTS ${quoteIdent(t.name)}`)
    }
  }
  // Indexes are `IF NOT EXISTS`, so a schema that gained relations gets them in place.
  statements.push(...createIndexesSql(schema))
  return { action: "additive", statements, addedColumns: added }
}

const resetStatements = (
  schema: SyncSchema,
  existing: ReadonlyArray<ExistingTable>,
  options: StorageOptions = {},
): ReadonlyArray<string> => [
  ...existing
    .filter((t) => t.name.startsWith("t_"))
    .map((t) => `DROP TABLE IF EXISTS ${quoteIdent(t.name)}`),
  ...allDdl(schema, options),
]
