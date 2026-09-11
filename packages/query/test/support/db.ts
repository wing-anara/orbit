/** In-memory SQLite (Node's built-in `node:sqlite`) loaded with the fixture schema. */

import { DatabaseSync } from "node:sqlite"
import * as fs from "node:fs"
import * as path from "node:path"
import { fileURLToPath } from "node:url"

import { Schema } from "effect"
import { SyncSchema } from "@orbit/protocol"
import type { RowImage } from "@orbit/protocol/client"
import { allDdl, localTableName, quoteIdent, SchemaRuntime } from "@orbit/schema"

import { literalParam, type SqlParam } from "../../src/sql.ts"

const here = path.dirname(fileURLToPath(import.meta.url))

export const loadFixtureSchema = (): SyncSchema => {
  const text = fs.readFileSync(
    path.resolve(here, "../../../../schema/fixtures/SyncSchema.json"),
    "utf8",
  )
  return Schema.decodeUnknownSync(SyncSchema)(JSON.parse(text))
}

export const openDb = (schema: SyncSchema): DatabaseSync => {
  const db = new DatabaseSync(":memory:")
  for (const stmt of allDdl(schema)) db.exec(stmt)
  db.exec(
    `CREATE TABLE membership (subscription TEXT NOT NULL, tbl TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY (subscription, tbl, key))`,
  )
  return db
}

export const upsertRow = (
  db: DatabaseSync,
  rt: SchemaRuntime,
  table: string,
  row: RowImage,
): void => {
  const t = rt.table(table)
  if (t === undefined) throw new Error(`unknown table ${table}`)
  const key = SchemaRuntime.keyString(rt.keyOf(t, row))
  const cols = ["__key", ...t.columns.map((c) => c.name)]
  const params: Array<SqlParam> = [
    key,
    ...t.columns.map((c) => literalParam(c.kind, row[c.name] ?? null)),
  ]
  db.prepare(
    `INSERT OR REPLACE INTO ${quoteIdent(localTableName(table))} (${cols.map(quoteIdent).join(", ")}) VALUES (${cols.map(() => "?").join(", ")})`,
  ).run(...params)
}

export const deleteRow = (db: DatabaseSync, table: string, key: string): void => {
  db.prepare(`DELETE FROM ${quoteIdent(localTableName(table))} WHERE "__key" = ?`).run(key)
}
