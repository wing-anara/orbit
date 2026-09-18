/**
 * Row storage helpers shared by the Durable Object cache and the browser cache: conversion
 * between wire row images and SQLite parameters/records, plus the statements to write rows.
 */

import { Schema } from "effect"
import { JsonValue, type TableSchema } from "@orbit/protocol"
import { RowKey, type RowImage } from "@orbit/protocol/client"
import { KEY_COLUMN, localTableName, quoteIdent, SchemaRuntime } from "@orbit/schema"

import { literalParam, type SqlParam } from "./sql.ts"

export type SqlValue = string | number | null | Uint8Array

/** Decodes the text of a `json` cell. SQLite stores JSON cells as canonical text. */
const decodeJsonCell = Schema.decodeSync(Schema.fromJsonString(JsonValue))

/** Decodes a stored `__key` value back into its primary key array. */
const decodeKey = Schema.decodeSync(Schema.fromJsonString(RowKey))

export const keyStringOf = (table: TableSchema, row: RowImage): string =>
  SchemaRuntime.keyString(table.primary_key.map((c) => row[c] ?? null))

export const rowToParams = (table: TableSchema, row: RowImage): ReadonlyArray<SqlParam> => [
  keyStringOf(table, row),
  ...table.columns.map((c) => literalParam(c.kind, row[c.name] ?? null)),
]

/** `INSERT ... ON CONFLICT(__key) DO UPDATE` for one table. Parameter order: `rowToParams`. */
export const upsertSql = (table: TableSchema): string => {
  const cols = [KEY_COLUMN, ...table.columns.map((c) => c.name)]
  const updates = table.columns.map((c) => `${quoteIdent(c.name)} = excluded.${quoteIdent(c.name)}`)
  return `INSERT INTO ${quoteIdent(localTableName(table.name))} (${cols.map(quoteIdent).join(", ")}) VALUES (${cols.map(() => "?").join(", ")}) ON CONFLICT(${quoteIdent(KEY_COLUMN)}) DO UPDATE SET ${updates.join(", ")} WHERE ${upsertChangedWhere(table)}`
}

/** Avoid rewriting unchanged row images when snapshots overlap or are replayed. */
export const upsertChangedWhere = (table: TableSchema): string =>
  table.columns
    .map(
      (c) =>
        `${quoteIdent(localTableName(table.name))}.${quoteIdent(c.name)} IS NOT excluded.${quoteIdent(c.name)}`,
    )
    .join(" OR ")

export const deleteByKeySql = (table: TableSchema): string =>
  `DELETE FROM ${quoteIdent(localTableName(table.name))} WHERE ${quoteIdent(KEY_COLUMN)} = ?`

export const selectByKeySql = (table: TableSchema): string => {
  const cols = table.columns.map((c) =>
    c.kind === "bigint"
      ? `CAST(${quoteIdent(c.name)} AS TEXT) AS ${quoteIdent(c.name)}`
      : quoteIdent(c.name),
  )
  return `SELECT ${quoteIdent(KEY_COLUMN)}, ${cols.join(", ")} FROM ${quoteIdent(localTableName(table.name))} WHERE ${quoteIdent(KEY_COLUMN)} = ?`
}

export const countSql = (table: TableSchema): string =>
  `SELECT COUNT(*) AS n FROM ${quoteIdent(localTableName(table.name))}`

export const deleteAllSql = (table: TableSchema): string =>
  `DELETE FROM ${quoteIdent(localTableName(table.name))}`

/** Converts a SQLite record (as returned by the compiled selects) back to a wire row image. */
export const rowFromRecord = (
  table: TableSchema,
  record: Readonly<Record<string, SqlValue>>,
): RowImage => {
  const out: Record<string, JsonValue> = {}
  for (const c of table.columns) {
    const v = record[c.name]
    if (v === null || v === undefined) {
      out[c.name] = null
      continue
    }
    switch (c.kind) {
      case "bool":
        out[c.name] = v === 1 || v === "1"
        break
      case "int":
      case "float":
        out[c.name] = typeof v === "number" ? v : Number(v)
        break
      case "json":
        out[c.name] = typeof v === "string" ? decodeJsonCell(v) : null
        break
      default:
        out[c.name] = typeof v === "string" ? v : String(v)
    }
  }
  return out
}

export const keyOfRecord = (record: Readonly<Record<string, SqlValue>>): string => {
  const k = record[KEY_COLUMN]
  if (typeof k !== "string") throw new Error("record has no __key column")
  return k
}

export const parseKey = (key: string): RowKey => decodeKey(key)
