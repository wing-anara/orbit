/**
 * Conversion between wire cells (the shapes `crates/orbit-vstream/src/normalize.rs` emits) and
 * MySQL driver values, per column kind.
 *
 * Wire to parameter: booleans become 0/1, json becomes its text, `bigint`/`decimal`/`datetime`
 * text passes through, and `bytes` (base64 text) is bound as text under a `FROM_BASE64(?)`
 * placeholder so the parameter type stays a JSON scalar.
 *
 * Driver value to wire: drivers disagree on what they return (mysql2 gives `Date`, `Buffer` and
 * parsed JSON; the PlanetScale HTTP driver gives strings), so every kind accepts every plausible
 * driver shape. `Date` objects are formatted from their local components, which is how mysql2
 * builds them with its default `timezone: "local"`; prefer `dateStrings: true` in the driver so
 * the database text passes through unchanged.
 */

import { Schema } from "effect"
import { JsonValue, type ColumnSchema, type TableSchema } from "@orbit/protocol"
import type { RowImage } from "@orbit/protocol/client"
import { decodeRowSync, type RowCodec } from "@orbit/schema"

import type { SqlParam } from "./db.ts"

const decodeJson = Schema.decodeUnknownSync(JsonValue)
const parseJsonText = Schema.decodeSync(Schema.fromJsonString(JsonValue))

const BIGINT_TEXT = /^-?\d+$/
const BASE64_TEXT = /^[A-Za-z0-9+/]*={0,2}$/

export class ValueError extends Error {
  constructor(
    readonly column: string,
    readonly kind: ColumnSchema["kind"],
    readonly value: unknown,
    reason: string,
  ) {
    super(`column ${column} (${kind}): ${reason}`)
    this.name = "ValueError"
  }
}

const describe = (value: unknown): string => {
  if (value instanceof Uint8Array) return `<${value.byteLength} bytes>`
  if (value instanceof Date) return value.toISOString()
  if (typeof value === "bigint") return `${value}n`
  const text = JSON.stringify(value)
  return text === undefined ? typeof value : text.slice(0, 80)
}

const invalid = (column: ColumnSchema, value: unknown): ValueError =>
  new ValueError(column.name, column.kind, value, `${describe(value)} is not a valid wire value`)

const unreadable = (column: ColumnSchema, value: unknown): ValueError =>
  new ValueError(column.name, column.kind, value, `cannot convert ${describe(value)} from MySQL`)

/** Wire cell to MySQL parameter. Undefined and null both bind as NULL. */
export const toParam = (column: ColumnSchema, value: unknown): SqlParam => {
  if (value === null || value === undefined) return null
  switch (column.kind) {
    case "bool":
      if (typeof value === "boolean") return value ? 1 : 0
      break
    case "int":
      if (typeof value === "number" && Number.isInteger(value)) return value
      break
    case "float":
      if (typeof value === "number" && Number.isFinite(value)) return value
      break
    case "bigint":
      if (typeof value === "string" && BIGINT_TEXT.test(value)) return value
      break
    case "decimal":
    case "string":
    case "datetime":
    case "date":
    case "time":
      if (typeof value === "string") return value
      break
    case "bytes":
      if (typeof value === "string" && value.length % 4 === 0 && BASE64_TEXT.test(value))
        return value
      break
    case "json":
      return JSON.stringify(decodeJson(value))
  }
  throw invalid(column, value)
}

/** Placeholder for a wire cell: `bytes` decode server side from their base64 text. */
export const placeholder = (column: ColumnSchema, value: unknown): string =>
  column.kind === "bytes" && value !== null && value !== undefined ? "FROM_BASE64(?)" : "?"

const pad = (n: number, width = 2): string => String(n).padStart(width, "0")

/** `YYYY-MM-DD` from a driver `Date`, local components. */
export const formatDate = (d: Date): string =>
  `${pad(d.getFullYear(), 4)}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`

/** `YYYY-MM-DD HH:MM:SS.mmm` from a driver `Date`, local components. */
export const formatDateTime = (d: Date): string =>
  `${formatDate(d)} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`

/** Standard base64 of raw bytes; `btoa` exists in Node and in Workers. */
export const base64Encode = (bytes: Uint8Array): string => {
  let binary = ""
  const chunk = 0x8000
  for (let i = 0; i < bytes.length; i += chunk)
    binary += String.fromCharCode(...bytes.subarray(i, i + chunk))
  return btoa(binary)
}

const integerText = (value: number | bigint): string =>
  typeof value === "bigint" || !Number.isInteger(value) ? String(value) : BigInt(value).toString()

/** MySQL driver value to wire cell. */
export const fromMysql = (column: ColumnSchema, value: unknown): JsonValue => {
  if (value === null || value === undefined) return null
  switch (column.kind) {
    case "bool":
      if (typeof value === "boolean") return value
      if (typeof value === "number") return value !== 0
      if (typeof value === "bigint") return value !== 0n
      if (typeof value === "string") return Number(value) !== 0
      break
    case "int":
    case "float":
      if (typeof value === "number") return value
      if (typeof value === "bigint" || typeof value === "string") return Number(value)
      break
    case "bigint":
      if (typeof value === "string") return value
      if (typeof value === "number" || typeof value === "bigint") return integerText(value)
      break
    case "decimal":
      if (typeof value === "string") return value
      if (typeof value === "number" || typeof value === "bigint") return String(value)
      break
    case "datetime":
      if (typeof value === "string") return value
      if (value instanceof Date) return formatDateTime(value)
      break
    case "date":
      if (typeof value === "string") return value
      if (value instanceof Date) return formatDate(value)
      break
    case "time":
    case "string":
      if (typeof value === "string") return value
      if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean")
        return String(value)
      break
    case "bytes":
      if (typeof value === "string") return value
      if (value instanceof Uint8Array) return base64Encode(value)
      if (value instanceof ArrayBuffer) return base64Encode(new Uint8Array(value))
      break
    case "json":
      return typeof value === "string" ? parseJsonText(value) : decodeJson(value)
  }
  throw unreadable(column, value)
}

/**
 * Converts a driver record to a validated wire row. Columns the table does not declare are
 * ignored; every declared column must be present (as NULL at least) or decoding fails.
 */
export const rowFromMysql = (
  table: TableSchema,
  codec: RowCodec,
  record: Readonly<Record<string, unknown>>,
): RowImage => {
  const out: Record<string, JsonValue> = {}
  for (const column of table.columns) out[column.name] = fromMysql(column, record[column.name])
  return decodeRowSync(codec)(out)
}
