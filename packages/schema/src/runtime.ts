/**
 * Runtime helpers over a compiled `SyncSchema` artifact: table lookup, per-table row codecs,
 * primary key extraction and the compatibility summary exchanged with clients.
 */

import { Data, Schema } from "effect"
import {
  JsonValue,
  type ColumnSchema,
  type SyncSchema,
  type TableSchema,
  type ValueKind,
} from "@orbit/protocol"
import type { RowImage, RowKey, SchemaSummary } from "@orbit/protocol/client"

export class UnknownTableError extends Data.TaggedError("UnknownTableError")<{
  readonly table: string
}> {}

/** The codec for one cell of a given kind. Every cell type is a subset of `JsonValue`. */
const cellSchema = (kind: ValueKind): Schema.Codec<JsonValue, JsonValue> => {
  switch (kind) {
    case "bool":
      return Schema.Boolean
    case "int":
      return Schema.Finite.check(Schema.isInt())
    case "float":
      return Schema.Finite
    case "bigint":
      return Schema.String.check(Schema.isPattern(/^-?\d+$/))
    case "decimal":
    case "string":
    case "bytes":
    case "datetime":
    case "date":
    case "time":
      return Schema.String
    case "json":
      return JsonValue
  }
}

export type RowCodec = Schema.Codec<RowImage, RowImage>

/** Decoding options for rows: unknown columns are an error, never silently dropped. */
export const strictDecode = { onExcessProperty: "error" } as const

export const decodeRowSync = (codec: RowCodec): ((input: unknown) => RowImage) =>
  Schema.decodeUnknownSync(codec, strictDecode)

export const decodeRowEffect = (codec: RowCodec) => Schema.decodeUnknownEffect(codec, strictDecode)

/** Builds a strict row codec for a table from the artifact: exact keys, per-kind cell checks. */
export const rowCodecFor = (table: TableSchema): RowCodec => {
  const fields: Record<string, Schema.Codec<JsonValue, JsonValue>> = {}
  for (const col of table.columns) {
    const base = cellSchema(col.kind)
    fields[col.name] = col.nullable ? Schema.NullOr(base) : base
  }
  // A struct of `JsonValue` cells is a `RowImage` (a record of `JsonValue`) by structure.
  return Schema.Struct(fields)
}

export class SchemaRuntime {
  readonly tables: ReadonlyMap<string, TableSchema>
  readonly codecs: ReadonlyMap<string, RowCodec>

  constructor(readonly artifact: SyncSchema) {
    this.tables = new Map(artifact.tables.map((t) => [t.name, t] as const))
    this.codecs = new Map(artifact.tables.map((t) => [t.name, rowCodecFor(t)] as const))
  }

  table(name: string): TableSchema | undefined {
    return this.tables.get(name)
  }

  column(table: string, column: string): ColumnSchema | undefined {
    return this.tables.get(table)?.columns.find((c) => c.name === column)
  }

  keyOf(table: TableSchema, row: RowImage): RowKey {
    return table.primary_key.map((c) => row[c] ?? null)
  }

  /** Stable string form of a primary key, used as a map key and in local tables. */
  static keyString(key: RowKey): string {
    return JSON.stringify(key)
  }

  summary(): SchemaSummary {
    return {
      schemaHash: this.artifact.schema_hash,
      tables: this.artifact.tables.map((t) => ({
        name: t.name,
        columns: t.columns.map((c) => c.name),
      })),
    }
  }
}

/**
 * Additive compatibility: a client may connect when every table and column it knows exists on
 * the server with the same name. The server projects rows to the client's columns. Removed or
 * unknown columns on the client side are a mismatch.
 */
export const compatibility = (
  server: SyncSchema,
  client: SchemaSummary,
):
  | { readonly compatible: true; readonly identical: boolean }
  | { readonly compatible: false; readonly reason: string } => {
  if (server.schema_hash === client.schemaHash) return { compatible: true, identical: true }
  const serverTables = new Map(
    server.tables.map((t) => [t.name, new Set(t.columns.map((c) => c.name))] as const),
  )
  for (const t of client.tables) {
    const cols = serverTables.get(t.name)
    if (cols === undefined)
      return { compatible: false, reason: `client table ${t.name} is not synced by the server` }
    for (const c of t.columns)
      if (!cols.has(c))
        return {
          compatible: false,
          reason: `client column ${t.name}.${c} is not synced by the server`,
        }
  }
  return { compatible: true, identical: false }
}

/** Keeps only the columns a client knows about. */
export const projectRow = (row: RowImage, columns: ReadonlySet<string>): RowImage => {
  const out: Record<string, JsonValue> = {}
  for (const [k, v] of Object.entries(row)) if (columns.has(k)) out[k] = v
  return out
}
