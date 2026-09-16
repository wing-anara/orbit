/**
 * The sync schema definition DSL.
 *
 * An application describes what to sync in TypeScript against the introspected database
 * metadata (`orbit-server schema introspect --out schema.introspected.ts`). The definition is
 * checked at compile time: table and column names must exist in the introspection, the
 * partition column must be a column of every synced table, and relations must reference synced
 * tables. Row types are derived from the same definition, so the application, the client SDK and
 * the query builder all agree on shapes without any hand-written duplication.
 */

import type { JsonValue, ValueKind } from "@orbit/protocol"

/** The introspected metadata shape, as emitted by `orbit-server schema introspect`. */
export interface IntrospectedColumnShape {
  readonly name: string
  readonly column_type: string
  readonly data_type: string
  readonly nullable: boolean
  readonly kind: ValueKind
  readonly enum_values?: ReadonlyArray<string>
}

export interface IntrospectedTableShape {
  readonly name: string
  readonly primary_key: ReadonlyArray<string>
  readonly columns: ReadonlyArray<IntrospectedColumnShape>
}

export interface IntrospectedShape {
  readonly keyspace: string
  readonly server_version: string
  readonly tables: ReadonlyArray<IntrospectedTableShape>
}

export type TableNamesOf<I extends IntrospectedShape> = I["tables"][number]["name"]

export type TableOf<I extends IntrospectedShape, N extends string> = Extract<
  I["tables"][number],
  { readonly name: N }
>

export type ColumnNamesOf<I extends IntrospectedShape, N extends string> = TableOf<
  I,
  N
>["columns"][number]["name"]

export type ColumnOf<I extends IntrospectedShape, N extends string, C extends string> = Extract<
  TableOf<I, N>["columns"][number],
  { readonly name: C }
>

/** TypeScript type of a cell for a given value kind. `bigint` and `decimal` are exact strings. */
export type KindToType<K extends ValueKind> = K extends "bool"
  ? boolean
  : K extends "int" | "float"
    ? number
    : K extends "bigint" | "decimal" | "string" | "bytes" | "datetime" | "date" | "time"
      ? string
      : K extends "json"
        ? JsonValue
        : never

export type CellType<Col extends IntrospectedColumnShape> = Col["nullable"] extends true
  ? KindToType<Col["kind"]> | null
  : KindToType<Col["kind"]>

export type PartitionKeyKind = "string" | "int" | "bigint"

export interface RelationConfig<Tables extends string> {
  readonly kind: "one" | "many"
  readonly to: Tables
  readonly from: ReadonlyArray<string>
  readonly toColumns: ReadonlyArray<string>
}

/** How the engine computes a derived column from the raw cell of its source column. */
export type DerivedRule =
  | { readonly kind: "not_null" }
  | { readonly kind: "starts_with"; readonly prefix: string }

export interface DerivedColumnConfig<I extends IntrospectedShape, N extends string> {
  /** Source column of the live table. It need not be synced; then its value stays in the engine. */
  readonly from: ColumnNamesOf<I, N>
  readonly rule: DerivedRule
}

export interface TableConfig<I extends IntrospectedShape, N extends string, Tables extends string> {
  /**
   * Column holding the partition key. With `partitionVia`, the column holds the primary key of
   * a row of the parent table instead, and the row's partition is the parent's partition.
   */
  readonly partitionBy: ColumnNamesOf<I, N>
  /**
   * Parent table for a derived partition: a synced table that is partitioned directly and has a
   * single-column primary key (see docs/partitioning.md, "Derived partitions").
   */
  readonly partitionVia?: Tables
  /** Additional recipient partitions reached by 1–8 declared relations, ending at a directly
   * partitioned table (for example ["permissions"]). Subscription queries must still enforce
   * the caller's permission; these paths only control replication into partition caches. */
  readonly partitionRoutes?: ReadonlyArray<ReadonlyArray<string>>
  /** Columns to sync; defaults to all columns. Primary key and partition columns are always included. */
  readonly columns?: ReadonlyArray<ColumnNamesOf<I, N>>
  /**
   * Non-nullable boolean columns the engine computes from a source column (see
   * docs/adding-tables.md, "Derived columns"). Keys are the new column names.
   */
  readonly derived?: Readonly<Record<string, DerivedColumnConfig<I, N>>>
  readonly relations?: Readonly<Record<string, RelationConfig<Tables>>>
}

export interface SyncSchemaConfig<
  I extends IntrospectedShape,
  T extends { readonly [N in TableNamesOf<I>]?: TableConfig<I, N, Extract<keyof T, string>> },
> {
  readonly app: string
  readonly introspected: I
  readonly partition: { readonly name: string; readonly kind: PartitionKeyKind }
  readonly tables: T
  /** Physical placement version; bump to migrate all Durable Objects (see docs/partitioning.md). */
  readonly placementVersion?: number
}

export interface SyncSchemaDefinition<I extends IntrospectedShape, T> {
  readonly _tag: "SyncSchemaDefinition"
  readonly config: SyncSchemaConfig<
    I,
    T & { readonly [N in TableNamesOf<I>]?: TableConfig<I, N, Extract<keyof T, string>> }
  >
}

/** Names of synced tables in a definition. */
export type SyncedTables<D> =
  D extends SyncSchemaDefinition<infer _I, infer T> ? Extract<keyof T, string> : never

type SelectedColumns<I extends IntrospectedShape, N extends string, Cfg> = Cfg extends {
  readonly columns: ReadonlyArray<infer C extends string>
}
  ?
      | C
      | TableOf<I, N>["primary_key"][number]
      | (Cfg extends { readonly partitionBy: infer P extends string } ? P : never)
  : ColumnNamesOf<I, N>

type DerivedNames<Cfg> = Cfg extends { readonly derived: infer R }
  ? Extract<keyof R, string>
  : never

/** The row type of a synced table: the selected columns plus the derived booleans. */
export type RowOf<D, N extends string> =
  D extends SyncSchemaDefinition<infer I, infer T>
    ? N extends keyof T
      ? {
          readonly [C in Extract<SelectedColumns<I, N, T[N]>, ColumnNamesOf<I, N>>]: CellType<
            ColumnOf<I, N, C>
          >
        } & { readonly [K in DerivedNames<T[N]>]: boolean }
      : never
    : never

export type ColumnsOf<D, N extends string> = Extract<keyof RowOf<D, N>, string>

/** Primary key column names of a synced table. */
export type PrimaryKeyOf<D, N extends string> =
  D extends SyncSchemaDefinition<infer I, infer _T> ? TableOf<I, N>["primary_key"][number] : never

/** The primary key of a row as an object: `{ id: "doc_1" }`. */
export type KeyOf<D, N extends string> = {
  readonly [C in Extract<PrimaryKeyOf<D, N>, keyof RowOf<D, N>>]: RowOf<D, N>[C]
}

export type RelationsOf<D, N extends string> =
  D extends SyncSchemaDefinition<infer _I, infer T>
    ? N extends keyof T
      ? T[N] extends { readonly relations: infer R }
        ? R
        : Record<never, never>
      : never
    : never

export type RelationNamesOf<D, N extends string> = Extract<keyof RelationsOf<D, N>, string>

export type RelationTarget<D, N extends string, R extends string> = RelationsOf<D, N>[R &
  keyof RelationsOf<D, N>] extends { readonly to: infer To extends string }
  ? To
  : never

export type RelationKindOf<D, N extends string, R extends string> = RelationsOf<D, N>[R &
  keyof RelationsOf<D, N>] extends { readonly kind: infer K }
  ? K
  : never

export const defineSyncSchema = <
  const I extends IntrospectedShape,
  const T extends { readonly [N in TableNamesOf<I>]?: TableConfig<I, N, Extract<keyof T, string>> },
>(
  config: SyncSchemaConfig<I, T>,
): SyncSchemaDefinition<I, T> => ({ _tag: "SyncSchemaDefinition", config })
