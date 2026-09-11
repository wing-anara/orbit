/**
 * The server-side `MutationTx`: plain parameterized MySQL over the application's `SqlTx`, shaped
 * by the sync schema. Column kinds decide parameter conversion, primary keys address rows, and
 * only synced tables and synced columns are reachable, so a mutator's effects are exactly what
 * the CDC stream will carry back to clients.
 *
 * Rows a mutator inserts through this interface still get server defaults for the columns it
 * leaves out: the insert lists only the columns present in the row.
 */

import { Result } from "effect"
import type { ColumnSchema, TableSchema } from "@orbit/protocol"
import type { RowImage } from "@orbit/protocol/client"
import {
  attachIncludes,
  compileIncludeSelect,
  compileSelect,
  flattenIncludes,
  flattenNode,
  mysqlDialect,
  planQuery,
  type IncludeShape,
  type ResultRow,
  type TypedQuery,
} from "@orbit/query"
import { SchemaRuntime, type KeyOf, type RowOf, type SyncedTables } from "@orbit/schema"

import { MutatorError, type MutationTx } from "../index.ts"
import type { SqlParam, SqlTx } from "./db.ts"
import { placeholder, rowFromMysql, toParam } from "./values.ts"

const ident = mysqlDialect.ident

/** Entries of a typed row or key; values are checked per column kind when bound. */
const entriesOf = (o: object): ReadonlyArray<readonly [string, unknown]> => Object.entries(o)

interface Keyed {
  readonly key: string
  readonly row: RowImage
}

/**
 * Builds the `MutationTx` for one mutation. `mutator` names the mutation in errors.
 */
export const createMysqlTx = <D>(rt: SchemaRuntime, sql: SqlTx, mutator: string): MutationTx<D> => {
  const fail = (message: string): MutatorError =>
    new MutatorError({ name: mutator, reason: "apply_failed", message })

  const tableOf = (name: string): TableSchema => {
    const table = rt.table(name)
    if (table === undefined) throw fail(`table ${name} is not in the sync schema`)
    return table
  }

  const columnOf = (table: TableSchema, name: string): ColumnSchema => {
    const column = table.columns.find((c) => c.name === name)
    if (column === undefined) throw fail(`column ${table.name}.${name} is not in the sync schema`)
    return column
  }

  /** Assignments (`col = ?` or `col = FROM_BASE64(?)`) and their parameters, in row order. */
  const bind = (
    table: TableSchema,
    values: object,
  ): {
    readonly columns: ReadonlyArray<string>
    readonly slots: ReadonlyArray<string>
    readonly params: Array<SqlParam>
  } => {
    const columns: Array<string> = []
    const slots: Array<string> = []
    const params: Array<SqlParam> = []
    for (const [name, value] of entriesOf(values)) {
      if (value === undefined) continue
      const column = columnOf(table, name)
      columns.push(name)
      slots.push(placeholder(column, value))
      params.push(toParam(column, value))
    }
    return { columns, slots, params }
  }

  /** `pk1 = ? AND pk2 = ?` over the full primary key; extra or missing key columns are errors. */
  const whereKey = (
    table: TableSchema,
    key: object,
  ): { readonly sql: string; readonly params: Array<SqlParam> } => {
    const given = new Map(entriesOf(key))
    for (const name of given.keys()) {
      if (!table.primary_key.includes(name))
        throw fail(`column ${table.name}.${name} is not a primary key column`)
    }
    const params: Array<SqlParam> = []
    const parts = table.primary_key.map((name) => {
      if (!given.has(name)) throw fail(`key for ${table.name} is missing column ${name}`)
      params.push(toParam(columnOf(table, name), given.get(name)))
      return `${ident(name)} = ?`
    })
    return { sql: parts.join(" AND "), params }
  }

  const codecOf = (table: TableSchema) => {
    const codec = rt.codecs.get(table.name)
    if (codec === undefined) throw fail(`table ${table.name} is not in the sync schema`)
    return codec
  }

  const keyed = (table: TableSchema, record: Record<string, unknown>): Keyed => {
    const row = rowFromMysql(table, codecOf(table), record)
    return { key: SchemaRuntime.keyString(rt.keyOf(table, row)), row }
  }

  async function insert<N extends SyncedTables<D>>(
    table: N,
    row: Partial<RowOf<D, N>> & KeyOf<D, N>,
  ): Promise<void>
  async function insert(name: string, row: object): Promise<void> {
    const table = tableOf(name)
    const { columns, slots, params } = bind(table, row)
    if (columns.length === 0) throw fail(`insert into ${name} has no columns`)
    await sql.execute(
      `INSERT INTO ${ident(name)} (${columns.map(ident).join(", ")}) VALUES (${slots.join(", ")})`,
      params,
    )
  }

  /** Rows per multi-row `INSERT`; keeps statements well under MySQL's packet and placeholder limits. */
  const INSERT_CHUNK = 200

  async function insertMany<N extends SyncedTables<D>>(
    table: N,
    rows: ReadonlyArray<Partial<RowOf<D, N>> & KeyOf<D, N>>,
  ): Promise<void>
  async function insertMany(name: string, rows: ReadonlyArray<object>): Promise<void> {
    const table = tableOf(name)
    // Rows with the same column set share a statement, so each keeps its own defaults.
    const groups = new Map<
      string,
      Array<{ slots: ReadonlyArray<string>; params: ReadonlyArray<SqlParam> }>
    >()
    const columnsOf = new Map<string, ReadonlyArray<string>>()
    for (const row of rows) {
      const bound = bind(table, row)
      if (bound.columns.length === 0) throw fail(`insert into ${name} has no columns`)
      const signature = bound.columns.join(",")
      columnsOf.set(signature, bound.columns)
      const list = groups.get(signature) ?? []
      list.push({ slots: bound.slots, params: bound.params })
      groups.set(signature, list)
    }
    for (const [signature, list] of groups) {
      const columns = columnsOf.get(signature) ?? []
      for (let i = 0; i < list.length; i += INSERT_CHUNK) {
        const chunk = list.slice(i, i + INSERT_CHUNK)
        await sql.execute(
          `INSERT INTO ${ident(name)} (${columns.map(ident).join(", ")}) VALUES ${chunk
            .map((r) => `(${r.slots.join(", ")})`)
            .join(", ")}`,
          chunk.flatMap((r) => [...r.params]),
        )
      }
    }
  }

  async function update<N extends SyncedTables<D>>(
    table: N,
    key: KeyOf<D, N>,
    patch: Partial<RowOf<D, N>>,
  ): Promise<void>
  async function update(name: string, key: object, patch: object): Promise<void> {
    const table = tableOf(name)
    const set = bind(table, patch)
    if (set.columns.length === 0) return
    const where = whereKey(table, key)
    const assignments = set.columns.map((c, i) => `${ident(c)} = ${set.slots[i] ?? "?"}`)
    await sql.execute(`UPDATE ${ident(name)} SET ${assignments.join(", ")} WHERE ${where.sql}`, [
      ...set.params,
      ...where.params,
    ])
  }

  async function del<N extends SyncedTables<D>>(table: N, key: KeyOf<D, N>): Promise<void>
  async function del(name: string, key: object): Promise<void> {
    const table = tableOf(name)
    const where = whereKey(table, key)
    await sql.execute(`DELETE FROM ${ident(name)} WHERE ${where.sql}`, where.params)
  }

  async function get<N extends SyncedTables<D>>(
    table: N,
    key: KeyOf<D, N>,
  ): Promise<RowOf<D, N> | null>
  async function get(name: string, key: object): Promise<object | null> {
    const table = tableOf(name)
    const where = whereKey(table, key)
    const columns = table.columns.map((c) => ident(c.name)).join(", ")
    const rows = await sql.query(
      `SELECT ${columns} FROM ${ident(name)} WHERE ${where.sql}`,
      where.params,
    )
    const first = rows[0]
    return first === undefined ? null : rowFromMysql(table, codecOf(table), first)
  }

  async function query<N extends string, I extends IncludeShape>(
    query: TypedQuery<D, N, I>,
  ): Promise<ReadonlyArray<ResultRow<D, N, I>>>
  async function query(typed: {
    readonly ast: TypedQuery<D, string, IncludeShape>["ast"]
  }): Promise<ReadonlyArray<object>> {
    const planned = planQuery(rt, typed.ast)
    if (Result.isFailure(planned)) throw fail(planned.failure.message)
    const plan = planned.success
    const primary = compileSelect(plan, { dialect: mysqlDialect })
    const primaryRows = (await sql.query(primary.sql, primary.params)).map((r) =>
      keyed(plan.table, r),
    )
    const byPath = new Map<string, ReadonlyArray<Keyed>>()
    for (const include of flattenIncludes(plan)) {
      const compiled = compileIncludeSelect(plan, include, { dialect: mysqlDialect })
      const rows = await sql.query(compiled.sql, compiled.params)
      byPath.set(
        include.path.join("/"),
        rows.map((r) => keyed(include.target, r)),
      )
    }
    return attachIncludes(plan, primaryRows, byPath).map(flattenNode)
  }

  return { insert, insertMany, update, delete: del, get, query }
}
