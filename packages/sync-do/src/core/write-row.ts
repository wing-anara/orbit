import type { TableSchema } from "@orbit/protocol"
import type { RowImage } from "@orbit/protocol/client"
import { keyStringOf, rowFromRecord, rowToParams, selectByKeySql, upsertSql } from "@orbit/query"
import { KEY_COLUMN, localTableName, quoteIdent } from "@orbit/schema"
import type { SqlDriver } from "./driver.ts"

/** Called inside the engine transaction. Compare the actual cache, not a possibly stale
 * CDC before-image, and leave unaffected secondary indexes out of the UPDATE entirely.
 * The shared storage codecs preserve bigint precision and all wire scalar encodings.
 */
export const writeCachedRow = (db: SqlDriver, table: TableSchema, row: RowImage): void => {
  const next = rowToParams(table, row)
  const key = keyStringOf(table, row)
  const stored = db.query(selectByKeySql(table), [key])[0]
  if (stored === undefined) {
    db.run(upsertSql(table), next)
    return
  }
  const previous = rowToParams(table, rowFromRecord(table, stored))
  const changed = table.columns.flatMap((column, index) =>
    previous[index + 1] === next[index + 1] ? [] : [{ column, value: next[index + 1] ?? null }],
  )
  if (changed.length === 0) return
  db.run(
    `UPDATE ${quoteIdent(localTableName(table.name))} SET ${changed.map(({ column }) => `${quoteIdent(column.name)} = ?`).join(", ")} WHERE ${quoteIdent(KEY_COLUMN)} = ?`,
    [...changed.map(({ value }) => value), key],
  )
}
