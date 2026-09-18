/**
 * `@orbit/mutators/server`: the authoritative side of custom mutators.
 *
 * * `createPushHandler` is the HTTP endpoint the client pushes its pending mutations to.
 * * `createMysqlTx` is the `MutationTx` that runs a mutator's reads and writes as parameterized
 *   MySQL inside the application's transaction.
 *
 * The application supplies a `PushDb` (one `transaction` method over a tiny `SqlTx`), the
 * compiled sync schema, the shared mutator definitions and an `authorize` callback.
 */

export type { PushDb, SqlParam, SqlTx } from "./db.ts"
export { createMysqlTx } from "./mysql-tx.ts"
export {
  createPushHandler,
  RetryableMutationError,
  MAX_ERROR_LENGTH,
  SELECT_LAST_MUTATION_SQL,
  UPSERT_CLIENT_SQL,
  type PushAuthorization,
  type PushHandler,
  type PushHandlerOptions,
} from "./push-handler.ts"
export {
  base64Encode,
  formatDate,
  formatDateTime,
  fromMysql,
  rowFromMysql,
  toParam,
  ValueError,
} from "./values.ts"
