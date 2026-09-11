# Schema evolution

The compiled sync schema has a hash. Every runtime compares that hash: the Rust distributor, the Sync Durable Object, and the browser. This document explains which changes are additive, which changes are breaking, how each runtime reacts to a new hash, and the operator procedure for a rollout.

Related documents: [type-safety.md](type-safety.md) (how the hash is computed), [adding-tables.md](adding-tables.md), [protocol.md](protocol.md), [operations-runbook.md](operations-runbook.md), [bootstrap.md](bootstrap.md).

## Two compatibility checks

Orbit has two separate checks. They answer different questions.

`compatibility` in `packages/schema/src/runtime.ts` compares the server artifact with the `SchemaSummary` a client sends in `hello`. The summary holds the client hash plus table names and column names. The rules are:

- Equal hashes: compatible and identical.
- Every client table exists on the server, and every client column exists on that table: compatible, not identical.
- Any client table or column is unknown to the server: not compatible.

A compatible, non-identical client receives rows projected to its columns. `projectRow` keeps only the columns the client knows. The Durable Object stores `identical_schema` per session and projects every snapshot and delta for that session. An old client keeps working after the server adds a column. A new client that knows a column the server lacks is rejected with close code 4409.

`planMigration` in `packages/schema/src/ddl.ts` compares the artifact with the local SQLite tables. Both the Durable Object and the browser run it on open. The result is one of:

| Action     | When                                                             | Effect                                                                                  |
| ---------- | ---------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `none`     | The stored hash equals the artifact hash and tables exist.       | Nothing.                                                                                |
| `create`   | No `t_` tables exist.                                            | Run the full DDL.                                                                       |
| `additive` | Only new tables or new nullable columns are needed.              | `CREATE TABLE` and `ALTER TABLE ... ADD COLUMN`. Drop `t_` tables that left the schema. |
| `reset`    | A required column is missing, or a local column left the schema. | Drop every `t_` table and run the full DDL.                                             |

The plan never inspects column types. A changed type on a column that keeps its name is invisible to `planMigration`.

## Additive changes

These changes keep old clients connected and migrate local caches in place:

- Add a synced table.
- Add a nullable column to a synced table.
- Add a relation.

The hash still changes. Every runtime notices a new hash, so the rollout procedure below still applies. The difference is that no local cache is dropped by `planMigration`.

## Breaking changes

These changes force a `reset` plan, a 4409 rejection, or both:

- Remove a synced column or a synced table. Clients that still know the column are rejected with 4409 until they redeploy. Local caches that have the column are reset.
- Add a `NOT NULL` column. `planMigration` cannot add it in place, so the cache is reset.
- Change the primary key. The local table layout depends on it.
- Change the partition column or the partition key kind. Rows route to different partitions.
- Change the placement version. Durable Object names change, so every partition starts empty.

Column type changes are a special case. The hash changes, because `kind` and `source_type` are part of the artifact. `planMigration` does not detect the type change. The Durable Object still drops its cached rows through the scope reset described below, so the cache is rebuilt from a fresh fill. The browser cache is not reset by a type change alone. Treat a type change as breaking and bump a client-side database name, or accept that stale cells persist until a snapshot replaces them.

## How each runtime reacts

### The Sync Durable Object

`SyncEngine.init` in `packages/sync-do/src/core/engine.ts` runs on every construction. It compares the stored `schema_hash` with the artifact compiled into the Worker. When the hashes differ:

1. It runs the `planMigration` statements.
2. It calls `resetScopes`. Every `t_` table is emptied. The `scopes`, `held`, `membership`, and `fills` tables are cleared. Subscriptions stay registered but become not live.
3. It emits a `scopes_reset` event. The Durable Object marks every client subscription pending, sends `subscribed pending` to every session, and re-subscribes each query. Each query requests a fresh fill.
4. It stores the new hash.

Cached rows are dropped even for additive changes, because they were projected with the old column set and lack the new columns.

`applyBatch` rejects a `CdcBatch` whose `schema_hash` differs from the Durable Object's hash with reason `schema_mismatch`. The distributor retries that batch with backoff, up to 20 attempts, then quarantines the partition. A mismatch that lasts longer than the retry window needs an operator replay (see [operations-runbook.md](operations-runbook.md)).

### The distributor

`StateStore::load_or_init` in `crates/orbit-distributor/src/state.rs` reads the stored `schema_hash` from the state database. When the stored hash differs from the running schema, it returns `StateError::SchemaChanged` and the server does not start. Every `orbit-server` subcommand that opens the state store goes through this check.

The library has `StateStore::migrate_schema`, which overwrites the stored hash and keeps the sequence counters. No `orbit-server` subcommand calls it today. The practical options are to start with a new `--state` path, or to delete the state file. Both start from an empty checkpoint with a fresh, time-based epoch, so every Durable Object resets and refills. See [checkpoints.md](checkpoints.md).

### The browser

`LocalStore.open` in `packages/client/src/store.ts` runs `planMigration` against the local SQLite database. A `reset` or `create` plan also clears `membership`, `subscriptions`, and `meta`, so the cursor becomes `null`. An `additive` plan keeps rows, memberships, and the cursor.

The client then connects and sends its summary. A 4409 close is fatal: the client stops reconnecting and sets `fatalError` with code `schema_mismatch`. The application must ship a build with a compatible artifact.

## Operator procedure

Every change follows the same order. The steps are sequential.

1. Regenerate the introspection when the database changed:

   ```sh
   orbit-server schema introspect --keyspace <keyspace> --out apps/<app>/orbit/schema.introspected.ts
   ```

2. Edit `sync.config.ts` and recompile the artifact with the application's compile script. The script calls `compileSyncSchema` and writes `orbit.schema.json`.

3. Deploy the Worker with the new artifact. From this point, every Durable Object that wakes up resets its scopes and refills. Clients with the old artifact stay connected when the change is additive.

4. Stop the running engine. Start it with the new artifact and a new state path:

   ```sh
   orbit-server run --schema apps/<app>/orbit/orbit.schema.json --state data/orbit-state-<hash>.sqlite ...
   ```

   The old state file cannot be reused, because `load_or_init` refuses the new hash.

5. Deploy the client build that imports the new artifact.

Between steps 3 and 4 the engine delivers batches with the old hash. The Durable Objects reject them with `schema_mismatch` and the distributor retries. Keep this window short, under the retry budget of 20 attempts with backoff up to 30 seconds, or quarantined batches must be replayed.

The reverse order (engine first, Worker second) is also possible. In that window the Durable Objects reject batches with the new hash for the same reason. Choose the order that gives the shortest window.

## What the code does not enforce

- Nobody checks that a column type in the artifact still matches the live database. A fill or a CDC row that fails row validation surfaces later as `invalid_row` or `normalization`.
- The browser does not detect a type change on a column with an unchanged name.
- There is no automatic replay of batches quarantined during a rollout.
