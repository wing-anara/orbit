# Mutations

Orbit writes are mutators: named functions with typed arguments, defined once by the application and run twice. The browser runs a mutator at once against its local cache (optimistic). The application's server runs the same mutator against the authoritative database inside one transaction. The database stays the only source of truth: the effect of the server run comes back through change data capture like any other write, and that delta is the confirmation. This document describes the shared definitions, the client side (overlay, log, push, rebase), the confirmation path, and the guarantees.

Related documents: [architecture.md](architecture.md), [auth.md](auth.md), [client-persistence.md](client-persistence.md), [consistency-invariants.md](consistency-invariants.md), [queries.md](queries.md).

## Shared definitions

Mutators live in shared code, next to the named queries. An application defines them like this:

```ts
import { defineMutator, defineMutators } from "@orbit/mutators"

const define = defineMutator(sync)
export const mutators = defineMutators(sync, {
  renameChatbot: define(
    Schema.Struct({ id: Schema.String, name: Schema.String }),
    async (tx, { id, name }, ctx) => {
      const row = await tx.get("Chatbot", { id })
      if (row === null || row.organizationId !== ctx.partition) throw new Error("not found")
      await tx.update("Chatbot", { id }, { name, updatedAt: ctx.now })
    },
  ),
})
```

`tx` is a `MutationTx` with `insert`, `insertMany`, `update`, `delete`, `get`, and `query` (a typed query, see [queries.md](queries.md)). Use `insertMany` for bulk writes: on the server it becomes one multi-row statement per chunk of 200 rows, where a loop of `insert` calls would cost one database round trip each. Every call is typed against the sync schema. A mutator may touch synced tables only.

`ctx` is a `MutationContext`: `partition`, `subject`, `clientId`, `mutationId`, `now` (wire datetime), and `side` (`client` or `server`). On the server, `partition` and `subject` come from the session. A mutator never trusts an organization id in its arguments; it throws to refuse a write. That throw is the authorization model for writes, the counterpart of the named query resolver for reads (see [auth.md](auth.md)).

Ids are chosen by the caller (for example with a `newId("doc")` helper). The optimistic row and the confirmed row are then the same row, keyed the same way.

## The client side

`createOrbitClient({ mutators, pushUrl })` enables `client.mutate.<name>(args)`. The call returns `{ id, local, server }`:

- `id` is the mutation id, dense per client.
- `local` resolves when the mutator has been applied to the local cache.
- `server` resolves with the push outcome (`applied`, `duplicate`, or `failed` with an error). It rejects when the mutator throws locally; nothing is queued in that case.

`client.onMutation(listener)` reports `applied_locally`, `pushed`, `confirmed`, and `failed` per mutation. `SyncStatus.pendingMutations` counts the mutations applied locally and not yet confirmed.

### The overlay and the log

The local store (`packages/client/src/store.ts`) keeps, per synced table, an overlay table `o_<table>` with the same columns as the cache table plus `__op` (`upsert` or `delete`) and `__mutation`, and a view `v_<table>` that shows the cache with the overlay applied on top. Live queries and `client.read` compile against the views, and a row in the overlay is admitted to a live query even before the server admits it. The table `pending_mutations` is the client's log: `id`, `name`, `args`, `created_at`, `pushed`.

A call to `client.mutate.<name>(args)`:

1. Encodes and validates the arguments with the mutator's codec.
2. Allocates `id` as the next dense id.
3. Runs the mutator against a `MutationTx` over the views. Reads see the overlay; writes collect overlay statements.
4. Writes the pending row and the overlay rows in one batch. A mutator that throws writes nothing.
5. Refreshes the live queries whose tables were touched, resolves `local`, and wakes the push loop.

The overlay is derived state. It is a function of the canonical rows and the log, and the engine rebuilds it whenever either changes. On open, the engine rebuilds it from the persisted log before the first render, so a reload keeps the optimistic state and the pending log.

### Push

The push loop sends the unpushed mutations in id order as one `PushRequest` to `pushUrl` (`POST`, JSON, `credentials: include`). One push is in flight at a time. The wire shape is in `packages/protocol/src/mutation-protocol.ts`.

- `ok`: each outcome resolves its `server` promise. `applied` and `duplicate` mutations stay pending until sync confirms them. A `failed` mutation is dropped from the log, its overlay rows are removed, the remaining log is replayed, and `failed` is reported.
- `refused` with `out_of_order`: the client drops every pending id at or below the server's `lastMutationId` (the server applied them in a push whose response was lost) and retries.
- Network failure: retry with exponential backoff (250 ms to 30 s), and at once on `online` or on reconnect. Pending mutations are persisted, so nothing is lost while offline.

### Confirmation through sync

The server writes the client's row in `orbit_clients` (`client_id`, `partition_key`, `last_mutation_id`, `updated_at`) in the same database transaction as the mutator's writes. That row is a synced table. When the client has mutators configured, the engine subscribes to the built-in named query `$orbit.client`, which the Durable Object resolves to the caller's own row.

After every applied delta or snapshot the engine reads `last_mutation_id` from the local copy. Every pending mutation with `id <= last_mutation_id` is confirmed: the engine deletes it from the log, drops its overlay rows, replays the remaining mutations in order on top of the new canonical rows (the rebase), and then refreshes the live queries. All of this happens in the same local batch that applied the delta. Because the delta carries both the effect rows and the bookkeeping row, a confirmed row moves from the overlay to the canonical table in one step, and a live query never shows an empty gap.

If `orbit_clients` is not in the sync schema, mutations still apply and push, but they never confirm; the engine logs `mutations.unconfirmable`.

### Rebase

The rebase runs whenever a delta lands or a mutation is dropped: clear every overlay table, then run every remaining pending mutation in id order against the views. Reads inside a mutator see the new canonical rows plus the mutations before it, so a pending update merges onto a row the server changed meanwhile. A mutation whose replay throws (for example, its row was deleted by someone else) is dropped and reported as `failed`. This is the Replicache and Zero model.

## Guarantees

- At-least-once push, exactly-once apply. Ids are dense per client; the server treats a replay as `duplicate` and a gap as `out_of_order`.
- The database stays the only source of truth. Constraints, triggers, and existing backend code keep working.
- Confirmation is the observed effect, not the HTTP response. A client never marks a mutation confirmed before its effects are in the local cache.
- A failed mutator on the server consumes its id and reports the error. The client rolls the optimistic effect back.
- Offline writes queue in the persistent log and push when the network returns.

## Limits

- A mutator may write synced tables only. Unsynced tables need a normal backend call.
- The client cannot see server-side defaults until the confirming delta arrives; a mutator should set every synced column it needs to render.
- Server-generated timestamps differ from the client's `ctx.now`; the confirmed row replaces the optimistic one.
- Mutations over the WebSocket, and applying optimistic effects in the shared Durable Object cache, are not planned (see [future-scaling.md](future-scaling.md)).

## Server push endpoint

`@orbit/mutators/server` implements the application's mutation endpoint. `createPushHandler` returns a `(request: Request) => Promise<Response>` function. It uses the Web standard `Request` and `Response`, so it runs in a Cloudflare Worker and in Node.

```ts
import { createPushHandler } from "@orbit/mutators/server"

export const push = createPushHandler({
  schema, // the compiled SyncSchema artifact
  mutators, // the shared defineMutators(...) value
  db, // a PushDb over the application's database driver
  authorize: async (request, body) => {
    const user = await userFromRequest(request)
    if (user === null || !(await isMember(user.id, body.partition))) return null
    return { subject: user.id }
  },
})
```

The application supplies four things:

- `schema`: the compiled sync schema. The handler uses it for column kinds, primary keys, and the list of synced tables.
- `mutators`: the same `defineMutators` value the client uses.
- `db`: a `PushDb`. It has one method, `transaction(f)`. The method runs `f` with a `SqlTx` inside one transaction. It commits when `f` resolves and rolls back when `f` rejects. `SqlTx` has `query(sql, params)` and `execute(sql, params)`. Parameters are `string | number | boolean | null`. An adapter over a `mysql2` connection fits in a few lines.
- `authorize(request, body)`: authenticates the caller and checks that the caller may write to `body.partition`. It returns `{ subject }` or `null`.

### Request flow

The handler decodes the body as a `PushRequest`. A body that does not decode gets status 400. Every other answer is a `PushResponse` with status 200.

1. A `protocolVersion` that differs from the server's version is `refused` with reason `protocol_version_mismatch`.
2. When `authorize` returns `null`, the push is `refused` with reason `unauthorized`.
3. The handler runs each mutation in order. Each mutation gets its own database transaction.

Inside the transaction for one mutation, the handler:

1. Reads the client's row: `SELECT last_mutation_id FROM orbit_clients WHERE client_id = ? FOR UPDATE`. A missing row counts as 0.
2. Returns outcome `duplicate` when `id <= last_mutation_id`. Nothing is written.
3. Stops the push when `id > last_mutation_id + 1`. The response is `refused` with reason `out_of_order` and the server's `lastMutationId`. Mutations applied earlier in the same push stay committed. The client resynchronizes from `lastMutationId`.
4. Decodes the arguments with the mutator's schema and runs `apply(tx, args, ctx)`. The context carries `partition`, `subject`, `clientId`, `mutationId`, `now`, and `side: "server"`.
5. Upserts the client's row with `last_mutation_id = id` and commits. The outcome is `applied`.

The response ends with `lastMutationId`, the server's value for the client after the push.

### The `orbit_clients` table

The application creates the table with `orbitClientsDdl()` from `@orbit/protocol` and adds it to the sync schema with `partition_key` as the partition column. The columns are `client_id` (primary key), `partition_key`, `last_mutation_id`, and `updated_at`.

The handler writes the client's row in the same transaction as the mutator's writes. The CDC transaction that carries the effects also carries the confirmation. A client that syncs its own `orbit_clients` row sees `last_mutation_id` reach the mutation's id in the delta that applies the mutation's rows. It drops the optimistic overlay for that mutation in the same local transaction. The HTTP response is informational. The confirmation is the synced row.

### Failure semantics

A mutation fails when its arguments do not decode, when the mutator name is unknown, or when `apply` throws. A database error inside `apply`, for example a unique key violation, is also a failure.

On failure the handler:

1. Rolls back the transaction. None of the mutator's writes reach the database.
2. Opens a second transaction and records `last_mutation_id = id` for the client. This consumes the id.
3. Reports the outcome `failed` with the error message, cut to 500 characters.

The client drops the mutation when it sees the consumed id and reports the error to the application. The next mutation in the push then runs normally.

When the second transaction itself fails, or when `authorize` throws, the handler answers status 500 with `{ "error": message }`. No outcome is reported for that push. The client retries the push later, and the duplicate and out-of-order rules make the retry safe.

### The MySQL `MutationTx`

`createMysqlTx(rt, sqlTx, mutatorName)` builds the `MutationTx` a mutator runs against. Every statement is parameterized. Table and column names come from the sync schema, so a mutator can only touch synced tables and synced columns. A table outside the sync schema throws a `MutatorError`.

- `insert(table, row)` lists only the columns present in `row`. Server defaults fill the rest.
- `update(table, key, patch)` sets the columns in `patch` and addresses the row by its full primary key. An empty patch does nothing.
- `delete(table, key)` addresses the row by its full primary key.
- `get(table, key)` selects the synced columns and returns a wire row, or `null`.
- `query(typedQuery)` plans the query, compiles it with the MySQL dialect, runs one statement per include level, and returns the typed rows with relations attached.

Values are converted per column kind. On the way in: `bool` becomes 0 or 1, `json` becomes its text, `bigint` and `decimal` strings pass through, and `bytes` (base64 text) is bound under a `FROM_BASE64(?)` placeholder. On the way out: tinyint values become booleans for `bool` columns, `Date` objects become `YYYY-MM-DD HH:MM:SS.mmm` text, numbers become strings for `bigint` and `decimal` columns, JSON text is parsed, and `Buffer` values become base64. Configure the driver with `dateStrings: true` and `bigNumberStrings: true` so the database text passes through unchanged.

### Tests

`pnpm --filter @orbit/mutators test` runs the unit tests against an in-memory fake `SqlTx` that records every statement. `ORBIT_TEST_VITESS=1 pnpm --filter @orbit/mutators test` also runs the handler end to end against the local docker Vitess with `mysql2`.
