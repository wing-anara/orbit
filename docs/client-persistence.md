# Client persistence

The browser client keeps a relational cache in SQLite. This document describes where the database lives, the table layout, how snapshots and deltas are applied, what survives a reload, and how the connection reconnects.

Related documents: [protocol.md](protocol.md), [queries.md](queries.md), [schema-evolution.md](schema-evolution.md), [scalability-limits.md](scalability-limits.md).

## Storage

The client uses `@sqlite.org/sqlite-wasm`. The database runs in a dedicated Web Worker (`packages/client/src/worker/sqlite.worker.ts`). The main thread talks to it through a small RPC (`packages/client/src/worker/proxy.ts`). The RPC has five requests: `open`, `query`, `batch`, `estimate`, and `close`. Every request and response is validated with an Effect Schema on both sides.

Two storage modes exist:

- `opfs` (default). The worker installs the `opfs-sahpool` VFS with `installOpfsSAHPoolVfs({ name: "orbit-sahpool", initialCapacity: 6 })`. It opens `/<name>.sqlite3` in that pool. The VFS needs no COOP or COEP headers.
- `memory`. The worker opens `:memory:`.

The default database name is `orbit-<app>-<partition>`. Pass `databaseName` to override it. The worker sets `PRAGMA synchronous = NORMAL` and `PRAGMA foreign_keys = OFF`.

A pool is exclusive. Only one browsing context per origin can hold it. `createOrbitClient` in `packages/client/src/client.ts` gives each tab its own pool (a slot): the first tab holds `orbit-sahpool`, the next tab `orbit-sahpool-1`, and so on up to `tabs` slots (default 4). The worker marks its slot with a Web Lock (`orbit-slot:<pool>`), so a later tab skips a held slot without touching its files. Each slot is a full database with its own client id, cursor and pending log, so a mutation queued offline outlives the tab that queued it. The first tab drains the other slots at startup: it opens each slot nobody holds, and when its pending log is not empty it runs a drain engine as that client (`drainOnly`, no subscriptions restored) until the log is confirmed or 60 seconds pass. The log events are `store.slot`, `store.drain`, `store.drained` and `store.drain_failed`.

When every slot is held, or the open fails with `unsupported` (private mode, an old browser), the client logs `store.fallback` and opens a `memory` database with a fresh worker. Any other error is thrown as `OrbitClientError`. If the caller asked for `memory`, no fallback happens.

The chosen mode is visible as `storageMode` in the status object. An application can show it in its user interface. A tab in `memory` mode works, but it starts empty and loses everything on reload.

The worker classifies errors by message text into `quota_exceeded`, `locked`, `corrupt`, `unsupported`, or `sql`. The classification reaches the main thread as a typed `SqlDriverError`.

## Table layout

The store (`packages/client/src/store.ts`) creates one table per synced table plus three bookkeeping tables. The synced tables use the shared DDL from `packages/schema/src/ddl.ts`:

```sql
CREATE TABLE IF NOT EXISTS "t_Chatbot" (
  "__key" TEXT NOT NULL PRIMARY KEY,
  "id" TEXT NOT NULL,
  "name" TEXT,
  ...,
  UNIQUE ("id")
) WITHOUT ROWID
```

`__key` is the JSON-encoded primary key. The engine addresses rows by it. The real primary key columns keep a unique index. An index is created for the `from_columns` of every `one` relation.

The bookkeeping tables are:

| Table               | Columns                                      | Purpose                                              |
| ------------------- | -------------------------------------------- | ---------------------------------------------------- |
| `meta`              | `key`, `value`                               | `schema_hash`, `partition`, `cursor`.                |
| `membership`        | `subscription`, `tbl`, `key`                 | Rows each subscription references.                   |
| `subscriptions`     | `id`, `ref`, `query`, `cursor`, `complete`   | Persisted query references and resolved queries.     |
| `pending_mutations` | `id`, `name`, `args`, `created_at`, `pushed` | The mutation log (see [mutations.md](mutations.md)). |
| `o_<table>`         | cache columns, `__op`, `__mutation`          | Optimistic overlay per synced table.                 |
| `v_<table>`         | view                                         | Cache with the overlay applied.                      |

A row exists only while at least one subscription references it. This is reference counting through the `membership` table. When the last reference goes away, the row is deleted. All garbage collection is expressed in SQL, so it runs inside the same transaction as the change that caused it.

## Applying snapshots and deltas

Every protocol message becomes one `batch` call. The worker runs the statements inside one SQLite transaction. A snapshot or a delta is therefore durable atomically or not at all.

`applySnapshot` runs, in order:

1. Delete rows that only this subscription references.
2. Delete the subscription's membership.
3. Insert the new membership.
4. Upsert every snapshot row.
5. Mark the subscription complete and store its cursor.
6. Store the cursor in `meta`.

`applyDelta` runs, in order:

1. Remove the removed memberships and add the added ones.
2. Upsert every row, gated by membership. The insert has `WHERE EXISTS (SELECT 1 FROM membership WHERE tbl = ? AND key = ?)`, so a row that no subscription references is not stored. A delete (`row` null) is applied without a gate.
3. Delete each removed row when no membership references it anymore.
4. Store the cursor in `meta`.

Snapshot chunks are buffered in memory. The store is called once, when the chunk with `complete: true` arrives.

After a delta, the engine re-runs every live query whose tables were touched or whose memberships changed. Listeners are notified with the new rows.

## What survives a reload

The `opfs` database persists these values across reloads:

- Every cached row in the `t_` tables.
- The `membership` table.
- The `subscriptions` table, with each query and its `complete` flag.
- `meta.cursor`, the cursor of the last applied snapshot or delta.
- `meta.schema_hash` and `meta.partition`.

On open, `ClientEngine.open` restores the persisted subscriptions. Each one is registered with zero application references and gets status `stale` when it was complete, or `pending` otherwise. The engine runs the query against the local store, so the application renders cached rows before the socket opens. The restored cursor is reported in the status. A restored subscription waits out `queryTtlMs` like a released one: the queries the application subscribes to again within that time stay, the others retire, so a load replays only recent views.

The test `survives a reload` in `packages/client/test/engine.test.ts` confirms this. After a reload the query reports `stale` with the persisted rows. It becomes `live` when the server sends a fresh snapshot.

A restored subscription is re-sent in `hello`. It stays registered until the application subscribes to the same query and later releases it.

These values do not survive a reload:

- Connection state and the session id.
- Snapshot chunks that were in flight.
- Application reference counts on subscriptions.
- The `live` status. Every restored query starts as `stale` or `pending`.

The `memory` mode persists nothing.

The store resets itself in two cases: `planMigration` returns `reset` (see [schema-evolution.md](schema-evolution.md)), or the stored partition differs from the requested one. A reset drops every `t_` table, clears `membership`, `subscriptions`, and `meta`, and sets the cursor to `null`.

## Local-first reads and query retention

A live query renders before the server answers. While a subscription has no snapshot yet, the engine evaluates it over the local cache, so a query whose rows other subscriptions already hold (a narrower filter, a folder that was preloaded) shows its rows at once with status `pending`. The server's snapshot then replaces the set and the status turns `live`. Rows the cache does not hold appear when the snapshot lands.

A window that grows on scroll extends its previous subscription (`subscribe.basedOn`). The store records the link in `subscriptions.based_on` and keeps the base's rows under the base: a read follows the chain, a membership removal reaches every window in the chain (they are subsets of each other), and when a base retires or gets a complete snapshot its rows move to the windows that extend it. A grown window therefore writes only its new members.

A retained query that nobody references is not re-read on every change. It is marked dirty and reads once when it is referenced again. Without this, every growth of a window re-reads every smaller window it grew from.

A released query is not dropped at once. It stays subscribed for `queryTtlMs` (default five minutes), so its rows stay cached and current through deltas, and a component that subscribes again within that time is `live` immediately, online or offline. When the TTL passes with no reference, the engine unsubscribes and garbage-collects the rows only that query referenced. Set `queryTtlMs: 0` to retire queries at once. Zero calls the same idea the query TTL.

Applications keep frequent views instant by subscribing once to a broad "preload" query, for example an `allDocuments` query.

## The optimistic overlay

With mutators configured, the store adds three kinds of objects (see [mutations.md](mutations.md)):

- `pending_mutations`: the client's mutation log, pushed in id order.
- `o_<table>`: an overlay table per synced table, with the cache columns plus `__op` and `__mutation`.
- `v_<table>`: a view that shows `t_<table>` with the overlay applied on top.

Live queries and `client.read` compile against the views, so rows written by pending mutations are visible at once. The overlay is derived state: the engine rebuilds it from the log on open and after every delta (the rebase). A reset of the local schema clears the log and the overlay together with the memberships. The client id is persisted in `meta`, so the server's `orbit_clients` row keeps matching the same browser database across reloads.

## Status object

`getStatus` returns a `SyncStatus`:

- `connection`: `connecting`, `open`, `reconnecting` (with `attempt` and `retryInMs`), or `closed` (with `fatal` and `code`).
- `cursor`: the current local cursor or `null`.
- `partition`.
- `pendingSubscriptions`: number of subscriptions that are not live.
- `pendingMutations`: mutations applied locally and not yet confirmed through sync.
- `lastDeltaAt` and `lastCommitTimestamp`: for lag display.
- `storageMode`: `opfs` or `memory`.
- `storage`: usage, quota, and database size. The current engine never fills this field; it stays `null`. The worker `estimate` request exists but is not called by the engine.
- `fatalError`: set after a fatal close or a fatal server error.

## Reconnect and resume

`packages/client/src/connection.ts` owns the socket. It runs a loop of connection attempts:

1. Ask the application for a fresh token. `getToken` runs on every attempt, so a short-lived token is fine. The token is offered as the WebSocket subprotocol `orbit.token.<token>` next to `orbit`, never in the URL (see [auth.md](auth.md)).
2. Open the socket. On `open`, call `sendHello`. The engine sends its cursor and every active subscription, and marks every live query `stale`.
3. Wait for `close`.
4. If the close code is 4400, 4401, 4403, or 4409, stop. The status becomes `closed` with `fatal: true`.
5. Otherwise wait and retry.

The backoff starts at `backoffMinMs` (default 500 ms) and doubles up to `backoffMaxMs` (default 30 s). A random jitter of up to 30 percent is added. A successful open resets the backoff to the minimum.

The heartbeat sends `ping` every 10 seconds (`pingIntervalMs`). When a `ping` gets no `pong` within 5 seconds (`pongTimeoutMs`), the attempt ends with code 4000 and the loop reconnects. The client does not wait for the socket's `close` event, because an offline browser never completes the closing handshake. An established WebSocket does not notice a dead network by itself.

Browser events shorten the wait. An `offline` event closes the socket with code 4001. An `online` event ends the backoff sleep at once.

Resume is snapshot-based. The server answers every reconnect with a fresh snapshot per subscription, taken at one cursor. The store applies each snapshot as a diff against what it has, so unchanged rows are rewritten but not duplicated. The test `reconnects after a drop` confirms that changes made while offline appear after the reconnect.

A malformed server message is not dropped. The connection turns it into a fatal `invalid_message` error that the engine exposes as `fatalError`.

## Sharing a browser cache across tabs

Use `createSharedOrbitClient` when tabs for the same signed-in identity should share a
cache. It accepts the ordinary client configuration plus a required stable `subject`.
The scope includes the normalized sync server URL, schema app, partition, subject, and
optional database name. Different identities never share an OPFS pool or channel.

```ts
const client = await createSharedOrbitClient({
  definition: sync,
  schema: artifact,
  url: "https://app.example.com/orbit",
  partition: organizationId,
  subject: userId,
  getToken,
  worker: () => new SqliteWorker(),
  mutators,
  pushUrl: "https://app.example.com/orbit/push",
})
const mutation = client.mutate.renameDocument({ id, name })
await mutation.local
const idInDurableQueue = await mutation.id
const outcome = await mutation.server
```

Web Locks elect one owner. That tab runs the existing dedicated SQLite worker, engine,
WebSocket, and mutation queue. Other tabs use BroadcastChannel RPC and keep only their
query results. Identical queries share an engine subscription and each changed result
is broadcast once. Peer Web Locks release query references when tabs disappear.
There is no timer-based lease, competing writer, or silent in-memory fallback.

Closing or crashing the owner releases its locks. A waiting tab opens the same OPFS
pool, resumes the same client id and pending log, and restores active queries. The
`freeze`/`pagehide` hooks terminate the worker and socket before releasing ownership;
`resume`/`pageshow` rejoin. Browsers must support Web Locks, BroadcastChannel, and the
configured SQLite storage. An incompatible schema in another tab fails explicitly;
reload or close old tabs before using the new schema.

Mutation ids are allocated by the owner, so **shared mutation handles expose
`id: Promise<number>`**, while `local` and `server` retain their usual meaning. The
ordinary `createOrbitClient` API still exposes a synchronous id. A locally acknowledged
mutation survives owner handoff and its `server` promise reconnects to the durable
outcome. The last 1,024 outcomes are retained. Requests whose local commit was not
acknowledged reject with `SharedOwnerChangedError`; inspect current state before retrying
because the write may have committed immediately before the owner disappeared.
`awaitMutation(id)` recovers a known outcome or waits on a still-pending mutation; it
rejects for unknown/pruned ids. These receipts are local recovery metadata, not an audit log.

For an upgrade from the original per-tab storage, opt into `legacySlots: 4` (or the old
configured slot count). On owner startup, available old slots with the same partition
and schema are drained with their original client ids and current authenticated push
transport. Their cached views are never imported into the new identity-scoped database.
Recovery does not clear old databases. Slots held by old tabs are skipped; reload the
remaining old tabs, then reopen a shared owner to recover their queues. Incompatible
legacy schemas are left intact and logged as `store.recovery_skipped` rather than reset.
Temporary recovery workers/connections may exist until those old queues drain.
