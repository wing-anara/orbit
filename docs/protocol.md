# Protocol

Orbit has two protocols. The client protocol runs between the browser and a Sync Durable Object over a WebSocket. The internal protocol runs between the Rust server and the Worker over HTTP. This document lists every message, field, close code, and route.

Related documents: [architecture.md](architecture.md), [auth.md](auth.md), [queries.md](queries.md), [client-persistence.md](client-persistence.md), [failure-model.md](failure-model.md), [bootstrap.md](bootstrap.md).

## Client protocol v1

The client protocol is defined in `packages/protocol/src/client-protocol.ts`. `CLIENT_PROTOCOL_VERSION` is `1`. Application messages are JSON; negotiated native heartbeats use a fixed text pair. Every JSON message is a member of a tagged union. Decoding is exhaustive: an unknown message is an error.

The cursor is the partition sequence (`applied_seq` of the Durable Object) at which the client's local state is consistent. Deltas carry the cursor they advance to. The client applies consecutive queued deltas in bounded, ordered atomic batches and acknowledges the last committed cursor. Snapshots and connection transitions are batching barriers.

### Connection

The client opens `GET {prefix}/ws?partition=P&token=T`. The Worker verifies the token before the upgrade (see [auth.md](auth.md)). A rejected token gets an HTTP response: `401` with `{ error: "unauthorized", reason }` or `403` with `{ error: "partition_denied", partition }`. The upgrade never happens in that case. An accepted request is forwarded to the Durable Object with the headers `x-orbit-partition` and `x-orbit-subject`.

### Client messages

`hello` must be the first message.

| Field             | Type                 | Meaning                                                        |
| ----------------- | -------------------- | -------------------------------------------------------------- |
| `protocolVersion` | integer              | Must equal `1`.                                                |
| `clientId`        | string               | Stable id of the client instance.                              |
| `token`           | string               | Reserved. The client sends `""`; the token travels in the URL. |
| `partition`       | string               | Must equal the partition the Worker authorized.                |
| `schema`          | `SchemaSummary`      | `schemaHash` plus table names and column names.                |
| `cursor`          | integer or null      | Cursor of the local state. `null` when starting fresh.         |
| `subscriptions`   | `SubscribeMessage[]` | Subscriptions to establish at once.                            |

The server does not infer completeness from `hello.cursor`. A complete, active in-memory subscription may instead include `resume: { version, query }`, using the last server-issued cache version and the previously resolved query. The server resolves the query again with the current session identity. Only an exact normalized-query and cache-version match permits `subscribed.resumed: { version, cursor }` instead of a snapshot. Otherwise it sends the normal snapshot. Cached views loaded from disk, incomplete snapshots, dirty/released views and views affected by a local apply failure never request this fast path.

Snapshots and deltas optionally carry `version`. The version combines schema, a persisted cache incarnation, stream epoch and source cursor. Cache fills, retries and resets invalidate the incarnation even when the cursor is unchanged. It survives Durable Object eviction. This is conditional revalidation, not historical delta replay: any source change the client has not applied requires a snapshot. Optional fields keep older clients and servers compatible; an older server simply sends a snapshot.

`subscribe` has `id` (string, chosen by the client) and `query`, a `QueryRef`: either a named query `{ name, args }` or a raw `Query` (see [queries.md](queries.md)). Raw queries are accepted only when the Durable Object allows ad-hoc queries. The client uses the canonical JSON of the reference as `id`.

`unsubscribe` has `id`.

`ack` has `cursor`. The server records changed acknowledgments in the hibernation attachment, without SQL writes. This is diagnostic state: recovery uses the durable engine and browser cursors, not the session acknowledgment.

`ping` has `sentAt` (unix milliseconds).

### Server messages

`welcome` answers a valid `hello`.

| Field             | Meaning                                                                              |
| ----------------- | ------------------------------------------------------------------------------------ |
| `protocolVersion` | `1`                                                                                  |
| `sessionId`       | Server-generated session id.                                                         |
| `partition`       | The authorized partition.                                                            |
| `schemaHash`      | The server's schema hash.                                                            |
| `cursor`          | The Durable Object's current `applied_seq`.                                          |
| `serverTime`      | Unix milliseconds.                                                                   |
| `sessionRenewal`  | Optional `{ expiresAt }` (unix milliseconds); advertises authenticated HTTP renewal. |
| `heartbeat`       | Optional `"static-v1"`; advertises native text auto-response.                        |

`subscribed` has `id`, `status`, and `query`. `status` is `pending` while the Durable Object fills the scope from the source, and `live` once the snapshot follows. `query` is the resolved, normalized query the server materializes; for a named query it is the resolver's output with the session identity applied. The client adopts it when it differs from its local resolution.

`snapshot` is the full result of one subscription at `cursor`.

| Field            | Meaning                                  |
| ---------------- | ---------------------------------------- |
| `subscriptionId` | The client's subscription id.            |
| `cursor`         | Cursor the snapshot is consistent at.    |
| `rows`           | `RowUpdate[]`: `{ table, key, row }`.    |
| `members`        | `MemberRef[]`: `{ table, key }`.         |
| `complete`       | `false` for every chunk except the last. |

Large results arrive in chunks of `snapshotChunkRows` (default 500). Only the last chunk carries `members`. The client buffers chunks and applies them as one unit when `complete` is `true`. The row set replaces the client's previous membership for the subscription.

A window that grows on scroll is the same query with a larger limit. The client sends `subscribe` with `basedOn`: the id of the live subscription it extends, which it keeps until the answer arrives. The server seeds the new membership from the base and answers with `snapshot.basedOn` set: `members` and `rows` are then only those the base does not hold, and the client copies the base membership before it applies them. The server omits `basedOn` (a complete snapshot) when the session does not hold the base live, or when the base is not a subset of the result. A client that receives `basedOn` for a base it no longer holds unsubscribes and subscribes again without `basedOn`. On reconnect the client replays its subscriptions without `basedOn`.

`delta` carries the changes of exactly one source transaction.

| Field         | Meaning                                                |
| ------------- | ------------------------------------------------------ |
| `cursor`      | The sequence this delta advances to.                   |
| `origin`      | `{ gtid, commitTimestamp, seq, trace, appliedAt }`.    |
| `rows`        | Rows to upsert (`row` present) or delete (`row` null). |
| `memberships` | Per subscription: `added` and `removed` member refs.   |

The Durable Object sends a delta only to sessions with at least one live subscription. It includes a row only when the row is a member of one of the session's live subscriptions, or when the row is a delete. Membership changes are re-keyed from engine subscription ids to the client's ids.

`unsubscribed` has `id`.

`subscription_error` has `id` and `error`. The engine sends it when a query is rejected (`unsupported_query`), when a named query does not exist (`unknown_query`), when a raw query is not allowed (`unauthorized`), or when a fill fails (`fill_failed`).

`error` has `error` and `fatal`. A fatal error is followed by a close.

`pong` has `sentAt` (echoed) and `serverTime`.

### Errors

`SyncError` has `code`, `message`, optional `details`, and optional `cause` (an `EngineError`). The codes are: `protocol_version_mismatch`, `schema_mismatch`, `unauthorized`, `partition_denied`, `unsupported_query`, `unknown_query`, `invalid_message`, `fill_failed`, `overloaded`, and `internal`.

Mutations do not travel over the WebSocket. The client pushes them to the application's endpoint with the push protocol in `packages/protocol/src/mutation-protocol.ts` (see [mutations.md](mutations.md)).

### Close codes

`CloseCode` defines these values:

| Code | Name                      | When the Durable Object sends it                                                                        |
| ---- | ------------------------- | ------------------------------------------------------------------------------------------------------- |
| 4400 | `protocolVersionMismatch` | `hello.protocolVersion` is not `1`.                                                                     |
| 4401 | `unauthorized`            | Defined. Not sent by the current Durable Object; the Worker rejects before the upgrade.                 |
| 4403 | `partitionDenied`         | `hello.partition` differs from the authorized partition.                                                |
| 4408 | `sessionExpired`          | The grant behind the socket expired (see [auth.md](auth.md)). The client reconnects with a fresh token. |
| 4409 | `schemaMismatch`          | The client schema is not additively compatible (see [schema-evolution.md](schema-evolution.md)).        |
| 4422 | `invalidMessage`          | Non-JSON, a message that fails validation, or a message before `hello`.                                 |
| 4429 | `overloaded`              | Defined. Not sent by the current code.                                                                  |
| 4500 | `internal`                | Missing session attachment, or an operator reset of the partition.                                      |

The client treats 4400, 4401, 4403, and 4409 as fatal. It stops reconnecting and exposes `fatalError` in the status. Every other close, 4408 included, triggers reconnect with backoff (see [client-persistence.md](client-persistence.md)).

### Message flow

1. The client connects and sends `hello` with its cursor and all active subscriptions.
2. The server checks the version, the partition, and the schema. It creates a session and sends `welcome`.
3. For each subscription the server sends `subscribed`. When every table of the query is live, it sends `subscribed` with `live` and then the snapshot chunks. Otherwise it sends `pending` and requests a fill.
4. When a fill completes, the server materializes the pending subscriptions and sends `subscribed live` plus a snapshot to every session that references them.
5. Each applied CDC transaction produces one `delta` per interested session. The client applies it and sends `ack`.
6. The client sends a heartbeat every 10 seconds. With `heartbeat: "static-v1"`, it sends the literal text `orbit:ping:v1`; Cloudflare auto-responds with `orbit:pong:v1` without waking the DO. Otherwise it uses JSON `ping`/`pong`. Only one heartbeat is outstanding; a missed reply disconnects. A native pong proves transport liveness, not current authorization. Expiry remains enforced by the alarm and the data path.
7. With `sessionRenewal`, the client obtains fresh credentials before expiry and renews the existing session over HTTP. Failure falls back to reconnect; no durable mutation or client cursor is dropped. See [auth.md](auth.md).

## Internal protocol

The internal protocol is defined in Rust in `crates/orbit-protocol/src/cdc.rs` and `fill.rs`. `INTERNAL_PROTOCOL_VERSION` is `1`. See [type-safety.md](type-safety.md) for how the TypeScript types are generated.

### CDC delivery

`CdcBatch` is the delivery unit from the distributor to one Durable Object.

| Field              | Meaning                                             |
| ------------------ | --------------------------------------------------- |
| `protocol_version` | Must equal `1`.                                     |
| `schema_hash`      | Must equal the Durable Object's schema hash.        |
| `stream_epoch`     | Incremented when an operator resets the checkpoint. |
| `partition`        | Partition key as a string.                          |
| `transactions`     | `PartitionTransaction[]`, contiguous in `seq`.      |
| `delivery_id`      | For log correlation only.                           |

A `PartitionTransaction` has `seq`, `keyspace`, `shard`, `gtid`, `position`, `commit_timestamp`, `changes` (`RowChange[]`), and `trace`. A `RowChange` has `table`, `op` (`insert`, `update`, `delete`), `key`, and the `before` and `after` images.

`CdcBatchAck` is the response. It is `applied` with `applied_seq`, `duplicates`, and `apply_ms`, or `rejected` with a `RejectReason`. The reasons are:

- `protocol_version_mismatch` with `expected` and `got`
- `schema_mismatch` with `do_schema_hash` and `got`
- `sequence_gap` with `applied_seq` and `first_seq`
- `sequence_conflict` with `seq`, `applied_gtid`, and `got_gtid`
- `stale_epoch` with `do_epoch` and `got`
- `invalid_row` with `table`, `seq`, and `message`
- `wrong_partition` with `do_partition` and `got`
- `internal` with `code` and `message`

The distributor treats `wrong_partition`, `stale_epoch`, `invalid_row`, and `sequence_conflict` as permanent. It quarantines the batch at once. It retries the other reasons with backoff, up to `max_reject_attempts` (default 20), then quarantines. See [failure-model.md](failure-model.md) and [operations-runbook.md](operations-runbook.md).

### Fills

A Durable Object that lacks a scope registers a `FillRequest`: `fill_id`, `schema_hash`, `partition`, `table`, and `requested_at_ms`. The Rust server long-polls for requests and receives a `FillPollResponse` with `requests`. It uploads the result as NDJSON lines of `FillChunk`:

- `{ type: "rows", fill_id, rows }`
- `{ type: "done", fill_id, result }`

`FillResult` is `completed` with `position`, `keyspace`, `shard`, `row_count`, and `duration_ms`, or `failed` with an `EngineError`. See [bootstrap.md](bootstrap.md).

### HTTP routes

The Worker router in `packages/sync-do/src/worker.ts` mounts these routes under the prefix (default `/orbit`). Every `/internal/*` route requires `Authorization: Bearer <internalSecret>`.

| Route                             | Purpose                                                                 |
| --------------------------------- | ----------------------------------------------------------------------- |
| `GET /ws?partition=P`             | Client WebSocket. Authorized, then forwarded to the Durable Object.     |
| `POST /session/renew?partition=P` | Fresh bearer authorization for an existing session; JSON `{sessionId}`. |
| `POST /internal/cdc/:partition`   | Distributor delivery of a `CdcBatch`. Returns a `CdcBatchAck`.          |
| `GET /internal/fills/next?wait=S` | Fill worker long poll. `S` defaults to 20 seconds, capped at 25.        |
| `POST /internal/fills/:fillId`    | Fill upload as NDJSON. The partition is parsed from the fill id.        |
| `GET /internal/status/:partition` | Durable Object status.                                                  |
| `POST /internal/reset/:partition` | Operator reset. Closes sockets with 4500 and deletes all storage.       |
| `GET /internal/registry/status`   | Number of queued fills.                                                 |

## Versioning fields

Three values guard compatibility:

- `protocol_version` in `CdcBatch` and `protocolVersion` in `hello` name the wire protocol. A mismatch is rejected explicitly on both sides.
- `format_version` in the sync schema artifact names the artifact layout. `SYNC_SCHEMA_FORMAT_VERSION` is `1`. Rust validation rejects another value.
- `schema_hash` identifies the compiled sync schema. The distributor, the Durable Object, and the browser compare it. See [schema-evolution.md](schema-evolution.md).
