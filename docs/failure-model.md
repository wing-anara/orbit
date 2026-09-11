# Failure model

This document lists each component, its failures, and the response in code. No component swallows an error. No component skips an event to make progress.

## Delivery guarantees

- Subscriber to distributor: at least once. A reconnect resumes from the last emitted position. The consumer may see a transaction twice.
- Distributor to Durable Object: at least once on the wire, exactly once in effect. The Durable Object dedupes by `seq`. See [consistency-invariants.md](consistency-invariants.md).
- Durable Object to client: at least once for snapshots. A reconnect always produces a fresh snapshot per subscription. Deltas are sent once per connection. A lost delta is repaired by the next snapshot on reconnect.
- Fill requests: at least once. The registry hands a request out again after a lease expires. The Durable Object ignores results for fill ids it no longer expects.

## Vitess and the subscriber

Errors are classified in `crates/orbit-vstream/src/error.rs`.

| Failure                                                                                                                                                                        | Classification      | Response                                                                           |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------- | ---------------------------------------------------------------------------------- |
| Connection lost, stream ended, no event for `stall_timeout` (30 s), gRPC `Unavailable`, `DeadlineExceeded`, `Aborted`, `Cancelled`, `ResourceExhausted`, `Unknown`, `Internal` | retryable           | Reconnect from the last emitted position. Backoff from 250 ms to 30 s with jitter. |
| Checkpoint not contained in the server position                                                                                                                                | `InvalidCheckpoint` | Fatal. The subscriber stops before it streams.                                     |
| Heartbeats arrive but the position does not move for `progress_timeout` (120 s) while the source's current position advanced                                                   | `NoProgress`        | Retryable. Reconnect; fatal after `max_consecutive_failures`.                      |
| "purged required binary logs" or "purged required gtids" in the status message                                                                                                 | `PurgedBinlog`      | Fatal.                                                                             |
| "GTIDSet Mismatch" or "could not decode position"                                                                                                                              | `InvalidCheckpoint` | Fatal.                                                                             |
| "persistent error in vstream" or "failed to build table replication plan"                                                                                                      | `PoisonPosition`    | Fatal.                                                                             |
| gRPC `Unauthenticated`                                                                                                                                                         | `Unauthenticated`   | Fatal.                                                                             |
| A synced column missing or retyped in a `FIELD` event, enum values changed                                                                                                     | `SchemaMismatch`    | Fatal.                                                                             |
| A cell that does not decode to its kind                                                                                                                                        | `Normalization`     | Fatal.                                                                             |
| A step that adds zero or several gtids                                                                                                                                         | `Step`              | Fatal, unless `--allow-merged-transactions` accepts merged steps.                  |
| Reshard `Journal` event, statement-based events                                                                                                                                | `Unsupported`       | Fatal.                                                                             |

`is_retryable` decides between the two groups. The failure budget `max_consecutive_failures` is 20 in `orbit-server` (`MAX_CONSECUTIVE_FAILURES`). A connection that moved the position before it failed resets the budget and the backoff.

A gRPC status whose message describes the connection is retryable whatever its code. PlanetScale's vtgate closes an HTTP/2 connection with `GOAWAY` from time to time; the client then reports `InvalidArgument: protocol error: incomplete envelope: http2: server sent GOAWAY ...`. The subscriber matches `GOAWAY`, `incomplete envelope`, `protocol error`, `connection reset`, `broken pipe`, `connection closed` and `transport error` (`is_connection_failure` in `crates/orbit-vstream/src/error.rs`) and reconnects from the checkpoint. Before this rule the server exited on such a close.

`NoProgress` exists because vtgate does not surface every tablet error. When the tablet reports "purged required binary logs", vtgate retries the tablet stream internally and keeps sending heartbeats. The client sees a live stream that never moves. The subscriber compares its position with `current_position` after `progress_timeout` without progress. An idle source (equal positions) is fine. A source that moved on is reported as `NoProgress`. The disruptive test `purged_binlogs_are_reported_explicitly` covers this path.

A fatal error ends `orbit_vstream::run`. `orbit-server run` then exits with that error. The operator resolves the cause. For a purged or invalid checkpoint the fix is:

```bash
orbit-server checkpoint reset --to current
```

See [checkpoints.md](checkpoints.md).

## The distributor and delivery

`deliver_with_retry` in `crates/orbit-distributor/src/distributor.rs` handles every outcome of one batch.

Transport failures (`DeliveryError::Transport`, HTTP 408, 429 and 5xx) are transient. The distributor retries without limit. Backoff starts at 100 ms and doubles up to 30 s with jitter. Only one batch per partition is in flight, so order is kept.

Other HTTP errors (for example 401) and an unparseable ack are not transient. Each one counts as a rejection. After `max_reject_attempts` (20) the batch is quarantined with reason `delivery_failed`.

An ack of `applied` with `applied_seq` below the last `seq` of the batch is a protocol violation. The batch is quarantined with reason `ack_below_batch`.

## Durable Object rejections

`RejectReason` in `crates/orbit-protocol/src/cdc.rs` has eight variants. The distributor treats them in two groups.

Permanent. The batch is quarantined at once:

- `wrong_partition`: the batch reached the wrong object.
- `stale_epoch`: the object is on a newer epoch.
- `invalid_row`: a row failed validation against the sync schema.
- `sequence_conflict`: a duplicate `seq` carried a different gtid.

Retried. The batch is retried with backoff. After 20 rejections it is quarantined:

- `protocol_version_mismatch`: a deploy is in progress.
- `schema_mismatch`: the Durable Object runs another schema.
- `sequence_gap`: the object is behind, for example after a reset.
- `internal`: any other failure, including `apply_failed` and `unstorable_value`.

## Schema mismatch

A schema change can appear at four places:

- Live table vs sync schema in the subscriber: fatal `SchemaMismatch`. Recompile the schema and redeploy.
- State store vs running schema: `StateError::SchemaChanged` at startup. The server does not start.
- Batch vs Durable Object: `schema_mismatch` rejection, retried up to 20 times. A redeploy of the Worker heals it. On its next start the Durable Object migrates its tables with `planMigration` and drops every scope. Subscriptions stay registered and fill again.
- Client vs Durable Object: `compatibility` allows a client that knows a subset of tables and columns. Any other difference closes the socket with code 4409. The client does not reconnect.

## Poison batches and quarantine

A batch that is quarantined goes to the `quarantine` table in the state SQLite file, with every transaction verbatim. Everything still queued for that partition is quarantined too, in order. The partition is marked quarantined. Later transactions for it go straight to quarantine. The checkpoint moves on.

If the quarantine record cannot be written, the process aborts. A lost record would lose data.

`invalid_row` needs care. `applyTransaction` applies changes to the cache one by one. A failure on a later change returns a rejection. The transaction callback returns normally, so the changes applied before the invalid row stay in the cache. `applied_seq` does not advance. The cache of that partition is then inconsistent until an operator resets the object.

Operator commands:

```bash
orbit-server quarantine list
orbit-server quarantine replay --partition <p>
orbit-server quarantine drop --partition <p>
```

`replay` sends the quarantined transactions as one batch with the current epoch. On `applied` it clears the records. The partition leaves quarantine after a restart of the server. `drop` discards the records. The partition then stays stale until the Durable Object is reset and refilled.

## Rust server restart

On start, the distributor loads the checkpoint, the counters and the quarantined partitions. The subscriber validates the checkpoint and resumes from it. Transactions after the checkpoint replay with the same `seq` values. The Durable Objects count them as duplicates. Work that was in flight at the crash replays in full, because the checkpoint never includes unacknowledged items.

A clean shutdown on Ctrl-C flushes the checkpoint a last time.

## Durable Object eviction and reset

All engine state lives in the Durable Object SQLite database. Sessions and client subscriptions live there too. WebSockets use the Hibernation API. After an eviction, the constructor reads the partition from `meta` and binds the engine. The next batch, message or alarm continues where the object stopped. The test `survives eviction` in `packages/sync-do/test/workers/sync-do.test.ts` covers this.

When a socket does close, the client reconnects and sends `hello` again with its subscriptions. The object answers with fresh snapshots.

`POST /orbit/internal/reset/:partition` deletes all storage. The next batch then arrives at `applied_seq = 0`. The object accepts the first batch at any `seq` in that state, because the gap check only runs when `applied_seq` is not 0.

## Client disconnect

The connection loop in `packages/client/src/connection.ts` reconnects with backoff from 500 ms to 30 s and 30 percent jitter. A `ping` goes out every 10 s. A missing `pong` for 5 s ends the attempt so the loop takes over, which bounds the detection of a dead link at 15 s even when the browser fires no `offline` event. A browser `offline` event ends the attempt as well. In both cases the client does not wait for the socket's `close` event: a browser that is offline starts the closing handshake and never finishes it, so the state would stay `open` for as long as the network is down. The attempt resolves at once, the socket is closed best-effort, and its later events are ignored. An `online` event ends the backoff wait at once.

Close codes 4400, 4401, 4403 and 4409 are fatal. The client stops and reports `fatalError` in its status. The application must change the token, the schema or the partition.

On every connect the client sends `hello` with its cursor and all subscriptions. Live queries become `stale` until the snapshot arrives. Resume is always a fresh snapshot per subscription. The Durable Object does not read the client cursor.

The local database survives a reload. Persisted subscriptions render as `stale` before the connection is open.

## Fill worker failures

- The fill worker crashes: the registry lease expires after 180 s. The next poll hands the request out again.
- The fill times out (`FILL_TIMEOUT_SECS`, default 120 s), or Vitess fails it: the worker uploads `FillResult::Failed`. The Durable Object drops the scope and reports `fill_failed` to pending subscriptions. The next subscribe requests a fresh fill.
- The upload fails: the worker retries up to 8 times. A 404 or 409 means the object no longer expects the fill; the worker stops and deletes the fill from the registry. The object deletes it too.
- Nothing arrives at all: the Durable Object alarm fires after `fillTimeoutMs` (default 120 s). `retryFill` issues a new fill id and keeps the held changes. After `maxFillAttempts` (default 5) the alarm fails the fill with a `timeout` error.
- The request carries another schema hash: the worker answers `SchemaMismatch` without a fill.
- The fill spans several shards: the worker answers an `internal` error. Multi-shard partitions are not supported.

See [bootstrap.md](bootstrap.md).

## Local test source

`vttestserver` re-initialises its data directory and its MySQL server UUID on every start. A restart is a new source with a new GTID history, not an outage. The persisted checkpoint is then invalid. Run `orbit-server checkpoint reset` before the next start. The disruptive tests in `crates/orbit-vstream/tests/disruptive.rs` use `docker pause` instead of `docker restart` for this reason.
