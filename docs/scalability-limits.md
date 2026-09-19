# Scalability limits

This document lists the limits that shape an Orbit deployment today. Each limit names its source in the code or in the platform. See [future-scaling.md](future-scaling.md) for the options to move past them.

## One Durable Object per partition

Placement is `one_per_partition` (`crates/orbit-protocol/src/schema.rs`, `PlacementConfig`). Consequences:

- A Durable Object is single-threaded. It applies one CDC batch at a time inside one SQLite transaction and maintains every affected subscription in that transaction. Maintenance is incremental: its cost depends on the rows the batch touched, not on the partition size (see [ivm.md](ivm.md) and the table in [benchmarks.md](benchmarks.md)).
- Cloudflare states a practical range of about 500 to 1,000 simple requests per second for one object. Batch application is heavier than a simple request.
- SQLite-backed Durable Objects hold up to 10 GB per object. The cache stores every synced column of every row of every filled table of the partition, plus `held`, `membership`, `seq_log`, and session tables.
- The WebSocket Hibernation API allows at most 32,768 connections per Durable Object. The code sets no lower limit, and each `delta` is evaluated per session, so CPU is the practical limit.

The distributor keeps every Durable Object's deliveries serial and in `seq` order. Throughput for one partition is therefore one batch at a time. Across partitions, up to `max_concurrent_deliveries` (256 by default, configurable through `--max-concurrent-deliveries` / `MAX_CONCURRENT_DELIVERIES`) deliveries run at once.

The scheduler maintains a FIFO of ready partitions instead of scanning every known partition on each wake. It dequeues a batch only after reserving a delivery slot, so bulk arrivals cannot pin thousands of prebuilt batches ahead of subsequently queued work. A retrying partition remains serial, but releases its global delivery slot during backoff. `orbit_distributor_dispatch_wait_seconds` measures subscriber receipt to actual dispatch, including queue delay. Increasing concurrency is bounded, not an unlimited throughput guarantee; source routing, Worker capacity and latency must still be measured under the intended fleet profile.

## SQLite bound parameters

Durable Object SQLite accepts 100 bound parameters per statement. The engine limits itself to `MAX_BOUND_PARAMS = 90` (`packages/sync-do/src/core/maintain.ts`) and classifies candidate rows in chunks that leave room for the predicate's parameters. The planner caps predicate parameters at `MAX_PREDICATE_PARAMS = 80`, so a compiled select plus limit and membership parameters stays under 100.

The first 300-row transaction observed in this environment hit this limit before the chunking existed. The batch was rejected nine times with `too many SQL variables`, then applied after the fix. See [benchmarks.md](benchmarks.md).

## What still grows with the partition

Per change, nothing: every maintenance step is an index probe bounded by the rows the change touched and by the fan-in of the relation values involved (for example, the documents of one folder). The remaining costs that are linear in the partition are:

- The fill of a table, once per partition and once after a reset.
- The first materialization of a query without a limit (a preload such as an `allDocuments` query): one membership row per result row, and a snapshot of every row to the client. Applications that preload a whole table into the browser pay this on purpose; a 100,000-row preload writes 100,000 membership rows once and sends 100,000 rows to each new client.
- Repairs of a limited window: when rows left a full window, or a member's sort key changed, the next candidates are found with a `LIMIT` query over the primary table in the query's order. Without an index that matches the order, SQLite sorts the matching rows.
- A `scopes_reset` (schema change, stream epoch change) re-materializes every subscription.

Cloudflare bills SQLite rows written and read. The measured profile is 3 to 5 rows written per source change plus one `seq_log` row and one `meta` row per transaction, whatever the partition size; index rows may count on top. A subscription dropped and re-created rewrites its whole membership, which is why the Durable Object keeps orphaned subscriptions for `subscriptionGraceMs` (default one hour) before it drops them.

## Planner caps

`packages/query/src/plan.ts` rejects queries above these values:

| Cap                    | Value              |
| ---------------------- | ------------------ |
| `MAX_LIMIT`            | 10,000 rows        |
| `MAX_IN_VALUES`        | 40 values per `in` |
| `MAX_PREDICATE_NODES`  | 60 nodes           |
| `MAX_PREDICATE_PARAMS` | 80 parameters      |

A query without `limit` returns every matching row of the partition. Every row that a subscription returns is stored in the browser.

## Distributor batches and backpressure

`DistributorConfig` defaults (`crates/orbit-distributor/src/distributor.rs`):

| Setting                     | Default                                               |
| --------------------------- | ----------------------------------------------------- |
| `max_batch_transactions`    | 200                                                   |
| `max_batch_bytes`           | 4 MiB (soft; one oversized transaction is sent alone) |
| `max_inflight_transactions` | 32,768                                                 |
| `max_inflight_bytes` | 128 MiB serialized work (soft, not RSS) |
| `max_concurrent_deliveries` | 256                                                    |
| `retry_backoff_min` / `max` | 100 ms / 30 s                                         |
| `max_reject_attempts`       | 20                                                    |
| `checkpoint_interval`       | 500 ms                                                |

When either the in-flight count or serialized-work byte budget reaches its cap, the distributor stops reading the subscriber channel. The channel holds 256 items (`SubscriberConfig.channel_capacity`); when it is full, the gRPC stream is not read and HTTP/2 flow control pushes back on vtgate. One slow partition can still slow the whole stream once the uncheckpointed window fills, including completed transactions behind its acknowledgment. The count budget covers 30 seconds at 833 transactions/s; the byte budget separately constrains large payloads. A single oversized transaction can exceed the byte budget, then blocks subsequent routing until checkpoint progress releases it. Subscriber/hydration buffers are separate from this accounting.

A single source transaction is one `PartitionTransaction`. It is never split. A transaction with 5,000 row changes was streamed intact in the live Vitess test (`large_transaction_arrives_intact`), but its JSON body must fit the Worker request limits and the Durable Object must apply it in one SQLite transaction.

## One subscriber process

One `orbit-server run` process opens one VStream for the whole keyspace and owns the checkpoint file. Two processes with the same state file would corrupt sequence assignment. There is no horizontal scaling of the subscriber or the distributor yet. The fill worker inside the same process runs `FILL_CONCURRENCY` (4) fills at once and polls up to 16 requests per poll (`MAX_PER_POLL` in `packages/sync-do/src/registry-do.ts`).

Reshards are not followed: the stream stops at a reshard journal (`stop_on_reshard`), and a multi-shard fill fails.

## Fills

A fill runs a VStream copy phase for one table and one partition with a 120 second timeout (`FILL_TIMEOUT_SECS`). Rows are held in memory in the Rust process until `COPY_COMPLETED`, then uploaded as NDJSON in lines of 500 rows in one HTTP request. The Durable Object applies the whole upload in one request. A partition with a very large table therefore needs memory in the Rust process and a long request in the Worker. Measured fills in this environment are small (620 rows in about 250 ms); see [benchmarks.md](benchmarks.md).

The fill registry leases each request for 180 seconds. The Durable Object retries a fill after 120 seconds and gives up after 5 attempts.

## Delta fan-out

A delta is prepared once per connected session. The membership check for the rows of a delta is one indexed query per chunk of 45 rows, shared by every session, so the cost of a delta grows with its rows and its sessions, not with the size of the memberships. Sessions whose socket is gone are pruned at the next dispatch, so an object never prepares deltas for dead connections.

## Snapshots and sessions

A snapshot is sent in chunks of `snapshotChunkRows` (default 500) and applied by the client as one unit. A snapshot of a large subscription is held in client memory until the last chunk arrives. The browser store keeps rows only while a subscription references them.

The Durable Object filters every `delta` per session and projects rows for clients with an older schema. Cost per delta grows with the number of sessions.

## Browser storage

The OPFS store uses the `opfs-sahpool` VFS. Only one browser context per origin can hold the pool. A second tab falls back to an in-memory database and re-syncs on every load. Storage quota is the browser's OPFS quota; the worker reports `quota_exceeded` when it is hit.

## Sequence log retention

The Durable Object keeps the last 2,000 `(seq, gtid)` pairs (`SEQ_LOG_RETENTION`). Duplicates older than that are skipped without gtid verification.

### Routing journal storage

The routing journal uses SQLite WAL with synchronous FULL. Do not estimate fleet capacity from tmpfs runs: disk fsync latency can dominate even when delivery queues are empty. The distributor commits up to 64 already-ready routing decisions together, preserving source order and per-transaction replay records. A group failure rolls back the entire group; delivery checkpoints can only prune covered decisions. The in-flight delivery cap still applies to each transaction.

Run `cargo run --release -p orbit-distributor --example durable_journal_capacity -- /path/on/target-disk/new-directory` to compare group sizes on the intended storage. This is a component diagnostic, not a substitute for an end-to-end fleet hold.

### Cold-fill dispatch

Fill concurrency bounds both reserved poll capacity and executing source reads. Larger budgets use up to eight polling lanes, each requesting at most 16 fills. This overlaps registry polling and lease-receipt round trips without creating an unbounded queue of leased work. A received request keeps its reserved permit until its fill finishes; responses exceeding the reserved capacity are rejected before acknowledgement.

Size `FILL_CONCURRENCY` against the source database and measure cold-fill throughput separately from steady CDC delivery. A large fleet of warm caches does not validate simultaneous cold startup.
