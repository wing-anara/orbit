# Benchmarks

This document reports what was measured in this development environment and how to reproduce it. Numbers come from the engine log (`/tmp/engine.log`), the test suites, and the code. Where a number does not exist, it is marked "not measured".

## Environment

- Source: a hosted Vitess keyspace with one shard, reached over TLS gRPC. The engine ran with `VITESS_CELLS=planetscale_operator_default`.
- Worker: the application's Vite dev server with workerd on `http://127.0.0.1:5173`.
- Engine: `cargo run -p orbit-server` in the `dev` profile (`opt-level = 1`, dependencies at `opt-level = 2`).
- Browser: Chromium under Playwright.

Latencies include the network path to the hosted database, so they are not representative of a co-located deployment.

## Demand fills

The fill log line `fill completed table=... rows=... ms=...` measures the time from opening the copy-phase stream to `COPY_COMPLETED`, inside `orbit_vstream::fill::run_fill`. Observed values from `/tmp/engine.log`:

| Table            | Rows     | Duration      |
| ---------------- | -------- | ------------- |
| `organization`   | 1        | 91 to 159 ms  |
| `member`         | 2        | 73 to 95 ms   |
| `ChatV3`         | 2 to 7   | 84 to 169 ms  |
| `DocumentEntity` | 2 to 3   | 80 to 172 ms  |
| `Chatbot`        | 14 to 15 | 85 to 147 ms  |
| `Chatbot`        | 316      | 183 ms        |
| `Chatbot`        | 618      | 245 ms        |
| `Chatbot`        | 620      | 185 to 251 ms |

Small tables fill in about 100 ms. The cost is dominated by the round trip to open the stream; 620 rows add about 100 ms. Five tables of one partition fill in parallel (`FILL_CONCURRENCY=4` plus one queued) and complete within about 400 ms of the first request.

The upload to the Durable Object and the apply time inside it were not measured separately. The Durable Object logs `orbit.fill.completed` with `duration_ms`, but those lines were not captured in this environment.

## CDC latency

The Playwright end-to-end suite lived with the example application and is not part of this repository. The numbers here are historical measurements. The suite wrote through the application backend and waited for the live query to show the change. Every assertion used the default expect timeout of 20 seconds. The tests passed, which bounds the write-to-browser latency at under 20 seconds. A precise end-to-end number is not measured: the suite did not record timings, and the browser-side `delta.applied` log (with `applyMs` and the Durable Object `appliedAt`) is only printed when `localStorage["orbit:debug"]` is `1`.

The Rust side records `orbit_distributor_delivery_seconds` and `orbit_distributor_do_apply_ms` as Prometheus histograms. They were not scraped during this run.

## A 300-row transaction

The end-to-end test `a large transaction arrives intact and ordered, and a multi-row delete propagates` inserted 300 `Chatbot` rows in one source transaction through a bulk write of the application backend. The live query had `limit(200)` and `orderBy("displayOrder")`. The test asserted 200 rows in order within 60 seconds, then deleted the folder and its 300 rows in one transaction.

The engine log shows what happened on the first run. The batch with `seq=30` was rejected nine times over about 30 seconds with:

```
apply_failed: too many SQL variables at offset 1494: SQLITE_ERROR
```

The predicate guard of that version evaluated all 300 images in one statement and exceeded the 100-parameter limit of Durable Object SQLite. The Durable Object was fixed to evaluate in chunks under `MAX_BOUND_PARAMS = 90`, the dev server reloaded, and the retried batch was applied. The checkpoint then advanced from `1-5035` to `1-5313` in one status interval. In-flight transactions peaked at 239 while the partition was blocked, below the 2,000 cap, so the stream did not pause.

The apply time of a 300-row batch is in the table below (about 160 ms on Node at any partition size). The engine core test `applies 300 inserts in one transaction while predicate subscriptions are live` (`packages/sync-do/test/core/engine.test.ts`) exercises the same shape.

## Maintenance cost per change

Measured on 2026-09-11 with the engine core on Node 22 (`node:sqlite`, in memory), the example schema and its named queries (11 subscriptions: the `allDocuments` preload with nested includes, three `documents` windows of 200 rows, one of them with an `exists` tag filter, folders, tags with links, chats, members, trash, organization). Rows written are SQLite `changes()`, so index rows are not counted. Two partition sizes, same code:

| Operation                                |            2,000 documents |           100,000 documents |
| ---------------------------------------- | -------------------------: | --------------------------: |
| Fill of the Chatbot table                |                     122 ms |                       4.7 s |
| Subscribe to all 11 queries (first time) | 108 ms, 2,831 rows written | 3.5 s, 120,604 rows written |
| Insert one document                      |             7.8 ms, 5 rows |              2.9 ms, 4 rows |
| Rename one document                      |             2.1 ms, 3 rows |              1.1 ms, 3 rows |
| Move a document to another folder        |             2.2 ms, 4 rows |              2.4 ms, 5 rows |
| Soft delete (leaves a full window)       |             1.8 ms, 5 rows |              1.3 ms, 5 rows |
| Tag a document (link insert)             |             1.5 ms, 5 rows |              1.8 ms, 5 rows |
| Rename a tag                             |             1.0 ms, 3 rows |              1.3 ms, 3 rows |
| Rename a folder                          |             1.1 ms, 3 rows |              2.3 ms, 3 rows |
| 300-row insert transaction               |           167 ms, 602 rows |            161 ms, 602 rows |
| Hard delete of a document                |             1.3 ms, 4 rows |              0.8 ms, 4 rows |

The cost per change does not depend on the partition size. The one-time costs (fill, first materialization of a preload query) are linear in it, as they must be. Before the incremental maintainer, the same insert cost 1,241 ms at 2,000 documents (a correlated include select plus a full re-evaluation) and grew linearly; on Cloudflare it cost 5.4 s of Durable Object CPU per batch.

In the cloud, on a deployed development Worker with three browser sessions on a 2,174-document partition, a write in one browser became visible in another in 909 to 930 ms over five samples (Playwright, two contexts). Most of that is the push through the hosted database's HTTP driver and the VStream pickup; the engine-to-Durable-Object delivery was 140 to 440 ms. On the hosted stack (engine on Railway in us-east, Worker and Durable Objects on Cloudflare, see `deployment-railway.md`) the same probe measured 948 to 1,307 ms.

The live Vitess test `large_transaction_arrives_intact` streams a 5,000-row transaction intact within its 60 second timeout. Its duration is not recorded.

## Distributor randomized tests

`crates/orbit-distributor/tests/distributor.rs` runs the distributor against a fake Durable Object that implements the real sequence rules. `randomized_failures_preserve_exactly_once_order` runs six seeds. Each seed:

- picks `max_batch_transactions` in 1 to 7 and `max_inflight_transactions` in 2 to 49,
- sends 60 transactions with 0 to 3 inserts each across three partitions,
- injects transport failures and lost acknowledgements at random,
- then asserts that every partition holds exactly the expected rows in order, and that the applied sequence numbers are dense and increasing.

The other tests cover ordering and checkpoints, transport failures and lost acks, restart replay with identical sequences, quarantine of a poison partition, retry on schema mismatch, and backpressure. These are correctness tests, not throughput tests. The whole file runs in seconds under `cargo test --workspace`.

Similar randomized oracles exist for incremental maintenance (`holds across random inserts, updates, deletes and query shapes` in the engine core tests) and for the client (`randomized: live query rows always equal the server's recomputation` in `packages/client/test/engine.test.ts`).

## Not measured

- Sustained CDC throughput (transactions per second) for one partition or for the whole stream.
- Durable Object apply time per batch size on Cloudflare (the Node numbers above stand in; the Workers runtime freezes `Date.now()` during synchronous work, so read `cpuTime` from `wrangler tail --format json`).
- Snapshot size versus client apply time.
- Memory of the Rust process during a large fill.
- Any number for a keyspace with more than one shard.

## Reproduce

The end-to-end suite that produced the fill and latency numbers lived with the example application and is not part of this repository. To measure the same things with an application:

1. Start the local stack: `pnpm vitess:up`, `pnpm vitess:schema`, the application Worker, and `orbit-server run` with its log redirected to `/tmp/engine.log`.
2. Write through the application and wait for the live queries to show the change.
3. Read fill timings with `grep "fill completed" /tmp/engine.log`.
4. Scrape `http://127.0.0.1:9464` during the run to get the delivery and apply histograms.
5. Set `localStorage["orbit:debug"] = "1"` in the browser to see `delta.applied` with `applyMs`.
6. Run `cargo test -p orbit-distributor` and `pnpm --filter @orbit/sync-do test:core` for the randomized suites.
