# Operations runbook

This document covers daily operation of an Orbit deployment: commands, configuration, observability, and common incidents. See [failure-model.md](failure-model.md) for the error classes and [checkpoints.md](checkpoints.md) for positions and epochs.

## Processes

A deployment has three parts:

- The Worker with the Sync Durable Objects and the fill registry (`wrangler deploy`).
- One `orbit-server run` process. It runs the subscriber, the distributor, the fill worker, and the metrics endpoint.
- The Vitess keyspace.

Run the application Worker locally with its dev server (`wrangler dev` or the application's Vite dev server). Deploy it with `wrangler deploy`. Run the engine with the binary:

```bash
cargo run -p orbit-server -- run --schema orbit.schema.json --state data/orbit-state.sqlite \
  --worker-url https://app.example.com/orbit --metrics-addr 127.0.0.1:9464
```

The process stops on `Ctrl-C` and on a fatal subscriber error. Restart it under a supervisor. It resumes from the persisted checkpoint. The repository's `Dockerfile` packages the process for a container host; [deployment-railway.md](deployment-railway.md) describes the Railway service (one replica, a volume for the state, health check on `/metrics`).

## Environment variables

All flags of `orbit-server run` accept environment variables (`crates/orbit-server/src/config.rs`):

| Variable                             | Default                   | Meaning                                      |
| ------------------------------------ | ------------------------- | -------------------------------------------- |
| `VITESS_GRPC_URI`                    | required                  | vtgate gRPC endpoint.                        |
| `VITESS_USERNAME`, `VITESS_PASSWORD` | none                      | Basic auth.                                  |
| `VITESS_CELLS`                       | none                      | Cells for tablet selection.                  |
| `SYNC_SCHEMA_PATH`                   | fetched from the Worker   | Local artifact; else `GET /internal/schema`. |
| `STATE_PATH`                         | `data/orbit-state.sqlite` | Checkpoint, counters, quarantine.            |
| `WORKER_URL`                         | required                  | Base URL of the router.                      |
| `WORKER_SECRET`                      | required                  | Equals `ORBIT_INTERNAL_SECRET`.              |
| `METRICS_ADDR`                       | `127.0.0.1:9464`          | Prometheus listen address.                   |
| `FILL_CONCURRENCY`                   | `4`                       | Concurrent demand fills.                     |
| `FILL_TIMEOUT_SECS`                  | `120`                     | Per-fill timeout.                            |
| `DELIVERY_TIMEOUT_SECS`              | `30`                      | HTTP timeout per batch delivery.             |
| `ALLOW_MERGED_TRANSACTIONS`          | `false`                   | Accept stream steps that merge transactions. |
| `STALL_TIMEOUT_SECS`                 | `30`                      | Fail the stream with no events at all.       |
| `PROGRESS_TIMEOUT_SECS`              | `120`                     | Fail a stream whose position never moves.    |
| `MAX_CONSECUTIVE_FAILURES`           | `20`                      | Retryable failures before the engine exits.  |
| `DISABLE_FILLS`                      | `false`                   | Do not run the fill worker.                  |
| `MAX_BATCH_TRANSACTIONS`             | `200`                     | Transactions per batch.                      |
| `MAX_INFLIGHT_TRANSACTIONS`          | `2000`                    | Routed but not checkpointed transactions.    |

`VITESS_KEYSPACE` is used by `schema introspect`. `RUST_LOG` sets the log filter (default `info,h2=warn,hyper=warn`). `LOG_FORMAT=json` switches to JSON logs.

The Worker needs the secrets `ORBIT_TOKEN_SECRET` and `ORBIT_INTERNAL_SECRET`. The application adds its own secrets, for example a session secret and the database credentials of its push endpoint.

## CLI reference

```bash
orbit-server schema introspect --keyspace <ks> --out <path> [--table <name>]...
orbit-server schema validate --schema <path>
orbit-server checkpoint show --state <path> --schema <path>
orbit-server checkpoint reset --state <path> --schema <path> [--to current|<keyspace>/<shard>@<position>]
orbit-server quarantine list --state <path> --schema <path>
orbit-server quarantine replay --state <path> --schema <path> --worker-url <url> --worker-secret <s> --partition <p>
orbit-server quarantine drop --state <path> --schema <path> --partition <p>
```

`checkpoint show` prints the epoch, the positions per shard, the number of partition counters, and the quarantined partitions.

`checkpoint reset` writes a new checkpoint and moves the epoch forward (to the current unix time). `--to current` (the default) reads the server's current position and skips history. `--to <pos>` accepts `keyspace/shard@MySQL56/...`, with `|` between shards. Every Durable Object drops its cached scopes when it sees the new epoch and refills on demand. Run it while the server is stopped.

`quarantine replay` re-delivers the stored transactions of one partition and clears them on success. Restart the server afterwards, because the running process still marks the partition as quarantined. `quarantine drop` discards them; the partition then stays stale until you reset its Durable Object.

## Metrics

The Prometheus endpoint listens on `METRICS_ADDR`. Metric names in the crates:

Subscriber (`orbit-vstream`):

- `orbit_vstream_connected` (gauge)
- `orbit_vstream_lag_seconds` (gauge)
- `orbit_vstream_responses_total`, `orbit_vstream_events_total{type}`, `orbit_vstream_transactions_total`, `orbit_vstream_row_changes_total`, `orbit_vstream_reconnects_total`

Distributor (`orbit-distributor`):

- `orbit_distributor_inflight_transactions`, `orbit_distributor_quarantined_partitions` (gauges)
- `orbit_distributor_routed_rows_total`, `orbit_distributor_unpartitioned_rows_total`, `orbit_distributor_partition_moves_total`
- `orbit_distributor_unresolved_parent_total`: changes of a derived table (`partition_parent`) whose parent is NULL or not in the parent index. Such a change is skipped. See [partitioning.md](partitioning.md#derived-partitions).
- `orbit_distributor_deliveries_total{result=applied|rejected|failed}`, `orbit_distributor_delivery_retries_total`, `orbit_distributor_duplicate_transactions_total`
- `orbit_distributor_quarantined_transactions_total`, `orbit_distributor_checkpoints_total`, `orbit_distributor_backpressure_waits_total`
- `orbit_distributor_delivery_seconds`, `orbit_distributor_do_apply_ms` (histograms)

Fills (`orbit-vstream`, `orbit-server`):

- `orbit_fill_requests_total`, `orbit_fill_completed_total`, `orbit_fill_failed_total`, `orbit_fill_poll_errors_total`, `orbit_fill_upload_rejected_total`, `orbit_fill_upload_failed_total`
- `orbit_fill_duration_seconds` (histogram)

Alert on `orbit_vstream_connected == 0`, on growth of `orbit_vstream_lag_seconds`, and on `orbit_distributor_quarantined_partitions > 0`.

## Logs

The Rust server logs a `status` line every 10 seconds with the checkpoint, in-flight count, queued count, and quarantined count. Fills log `fill started`, `fill completed table=... rows=... ms=...`, and `fill uploaded`.

At start, before the subscriber opens the stream, the server bootstraps the parent index of every table named as `partition_parent`. It logs one line per parent table: `parent index bootstrapped table=... rows=... ms=... position=...`. A parent table that is already marked ready in the state file is skipped and logs nothing. A change of a derived table whose parent cannot be resolved logs `parent partition unresolved; change skipped` at warn level with `table`, `key`, `parent` and `parent_key`.

The Durable Object writes one JSON line per event to `console.log` (`packages/sync-do/src/log.ts`). Read them with `wrangler tail` or Workers Logs:

- `orbit.cdc.batch`: `partition`, `status`, `applied_seq`, `duplicates`, `apply_ms`, `transactions`, `first_gtid`, `sessions`, `total_ms`. A rejected batch carries `reason`.
- `orbit.fill.completed`: `partition`, `fill_id`, `status`, `rows`, `position`, `duration_ms`, or `error`.
- `orbit.subscription.subscribed`: `partition`, `status`, `engine_ms`, `rows`, `members`, `based_on`. One line per subscribe: how long the engine took to register and materialize the query, and what the first snapshot carries (`based_on` when the client extended a window in place).
- `orbit.subscriptions.swept`: `partition`, `count`. The alarm dropped subscriptions that no session held for longer than `subscriptionGraceMs` (default one hour, `makeSyncDurableObject` config). Until then an orphaned subscription stays materialized so a reload or a redeploy does not rewrite its membership.

Row contents never appear in logs.

`apply_ms` and `total_ms` are not a CPU measurement in production. The Workers runtime freezes `Date.now()` during synchronous work, so a batch that costs seconds of CPU still reports `0`. Read the `cpuTime` and `wallTime` fields of the `durableObject` event in `wrangler tail --format json` instead. A healthy batch of one transaction costs about 50 to 150 ms of CPU on a partition with 2,000 cached documents and a dozen subscriptions.

## Internal status routes

All routes under `<prefix>/internal/` need `Authorization: Bearer <ORBIT_INTERNAL_SECRET>` (`packages/sync-do/src/worker.ts`):

- `GET /internal/status/{partition}`: engine status of one Durable Object: `appliedSeq`, `epoch`, `scopes` with row counts, `subscriptions`, `heldChanges`, `sessions`, `sockets`, `outstandingFills`.
- `GET /internal/registry/status`: number of queued fill requests.
- `POST /internal/reset/{partition}`: closes every socket, deletes all storage of the Durable Object, and unbinds it. Clients reconnect and refill.

Example:

```bash
curl -H "Authorization: Bearer $ORBIT_INTERNAL_SECRET" https://app.example.com/orbit/internal/status/org_acme
```

## Common incidents

### Purged binlogs or invalid checkpoint

Symptom: the server exits with `PurgedBinlog`, `InvalidCheckpoint`, or `PoisonPosition`. The subscriber validates the checkpoint before it streams and never skips history by itself.

Action:

1. Run `orbit-server checkpoint reset --to current`.
2. Restart the server. The epoch increases, and every Durable Object refills on demand.

The same applies after a `vttestserver` restart, because it creates a new GTID history.

### Schema hash mismatch

Symptom: the server refuses to start with `state was created for schema ... but the running schema is ...`, or batches are rejected with `schema_mismatch` and retried.

Action:

1. Deploy the Worker with the same artifact that the server loads. The distributor retries `schema_mismatch` rejections until the Durable Objects run the new hash.
2. If the state file was created for the old hash, start the server with a new `STATE_PATH`. There is no CLI command for `StateStore::migrate_schema` yet. A new state file starts at an epoch equal to the current unix time, which is above any epoch the Durable Objects stored, so they reset their caches and refill on demand.

### Quarantined partition

Symptom: `orbit_distributor_quarantined_partitions > 0`; the status line shows `quarantined=N`. The distributor quarantines a partition after a permanent rejection or after 20 consecutive rejections of one batch, and keeps every later transaction for it on disk. Other partitions continue.

Action:

1. Run `quarantine list` to see the reason and the sequence numbers.
2. Fix the cause (for example redeploy the Worker).
3. Run `quarantine replay --partition <p>`, then restart the server.
4. If replay is impossible, run `quarantine drop`, then `POST /internal/reset/{partition}`. The Durable Object refills from the source.

### Stuck fill

Symptom: a subscription stays `pending`; `GET /internal/status/{partition}` shows a scope in state `filling` and `outstandingFills > 0`.

Checks:

- The fill worker must poll: look for `fill poll failed` and `orbit_fill_poll_errors_total`.
- `DISABLE_FILLS` must be false on the process that serves fills.
- A fill that spans several shards fails with `multi-shard partitions are not supported`.

The Durable Object retries a fill after 120 seconds and gives up after 5 attempts, then reports `fill_failed` to the clients. `GET /internal/registry/status` shows the queue size.

### Unresolved parents

Symptom: `orbit_distributor_unresolved_parent_total` grows; the log shows `parent partition unresolved; change skipped`.

The child row references a parent key that is not in the parent index. Check the `parent_key` in the log line against the source. A parent row that exists in the source but not in the index means the index is incomplete; stop the server, delete the row of that parent table from `parent_index_ready` in the state file, and restart. The bootstrap runs again for that table. A parent that does not exist in the source is an orphan row; the engine does not sync it.

### Durable Object reset

Use `POST /internal/reset/{partition}` when a Durable Object holds bad data. Storage is deleted, sockets close, and the next connection refills every scope. The distributor's sequence counter for the partition keeps increasing; the empty Durable Object accepts any first sequence.

## Cloudflare rules

Each application names its own Worker in its `wrangler` configuration. All Durable Object classes and bindings are namespaced by the `app` name of the sync schema. Set secrets with `wrangler secret put`; never commit `.dev.vars`.
