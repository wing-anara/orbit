# Bootstrap

## Scopes

A Durable Object does not load its partition in advance. It loads one table at a time, on demand. The unit is a scope: one table within one partition. Because one object serves one partition, the `scopes` table is keyed by table name.

A scope is in one of three states (`ScopeState` in `packages/sync-do/src/core/engine.ts`):

- `absent`: no row in `scopes`. Changes for the table are ignored.
- `filling`: a fill is in flight. The row holds the `fill_id` and `hold_from_seq`. Changes for the table are held.
- `live`: the cache is complete for the table. Changes are applied.

The engine uses no sleeps and no clock-based assumptions to reach `live`. State changes happen only on fill upload, on batch application and on the retry alarm.

## Starting a fill

`subscribe` plans the query and calls `ensureScopes` with every table the query touches, includes included. For each table that is not `live`:

- The table is added to the `pending` list.
- If the scope is `absent`, a new fill id `${partition}:${uuid}` is created. The scope row is written with state `filling` and `hold_from_seq = applied_seq`. A row in `fills` records the request with `attempts = 1`. A `FillRequest` is returned.

When any table is pending, the subscription stays `pending` and the client receives `subscribed` with status `pending`. The Durable Object turns each `fill_needed` event into an `enqueueFill` call.

`enqueueFill` posts the request to the fill registry. It then sets an alarm at `now + fillTimeoutMs` (default 120 s) when no alarm is set.

## The registry and the fill worker

The registry (`packages/sync-do/src/registry-do.ts`) stores requests in a SQLite table with `INSERT OR IGNORE`, so an enqueue is idempotent per fill id.

The Rust fill worker (`crates/orbit-server/src/fill_worker.rs`) reserves execution slots, then calls `GET /orbit/internal/fills/next?wait=25&ack=1&limit=N`, with at most 16 requests per batch. The registry answers immediately when work is ready, or waits up to 25 seconds for an enqueue or lease expiry. Receipt-aware polls get a five-second provisional lease and an `x-orbit-fill-lease` response header. After decoding the complete response, the engine posts that header to `/orbit/internal/fills/claim` to extend the batch lease to 180 seconds. A lost poll response therefore delays redelivery by five seconds rather than three minutes. A stale receipt cannot extend work offered to another poller. Receipt lookup is indexed.

The engine still executes received work if the claim response is lost: completion is idempotent, and discarding received work would recreate the lost-response stall. The DO buffers each upload before synchronously applying its rows and completion; subsequent uploads for the same completed fill are ignored. Completion deletes the registry row. Legacy engines that omit `ack=1` retain the original 180-second lease; new engines also accept legacy responses without a receipt header. This permits either deployment order.

The worker checks the `schema_hash` of the request. A mismatch produces a failed result without a fill. It then runs the fill under a per-fill timeout (`FILL_TIMEOUT_SECS`, default 120 s).

## The copy phase

`run_fill` in `crates/orbit-vstream/src/fill.rs` opens a `VStream` with an empty starting position. An empty position tells Vitess to run the copy phase. The request has one filter rule:

```sql
select * from `Chatbot` where `organizationId` = 'org_42'
```

`tables_to_copy` names the one table. The assembler runs in `Lenient` mode because copy-phase positions repeat and start unknown.

The worker applies every row image it receives to an in-memory map keyed by primary key. Copy rows and interleaved catch-up transactions both go through the same map. Inserts and updates set the row. Deletes remove it. Every transaction or position event updates the position per shard.

The stream ends with a `COPY_COMPLETED` event without a shard. The map is then the exact set of rows for the partition at the last observed position of each shard. Vitess guarantees this: the copy phase interleaves binlog catch-up so that copied rows are consistent up to each emitted `VGTID`.

A fill that observed more than one shard is failed with an `internal` error. The Durable Object stores one position per scope, so multi-shard partitions are not supported.

## Fills of derived tables

A table with `partition_parent` (see [partitioning.md](partitioning.md#derived-partitions)) has no partition column. The copy phase cannot filter it, because the filter would need a subquery on the parent. `run_derived_fill` in `crates/orbit-vstream/src/fill.rs` reads the rows through vtgate `Execute` instead:

1. Read the current stream position. This is the fill position.
2. Read the parent keys of the partition: `SELECT <pk> FROM <parent> WHERE <partition_column> = <value>`.
3. Read the child rows in chunks of at most 500 keys: `SELECT <synced columns> FROM <child> WHERE <partition_column> IN (...)`.
4. Upload the rows and a `done` chunk with the position from step 1.

The worker projects every row with the same code the CDC path uses (`TableProjection` in `crates/orbit-vstream/src/normalize.rs`). `Execute` results carry the Vitess type but no MySQL column type, so `query_fields` fills the column type from the sync schema and checks the Vitess type against the schema kind. A `bool` column arrives as `INT8`, the type of `tinyint(1)`. The row images are identical to the images of a copy-phase fill and of a CDC change. The live test `derived_fill_returns_exact_child_rows_with_cdc_images_and_position` compares a fill image with a CDC image of the same row.

The position is taken before the selects. This is correct for the same reason the copy phase is correct. Every select runs after the position, so the rows reflect every change up to the position, and possibly some later changes. `completeFill` skips every held change whose gtid is in the position; the select reflects those. It applies every held change after the position; a change the select already reflects is applied again with the same result, because a change carries a full row image. A row deleted after the position is absent from the select, and the later delete is a no-op.

The parent keys come from the source table, not from the parent index of the distributor. The persisted index holds only the parents of checkpointed transactions, so it can lag behind the fill position. A parent inserted in that window would be missing from the key set, and the Durable Object would skip the changes of its children as already in the fill. The source table is always at or after the fill position.

A partition without parent rows uploads only the `done` chunk.

## Upload

The worker posts the result to `POST /orbit/internal/fills/:fillId` as NDJSON. Rows travel in `FillChunk::Rows` lines of 500 rows. A final `FillChunk::Done` line carries `FillResult::Completed` with `position`, `keyspace`, `shard`, `row_count` and `duration_ms`, or `FillResult::Failed` with an `EngineError`.

The Worker extracts the partition from the fill id and forwards the body to the object. `acceptFill` in `do.ts` processes the lines in order:

- A `rows` line calls `applyFillRows`. Each row is decoded with the strict row codec of the table. A row that fails returns status 422 and fails the whole fill.
- A `done` line calls `completeFill`. The object then deletes the request from the registry.
- A line for another fill id, or an invalid line, returns status 400.
- An upload when no scope is filling, or for an inactive fill id, returns status 409.

The worker retries a failed upload up to 8 times. It stops on 404 or 409, because the object no longer expects the fill. Both sides then delete the fill from the registry, so an expired lease cannot hand out the same fill again.

## Hold and skip

While a scope is `filling`, `applyTransaction` writes every change for that table into `held`:

```sql
INSERT INTO held (seq, ord, tbl, gtid, change) VALUES (?, ?, ?, ?, ?)
```

`ord` is the index of the change inside its transaction. `applied_seq` keeps advancing for the batch as normal. Changes for other live tables in the same transaction are applied at once.

`completeFill` with a `completed` result does this in one transaction:

1. Parse the fill position into a GTID set.
2. Read the held changes for the table, ordered by `seq, ord`.
3. For each change: if its gtid is contained in the fill position, skip it. The fill rows already reflect it. Otherwise apply it to the cache.
4. Delete the held changes for the table.
5. Set the scope to `live` with `fill_position` and clear `fill_id`.
6. Delete the `fills` row and record `fill_stats:<table>` in `meta`.
7. Materialize every pending subscription whose tables are now all live.

The rule holds in both directions. A held change newer than the fill position is applied over the snapshot. A held change older than the fill position is skipped. A stale fill is therefore impossible by construction. The engine test `SyncEngine: bootstrap race (hold and skip)` covers both cases.

An empty fill position (`""`) parses to an empty GTID set. Nothing is contained in it, so every held change is applied.

## Completion and materialization

`materialize` evaluates the query over the cache, replaces the membership rows, marks the subscription `live`, and returns a `snapshot` event with `cursor = applied_seq`. The Durable Object sends `subscribed` with status `live` and then the snapshot to every session that references the subscription. Large snapshots go out in chunks of 500 rows.

## Failure and retry

`completeFill` with a `failed` result drops the table rows, the scope row, the held changes for the table and the `fills` row. It emits `subscription_failed` with code `fill_failed` for every subscription on the table. The scope is `absent` again. The next `subscribe` requests a fresh fill.

The alarm handles a fill that never completes. When it fires, it looks at every outstanding fill:

- A fill younger than `fillTimeoutMs` stays pending.
- A fill whose `attempts` reached `maxFillAttempts` (default 5) is failed with a `timeout` error.
- Any other fill is retried with `retryFill`.

`retryFill` creates a new fill id, deletes the `fills` row of the old id and inserts a new one with `attempts + 1`. It deletes the rows the abandoned fill uploaded, because the new fill is a fresh snapshot. It resets `hold_from_seq` to the current `applied_seq`. It keeps the held changes, because they are still needed after the new fill. The alarm then enqueues the new request and re-arms itself while fills are pending.

The workers test `fill failures are reported and retried by the alarm, then give up` covers this path.

## Resets

Two events drop every scope at once (`resetScopes`):

- A batch with a higher `stream_epoch` than the stored epoch. See [checkpoints.md](checkpoints.md).
- A start of the Durable Object with a stored schema hash that differs from the running one.

Both events delete all cached rows, scopes, held changes, memberships and fill requests. Subscriptions stay registered with `live = 0`. The object marks every client subscription `pending` and calls `subscribe` again for each one, which starts new fills.

## Related documents

- [consistency-invariants.md](consistency-invariants.md): invariant 7.
- [failure-model.md](failure-model.md): fill worker failures.
- [placement.md](placement.md): the fill registry and fill id routing.
