# Consistency invariants

This document lists the guarantees the engine keeps. Each invariant names the code that enforces it and the test that checks it.

## 1. Per-partition total order by seq

Every partition sees its transactions in one total order. The order is the `seq` assigned by the distributor.

Mechanism:

- `route_transaction` in `crates/orbit-distributor/src/distributor.rs` assigns a dense `seq` per partition in stream order.
- The scheduler marks a partition `busy` while a batch is in flight. Only one batch per partition is in flight at a time.
- Batches carry contiguous `seq` values in stream order.
- `applyBatch` in `packages/sync-do/src/core/engine.ts` rejects a batch with `sequence_gap` when a `seq` is beyond `applied_seq + 1`. It never skips.

Test: `SyncEngine: cursor and deduplication` in `packages/sync-do/test/core/engine.test.ts`.

## 2. Each partition transaction is applied at most once

A redelivered transaction does not change the cache a second time.

Mechanism:

- `applyBatch` treats `seq <= applied_seq` as a duplicate and counts it in the ack.
- `seq_log` keeps the last 2000 `(seq, gtid)` pairs. A duplicate `seq` with a different `gtid` is rejected with `sequence_conflict`.
- The ack reports `duplicates` so the distributor can count them.

Test: `applies in order, skips duplicates, rejects gaps and conflicting duplicates`.

## 3. applied_seq moves only with a complete transaction

`applied_seq` advances only inside the SQLite transaction that applied every row change of the source transaction and updated every affected subscription.

Mechanism:

- `applyBatch` runs inside `db.transaction`. For each transaction it calls `applyTransaction`, then sets `applied_seq`, then writes `seq_log`.
- `applyTransaction` returns a failure before `applied_seq` changes when a row is invalid.
- On the Durable Object, `transactionSync` rolls back when the callback throws. An exception during apply therefore leaves nothing behind.

Note: a rejection that is returned, not thrown, commits the work done before it. Transactions earlier in the same batch stay applied with their own `applied_seq`. See [failure-model.md](failure-model.md) for the `invalid_row` case.

Test: `rejects invalid rows without advancing the cursor`.

## 4. A client never observes a partial transaction

A client applies each source transaction as one unit.

Mechanism:

- The engine emits one `delta` event per source transaction, after the whole transaction is applied. The event carries `cursor = seq`.
- The Durable Object sends one `delta` message per session for that event.
- `LocalStore.applyDelta` turns the message into one `batch` call. The SQLite worker runs the batch inside one transaction.
- The client updates its cursor inside the same batch.

Test: `bootstraps a subscription through a demand fill, then streams deltas` in `packages/sync-do/test/workers/sync-do.test.ts`.

## 5. Snapshot cursor consistency

A snapshot is the full result of one subscription at one cursor.

Mechanism:

- `materialize` and `snapshot` evaluate the query and read `applied_seq` inside one engine transaction. No batch can interleave.
- A large snapshot travels in chunks with `complete: false`. The client buffers chunks and applies them in one batch when the last chunk arrives.
- `applySnapshot` replaces the membership of that subscription and sets the cursor in the same batch.

Test: `large snapshots are chunked and applied as one unit`.

## 6. Membership equals the query result

For every live subscription, the `membership` rows equal the result of the query over the cache. This holds at every point where `applied_seq` is observable.

Mechanism:

- `materialize` writes membership from a full evaluation, once per subscription (and again after a scope reset).
- `maintainSubscriptions` maintains every affected subscription after each transaction, level by level: candidates are the changed rows and the primary rows they reach through `exists` chains, each candidate is classified by running the predicate in SQLite over its current cache row, and include levels follow their parent level through indexed relation lookups. Limited queries keep an exact top-k window. See [ivm.md](ivm.md).
- All of this runs in the transaction of invariant 3.

Test: `randomized incremental maintenance equals full recomputation` compares `membershipOf` with `recompute` after every step.

## 7. A scope is live only after an exact fill

A table of a partition is `live` only after a fill completed at position P and every held change with a gtid outside P was applied in `seq` order.

Mechanism:

- `applyTransaction` writes changes for a `filling` scope into `held` with `(seq, ord)`.
- `completeFill` reads held changes ordered by `seq, ord`. It skips a change whose gtid is contained in P (`gtidSetContains`). It applies the rest. It then sets the scope to `live` in the same transaction.
- The fill worker produces rows that are exact at P. See [bootstrap.md](bootstrap.md).

Test: `SyncEngine: bootstrap race (hold and skip)`.

## 8. The checkpoint is safe to resume from

Every stream item at or before the persisted checkpoint is acknowledged by every partition it touched, or is quarantined.

Mechanism:

- The distributor keeps an in-flight ledger keyed by stream index. `advance_checkpoint` moves the checkpoint only over the contiguous prefix of items with `pending == 0`.
- Quarantine counts as done, so one bad partition cannot stall the checkpoint. The quarantined transactions are stored verbatim.
- `commit_checkpoint` writes positions, epoch and counters in one SQLite transaction with `synchronous = FULL`.

Test: `crates/orbit-distributor/tests/distributor.rs`.

## 9. Replay reproduces the same batches

After a restart, the same source transaction gets the same `(partition, seq)` assignment.

Mechanism:

- Routing is a pure function of the schema and the row images.
- `partition_counters` are persisted in the same transaction as the checkpoint. They hold the next `seq` per partition as of the checkpoint.
- The state store refuses a different schema hash, so counters are never reused with another schema.

## 10. The stream never skips a transaction silently

Mechanism:

- The assembler computes the gtid of each commit with `diff_single`. A step that adds zero or several ids is an error, unless `--allow-merged-transactions` is set.
- A reconnect resumes from the last emitted position.
- At startup the subscriber checks that the checkpoint is contained in the server position. A future position is refused with `InvalidCheckpoint`.
- `Journal` events (reshards) are refused as unsupported.

Test: `skipped_position_is_malformed_unless_allowed` in `crates/orbit-vstream/src/stream.rs`.

## 11. No lost updates across a reconnect

A client that reconnects converges to the current state of every subscription.

Mechanism:

- On every connect the client sends `hello` with all its subscriptions. It marks live queries `stale` until a fresh snapshot arrives.
- The Durable Object answers with a snapshot per subscription. The snapshot replaces the local membership and rows. Deltas continue from the snapshot cursor.
- The Durable Object does not replay deltas from the client cursor. `hello.cursor` is not read by the Durable Object.

Test: `survives eviction: sessions and subscriptions come back from storage`.

## 12. The local cache holds only referenced rows

A row exists in the browser database only while at least one subscription references it.

Mechanism:

- `applyDelta` upserts a row only when a membership row exists for it (`WHERE EXISTS` on `membership`).
- Removed members and removed subscriptions delete rows that no other subscription references.

## 13. Schema identity is checked at every boundary

Mechanism:

- The state store stores the schema hash and refuses another one.
- `applyBatch` rejects a batch with another `schema_hash`.
- The fill worker refuses a fill request with another `schema_hash`.
- The Durable Object checks additive compatibility on `hello` and closes with code 4409 on mismatch.
- `SyncEngine.init` resets every scope when the stored hash changes.

## 14. A Durable Object serves one partition

Mechanism: the partition is stored in `meta`. `bind`, `init` and `applyBatch` refuse another partition. See [placement.md](placement.md).

## Related documents

- [failure-model.md](failure-model.md): what happens when a mechanism cannot run.
- [checkpoints.md](checkpoints.md): invariants 8 and 9 in detail.
