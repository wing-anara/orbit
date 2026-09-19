# Checkpoints

## What a checkpoint contains

A checkpoint tells the subscriber where to resume. The type `Checkpoint` in `crates/orbit-vstream/src/checkpoint.rs` has two fields:

- `epoch`: the unix time (seconds) at which the state was created or last reset.
- `positions`: one Vitess position per shard, keyed by `ShardId { keyspace, shard }`.

A position is a MySQL GTID set with the `MySQL56/` prefix, for example `MySQL56/a2523813-adbe-11f1-b19c-0a2250a7ed6c:1-209`. The crate `orbit-gtid` parses positions and offers the three operations the engine needs: `contains`, `is_subset_of` and `diff_single`.

A GTID set is not totally ordered. The engine never compares two sets for order. Order inside one shard stream comes from delivery order.

## Where the checkpoint is stored

The distributor persists the checkpoint in the state SQLite file (`STATE_PATH`, default `data/orbit-state.sqlite`). The store in `crates/orbit-distributor/src/state.rs` opens the file with `journal_mode = WAL` and `synchronous = FULL`. It uses three tables:

```sql
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS partition_counters (partition TEXT PRIMARY KEY, next_seq INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS quarantine (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  partition TEXT NOT NULL,
  seq INTEGER NOT NULL,
  reason TEXT NOT NULL,
  transaction_json TEXT NOT NULL,
  quarantined_at INTEGER NOT NULL,
  UNIQUE (partition, seq)
);
```

`meta` holds `schema_hash`, `epoch` and `positions` (a JSON object keyed by `keyspace/shard`). `partition_counters` holds the next `seq` per partition as of the checkpoint. `commit_checkpoint` writes positions, epoch and counter updates in one transaction. A crash between them cannot desynchronize the sequence assignment from the resume position.

A fresh store starts at the current unix time as its epoch, with no positions. The store records the schema hash at that time. A later start with another hash fails with `SchemaChanged`.

## How the checkpoint advances

The distributor keeps an in-flight ledger in memory. Each stream item gets a stream index. The ledger entry records the shard, the position after the item, the `(partition, seq)` assignments, and a `pending` count.

The count rules are:

- A transaction that routes to N partitions starts with `pending = N`.
- A transaction that routes to no partition starts with `pending = 0`.
- A position-only item (`VGTID` at stream start, DDL) starts with `pending = 0`.
- An `applied` ack decrements `pending` for every item in the batch.
- A quarantine decrements `pending` for every quarantined item.

`advance_checkpoint` walks the ledger from the lowest index. It removes each entry with `pending == 0` and sets the shard position. It stops at the first entry that is still pending. The checkpoint therefore covers a contiguous prefix of the stream. A restart can replay work, but it can never skip work.

The ledger also provides backpressure. When it holds `MAX_INFLIGHT_TRANSACTIONS` items (default 32,768), or its estimated serialized work reaches `MAX_INFLIGHT_BYTES` (default 128 MiB), the distributor stops reading from the subscriber. The subscriber channel then fills, and HTTP/2 flow control slows vtgate down. One oversized transaction may cross the byte budget so it can still make progress. Its charge is released only when the checkpoint advances, even if some rows have already been delivered. This budget covers routed work, not the subscriber channel, hydration buffers, or total process RSS.

The count window covers a 30-second delivery timeout at 833 source writes/second. The previous 2,000-item window provided only 2.4 seconds at that rate: a transient slow acknowledgment stalled healthy partitions behind it. The byte budget limits large imports independently of transaction count. Persistent partition failures can still fill this finite window; this is bounded backpressure, not an unbounded durable outbox.

## Flush timing

A flusher task writes the checkpoint every `checkpoint_interval` (500 ms) when it changed. Only the counters of partitions that advanced since the last flush are written. `Distributor::run` flushes once more on shutdown, so a clean stop resumes exactly where it stopped.

The subscriber does not persist anything. On a reconnect inside one process it resumes from the last position it emitted. On a restart it resumes from the persisted checkpoint. Both cases can deliver a transaction twice. The Durable Object dedupes by `seq`.

## Validation at startup

`orbit_vstream::run` validates the checkpoint before it streams. It calls `current_position`, which opens a stream at `current` and takes the first `VGTID` event. It then checks `start.is_contained_in(current)`. Every shard in the checkpoint must be present in the server position, and its GTID set must be a subset.

The check exists because Vitess accepts a position that lies in the future. Such a stream would stay silent forever. The check turns that silence into `InvalidCheckpoint`.

`to_vgtid` builds the stream request:

- With explicit shards, each listed shard resumes from its checkpoint or from `current`.
- With no explicit shards, every shard that has a checkpoint resumes from it. When none has one, the request asks for all shards at `current`.

A keyspace that gains a shard after the checkpoint therefore fails validation instead of streaming silently.

Purged binlogs are detected in two ways. When the gRPC status message names them, the subscriber reports `PurgedBinlog` and stops. When vtgate hides the error behind internal retries, the stream stays silent while the source moves on. The subscriber then reports `NoProgress` after `progress_timeout`. See [failure-model.md](failure-model.md).

## Reset and epochs

An operator resets the checkpoint with:

```bash
orbit-server checkpoint reset --to current
orbit-server checkpoint reset --to 'orbit/0@MySQL56/<uuid>:1-500'
```

`--to current` reads the server position first. An explicit value lists `keyspace/shard@position` pairs joined by `|`.

`reset_checkpoint` writes the new positions and moves the epoch forward to the current unix time (at least one above the old epoch). The counters stay, so sequence numbers keep increasing. The distributor stamps every batch with `stream_epoch` from the persisted epoch.

A Durable Object stores its own epoch in `meta`. `applyBatch` compares the two:

- `stream_epoch` below the stored epoch: the batch is rejected with `stale_epoch`. The distributor quarantines it.
- `stream_epoch` above the stored epoch: history may have been skipped. The engine drops every scope, every held change and every membership. It sets `applied_seq` to 0 and clears `seq_log`. It records the new epoch. Subscriptions stay registered and become pending. Each connected client receives `subscribed` with status `pending` and a new fill starts. The workers test `epoch changes reset scopes and re-bootstrap subscriptions` covers this.

A fresh Durable Object starts at epoch 0. A fresh state store starts at the current unix time. The first batch from a fresh server therefore triggers one reset on a fresh object. That reset drops nothing of value, but it does re-issue fills for subscriptions made before the first batch. Because the epoch is time-based, a state file that is deleted and recreated always carries a higher epoch than the Durable Objects stored, so the objects reset instead of rejecting the restarted sequence as a `SequenceConflict`.

## Inspection

```bash
orbit-server checkpoint show
```

The command prints the epoch, the positions, the number of partitions with counters and the quarantined partitions.

## Fill positions are separate

A fill records its own position in `scopes.fill_position`. That position is the point at which the fill rows are exact. It is only used to skip held changes. It is not a checkpoint. See [bootstrap.md](bootstrap.md).

## Related documents

- [failure-model.md](failure-model.md): purged binlogs and the local test source.
- [consistency-invariants.md](consistency-invariants.md): invariants 8 and 9.
