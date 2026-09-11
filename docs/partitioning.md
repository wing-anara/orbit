# Partitioning

## The logical partition

Orbit divides all synced data into logical partitions. A partition is a value of the partition key. The sync schema declares the key once:

- `partition.name`: a human name, for example `organization`.
- `partition.key_kind`: `string`, `int` or `bigint`. Schema validation rejects other kinds.
- `partition.placement`: the physical placement strategy. See [placement.md](placement.md).

Every synced table declares a `partition_column`. The column must exist in the table. Its kind must equal `partition.key_kind`. `SyncSchema::validate` in `crates/orbit-protocol/src/schema.rs` enforces both rules.

In the TypeScript DSL, each table sets `partitionBy`. The compiler always includes the primary key columns and the partition column, even when `columns` lists a subset. The example configuration uses the organization as the partition:

```ts
partition: { name: "organization", kind: "string" },
tables: {
  organization: { partitionBy: "id", columns: [...] },
  Chatbot: { partitionBy: "organizationId", columns: [...] },
}
```

The engine routes a row by a column of that row. A table that has no partition column can still be synced when it references a partitioned table: it declares a `partition_parent`. See [Derived partitions](#derived-partitions).

## Partition keys on the wire

The distributor renders each partition key as a string (`render_partition_key` in `crates/orbit-distributor/src/router.rs`):

- A `string` key is used as is.
- An `int` or `bigint` key is rendered as decimal digits.

The rendered key appears in `CdcBatch.partition`, in the delivery URL (percent-encoded by `urlencode`), in fill ids, and in the client `hello` message. The fill worker renders the key back into a SQL literal for the copy filter (`sql_literal` in `crates/orbit-vstream/src/fill.rs`).

## Routing rules

Routing is a pure function of the sync schema and the row images. There is no application code in the path. The function `route` in `crates/orbit-distributor/src/router.rs` applies these rules to each row change:

- Insert: the partition is the value in the `after` image.
- Delete: the partition is the value in the `before` image.
- Update with the same value in both images: that partition.
- Update with different values: a move. See the next section.
- A NULL partition value: the change is not routed. See below.

The output keeps the row order of the source transaction inside each partition. A parent row therefore stays before its child rows.

## Partition moves

When an update changes the partition column, the router splits the change:

- A delete goes to the old partition. Its key comes from the `before` image. Its `before` image is the old row.
- An insert goes to the new partition. Its key and `after` image are the new row.

Each partition then sees a self-contained change. The old partition removes the row. The new partition adds it. The router counts each move in `orbit_distributor_partition_moves_total`.

A transition from NULL to a value is also a move. The delete side has no partition and is dropped. The insert side reaches the new partition. The reverse transition sends only the delete.

## NULL partition values

A row whose partition column is SQL NULL belongs to no partition. `partition_of` returns `None` for it. The router does not send the change anywhere. It counts the row in `RoutingStats.unpartitioned` and in the metric `orbit_distributor_unpartitioned_rows_total`. The distributor writes a debug log line with the gtid and the count.

Such a row is never in any Durable Object cache. A client cannot query it.

## Derived partitions

Some tables do not carry the partition column. The example table `DocumentEntityLink(chatbotId, entityId)` has no `organizationId`. Its partition is the `organizationId` of the referenced `Chatbot`. The sync schema declares this with `partition_parent`:

```json
{
  "name": "DocumentEntityLink",
  "primary_key": ["chatbotId", "entityId"],
  "partition_column": "chatbotId",
  "partition_parent": "Chatbot",
  "columns": [...]
}
```

The meaning: `partition_column` holds the primary key of a row of `partition_parent`. The partition of the row is the partition of that parent row. `SyncSchema::validate` checks these rules:

- The parent is a synced table.
- The parent has no `partition_parent` of its own. Only one level is allowed.
- The parent primary key is one column of kind `string`, `int` or `bigint`.
- The child `partition_column` exists and has the kind of the parent primary key.

The field is omitted from the artifact when it is not set, so schemas without derived tables keep their hash. [adding-tables.md](adding-tables.md) shows the TypeScript DSL form.

### The parent index

The distributor keeps a parent index in its state file (`crates/orbit-distributor/src/state.rs`). The table `parent_index (tbl, key, partition)` maps the rendered primary key of every parent row to its rendered partition. The table `parent_index_ready (tbl, position)` marks each parent table whose bootstrap copy is complete.

`orbit-server run` fills the index before the subscriber starts (`bootstrap_parent_index` in `crates/orbit-distributor/src/distributor.rs`). For every parent table without a ready marker it reads the current stream position, then reads `SELECT <pk>, <partition_column> FROM <parent>` through vtgate in pages of 5000 rows with keyset pagination, writes the rows, and sets the marker. It logs one line per table: `parent index bootstrapped table=... rows=... ms=...`. The main stream resumes from the checkpoint, which is at or before the marker position. Parent changes since the checkpoint replay into the index. The writes are idempotent, so the replay is safe. A restart, a new parent table or a changed `partition_parent` bootstraps only the tables without a marker.

During streaming, every insert or update of a parent row updates the index entry of that row. The distributor applies the parent changes of a transaction before it routes the transaction, so a child inserted together with its parent resolves. The new entries live in an in-memory overlay until the checkpoint covers their transaction. They reach SQLite in the same SQLite transaction as that checkpoint. After a restart the persisted index is therefore exactly the index as of the checkpoint, and the replay derives the same routing as the first run.

Entries are never deleted, not even when the parent row is deleted. A late child change, for example a delete that arrives after the parent is gone, still routes to the partition the parent had. A parent update that sets the partition to NULL leaves the previous entry in place.

### Routing of derived rows

`route` applies the same rules as for a direct table. The only difference is how it finds the partition of an image: it renders the parent key from `partition_column` and looks it up in the index.

- Insert: the partition of the parent in the `after` image.
- Delete: the partition of the parent in the `before` image.
- Update with the same parent partition in both images: that partition.
- Update where the parent column changes to a parent in another partition: a move. The router sends a delete to the old partition and an insert to the new one, exactly as for a direct table.

A NULL parent column, or a parent key that is not in the index, gives no partition. The router does not send the change anywhere. It counts the change in `RoutingStats.unresolved_parent` and in the metric `orbit_distributor_unresolved_parent_total`, and writes a warn log line with the table, the row key and the parent key. The bootstrap makes an unknown parent rare: it happens when the child references a parent row that does not exist, for example an orphan row without a foreign key.

A move of the parent row itself does not move its existing children. Only later child changes route to the new partition. See [unsupported-behavior.md](unsupported-behavior.md).

Fills of derived tables cannot use the copy-phase filter. See [bootstrap.md](bootstrap.md#fills-of-derived-tables).

## Routing errors

Three conditions are errors, not skips:

- `UnknownTable`: the change names a table that is not in the sync schema.
- `MissingPartitionColumn`: the row image lacks the partition column.
- `InvalidPartitionValue`: the value does not match `key_kind`.

A routing error stops the distributor loop with `DistributorError::Routing`. The subscriber then fails with `ConsumerGone` because the channel is closed. That error is not retryable, so the process exits. An operator must correct the schema before a restart.

## Order and sequence numbers

Vitess emits committed transactions per shard in commit order. The distributor gives each partition a dense sequence number, `seq`, in stream order. A source transaction that touches two partitions produces two `PartitionTransaction` values with independent `seq` values.

A source transaction that touches no synced table still reaches the distributor with no changes. It routes to no partition. It counts as done at once, so the checkpoint can advance past it. See [checkpoints.md](checkpoints.md).

Sequence counters are persisted with the checkpoint. A replay after a restart re-derives the same `(partition, seq)` pairs, because routing is deterministic and the counters restart from the persisted values.

## Partitions on the client side

A client connects to exactly one partition. `createOrbitClient` takes a `partition` value and names the local database `orbit-<app>-<partition>`. The token minted by the application backend lists the partitions the subject may sync, or `"*"` for all. The Worker denies a WebSocket for a partition outside the grant with status 403.

Query planning is not routing. By the time a query reaches a Durable Object, the connection has already fixed the partition. A query can only see rows of that partition.

## Related documents

- [placement.md](placement.md): which Durable Object hosts a partition.
- [checkpoints.md](checkpoints.md): how sequence numbers survive a restart.
- [consistency-invariants.md](consistency-invariants.md): the per-partition order guarantee.
