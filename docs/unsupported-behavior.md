# Unsupported behavior

This document lists what Orbit does not do in v1. Each item names the code that enforces it. See [failure-model.md](failure-model.md) for how the engine reports these cases.

## Mutations confirm through sync only

Client mutations exist (see [mutations.md](mutations.md)), but the engine never acknowledges a write itself. The push endpoint answers `applied`, and the row becomes canonical only when the mutation's effects arrive as a delta together with the `orbit_clients` row. A client that never receives that delta keeps the mutation in its overlay. Mutators can read and write synced tables only; a write to an unsynced table needs a normal backend call.

## No rule-based row-level authorization

There is no rule language and no per-row policy check in the engine. Authorization is the named query resolver and the mutator: they receive the session's subject and partition and decide what the caller sees and writes (see [auth.md](auth.md)). Two sessions that resolve to the same query share one materialization; a resolver that returns a per-user query gives each user a materialization of their own. Column selection happens in `sync.config.ts` for everyone, not per user. When named queries are not configured, a token grants whole partitions and every session can query every synced table in them.

## No cross-partition queries

A live query runs inside one Durable Object. Includes join rows of the same partition only. A client that needs two partitions opens two clients, for example one client per organization.

## No aggregates, no projections, no joins beyond includes

The query AST (`packages/protocol/src/query-ast.ts`) supports `where` (with `exists` over declared relations), `orderBy`, `limit`, and nested, filtered `include` on declared relations. There is no `count`, `sum`, `group by`, `distinct`, `offset`, column projection, or ordering and limits inside includes. Relation depth is limited to three. A result row carries every synced column. Applications compute aggregates in the browser over the rows they subscribe to.

Unsupported query shapes are rejected at planning time with `unsupported_query` (`packages/query/src/plan.ts`). See [queries.md](queries.md).

## Derived partitions: one level only

A table without the partition column can declare a `partition_parent` (see [partitioning.md](partitioning.md#derived-partitions)). The parent must be partitioned directly. `SyncSchema::validate` rejects a parent that has a `partition_parent` of its own (`NestedPartitionParent`). A grandchild table needs the parent key of a direct table, or the partition column itself.

The parent primary key must be one column of kind `string`, `int` or `bigint` (`PartitionParentKey`). Rows with a NULL partition value in a direct table are counted as unpartitioned and dropped by the router. See [choosing-a-partition-key.md](choosing-a-partition-key.md).

## Unresolved parents

A change of a derived table whose parent column is NULL, or whose parent key is not in the parent index, is not routed. The router counts it in `orbit_distributor_unresolved_parent_total` and logs a warning with the table and the keys. The checkpoint advances past it. No Durable Object sees the row. The bootstrap copy and the stream keep the index complete for every parent row that exists, so this case means an orphan row: a child that references a parent that does not exist in the source.

A move of a parent row to another partition does not move its existing children. The router sends the parent delete and insert, but the children stay in the old partition until they change themselves. Later child changes route to the new partition. Parent index entries are never deleted, so a child change that arrives after its parent was deleted still routes to the partition the parent had.

## DDL on the source

The subscriber does not stop on DDL. The assembler (`crates/orbit-vstream/src/stream.rs`) clears its field projections, logs a warning, and emits `StreamItem::Ddl`. The distributor and the fill treat it as a position advance. On the next `FIELD` event the projection is rebuilt against the sync schema:

- An added column that is not in the sync schema is ignored.
- A renamed, removed, or retyped synced column is a `SchemaMismatch` error. This error is fatal: the server stops.

A DDL that arrives inside a transaction with row changes is reported as malformed. After a source schema change, re-run introspection, recompile, and redeploy. See [schema-evolution.md](schema-evolution.md).

## GTID history reset

A checkpoint must be contained in the server's current position. A source with a new GTID history (for example `vttestserver` after a restart, which re-initializes its data directory and server UUID) fails with `InvalidCheckpoint`. Purged binlogs fail with `PurgedBinlog`. Both are fatal and need `orbit-server checkpoint reset`. The engine never skips history on its own.

## Multi-shard keyspaces

The subscriber streams every shard of the keyspace, but several places assume one shard per partition:

- A demand fill that returns positions for more than one shard fails with `multi-shard partitions are not supported` (`crates/orbit-server/src/fill_worker.rs`). The Durable Object stores one fill position per scope.
- A reshard journal event stops the subscriber with `Unsupported` (`stop_on_reshard` is set). Restart after the reshard completes and reset the checkpoint.
- A keyspace that gains a shard after the checkpoint was taken fails checkpoint validation.

Ordering across shards is per shard. Two transactions on different shards have no defined order relative to each other.

## Merged transactions

vtgate can emit a step that adds several GTIDs at once. The assembler treats this as malformed unless `ALLOW_MERGED_TRANSACTIONS=1`. With the flag, the merged step becomes one transaction whose `gtid` is a rendered set.

## Statement-based binlogs

Events of type `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, and `SET` are rejected with `binlog_format must be ROW`.

## Large values

- `bigint` values outside the signed 64-bit range cannot be stored; the Durable Object rejects the batch with `unstorable_value` (`packages/query/src/sql.ts`).
- `bytes` columns travel as base64 text. There is no streaming of large blobs. A row is one JSON object in a batch, and a batch is one HTTP body, so very large blobs enlarge every message that carries the row. Keep large binary data outside the sync schema.
- `json` cells are stored as text and cannot be compared or ordered in queries.

## Partition moves

An update that changes the partition column becomes a delete in the old partition and an insert in the new one. Clients in the old partition see the row disappear. This is by design, but it means a row's history is not continuous across partitions.

## Two tabs on one origin

Only one browser context can hold the OPFS pool. A second tab falls back to memory storage (`packages/client/src/client.ts`). It stays live but does not persist. The comment in `sqlite.worker.ts` mentions leader election in `multitab.ts`; that module does not exist in v1.

## Single subscriber, no failover

One `orbit-server run` process owns the state file. There is no leader election and no standby. A crash pauses delivery until a supervisor restarts the process. Data is not lost: the checkpoint only advances over acknowledged transactions.

## No Durable Object placement other than one per partition

`PlacementConfig` has one variant. Hot partitions cannot be split and small partitions cannot be co-located. See [future-scaling.md](future-scaling.md).
