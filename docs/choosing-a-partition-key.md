# Choosing a partition key

The partition key is the most important decision in an Orbit configuration. This document explains what the partition does and how to choose it. See [partitioning.md](partitioning.md) for the routing rules.

## What a partition is

A partition is a value of one column that every synced table carries. In the example schema it is the organization id. The engine uses the partition for four things:

1. Authorization. A token grants a list of partitions. The Worker checks the requested partition against the grant before it opens a WebSocket. Nothing finer exists: a client that may open a partition sees every synced row in it.
2. Placement. Each partition has one Durable Object. Its name is `<app>/p<version>/<partition>` (`packages/sync-do/src/placement.ts`).
3. Bootstrap. A demand fill copies one table for one partition with `select * from T where <partition_column> = <value>`.
4. Ordering. The distributor assigns a dense sequence per partition and delivers batches one at a time and in order. A transaction that touches two partitions is split, and each part is ordered independently.

Queries never cross partitions. A live query runs inside one Durable Object and joins only rows of the same partition.

## Requirements

- The key kind must be `string`, `int`, or `bigint`. The Rust validator rejects other kinds.
- Every synced table must have a column with that kind that holds the key. The column may have a different name per table (`id` on `organization`, `organizationId` on `Chatbot`).
- Integer keys are rendered as decimal digits. `42` and `"42"` are the same partition.

## Guidance

Use the tenant or organization id. It matches how most applications authorize, it appears on most tables, and it keeps a user's working set in one place. The example schema uses `organization`.

Check cardinality. Every distinct value gets a Durable Object with its own SQLite database. Many small partitions are fine. A user id works when the application is personal, but it makes cross-user features impossible.

Watch for hot partitions. One Durable Object is single-threaded and applies one batch at a time. A partition that receives most of the write traffic, or that serves thousands of concurrent clients, becomes the bottleneck. Cloudflare states a practical range of about 500 to 1,000 simple requests per second for one object. See [scalability-limits.md](scalability-limits.md) and [future-scaling.md](future-scaling.md).

Watch partition size. Each fill copies the whole table for the partition into the Durable Object and each subscribed client receives its query result. A partition with millions of rows in one synced table makes fills slow and snapshots large.

Prefer immutable keys. When an update changes the partition value of a row, the router turns it into a delete in the old partition and an insert in the new one (`crates/orbit-distributor/src/router.rs`). Both partitions see a consistent change, but clients in the old partition lose the row and clients in the new one gain it. The metric `orbit_distributor_partition_moves_total` counts these events.

## NULL partition values

A row whose partition column is NULL is not routed anywhere. The distributor counts it in `orbit_distributor_unpartitioned_rows_total` and logs at debug level. No Durable Object sees the row. If the source can set the column to NULL, decide whether that is acceptable. In the example schema, `Chatbot.organizationId` is nullable in the source; rows without an organization simply do not sync.

An update from NULL to a value is an insert into the new partition. An update from a value to NULL is a delete in the old partition.

## Tables without the column

A table that does not carry the partition column can still be synced when it references a partitioned table by its primary key. The sync schema sets `partition_parent` on it, and the engine derives the partition from the parent row. See [partitioning.md](partitioning.md#derived-partitions) and [adding-tables.md](adding-tables.md). Only one level is supported: the parent must carry the partition column itself.

A table with neither the column nor such a reference cannot be synced. Options:

- Add the column to the source table and backfill it. This is what Vitess itself asks of a sharding column.
- Leave the table out of the sync schema and load it through the application's own API.

## Changing the key later

The partition key is part of the artifact hash and of the Durable Object name. Changing `partition.kind` or the partition column of a table produces a new hash. Existing Durable Objects reset their scopes on the next start, and their names stay the same. If the set of partition values changes meaning, bump `placementVersion` so old objects are not reused. See [placement.md](placement.md) and [schema-evolution.md](schema-evolution.md).
