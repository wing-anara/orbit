# Future scaling

This document describes options to move past the limits in [scalability-limits.md](scalability-limits.md). None of these are implemented. Each section names the code that a change would touch.

## Sharded placement

`PlacementConfig` in `crates/orbit-protocol/src/schema.rs` is an enum with one variant:

```rust
pub enum PlacementConfig {
    OnePerPartition { version: u32 },
}
```

The placement is part of the sync schema artifact, and the Durable Object name includes the version (`packages/sync-do/src/placement.ts`). A new variant can therefore be introduced without colliding with existing objects. Two shapes are useful:

- Co-location: many small partitions share one Durable Object. The object stores rows for several partitions and filters by partition per session. This reduces the number of objects and fills for tenants with little data.
- Splitting: one large partition maps to several Durable Objects, for example by table or by a hash of the primary key. Queries that need rows from several objects would then need a merge step in the Worker or in the client.

Changing the placement invalidates every Durable Object's state, as the field comment states. The rollout is a `placementVersion` bump followed by refills.

## Several subscriber processes by shard

`SubscriberConfig.shards` already accepts an explicit shard list (`crates/orbit-vstream/src/subscriber.rs`). The checkpoint stores positions per shard. A deployment could run one `orbit-server` per shard, each with its own state file and its own VStream. Requirements:

- A partition must live on one shard. Vitess sharding by the same column as the Orbit partition key gives this for free.
- Sequence counters per partition are then owned by one process, because only that process routes the partition.
- The fill worker must run the copy phase against the right shard, which the current `shards_or_all` helper already supports.

This removes the single-process bottleneck for both the stream and fills. It also makes reshards easier to follow, because each process restarts against its own shard.

## Fan-out for hot partitions

A partition with many readers is limited by the delta evaluation per session in one Durable Object. Options:

- Reader replicas: the owning Durable Object applies batches and forwards each `delta` to secondary objects that hold only sessions. Secondaries need the membership tables to filter rows, or the primary sends per-subscription payloads.
- Subscription grouping: identical queries already share one materialization (`SyncEngine.subscribe` keys by canonical query). Sending one payload per subscription instead of one per session would reduce work on the primary.

Both keep the ordering guarantee, because the primary still applies batches serially.

## Snapshot serving from R2

Today a fill is uploaded from the Rust process to the Durable Object, and a snapshot is sent per client over the WebSocket. Two changes would reduce Durable Object load for large partitions:

- Write fill results to R2 as NDJSON and let the Durable Object stream them in, instead of one HTTP body per fill.
- Publish a snapshot of a subscription (or a whole scope) to R2 with its cursor. New clients fetch it over HTTP and then subscribe from that cursor. The Durable Object sends only deltas after the cursor.

The client store already applies a snapshot as a diff against local rows, so the source of the snapshot does not matter to it.

## Mutations over the WebSocket

Mutations travel over HTTP to the application's push endpoint. Routing them through the Worker to the Durable Object would let the object apply the optimistic effect to its shared cache before the source confirms it, which Zero calls "instant server". That needs a rollback path in the object's cache and is not planned.

## Per-user materializations

A named query that depends on `ctx.subject` produces one materialization per user. Many users with distinct private queries multiply the subscriptions of one Durable Object. A later version could share the common part of such queries and apply the per-user filter at delivery time.

## Multi-tab persistence

One OPFS pool per origin means one persistent tab. A leader election with a `BroadcastChannel`, where follower tabs proxy queries to the leader, would give every tab the same local database. The worker comment in `packages/client/src/worker/sqlite.worker.ts` anticipates this design.
