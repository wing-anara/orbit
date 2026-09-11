# Placement

## Logical model and physical model

The logical model says which partition a row belongs to. See [partitioning.md](partitioning.md). The physical model says which Durable Object hosts a partition. Orbit keeps the two models separate.

The sync schema carries the placement in `partition.placement`. In Rust it is the enum `PlacementConfig` in `crates/orbit-protocol/src/schema.rs`. In JSON it is a tagged object:

```json
{ "strategy": "one_per_partition", "version": 1 }
```

The schema compiler writes this object from `placementVersion` in `defineSyncSchema`. The default version is 1.

## The one_per_partition strategy

`one_per_partition` is the only strategy today. Each logical partition gets its own Sync Durable Object. The object holds:

- The relational cache for every table of that partition.
- The scopes, held changes, subscriptions and membership.
- The sessions of every connected client.

Because one object owns the whole partition, every write to the partition runs in one SQLite database. That is what makes the per-partition guarantees in [consistency-invariants.md](consistency-invariants.md) simple to enforce.

## Durable Object names

`durableObjectNameFor` in `packages/sync-do/src/placement.ts` builds the object name:

```text
${app}/p${version}/${partition}
```

For an application with app `example` and partition `org_42` the name is `example/p1/org_42`. The Worker resolves the name with `env.ORBIT_SYNC.idFromName(name)`.

The version is part of the name. A future strategy can use a new version without a clash with existing objects. Clients never see these names. The client protocol carries only the partition value.

## How requests reach a Durable Object

The Worker router is the only entry point. It resolves the object for every request:

- For `GET /orbit/ws`, it authorizes the token first. It checks that the grant allows the requested partition. It then forwards the upgrade with the headers `x-orbit-partition` and `x-orbit-subject`.
- For `POST /orbit/internal/cdc/:partition`, it checks the internal secret. It forwards the batch to the object for `:partition` at the object path `/cdc`.
- For `POST /orbit/internal/fills/:fillId`, it extracts the partition from the fill id. It forwards the upload to the object path `/fill/:fillId`.
- For the status and reset endpoints, it forwards to `/status` and `/admin/reset`.

The Worker sets `x-orbit-partition` on every forwarded request. The Durable Object refuses a request without that header with status 400.

## Binding an object to its partition

A Durable Object learns its partition from the first request. `bind` in `packages/sync-do/src/do.ts` creates the engine and stores the partition in the `meta` table. The constructor reads that value back on every later start, so a restarted object binds itself before any request arrives.

A request for a different partition is refused:

- `bind` throws when the stored partition differs. The `fetch` handler returns status 409.
- `SyncEngine.init` also throws when the stored partition differs from the requested one.
- `applyBatch` rejects a batch whose `partition` field differs with `wrong_partition`.

These checks make sure a misrouted batch can never land in the wrong cache.

## Fill ids embed the partition

A fill id has the form `${partition}:${uuid}`. `fillIdPartition` takes the text before the last colon. The Worker uses it to route the upload without a lookup. A fill id with no colon is a malformed request and gets status 400.

## The fill registry

The fill registry is one Durable Object per deployment. The Worker and the Sync Durable Objects resolve it with `idFromName("registry")`. It stores fill requests in its own SQLite table and serves them by long poll. See [bootstrap.md](bootstrap.md).

The Wrangler configuration binds both classes with SQLite storage:

```jsonc
"durable_objects": {
  "bindings": [
    { "name": "ORBIT_SYNC", "class_name": "SyncDurableObject" },
    { "name": "ORBIT_FILL_REGISTRY", "class_name": "FillRegistryDurableObject" }
  ]
},
"migrations": [
  { "tag": "v1", "new_sqlite_classes": ["SyncDurableObject", "FillRegistryDurableObject"] }
]
```

## Changing the placement version

The placement object is a field of the sync schema. The schema hash covers every field except `schema_hash` itself. A new placement version therefore produces a new hash and new object names.

The effects are:

- The Worker routes every partition to a fresh object. The fresh object fills its scopes on demand.
- The old objects stay in storage. Nothing deletes them.
- The Rust state store refuses to start with a changed schema hash. `StateStore::load_or_init` returns `SchemaChanged`. `StateStore::migrate_schema` accepts a new hash, but no CLI command calls it today.

Plan a placement change as a schema change. Sequence counters and routing do not depend on the placement.

## Operator endpoints

- `GET /orbit/internal/status/:partition` returns the engine status: partition, schema hash, epoch, `appliedSeq`, scopes with row counts, subscription count, held change count, session count, socket count and outstanding fills.
- `POST /orbit/internal/reset/:partition` closes every socket with code 4500, deletes all storage of the object and unbinds it. The next request binds it again and fills on demand.
- `GET /orbit/internal/registry/status` returns the number of queued fill requests.

## Future placements

The placement enum is the extension point. A future strategy could split a hot partition over several objects, or co-locate small partitions in one object. Such a change needs:

- A new variant of `PlacementConfig` in Rust and in the generated TypeScript.
- A new case in `durableObjectNameFor`.
- A new placement version, so old and new objects do not collide.

The client protocol has no field for a Durable Object identity. A client only names the partition. This is why a placement change does not need a client change.

## Related documents

- [partitioning.md](partitioning.md): the logical model.
- [bootstrap.md](bootstrap.md): how a fresh object loads its data.
- [failure-model.md](failure-model.md): eviction and reset.
