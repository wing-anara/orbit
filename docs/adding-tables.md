# Adding tables

This document explains how to add a table to a running Orbit deployment. See [configuring-a-new-app.md](configuring-a-new-app.md) for the initial setup and [schema-evolution.md](schema-evolution.md) for the full change model.

## Steps

1. Make sure the table is in `schema.introspected.ts`. Re-run `orbit-server schema introspect` if it is missing.
2. Add an entry to `tables` in `sync.config.ts`.
3. Recompile the artifact with the application's compile script. The script calls `compileSyncSchema` and writes `orbit.schema.json`.
4. Commit the new `orbit.schema.json`. The `schema_hash` changes.
5. Deploy the Worker with the new artifact.
6. Restart `orbit-server run` with the new artifact.

Example entry:

```ts
DocumentEntity: {
  partitionBy: "organizationId",
  columns: ["id", "name", "type", "organizationId", "color", "createdAt"],
  relations: {
    organization: { kind: "one", to: "organization", from: ["organizationId"], toColumns: ["id"] },
  },
},
```

## Constraints

The compiler (`packages/schema/src/compile.ts`) and the Rust validator (`SyncSchema::validate`) reject a definition when:

- The table is not in the introspected schema.
- The table has no primary key. Every synced table needs one.
- A primary key column is nullable, or has kind `json` or `float`.
- `partitionBy` names a column that does not exist.
- The partition column kind differs from `partition.kind`.
- A listed column does not exist.
- A relation targets a table that is not synced.
- A relation has empty or mismatched column lists.
- A relation does not reference the full primary key of the key side.

The compiler reports every problem at once.

## Column selection

`columns` is optional. Without it, every column is synced. With it, the primary key and the partition column are added automatically. Columns not listed never leave the Rust process: the projection in `crates/orbit-vstream/src/normalize.rs` drops them.

Use the smallest column set the application needs. Every synced column is stored in the Durable Object and in each browser.

## Supported column kinds

The introspection maps MySQL types to a `ValueKind` (`crates/orbit-protocol/src/schema.rs`, `infer_kind`):

| MySQL type                                                   | Kind       | TypeScript type           |
| ------------------------------------------------------------ | ---------- | ------------------------- |
| `tinyint(1)`                                                 | `bool`     | `boolean`                 |
| `tinyint`, `smallint`, `mediumint`, `int`, `integer`, `year` | `int`      | `number`                  |
| `bigint`, `bit`                                              | `bigint`   | `string` (decimal digits) |
| `float`, `double`, `real`                                    | `float`    | `number`                  |
| `decimal`, `numeric`                                         | `decimal`  | `string`                  |
| `json`                                                       | `json`     | `JsonValue`               |
| `datetime`, `timestamp`                                      | `datetime` | `string`                  |
| `date`                                                       | `date`     | `string`                  |
| `time`                                                       | `time`     | `string`                  |
| `binary`, `varbinary`, `blob` family, `geometry`, `vector`   | `bytes`    | `string` (base64)         |
| everything else (`varchar`, `text`, `enum`, `set`, ...)      | `string`   | `string`                  |

Enum and set columns keep their allowed values in `enum_values`. The live stream decodes them to their string form.

Kinds affect what queries allow (see [queries.md](queries.md)):

- `json` supports only `isNull` and `isNotNull`, and cannot be ordered.
- `bytes` and `decimal` support equality and `in`, and cannot be ordered.
- `like` works on `string` only.

## Relations

A relation is a declared join that queries can `include`. The declaration is directional:

- `kind: "one"`: `from` columns on this table reference `toColumns` on the target. `toColumns` must be the target's full primary key.
- `kind: "many"`: `toColumns` on the target reference `from` columns on this table. `from` must be this table's full primary key.

Self relations are allowed (for example `Chatbot.folder` and `Chatbot.documents`). Both tables of a relation must be synced. Every row that an include returns belongs to the same partition, because the engine only ever joins inside one Durable Object.

The compiler orders tables so that `one` relation targets come first. The DDL generator adds an index on the `from` columns of each `one` relation.

## What happens after the change

The new artifact has a new `schema_hash`. Each runtime reacts as follows:

- Rust server: `StateStore::load_or_init` refuses a state file created for another hash. Reset or migrate the state before you restart (see [operations-runbook.md](operations-runbook.md)). The subscriber adds the new table to its VStream filter.
- Durable Object: on construction, `SyncEngine.init` runs `planMigration`. A new table is created in place. Because the stored hash differs, the engine drops every scope and refills them on demand. Subscriptions stay registered and are reported `pending` until the fills complete.
- Browser: `LocalStore.open` runs the same `planMigration`. A new table is created in place and the cursor is kept. A client that still runs the old artifact can connect: the server accepts a client whose tables and columns are a subset of its own, and projects rows to the client's columns.

Removing a column, adding a required column, or changing a primary key triggers a reset instead. See [schema-evolution.md](schema-evolution.md).

## Tables without the partition column

A table that does not carry the partition key can be synced through its parent. Set `partitionBy` to the column that holds the parent's primary key and `partitionVia` to the parent table:

```ts
DocumentEntityLink: {
  partitionBy: "chatbotId",
  partitionVia: "Chatbot",
  columns: ["chatbotId", "entityId", "createdAt", "order"],
  relations: {
    document: { kind: "one", to: "Chatbot", from: ["chatbotId"], toColumns: ["id"] },
    tag: { kind: "one", to: "DocumentEntity", from: ["entityId"], toColumns: ["id"] },
  },
},
```

The parent must be synced, partitioned directly, and have a single-column primary key. The Rust distributor keeps an index of parent keys to partitions and routes child rows through it. See [partitioning.md](partitioning.md), "Derived partitions", for the bootstrap and the limits. Tables whose parent is itself derived are not supported. The example schema leaves out `DocumentMetadata` and `MessageV3` because they hang off a chatbot or a chat through more than one level.
