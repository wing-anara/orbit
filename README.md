# Orbit

Orbit is a general-purpose sync engine. It moves committed rows from a Vitess database to the browser and keeps live queries current.

The data path is:

1. Vitess emits row changes over VStream (gRPC).
2. A Rust process (`orbit-server`) subscribes, assembles transactions, and routes each row to a logical partition.
3. The distributor delivers ordered batches to one Cloudflare Durable Object per partition. Each Durable Object holds a SQLite cache.
4. The Durable Object maintains subscriptions incrementally and pushes snapshots and deltas over WebSockets.
5. The browser client stores rows in SQLite WASM on OPFS and serves live queries from the local database.

Reads and writes are defined once, in shared code: named queries (`defineQueries`) resolve on the server with the caller's identity, and mutators (`defineMutators`) run optimistically in the browser and authoritatively on the application's server. Queries filter on related rows (`exists`) and include relations at several levels.

The TypeScript packages use Effect v4. The engine is schema driven: an application declares which tables to sync, and every runtime loads the same compiled artifact. Nothing in the engine is specific to one schema; `infra/vitess/schema.sql` is an example schema for local development and the integration tests.

The design is informed by two case studies: LiveStore and Zero. Orbit keeps a relational cache on the server and in the browser, like Zero, and treats the source database as the single writer, like an event log in LiveStore.

## Repository layout

| Path                       | Content                                                                                       |
| -------------------------- | --------------------------------------------------------------------------------------------- |
| `crates/orbit-protocol`    | Shared types: sync schema, CDC batches, fill protocol, errors. Exports a JSON Schema.         |
| `crates/orbit-gtid`        | MySQL GTID set parsing and containment.                                                       |
| `crates/orbit-vstream`     | VStream subscriber, transaction assembler, checkpoints, copy-phase fills, introspection.      |
| `crates/orbit-distributor` | Routing, per-partition ordering, batching, retries, quarantine, durable checkpoints.          |
| `crates/orbit-server`      | The `orbit-server` binary: `run`, `schema`, `checkpoint`, `quarantine`.                       |
| `packages/protocol`        | Generated Effect Schema types plus the client WebSocket protocol.                             |
| `packages/schema`          | `defineSyncSchema`, the compiler, SQLite DDL, migrations, row codecs.                         |
| `packages/query`           | Typed query builder, planner, SQL compiler, row storage helpers.                              |
| `packages/mutators`        | Custom mutators: shared definitions, the server push endpoint, the MySQL `MutationTx`.        |
| `packages/sync-do`         | The Sync Durable Object, the fill registry, sessions, the Worker router, the HMAC authorizer. |
| `packages/client`          | Browser client: OPFS SQLite worker, connection, local store, live queries, React hooks.       |
| `tools/codegen`            | Generates `packages/protocol/src/generated` from `schema/protocol.schema.json`.               |
| `infra/vitess`             | Docker Compose for a local `vttestserver` and the example schema.                             |
| `schema/`                  | The exported protocol JSON Schema and fixtures.                                               |
| `Dockerfile`               | The engine as a container image (see `docs/deployment-railway.md`).                           |
| `docs/`                    | Design and operations documents (see the index below).                                        |

## Quick start

Requirements: Node 22.12 or newer, pnpm 11, a stable Rust toolchain, `protoc`, Docker, and a MySQL client.

1. Install the dependencies.

```bash
pnpm install
```

2. Start the local Vitess cluster and load the example schema.

```bash
pnpm vitess:up
pnpm vitess:schema
```

3. Run the checks and the tests.

```bash
cargo test --workspace
pnpm test
ORBIT_TEST_VITESS=1 cargo test -p orbit-vstream --test live_vitess -- --test-threads=1
ORBIT_TEST_VITESS=1 pnpm --filter @orbit/mutators test
```

4. Adopt the engine in an application: [configuring-a-new-app.md](docs/configuring-a-new-app.md) walks through the sync schema, the compiled artifact, the Worker, the named queries, the mutators, the browser client, and the engine process.

To run the engine against the local cluster with the example fixture:

```bash
cargo run -p orbit-server -- run \
  --schema schema/fixtures/SyncSchema.json --state data/orbit-state.sqlite \
  --worker-url http://127.0.0.1:8787/orbit --worker-secret dev-internal-secret \
  --metrics-addr 127.0.0.1:9464
```

with `VITESS_GRPC_URI=http://127.0.0.1:33575` in the environment. The Worker URL is wherever the application's Worker mounts the router (`wrangler dev` listens on 8787 by default).

## Tests

| Command                                                                                                                | Scope                                                                                                                                                                    |
| ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cargo test --workspace`                                                                                               | Rust unit tests and the distributor fault tests with a fake Durable Object.                                                                                              |
| `pnpm test`                                                                                                            | Vitest for every package: protocol conformance, schema compiler, query planner, engine core on `node:sqlite`, the real Durable Object in workerd, and the client engine. |
| `ORBIT_TEST_VITESS=1 cargo test -p orbit-vstream --test live_vitess -- --test-threads=1`                               | Live VStream tests against the local Vitess.                                                                                                                             |
| `ORBIT_TEST_VITESS=1 ORBIT_TEST_VITESS_DISRUPTIVE=1 cargo test -p orbit-vstream --test disruptive -- --test-threads=1` | Freezes the container and purges binlogs. Needs `sudo -n docker`.                                                                                                        |
| `ORBIT_TEST_VITESS=1 pnpm --filter @orbit/mutators test`                                                               | The push handler and the MySQL transaction adapter against the local Vitess.                                                                                             |

Static checks: `pnpm typecheck`, `pnpm lint`, `pnpm format`, `pnpm codegen:check`, and `cargo clippy --all-targets -- -D warnings`.

## Continuous integration

`.github/workflows/ci.yml` runs five jobs on every push to `main` and on every pull request:

- `rust`: `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test --workspace`, and a check that `schema/protocol.schema.json` is current.
- `rust-vitess`: starts the Docker Vitess and runs the live and disruptive integration tests.
- `typescript`: `pnpm codegen:check`, typecheck, lint, format, and `pnpm test`.
- `typescript-vitess`: the mutator tests against the Docker Vitess.
- `image`: builds the engine container image and runs its `--help`.

## Documentation index

- [architecture.md](docs/architecture.md): the components and the data path.
- [partitioning.md](docs/partitioning.md): logical partitions and routing rules.
- [placement.md](docs/placement.md): how partitions map to Durable Objects.
- [consistency-invariants.md](docs/consistency-invariants.md): the guarantees each layer keeps.
- [failure-model.md](docs/failure-model.md): error classes and recovery.
- [checkpoints.md](docs/checkpoints.md): positions, epochs, and resume.
- [bootstrap.md](docs/bootstrap.md): demand fills through the VStream copy phase.
- [ivm.md](docs/ivm.md): incremental maintenance of subscriptions.
- [queries.md](docs/queries.md): named queries, the query language, and its limits.
- [type-safety.md](docs/type-safety.md): how Rust and TypeScript share one schema.
- [protocol.md](docs/protocol.md): internal and client wire protocols.
- [schema-evolution.md](docs/schema-evolution.md): what a schema change does at each layer.
- [client-persistence.md](docs/client-persistence.md): the OPFS store and reload behavior.
- [auth.md](docs/auth.md): tokens, grants, and the authorizer service.
- [mutations.md](docs/mutations.md): mutators, the optimistic overlay, and the push endpoint.
- [configuring-a-new-app.md](docs/configuring-a-new-app.md): step-by-step adoption guide.
- [adding-tables.md](docs/adding-tables.md): how to add a table to the sync schema.
- [choosing-a-partition-key.md](docs/choosing-a-partition-key.md): guidance for the partition column.
- [operations-runbook.md](docs/operations-runbook.md): commands, metrics, logs, incidents.
- [deployment-railway.md](docs/deployment-railway.md): the engine on Railway, the Worker on Cloudflare, and what it costs.
- [scalability-limits.md](docs/scalability-limits.md): known limits and their sources.
- [unsupported-behavior.md](docs/unsupported-behavior.md): what the engine does not do.
- [future-scaling.md](docs/future-scaling.md): options for growth.
- [benchmarks.md](docs/benchmarks.md): what was measured and how to reproduce it.
