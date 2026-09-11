# Configuring a new application

This guide shows how an application adopts Orbit. Each step names the package it uses. See [architecture.md](architecture.md) for the components.

## Prerequisites

- A Vitess keyspace with `binlog_format = ROW`. Hosted Vitess providers meet this requirement.
- vtgate gRPC access. Hosted providers expose it at `https://<host>:443` with basic auth; the local cluster in `infra/vitess` listens on `http://127.0.0.1:33575`.
- A Cloudflare account with Durable Objects (SQLite backend).
- A place to run the Rust process. It only opens outbound connections. See [deployment-railway.md](deployment-railway.md).

## Step 1: introspect the database

The Rust CLI reads `information_schema` and writes the table metadata as a TypeScript module.

```bash
cargo run -p orbit-server -- schema introspect \
  --vitess-uri https://<host>:443 \
  --vitess-username "$VITESS_USERNAME" \
  --vitess-password "$VITESS_PASSWORD" \
  --keyspace <keyspace> \
  --out orbit/schema.introspected.ts
```

Add `--table <name>` (repeatable) to limit the output. Omit it to include every table. A `.json` output path writes JSON instead of TypeScript.

The introspection infers a `kind` for each column (see `infer_kind` in `crates/orbit-protocol/src/schema.rs`). Re-run this step after every source schema change.

## Step 2: write `sync.config.ts`

Describe the tables to sync with `defineSyncSchema` from `@orbit/schema`. The definition is checked against the introspected metadata at compile time.

```ts
import { defineSyncSchema } from "@orbit/schema"
import { introspected } from "./schema.introspected.ts"

export const sync = defineSyncSchema({
  app: "my-app",
  introspected,
  partition: { name: "organization", kind: "string" },
  tables: {
    organization: { partitionBy: "id", columns: ["id", "name", "slug"] },
    member: {
      partitionBy: "organization_id",
      columns: ["id", "organization_id", "user_id", "role"],
      relations: {
        organization: {
          kind: "one",
          to: "organization",
          from: ["organization_id"],
          toColumns: ["id"],
        },
      },
    },
  },
})

export type Sync = typeof sync
```

Rules:

- `app` names the Durable Object namespace and metrics.
- `partition.kind` must be `string`, `int`, or `bigint`.
- Every table needs `partitionBy`, or `partitionVia` for a table that reaches the partition through a parent row. The column must exist and must have the partition kind.
- `columns` is optional. The primary key and the partition column are always included.
- `placementVersion` is optional. Bump it to move every partition to new Durable Objects.

See [adding-tables.md](adding-tables.md) and [choosing-a-partition-key.md](choosing-a-partition-key.md).

## Step 3: compile the artifact

Write `orbit/compile.ts`. It calls `compileSyncSchema` and writes `orbit.schema.json`; with `--check` it fails when the committed artifact is stale.

```ts
import * as fs from "node:fs"
import * as path from "node:path"
import { fileURLToPath } from "node:url"

import { Data, Effect, Schema } from "effect"
import { SyncSchema } from "@orbit/protocol"
import { compileSyncSchema } from "@orbit/schema"

import { sync } from "./sync.config.ts"

const out = path.join(path.dirname(fileURLToPath(import.meta.url)), "orbit.schema.json")

class StaleArtifact extends Data.TaggedError("StaleArtifact")<{ readonly path: string }> {}

const program = Effect.gen(function* () {
  const artifact = yield* compileSyncSchema(sync)
  const encoded = yield* Schema.encodeEffect(SyncSchema)(artifact)
  const text = JSON.stringify(encoded, null, 2) + "\n"
  if (process.argv[2] === "--check") {
    const current = fs.existsSync(out) ? fs.readFileSync(out, "utf8") : ""
    if (current !== text) return yield* new StaleArtifact({ path: out })
    return
  }
  fs.writeFileSync(out, text)
})

Effect.runPromise(program).catch(() => process.exit(1))
```

Run it with `tsx`:

```bash
tsx orbit/compile.ts
tsx orbit/compile.ts --check
```

The artifact contains `schema_hash`, a SHA-256 over the canonical JSON. The Rust server, the Durable Object, and the browser client all compare this hash. Commit the artifact and add the `--check` mode to CI.

Validate the artifact with the Rust side too:

```bash
cargo run -p orbit-server -- schema validate --schema orbit/orbit.schema.json
```

## Step 4: mount the Worker handler

Export the Durable Object classes and mount the router in the Worker entry:

```ts
import { Schema } from "effect"
import { SyncSchema } from "@orbit/protocol"
import { createOrbitHandler, hmacAuthorizer, makeSyncDurableObject } from "@orbit/sync-do"

import artifact from "../orbit/orbit.schema.json"
import { queries } from "../orbit/queries.ts"

export { FillRegistryDurableObject } from "@orbit/sync-do"

export const schema: SyncSchema = Schema.decodeUnknownSync(SyncSchema)(artifact)
export const SyncDurableObject = makeSyncDurableObject({ schema, queries })

const orbit = createOrbitHandler<Env>({
  schema,
  authorizer: (e) => hmacAuthorizer(e.ORBIT_TOKEN_SECRET),
  internalSecret: (e) => e.ORBIT_INTERNAL_SECRET,
  prefix: "/orbit",
})

export default {
  fetch: (request: Request, env: Env) => {
    const url = new URL(request.url)
    if (url.pathname.startsWith("/orbit/")) return orbit(request, env)
    return new Response("not found", { status: 404 })
  },
}
```

Declare the bindings in `wrangler.jsonc`:

```jsonc
{
  "durable_objects": {
    "bindings": [
      { "name": "ORBIT_SYNC", "class_name": "SyncDurableObject" },
      { "name": "ORBIT_FILL_REGISTRY", "class_name": "FillRegistryDurableObject" },
    ],
  },
  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["SyncDurableObject", "FillRegistryDurableObject"] },
  ],
}
```

Set two secrets with `wrangler secret put` in the cloud, or in `.dev.vars` locally:

- `ORBIT_TOKEN_SECRET`: the HMAC key for sync tokens.
- `ORBIT_INTERNAL_SECRET`: the shared secret for `/orbit/internal/*`. The Rust server sends it as a bearer token.

See [auth.md](auth.md) for other authorizer implementations.

## Step 5: mint sync tokens

The application backend decides which partitions a user may open. It signs a token with `signToken` from `@orbit/sync-do` and returns it to the browser:

```ts
const token = await Effect.runPromise(
  signToken(env.ORBIT_TOKEN_SECRET, {
    sub: userId,
    partitions,
    exp: Math.floor(Date.now() / 1000) + 5 * 60,
  }),
)
```

`partitions` is an array of partition keys or `"*"`. The Worker verifies the token before it forwards the WebSocket to a Durable Object.

## Step 5a: define the named queries

Write `orbit/queries.ts` with `defineQueries` and `defineQuery` from `@orbit/query`. Every query the client may run is defined here, as a function of the caller's identity and validated arguments. Pass the result as `queries` to `makeSyncDurableObject`; raw queries from clients are then refused. See [queries.md](queries.md).

## Step 5b: define the mutators and mount the push endpoint

Create the bookkeeping table in the database with `orbitClientsDdl()` from `@orbit/protocol` and add it to `sync.config.ts` (`orbit_clients`, partitioned by `partition_key`). Write `orbit/mutators.ts` with `defineMutators` and `defineMutator` from `@orbit/mutators`. Mount the push endpoint in the Worker with `createPushHandler` from `@orbit/mutators/server`, giving it a transaction adapter over your database driver and an `authorize` function that maps the request to a subject and checks the partition:

```ts
const push = createPushHandler({
  schema,
  mutators,
  db: {
    transaction: (f) =>
      withDb((db) =>
        db.transaction((tx) =>
          f({
            query: (sql, params) => tx.query(sql, [...params]),
            execute: (sql, params) => tx.execute(sql, [...params]),
          }),
        ),
      ),
  },
  authorize: async (request, body) => {
    const userId = await readSession(request)
    if (userId === null) return null
    const allowed = await memberPartitions(userId)
    return allowed.includes(body.partition) ? { subject: userId } : null
  },
})
if (url.pathname === "/api/orbit/push" && request.method === "POST") return push(request)
```

See [mutations.md](mutations.md).

## Step 6: create the browser client

Create one client per partition:

```ts
import SqliteWorker from "@orbit/client/worker?worker"

const client = await createOrbitClient({
  definition: sync,
  schema,
  url: `${window.location.origin}/orbit`,
  partition: organizationId,
  subject: userId,
  getToken: async () => (await syncToken()).token,
  worker: () => new SqliteWorker(),
  storage: "opfs",
  mutators,
  pushUrl: `${window.location.origin}/api/orbit/push`,
})
```

The `?worker` import lets Vite bundle the SQLite worker. In `vite.config.ts`, exclude `@sqlite.org/sqlite-wasm` from `optimizeDeps` and set `worker.format` to `es`. Then use `useLiveQuery` from `@orbit/client/react` with `queries.<name>(args)`, and write with `client.mutate.<name>(args)`. See [queries.md](queries.md), [mutations.md](mutations.md), and [client-persistence.md](client-persistence.md).

## Step 7: run the engine

`orbit-server run` needs these environment variables (see `crates/orbit-server/src/config.rs`):

| Variable                             | Meaning                                                                              |
| ------------------------------------ | ------------------------------------------------------------------------------------ |
| `VITESS_GRPC_URI`                    | vtgate gRPC endpoint.                                                                |
| `VITESS_USERNAME`, `VITESS_PASSWORD` | Basic auth, optional for local Vitess.                                               |
| `VITESS_CELLS`                       | Cells for tablet selection, when the provider requires one.                          |
| `SYNC_SCHEMA_PATH`                   | Optional local `orbit.schema.json`; by default the engine fetches the Worker's copy. |
| `STATE_PATH`                         | SQLite file for checkpoints and quarantine.                                          |
| `WORKER_URL`                         | Base URL of the mounted router, for example `https://app.example.com/orbit`.         |
| `WORKER_SECRET`                      | Must equal `ORBIT_INTERNAL_SECRET`.                                                  |

On first start the checkpoint is empty and the stream begins at the current position. Existing rows reach the Durable Objects through demand fills. See [bootstrap.md](bootstrap.md) and [checkpoints.md](checkpoints.md).

## Step 8: deploy

Deploy the Worker with `wrangler deploy`. Then start `orbit-server run` with `WORKER_URL` set to the deployed origin plus the prefix, either from the container image (`Dockerfile`, see [deployment-railway.md](deployment-railway.md)) or as a supervised process. See [operations-runbook.md](operations-runbook.md) for daily operation.
