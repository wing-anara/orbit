# Deployment: Railway + Cloudflare

The sync engine is one long-lived process; the Worker and the Durable Objects are serverless. Railway runs the engine, Cloudflare runs the rest. The Vitess keyspace sees one gRPC stream from the engine and the push endpoint's writes from the Worker.

```
Vitess keyspace  --VStream gRPC-->  orbit-server on Railway  --HTTPS-->  Worker + Durable Objects on Cloudflare  --WebSocket-->  browsers
                 <--SQL writes--   push endpoint in the Worker
```

## The engine on Railway

The image is `Dockerfile` at the repository root: a release build of `orbit-server` with a sync schema artifact at `/app/schema/orbit.schema.json`. The Dockerfile copies `schema/fixtures/SyncSchema.json` as a placeholder artifact. The build argument `SYNC_SCHEMA` overrides it at build time. A real deployment builds with `--build-arg SYNC_SCHEMA=path/to/orbit.schema.json`, or mounts the artifact and sets `SYNC_SCHEMA_PATH`. `railway.json` selects the Dockerfile builder and the deploy policy. Railway calls this format "config as code" and has deprecated it in favour of `.railway/railway.ts`; it keeps working until 2026-12-01, and `railway config migrate` converts it.

Deploy from the repository root after `railway link` to the project and the engine's service: `railway up --service <engine service> --detach`. The CLI uploads the repository (minus `.dockerignore`) and builds the image on Railway; a release build of the workspace takes several minutes.

Rules that come from the engine's design:

- **One replica.** The engine owns the checkpoint in `STATE_PATH`. Two processes with the same state would corrupt sequence assignment. `numReplicas` is 1 and `overlapSeconds` is 0, so a redeploy stops the old process before the new one starts. Do not enable horizontal scaling for this service.
- **A volume at `/data`.** The checkpoint and the parent index live in `STATE_PATH` (`/data/orbit-state.sqlite`). Without a volume every restart replays from the start of the retained binlog, or fails with an invalid checkpoint once the binlog is purged. Railway mounts volumes as root, and the image runs as the unprivileged `orbit` user, so set `RAILWAY_RUN_UID=0` on the service (Railway's documented switch for non-root images); otherwise the engine exits at start with `unable to open database file`.
- **No `VOLUME` instruction in the Dockerfile.** Railway's builder fails a Dockerfile that declares one, and the failure carries no log line (the deployment shows `BUILD_IMAGE` failed after a few seconds). The image only creates `/data`; the host attaches the volume.
- **Health check on `/metrics`.** The Prometheus listener binds `0.0.0.0:$PORT` (Railway sets `PORT`; the image defaults to 9464). The health check passes once the exporter is up, which happens before the stream opens.

Variables to set on the service (names only; values come from the database provider and from the Worker's secrets):

| Variable                             | Meaning                                                                     |
| ------------------------------------ | --------------------------------------------------------------------------- |
| `VITESS_GRPC_URI`                    | `https://<DATABASE_HOST>:443` for PlanetScale                               |
| `VITESS_USERNAME`, `VITESS_PASSWORD` | the branch password with VStream access                                     |
| `VITESS_CELLS`                       | `planetscale_operator_default` on PlanetScale                               |
| `WORKER_URL`                         | the Worker's engine mount, `https://<worker>/orbit` with the default prefix |
| `WORKER_SECRET`                      | equals the Worker's `ORBIT_INTERNAL_SECRET`                                 |
| `SYNC_SCHEMA_PATH`                   | already set in the image; override for a mounted artifact                   |
| `STATE_PATH`                         | already set in the image (`/data/orbit-state.sqlite`)                       |
| `RUST_LOG`                           | `info` by default                                                           |

Optional tuning: `FILL_CONCURRENCY`, `FILL_TIMEOUT_SECS`, `DELIVERY_TIMEOUT_SECS`, `STALL_TIMEOUT_SECS`, `PROGRESS_TIMEOUT_SECS`, `MAX_CONSECUTIVE_FAILURES` (see `crates/orbit-server/src/config.rs`).

Sizing: the engine is I/O bound. 1 vCPU and 1 GB are enough for one keyspace with a few million changes a day; fills hold one table of one partition in memory at a time, so a partition with a very large table needs more memory during its fill. Egress is the CDC traffic to Cloudflare, about one kilobyte per row change.

## The Worker on Cloudflare

Deploy the application Worker with `wrangler deploy`. The account needs the Workers Paid plan: SQLite Durable Objects on the free plan stop accepting writes after 100,000 rows written per day, which one refill of a mid-sized partition exhausts. Set `ORBIT_INTERNAL_SECRET` (shared with the engine), `ORBIT_TOKEN_SECRET` and the application's own secrets with `wrangler secret put`.

## What it costs

Cloudflare bills the Durable Objects by rows written (50 M per month included, then $1 per million), rows read (25 B included), requests (incoming WebSocket messages count at 20:1) and storage. The engine writes 3 to 5 cache and membership rows plus 2 bookkeeping rows per source change at any partition size (`docs/benchmarks.md`), so the bill is proportional to the write rate of the source: 2 M changes a day is about 420 M rows written a month. Use a client ping interval of 30 s in production (`pingIntervalMs`); a 5 s interval is for tests only.

Railway bills CPU, memory, the volume and egress per second of use. One engine at 2 vCPU and 2 GB is in the order of $60 a month plus the plan.

## Local check

Build and run the image against the local stack (docker Vitess and the Vite dev server on the host):

```
docker build -t orbit-server .
docker run --rm --network host -v orbit-state:/data \
  -e VITESS_GRPC_URI=http://127.0.0.1:33575 \
  -e WORKER_URL=http://127.0.0.1:5173/orbit -e WORKER_SECRET=dev-internal-secret \
  orbit-server
```

The log shows `sync schema loaded`, `metrics endpoint listening`, `opening vstream` and a `status` line every ten seconds. `curl http://127.0.0.1:9464/metrics` answers the health check.
