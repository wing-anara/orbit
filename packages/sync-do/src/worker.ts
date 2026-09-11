/**
 * The Worker router that fronts the Sync Durable Objects. Applications mount it in their own
 * Worker and provide the authorizer; the router never lets a client choose a Durable Object.
 *
 * Routes (under `prefix`, default `/orbit`):
 * * `GET  /ws?partition=P&token=T`      client WebSocket (authorized, then forwarded to the DO)
 * * `POST /internal/cdc/:partition`     distributor delivery (bearer `internalSecret`)
 * * `GET  /internal/fills/next?wait=S`  fill worker long poll
 * * `POST /internal/fills/:fillId`      fill upload (NDJSON)
 * * `GET  /internal/status/:partition`  Durable Object status
 * * `POST /internal/reset/:partition`   operator reset of one Durable Object
 * * `GET  /internal/schema`             the compiled sync schema the Worker runs
 */

import { Effect, Exit, ManagedRuntime, Schema, type Layer } from "effect"
import { SyncSchema } from "@orbit/protocol"

import { Authorizer, grantAllows, type Grant } from "./authorizer.ts"
import { AuthError, InternalAuthError, PartitionDenied } from "./errors.ts"
import { durableObjectNameFor, fillIdPartition } from "./placement.ts"

export interface OrbitWorkerEnv {
  readonly ORBIT_SYNC: DurableObjectNamespace
  readonly ORBIT_FILL_REGISTRY: DurableObjectNamespace
}

export interface OrbitHandlerConfig<Env extends OrbitWorkerEnv> {
  readonly schema: SyncSchema
  /** Layer providing the application's authorizer; built per Worker instance. */
  readonly authorizer: (env: Env) => Layer.Layer<Authorizer>
  /** Shared secret for the distributor and fill worker. */
  readonly internalSecret: (env: Env) => string
  readonly prefix?: string
}

const json = (body: unknown, status = 200): Response => Response.json(body, { status })
const encodeSchema = Schema.encodeSync(SyncSchema)

const constantTimeEqual = (a: string, b: string): boolean => {
  if (a.length !== b.length) return false
  let diff = 0
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i)
  return diff === 0
}

const bearer = (request: Request): string | null => {
  const h = request.headers.get("authorization")
  return h?.startsWith("Bearer ") === true ? h.slice(7) : null
}

export const createOrbitHandler = <Env extends OrbitWorkerEnv>(config: OrbitHandlerConfig<Env>) => {
  const prefix = (config.prefix ?? "/orbit").replace(/\/$/, "")
  const runtimes = new WeakMap<object, ManagedRuntime.ManagedRuntime<Authorizer, never>>()
  const runtimeFor = (env: Env): ManagedRuntime.ManagedRuntime<Authorizer, never> => {
    const existing = runtimes.get(env)
    if (existing !== undefined) return existing
    const rt = ManagedRuntime.make(config.authorizer(env))
    runtimes.set(env, rt)
    return rt
  }

  const stubFor = (env: Env, partition: string): DurableObjectStub =>
    env.ORBIT_SYNC.get(env.ORBIT_SYNC.idFromName(durableObjectNameFor(config.schema, partition)))

  const forward = async (
    env: Env,
    partition: string,
    path: string,
    request: Request,
    extraHeaders: Record<string, string> = {},
  ): Promise<Response> => {
    const headers = new Headers(request.headers)
    headers.set("x-orbit-partition", partition)
    for (const [k, v] of Object.entries(extraHeaders)) headers.set(k, v)
    const url = new URL(request.url)
    url.pathname = path
    // Bodies are read here so the outer request stream is fully consumed before the response.
    const body =
      request.method === "GET" || request.method === "HEAD" ? null : await request.arrayBuffer()
    return stubFor(env, partition).fetch(
      new Request(url, { method: request.method, headers, body }),
    )
  }

  const requireInternal = (request: Request, env: Env): Effect.Effect<void, InternalAuthError> => {
    const token = bearer(request)
    return token !== null && constantTimeEqual(token, config.internalSecret(env))
      ? Effect.void
      : Effect.fail(new InternalAuthError({ message: "missing or invalid internal secret" }))
  }

  const authorizeClient = (
    request: Request,
  ): Effect.Effect<
    { readonly grant: Grant; readonly partition: string },
    AuthError | PartitionDenied,
    Authorizer
  > =>
    Effect.gen(function* () {
      const url = new URL(request.url)
      const partition = url.searchParams.get("partition")
      const token = url.searchParams.get("token") ?? bearer(request)
      if (partition === null || partition === "")
        return yield* new AuthError({ reason: "malformed", message: "partition is required" })
      if (token === null)
        return yield* new AuthError({ reason: "malformed", message: "token is required" })
      const authorizer = yield* Authorizer
      const grant = yield* authorizer.authorize(token)
      if (!grantAllows(grant, partition))
        return yield* new PartitionDenied({ subject: grant.subject, partition })
      return { grant, partition }
    })

  return async (request: Request, env: Env): Promise<Response> => {
    const url = new URL(request.url)
    if (!url.pathname.startsWith(prefix + "/")) return json({ error: "not found" }, 404)
    const path = url.pathname.slice(prefix.length)

    if (path === "/ws") {
      if (request.headers.get("upgrade")?.toLowerCase() !== "websocket")
        return json({ error: "expected websocket upgrade" }, 426)
      const exit = await runtimeFor(env).runPromiseExit(authorizeClient(request))
      if (Exit.isFailure(exit)) {
        const failure = exit.cause.reasons.find((r) => r._tag === "Fail")
        const error = failure !== undefined && failure._tag === "Fail" ? failure.error : undefined
        if (error instanceof PartitionDenied)
          return json({ error: "partition_denied", partition: error.partition }, 403)
        if (error instanceof AuthError)
          return json({ error: "unauthorized", reason: error.reason }, 401)
        return json({ error: "internal" }, 500)
      }
      const { grant, partition } = exit.value
      return forward(env, partition, "/ws", request, { "x-orbit-subject": grant.subject })
    }

    if (path.startsWith("/internal/")) {
      const auth = await Effect.runPromiseExit(requireInternal(request, env))
      if (Exit.isFailure(auth)) return json({ error: "unauthorized" }, 401)
      const cdc = /^\/internal\/cdc\/(.+)$/.exec(path)
      if (cdc !== null && request.method === "POST")
        return forward(env, decodeURIComponent(cdc[1] ?? ""), "/cdc", request)
      if (path === "/internal/fills/next" && request.method === "GET") {
        const stub = env.ORBIT_FILL_REGISTRY.get(env.ORBIT_FILL_REGISTRY.idFromName("registry"))
        const target = new URL(request.url)
        target.pathname = "/next"
        return stub.fetch(new Request(target, { method: "GET" }))
      }
      const upload = /^\/internal\/fills\/(.+)$/.exec(path)
      if (upload !== null && request.method === "POST") {
        const fillId = decodeURIComponent(upload[1] ?? "")
        const partition = fillIdPartition(fillId)
        if (partition === null) return json({ error: "malformed fill id" }, 400)
        return forward(env, partition, `/fill/${encodeURIComponent(fillId)}`, request)
      }
      const status = /^\/internal\/status\/(.+)$/.exec(path)
      if (status !== null && request.method === "GET")
        return forward(env, decodeURIComponent(status[1] ?? ""), "/status", request)
      const reset = /^\/internal\/reset\/(.+)$/.exec(path)
      if (reset !== null && request.method === "POST")
        return forward(env, decodeURIComponent(reset[1] ?? ""), "/admin/reset", request)
      if (path === "/internal/registry/status") {
        const stub = env.ORBIT_FILL_REGISTRY.get(env.ORBIT_FILL_REGISTRY.idFromName("registry"))
        return stub.fetch(new Request("https://registry/status"))
      }
      // The engine loads the artifact from here when it has no local copy, so both sides
      // always run the same schema hash.
      if (path === "/internal/schema" && request.method === "GET")
        return json(encodeSchema(config.schema), 200)
      return json({ error: "not found" }, 404)
    }
    return json({ error: "not found" }, 404)
  }
}
