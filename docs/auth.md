# Authorization

Orbit asks the application one question: which partitions may the holder of this token access? The application answers with an `Authorizer`. The Worker enforces the answer before any request reaches a Sync Durable Object. This document describes the token format, the grant, the handler configuration, the internal secret, and the limits of the model.

Related documents: [protocol.md](protocol.md), [partitioning.md](partitioning.md), [configuring-a-new-app.md](configuring-a-new-app.md), [unsupported-behavior.md](unsupported-behavior.md).

## Two layers: the partition grant and the named query

A grant lists partitions. A client that holds a grant for partition `P` can open a WebSocket to `P`. This is the coarse layer: it decides which Durable Object a client may reach at all.

The fine layer is the named query (see [queries.md](queries.md)). With named queries configured, a client cannot send a raw query. It sends `{ name, args }`. The Durable Object resolves the name with a `QueryContext` built from the session: the authorized partition, the token subject, and the client id. The resolver returns the query the caller is allowed to see, for example with `c.eq("userId", ctx.subject)` for private rows. The client never chooses the filter.

This replaces row-level rules. There is no rule language and no per-row check in the engine. The Durable Object still stores the subject of each session, but it uses it only as resolver input. Two sessions that resolve to the same query share one materialization.

Writes follow the same model. A mutator receives `ctx.subject` and `ctx.partition` from the server's session and throws to refuse a write (see [mutations.md](mutations.md)).

Raw queries stay available for applications without named queries (`allowAdHocQueries` defaults to `true` only when no `queries` are configured). With raw queries, the partition is the only unit, and every session on a partition can query every synced table in it.

Claims in the token are echoed into the grant. The engine does not read them.

## The Authorizer service

`packages/sync-do/src/authorizer.ts` defines:

```ts
interface Grant {
  readonly subject: string
  readonly partitions: ReadonlyArray<string> | "*"
  readonly claims: Readonly<Record<string, string>>
  readonly expiresAt: number | null   // unix milliseconds; null when the grant does not expire
}

class Authorizer extends Context.Service<Authorizer, {
  readonly authorize: (token: string) => Effect.Effect<Grant, AuthError>
}>()
```

`grantAllows(grant, partition)` returns `true` when `partitions` is `"*"` or contains the partition.

`AuthError` has a `reason` of `invalid_token`, `expired`, or `malformed`. `PartitionDenied` carries the `subject` and the `partition`.

An application with its own session store implements `Authorizer` directly as an Effect `Layer`.

## The built-in HMAC authorizer

`hmacAuthorizer(secret)` is the built-in implementation. A token is `base64url(payload).base64url(signature)`. The signature is HMAC-SHA-256 over the payload bytes. The payload is JSON with this shape:

```ts
TokenPayload {
  sub: string                              // subject, usually the user id
  partitions: ReadonlyArray<string> | "*"  // partitions the subject may sync
  exp: number                              // expiry, unix seconds
  claims?: Record<string, string>          // optional application data
}
```

`authorize` does these checks in order:

1. The token has exactly two parts.
2. Both parts decode as base64url.
3. The signature verifies with `crypto.subtle.verify`.
4. The payload decodes with the `TokenPayload` schema.
5. `exp * 1000` is not in the past.

The result is a `Grant` with `subject: sub`, the partitions, the claims or `{}`, and `expiresAt: exp * 1000`.

`signToken(secret, payload)` mints a token. The application backend calls it. The browser never signs a token and never decides which partition it may open.

## Mounting the handler

`createOrbitHandler` in `packages/sync-do/src/worker.ts` builds the Worker router:

```ts
const orbit = createOrbitHandler<Env>({
  schema,
  authorizer: (e) => hmacAuthorizer(e.ORBIT_TOKEN_SECRET),
  internalSecret: (e) => e.ORBIT_INTERNAL_SECRET,
  prefix: "/orbit",
})
```

The application's Worker entry exports this handler. The `authorizer` function receives the Worker environment and returns a `Layer`. The router builds one `ManagedRuntime` per environment object and reuses it.

For `GET {prefix}/ws` the router:

1. Requires a WebSocket upgrade. Otherwise it answers `426`.
2. Reads `partition` from the query string. A missing partition is a `malformed` `AuthError`.
3. Reads the token from the `Sec-WebSocket-Protocol` entry that starts with `orbit.token.` (the browser client sends it this way, so the token never appears in a URL or a request log). When no such entry exists, it reads `token` from the query string, then `Authorization: Bearer`.
4. Calls `authorize`. A failure answers `401` with `{ error: "unauthorized", reason }`.
5. Calls `grantAllows`. A denial answers `403` with `{ error: "partition_denied", partition }`.
6. Forwards the request to the Durable Object for the partition, with the headers `x-orbit-partition`, `x-orbit-subject` and `x-orbit-expires` (the grant's expiry in unix milliseconds, empty when it does not expire).

The Durable Object name is `${app}/p${placementVersion}/${partition}` (see [placement.md](placement.md)). The client never sees it.

## What the Durable Object trusts

The Durable Object reads `x-orbit-partition`, `x-orbit-subject` and `x-orbit-expires` from the forwarded request. It does not verify the token itself. This is safe only because the Durable Object is reachable through the Worker binding alone. The Worker sets both headers after authorization. A client cannot set them, because the Worker overwrites `x-orbit-partition` on every forward and sets `x-orbit-subject` only on the authorized `/ws` path.

The Durable Object adds two checks of its own. `hello.partition` must equal the partition in the socket attachment; a mismatch closes the socket with code 4403. The expiry is stored in the socket attachment; an alarm closes the socket with code 4408 (`sessionExpired`) when it passes, and a message that arrives after the expiry gets the same close. The client treats 4408 as a normal reconnect and presents a fresh token. A client that offered the `orbit` subprotocol gets it echoed in the upgrade response, as browsers require.

The subject is stored in the `sessions` table for diagnostics.

## The internal secret

Every `/internal/*` route requires `Authorization: Bearer <secret>`. The router compares the presented value with `internalSecret(env)` in constant time. A missing or wrong secret answers `401`. These routes carry CDC batches, fill uploads, status, and reset (see [protocol.md](protocol.md)).

The Rust server sends the same secret. It reads it from `WORKER_SECRET` (`--worker-secret`). For local development, pass the `ORBIT_INTERNAL_SECRET` value from `.dev.vars` to the engine as `WORKER_SECRET`.

## How an application mints tokens

The example application keeps a signed session cookie and derives sync grants from a `member` table.

The session module writes a cookie as `<userId>.<exp>.<sig>`. The signature is HMAC-SHA-256 over `<userId>.<exp>` with `SESSION_SECRET`. The cookie is `httpOnly`, `sameSite: lax`, and lives seven days. `readSession` checks the expiry and the signature and returns the user id.

A `syncToken` server function mints the sync token:

```ts
const userId = await requireUser()
const partitions = await memberOrganizations(userId)
const token = await Effect.runPromise(
  signToken(env().ORBIT_TOKEN_SECRET, {
    sub: userId,
    partitions,
    exp: Math.floor(Date.now() / 1000) + 5 * 60,
  }),
)
```

`memberOrganizations` runs `SELECT organization_id FROM member WHERE user_id = ?`. The token lives five minutes. The browser client calls `syncToken` through `getToken` on every connection attempt, so expiry never blocks a reconnect.

The same `member` table authorizes writes. The push endpoint checks the membership of the caller for the partition before it runs a mutator (see [mutations.md](mutations.md)).

## Secrets

The example application needs three secrets:

- `ORBIT_TOKEN_SECRET` signs and verifies sync tokens.
- `ORBIT_INTERNAL_SECRET` protects the internal routes.
- `SESSION_SECRET` signs the session cookie.

Set them in `.dev.vars` for local development, or with `wrangler secret put` for a deployment. The application must throw at start when one is missing.

## Limits of the model

- Authorization happens at connection time. A revoked membership does not close an open socket before the grant expires. The socket lives at most until the token's `exp`; a short token lifetime bounds the window.
- The `"*"` partition grant gives access to every partition. Use it only for trusted services.
- There is no rate limit and no per-subject quota. Close code 4429 (`overloaded`) is defined but not sent.
- The token travels in the URL query string. Make sure the Worker does not log full request URLs.
