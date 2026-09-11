/**
 * Authorization is a service the application provides. The engine only asks one question:
 * "which partitions may the holder of this token access?" Decisions happen in the Worker, before
 * any request reaches a Sync Durable Object; a client can never pick a partition by itself.
 *
 * `hmacAuthorizer` is the built-in implementation: tokens are `base64url(payload).base64url(sig)`
 * with an HMAC-SHA-256 signature over the payload, minted by the application backend with
 * `signToken`. Applications with their own session stores implement the `Authorizer` interface
 * directly.
 */

import { Context, Effect, Layer, Schema } from "effect"

import { AuthError } from "./errors.ts"

export const TokenPayload = Schema.Struct({
  /** Subject (user id). */
  sub: Schema.String,
  /** Partitions the subject may sync, or "*" for all. */
  partitions: Schema.Union([Schema.Array(Schema.String), Schema.Literal("*")]),
  /** Expiry, unix seconds. */
  exp: Schema.Finite,
  /** Optional application data, echoed into the grant. */
  claims: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
})
export type TokenPayload = typeof TokenPayload.Type

export interface Grant {
  readonly subject: string
  readonly partitions: ReadonlyArray<string> | "*"
  readonly claims: Readonly<Record<string, string>>
}

export const grantAllows = (grant: Grant, partition: string): boolean =>
  grant.partitions === "*" || grant.partitions.includes(partition)

export class Authorizer extends Context.Service<
  Authorizer,
  {
    readonly authorize: (token: string) => Effect.Effect<Grant, AuthError>
  }
>()("@orbit/sync-do/Authorizer") {}

const encoder = new TextEncoder()
const decoder = new TextDecoder()

const base64url = (bytes: Uint8Array): string =>
  btoa(String.fromCharCode(...bytes))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/, "")

const fromBase64url = (text: string): Uint8Array<ArrayBuffer> | null => {
  try {
    const padded =
      text.replaceAll("-", "+").replaceAll("_", "/") + "=".repeat((4 - (text.length % 4)) % 4)
    const bin = atob(padded)
    const out = new Uint8Array(new ArrayBuffer(bin.length))
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i)
    return out
  } catch {
    return null
  }
}

const hmacKey = (secret: string): Effect.Effect<CryptoKey> =>
  Effect.promise(() =>
    crypto.subtle.importKey(
      "raw",
      encoder.encode(secret),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign", "verify"],
    ),
  )

export const signToken = (secret: string, payload: TokenPayload): Effect.Effect<string> =>
  Effect.gen(function* () {
    const key = yield* hmacKey(secret)
    const encoded = yield* Schema.encodeEffect(TokenPayload)(payload).pipe(Effect.orDie)
    const body = encoder.encode(JSON.stringify(encoded))
    const sig = yield* Effect.promise(() => crypto.subtle.sign("HMAC", key, body))
    return `${base64url(body)}.${base64url(new Uint8Array(sig))}`
  })

export const hmacAuthorizer = (
  secret: string,
  now: () => number = () => Date.now(),
): Layer.Layer<Authorizer> =>
  Layer.effect(
    Authorizer,
    Effect.gen(function* () {
      const key = yield* hmacKey(secret)
      const authorize = (token: string): Effect.Effect<Grant, AuthError> =>
        Effect.gen(function* () {
          const [bodyPart, sigPart, ...rest] = token.split(".")
          if (bodyPart === undefined || sigPart === undefined || rest.length > 0)
            return yield* new AuthError({
              reason: "malformed",
              message: "token must have two parts",
            })
          const body = fromBase64url(bodyPart)
          const sig = fromBase64url(sigPart)
          if (body === null || sig === null)
            return yield* new AuthError({ reason: "malformed", message: "token is not base64url" })
          const valid = yield* Effect.promise(() => crypto.subtle.verify("HMAC", key, sig, body))
          if (!valid)
            return yield* new AuthError({
              reason: "invalid_token",
              message: "signature does not verify",
            })
          const parsed = yield* Schema.decodeEffect(Schema.fromJsonString(TokenPayload))(
            decoder.decode(body),
          ).pipe(
            Effect.mapError(
              () =>
                new AuthError({
                  reason: "malformed",
                  message: "payload does not match the token schema",
                }),
            ),
          )
          if (parsed.exp * 1000 < now())
            return yield* new AuthError({ reason: "expired", message: "token expired" })
          return { subject: parsed.sub, partitions: parsed.partitions, claims: parsed.claims ?? {} }
        })
      return { authorize }
    }),
  )
