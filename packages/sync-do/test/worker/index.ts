/** Test Worker: mounts the engine with the fixture schema and the HMAC authorizer. */

import { Schema } from "effect"
import { SyncSchema } from "@orbit/protocol"
import { defineQueries, defineQuery, TypedQuery } from "@orbit/query"

import {
  createOrbitHandler,
  hmacAuthorizer,
  makeSyncDurableObject,
  FillRegistryDurableObject as BaseFillRegistry,
  type OrbitWorkerEnv,
} from "../../src/index.ts"
import fixture from "../../../../schema/fixtures/SyncSchema.json"

/** Fault injection is confined to the test Worker. */
export class FillRegistryDurableObject extends BaseFillRegistry {
  private readonly failNext = new Map<string, string>()
  readonly rejectedIds = new Map<string, string>()

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === "/test/fail-next") {
      const body = (await request.json()) as { partition: string; mode: string }
      this.failNext.set(body.partition, body.mode)
      return new Response(null, { status: 204 })
    }
    if (url.pathname === "/test/rejected")
      return Response.json({
        id: this.rejectedIds.get(url.searchParams.get("partition") ?? "") ?? null,
      })
    if (url.pathname === "/enqueue") {
      const body = (await request.clone().json()) as { partition: string; fill_id: string }
      const mode = this.failNext.get(body.partition)
      if (mode !== undefined) {
        this.failNext.delete(body.partition)
        this.rejectedIds.set(body.partition, body.fill_id)
        if (mode === "throw") throw new Error("injected registry connection failure")
        return new Response("injected registry failure", { status: 503 })
      }
    }
    return super.fetch(request)
  }
}

export interface Env extends OrbitWorkerEnv {
  readonly ORBIT_WARM_SYNC: DurableObjectNamespace
  readonly ORBIT_INTERNAL_SECRET: string
  readonly ORBIT_TOKEN_SECRET: string
}

export const schema: SyncSchema = Schema.decodeUnknownSync(SyncSchema)(fixture)

/** Named queries for the tests; the fixture has no typed definition, so ASTs are built directly. */
const definition = { _tag: "SyncSchemaDefinition" } as const
const define = defineQuery(definition)
export const revokedQuerySubjects = new Set<string>()
export const queries = defineQueries(definition, {
  documentsInFolder: define(
    Schema.Struct({ folderId: Schema.String }),
    (ctx, { folderId }) =>
      new TypedQuery<typeof definition, "Chatbot">({
        table: "Chatbot",
        where: {
          op: "and",
          args: [
            { op: "eq", column: "organizationId", value: ctx.partition },
            { op: "eq", column: "groupId", value: folderId },
          ],
        },
        orderBy: [{ column: "displayOrder", direction: "asc" }],
      }),
  ),
  mine: define(
    Schema.Struct({}),
    (ctx) =>
      new TypedQuery<typeof definition, "Chatbot">({
        table: "Chatbot",
        where: {
          op: "eq",
          column: "groupId",
          value: revokedQuerySubjects.has(ctx.subject ?? "") ? "__revoked__" : (ctx.subject ?? ""),
        },
      }),
  ),
})

export const SyncDurableObject = makeSyncDurableObject({
  schema,
  queries,
  // The protocol tests send raw queries; named queries are covered by their own test.
  allowAdHocQueries: true,
  fillTimeoutMs: 2_000,
  maxFillAttempts: 3,
  snapshotChunkRows: 3,
  subscriptionGraceMs: 1_500,
  warmSubscriptionRetentionMs: 0,
})

export const WarmSyncDurableObject = makeSyncDurableObject({
  schema,
  allowAdHocQueries: true,
  subscriptionGraceMs: 1_500,
})

const handler = createOrbitHandler<Env>({
  schema,
  authorizer: (env) => hmacAuthorizer(env.ORBIT_TOKEN_SECRET),
  internalSecret: (env) => env.ORBIT_INTERNAL_SECRET,
})

export default {
  fetch: (request: Request, env: Env): Promise<Response> => handler(request, env),
}
