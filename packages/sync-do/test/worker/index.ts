/** Test Worker: mounts the engine with the fixture schema and the HMAC authorizer. */

import { Schema } from "effect"
import { SyncSchema } from "@orbit/protocol"
import { defineQueries, defineQuery, TypedQuery } from "@orbit/query"

import {
  createOrbitHandler,
  hmacAuthorizer,
  makeSyncDurableObject,
  type OrbitWorkerEnv,
} from "../../src/index.ts"
import fixture from "../../../../schema/fixtures/SyncSchema.json"

export { FillRegistryDurableObject } from "../../src/index.ts"

export interface Env extends OrbitWorkerEnv {
  readonly ORBIT_INTERNAL_SECRET: string
  readonly ORBIT_TOKEN_SECRET: string
}

export const schema: SyncSchema = Schema.decodeUnknownSync(SyncSchema)(fixture)

/** Named queries for the tests; the fixture has no typed definition, so ASTs are built directly. */
const definition = { _tag: "SyncSchemaDefinition" } as const
const define = defineQuery(definition)
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
        where: { op: "eq", column: "contents", value: ctx.subject ?? "" },
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
})

const handler = createOrbitHandler<Env>({
  schema,
  authorizer: (env) => hmacAuthorizer(env.ORBIT_TOKEN_SECRET),
  internalSecret: (env) => env.ORBIT_INTERNAL_SECRET,
})

export default {
  fetch: (request: Request, env: Env): Promise<Response> => handler(request, env),
}
