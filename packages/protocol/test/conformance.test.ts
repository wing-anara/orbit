/**
 * Cross-language conformance: every fixture written by the Rust crate decodes with the generated
 * Effect Schema and re-encodes to the same JSON value. A Rust type change without regenerating
 * the TypeScript (or vice versa) fails here.
 */

import * as fs from "node:fs"
import * as path from "node:path"
import { fileURLToPath } from "node:url"

import { Schema } from "effect"
import { describe, expect, it } from "vitest"

import * as P from "../src/generated/protocol.gen.ts"

const here = path.dirname(fileURLToPath(import.meta.url))
const fixtureDir = path.resolve(here, "../../../schema/fixtures")

const roots: Record<
  string,
  { readonly schema: Schema.Codec<unknown, unknown>; readonly list: boolean }
> = {
  SyncSchema: { schema: P.SyncSchema, list: false },
  IntrospectedSchema: { schema: P.IntrospectedSchema, list: false },
  SourceTransaction: { schema: P.SourceTransaction, list: false },
  CdcBatch: { schema: P.CdcBatch, list: false },
  CdcBatchAck: { schema: P.CdcBatchAck, list: true },
  FillRequest: { schema: P.FillRequest, list: false },
  FillPollResponse: { schema: P.FillPollResponse, list: false },
  FillChunk: { schema: P.FillChunk, list: true },
  FillResult: { schema: P.FillResult, list: true },
  EngineError: { schema: P.EngineError, list: true },
}

describe("protocol fixtures from Rust", () => {
  it("covers every exported root", () => {
    expect([...P.PROTOCOL_ROOTS].sort()).toEqual(Object.keys(roots).sort())
  })

  for (const [name, { schema, list }] of Object.entries(roots)) {
    it(`${name} decodes and re-encodes identically`, () => {
      const text = fs.readFileSync(path.join(fixtureDir, `${name}.json`), "utf8")
      const value: unknown = JSON.parse(text)
      const codec = list ? Schema.Array(schema) : schema
      const decoded = Schema.decodeUnknownSync(codec)(value)
      const encoded = Schema.encodeUnknownSync(codec)(decoded)
      expect(encoded).toEqual(value)
    })
  }

  it("rejects unknown message variants instead of passing them through", () => {
    expect(() => Schema.decodeUnknownSync(P.CdcBatchAck)({ status: "maybe" })).toThrow()
    expect(() =>
      Schema.decodeUnknownSync(P.RowChange)({ table: "t", op: "upsert", key: [] }),
    ).toThrow()
  })
})
