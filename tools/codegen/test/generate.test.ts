import { describe, expect, it } from "vitest"

import { CodegenError, generate, type Document } from "../src/generate.ts"

const doc: Document = {
  $defs: {
    Kind: { type: "string", enum: ["a", "b"] },
    Thing: {
      type: "object",
      additionalProperties: false,
      required: ["name", "kind", "count"],
      properties: {
        name: { type: "string", description: "The name." },
        kind: { $ref: "#/$defs/Kind" },
        count: { type: "integer", format: "uint32", minimum: 0 },
        note: { type: ["string", "null"] },
        tags: { type: "array", items: { type: "string" } },
        extra: true,
      },
    },
    Ack: {
      oneOf: [
        {
          type: "object",
          additionalProperties: false,
          required: ["status"],
          properties: { status: { type: "string", const: "ok" } },
        },
        {
          type: "object",
          additionalProperties: false,
          required: ["status", "reason"],
          properties: { status: { type: "string", const: "no" }, reason: { type: "string" } },
        },
      ],
    },
  },
  "x-roots": ["Thing", "Ack"],
  "x-internal-protocol-version": 1,
  "x-sync-schema-format-version": 1,
}

describe("generate", () => {
  it("emits Effect schemas in dependency order with docs and optional keys", () => {
    const out = generate(doc)
    expect(out.indexOf("export const Kind")).toBeLessThan(out.indexOf("export const Thing"))
    expect(out).toContain('export const Kind = Schema.Literals(["a", "b"])')
    expect(out).toContain("kind: Kind,")
    expect(out).toContain("count: NonNegativeInt,")
    expect(out).toContain("note: Schema.optionalKey(Schema.NullOr(Schema.String)),")
    expect(out).toContain("tags: Schema.optionalKey(Schema.Array(Schema.String)),")
    expect(out).toContain("extra: Schema.optionalKey(JsonValue),")
    expect(out).toContain("* The name.")
    expect(out).toContain('Schema.Literal("ok")')
    expect(out).toContain('export const PROTOCOL_ROOTS = ["Thing", "Ack"] as const')
  })

  it("rejects constructs it cannot represent faithfully", () => {
    expect(() =>
      generate({
        ...doc,
        $defs: {
          Bad: {
            type: "object",
            properties: { a: { type: "string" } },
            additionalProperties: { type: "string" },
          },
        },
      }),
    ).toThrow(CodegenError)
    expect(() =>
      generate({
        ...doc,
        $defs: {
          Loop: {
            type: "object",
            additionalProperties: false,
            properties: { self: { $ref: "#/$defs/Loop" } },
          },
        },
      }),
    ).toThrow(/recursive/)
    expect(() => generate({ ...doc, $defs: { Mixed: { type: ["string", "number"] } } })).toThrow(
      /unsupported type union/,
    )
  })
})
