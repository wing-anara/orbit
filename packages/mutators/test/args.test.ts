import { Schema } from "effect"
import { describe, expect, it } from "vitest"
import { decodeArgs, defineMutator, defineMutators, encodeArgs } from "../src/index"
import { sync } from "./support/fixture"

const define = defineMutator(sync)
const mutators = defineMutators(sync, {
  create: define(
    Schema.Struct({
      id: Schema.String,
      name: Schema.NullOr(Schema.String),
      isPrivate: Schema.optional(Schema.Boolean),
      tags: Schema.optional(Schema.Array(Schema.Struct({ label: Schema.optional(Schema.String) }))),
    }),
    async () => {},
  ),
})

describe("argument encoding", () => {
  it("drops undefined properties, as JSON does, and decodes them back as absent", () => {
    const wire = encodeArgs(mutators, "create", {
      id: "f1",
      name: null,
      isPrivate: undefined,
      tags: [{ label: undefined }],
    })
    expect(wire).toEqual({ id: "f1", name: null, tags: [{}] })
    expect(decodeArgs(mutators, "create", wire).args).toEqual({ id: "f1", name: null, tags: [{}] })
  })
})
