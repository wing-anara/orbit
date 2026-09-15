/**
 * JSON Schema (draft 2020-12, the subset emitted by `schemars` for the Rust protocol crate) to
 * Effect `Schema` TypeScript source.
 *
 * The generator is deliberately strict: every construct it does not understand is an error, so a
 * Rust type that cannot be represented faithfully in TypeScript fails code generation instead of
 * silently widening to `unknown`.
 */

import { Schema } from "effect"

type JsonSchema =
  | boolean
  | {
      readonly $ref?: string
      readonly type?: string | ReadonlyArray<string>
      readonly const?: string | number | boolean | null
      readonly enum?: ReadonlyArray<string | number | boolean | null>
      readonly format?: string
      readonly minimum?: number
      readonly properties?: Readonly<Record<string, JsonSchema>>
      readonly required?: ReadonlyArray<string>
      readonly additionalProperties?: JsonSchema
      readonly items?: JsonSchema
      readonly oneOf?: ReadonlyArray<JsonSchema>
      readonly anyOf?: ReadonlyArray<JsonSchema>
      readonly description?: string
      readonly title?: string
    }

const Literal = Schema.Union([Schema.String, Schema.Finite, Schema.Boolean, Schema.Null])

/**
 * The JSON Schema subset the generator understands. Keys it does not know are dropped when a
 * document is decoded. The emitters below reject any value shape they cannot represent.
 */
const JsonSchema: Schema.Codec<JsonSchema, JsonSchema> = Schema.Union([
  Schema.Boolean,
  Schema.Struct({
    $ref: Schema.optionalKey(Schema.String),
    type: Schema.optionalKey(Schema.Union([Schema.String, Schema.Array(Schema.String)])),
    const: Schema.optionalKey(Literal),
    enum: Schema.optionalKey(Schema.Array(Literal)),
    format: Schema.optionalKey(Schema.String),
    minimum: Schema.optionalKey(Schema.Finite),
    properties: Schema.optionalKey(
      Schema.Record(
        Schema.String,
        Schema.suspend((): Schema.Codec<JsonSchema, JsonSchema> => JsonSchema),
      ),
    ),
    required: Schema.optionalKey(Schema.Array(Schema.String)),
    additionalProperties: Schema.optionalKey(
      Schema.suspend((): Schema.Codec<JsonSchema, JsonSchema> => JsonSchema),
    ),
    items: Schema.optionalKey(
      Schema.suspend((): Schema.Codec<JsonSchema, JsonSchema> => JsonSchema),
    ),
    oneOf: Schema.optionalKey(
      Schema.Array(Schema.suspend((): Schema.Codec<JsonSchema, JsonSchema> => JsonSchema)),
    ),
    anyOf: Schema.optionalKey(
      Schema.Array(Schema.suspend((): Schema.Codec<JsonSchema, JsonSchema> => JsonSchema)),
    ),
    description: Schema.optionalKey(Schema.String),
    title: Schema.optionalKey(Schema.String),
  }),
])

/** The exporter's document: definitions plus the protocol version markers. */
export const Document = Schema.Struct({
  $defs: Schema.Record(Schema.String, JsonSchema),
  "x-roots": Schema.Array(Schema.String),
  "x-internal-protocol-version": Schema.Finite,
  "x-sync-schema-format-version": Schema.Finite,
})

export type Document = typeof Document.Type

export class CodegenError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "CodegenError"
  }
}

const isObjectSchema = (s: JsonSchema): s is Exclude<JsonSchema, boolean> => typeof s === "object"

const refName = (ref: string): string => {
  const prefix = "#/$defs/"
  if (!ref.startsWith(prefix)) throw new CodegenError(`unsupported $ref ${ref}`)
  return ref.slice(prefix.length)
}

const quote = (s: string): string => JSON.stringify(s)

const isIdentifier = (s: string): boolean => /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(s)

const propKey = (name: string): string => (isIdentifier(name) ? name : quote(name))

const docComment = (text: string | undefined, indent: string): string => {
  if (text === undefined || text.trim() === "") return ""
  const lines = text.trim().split("\n")
  return `${indent}/**\n${lines.map((l) => `${indent} * ${l}`).join("\n")}\n${indent} */\n`
}

/** Emits the expression for a schema. `path` is used for error messages only. */
export const emitExpr = (schema: JsonSchema, path: string, indent: string): string => {
  if (schema === true) return "JsonValue"
  if (schema === false) throw new CodegenError(`${path}: the 'false' schema has no representation`)
  if (schema.$ref !== undefined) return refName(schema.$ref)
  if (schema.oneOf !== undefined || schema.anyOf !== undefined) {
    // `Option<T>` of a referenced type serializes as `anyOf: [T, null]`.
    const all = schema.oneOf ?? schema.anyOf ?? []
    const isNull = (m: JsonSchema) => isObjectSchema(m) && m.type === "null"
    const nullable = all.some(isNull)
    const members = all
      .filter((m) => !isNull(m))
      .map((m, i) => emitExpr(m, `${path}[${i}]`, indent + "  "))
    const inner =
      members.length === 1
        ? (members[0] ?? "")
        : `Schema.Union([\n${members.map((m) => `${indent}  ${m},`).join("\n")}\n${indent}])`
    return nullable ? `Schema.NullOr(${inner})` : inner
  }
  if (schema.const !== undefined) {
    if (
      typeof schema.const === "string" ||
      typeof schema.const === "number" ||
      typeof schema.const === "boolean"
    ) {
      return `Schema.Literal(${JSON.stringify(schema.const)})`
    }
    throw new CodegenError(`${path}: unsupported const ${String(schema.const)}`)
  }
  if (schema.enum !== undefined) {
    const values = schema.enum.map((v) => {
      if (typeof v === "string" || typeof v === "number" || typeof v === "boolean")
        return JSON.stringify(v)
      throw new CodegenError(`${path}: unsupported enum value ${String(v)}`)
    })
    return `Schema.Literals([${values.join(", ")}])`
  }
  const types = typeof schema.type === "string" ? [schema.type] : [...(schema.type ?? [])]
  if (types.length === 0) throw new CodegenError(`${path}: schema without type`)
  const nullable = types.includes("null")
  const base = types.filter((t) => t !== "null")
  if (base.length !== 1)
    throw new CodegenError(`${path}: unsupported type union ${types.join("|")}`)
  const inner = emitTyped(schema, base[0] ?? "", path, indent)
  return nullable ? `Schema.NullOr(${inner})` : inner
}

const emitTyped = (
  schema: Exclude<JsonSchema, boolean>,
  type: string,
  path: string,
  indent: string,
): string => {
  switch (type) {
    case "string":
      return "Schema.String"
    case "boolean":
      return "Schema.Boolean"
    case "number":
      return "Schema.Finite"
    case "integer": {
      const unsigned = schema.format?.startsWith("uint") === true || schema.minimum === 0
      return unsigned ? "NonNegativeInt" : "Int"
    }
    case "array": {
      if (schema.items === undefined) throw new CodegenError(`${path}: array without items`)
      return `Schema.Array(${emitExpr(schema.items, `${path}.items`, indent)})`
    }
    case "object": {
      if (schema.properties !== undefined) {
        if (schema.additionalProperties !== false && schema.additionalProperties !== undefined) {
          throw new CodegenError(
            `${path}: objects with both properties and additionalProperties are not supported`,
          )
        }
        const required = new Set(schema.required ?? [])
        const fields = Object.entries(schema.properties).map(([name, prop]) => {
          const expr = emitExpr(prop, `${path}.${name}`, indent + "  ")
          const value = required.has(name) ? expr : `Schema.optionalKey(${expr})`
          const doc = isObjectSchema(prop) ? docComment(prop.description, indent + "  ") : ""
          return `${doc}${indent}  ${propKey(name)}: ${value},`
        })
        return `Schema.Struct({\n${fields.join("\n")}\n${indent}})`
      }
      if (schema.additionalProperties === undefined || schema.additionalProperties === true) {
        return "Schema.Record(Schema.String, JsonValue)"
      }
      if (schema.additionalProperties === false) {
        return "Schema.Struct({})"
      }
      return `Schema.Record(Schema.String, ${emitExpr(schema.additionalProperties, `${path}.additionalProperties`, indent)})`
    }
    default:
      throw new CodegenError(`${path}: unsupported type ${type}`)
  }
}

/** References inside a schema, for topological ordering. */
const refsOf = (schema: JsonSchema, out: Set<string>): void => {
  if (!isObjectSchema(schema)) return
  if (schema.$ref !== undefined) out.add(refName(schema.$ref))
  for (const child of [...(schema.oneOf ?? []), ...(schema.anyOf ?? [])]) refsOf(child, out)
  if (schema.items !== undefined) refsOf(schema.items, out)
  if (schema.properties !== undefined)
    for (const p of Object.values(schema.properties)) refsOf(p, out)
  if (schema.additionalProperties !== undefined) refsOf(schema.additionalProperties, out)
}

const topoSort = (defs: Readonly<Record<string, JsonSchema>>): ReadonlyArray<string> => {
  const order: Array<string> = []
  const state = new Map<string, "visiting" | "done">()
  const visit = (name: string, chain: ReadonlyArray<string>): void => {
    const s = state.get(name)
    if (s === "done") return
    if (s === "visiting")
      throw new CodegenError(
        `recursive definitions are not supported: ${[...chain, name].join(" -> ")}`,
      )
    const schema = defs[name]
    if (schema === undefined) throw new CodegenError(`unknown definition ${name}`)
    state.set(name, "visiting")
    const refs = new Set<string>()
    refsOf(schema, refs)
    for (const r of [...refs].toSorted()) visit(r, [...chain, name])
    state.set(name, "done")
    order.push(name)
  }
  for (const name of Object.keys(defs).toSorted()) visit(name, [])
  return order
}

export const generate = (doc: Document): string => {
  const lines: Array<string> = []
  lines.push("// GENERATED FILE. Do not edit.")
  lines.push("// Source: schema/protocol.schema.json (exported from crates/orbit-protocol).")
  lines.push("// Regenerate with `pnpm codegen`; CI fails when this file is stale.")
  lines.push("")
  lines.push('import { Schema } from "effect"')
  lines.push("")
  lines.push(
    `export const INTERNAL_PROTOCOL_VERSION = ${doc["x-internal-protocol-version"]} as const`,
  )
  lines.push(
    `export const SYNC_SCHEMA_FORMAT_VERSION = ${doc["x-sync-schema-format-version"]} as const`,
  )
  lines.push("")
  lines.push(
    "/** Any JSON value. Column cells are typed per table by the sync schema, not here. */",
  )
  lines.push(
    "export type JsonValue = string | number | boolean | null | ReadonlyArray<JsonValue> | { readonly [key: string]: JsonValue }",
  )
  lines.push("export const JsonValue: Schema.Codec<JsonValue, JsonValue> = Schema.Union([")
  lines.push("  Schema.String,")
  lines.push("  Schema.Finite,")
  lines.push("  Schema.Boolean,")
  lines.push("  Schema.Null,")
  lines.push("  Schema.Array(Schema.suspend((): Schema.Codec<JsonValue, JsonValue> => JsonValue)),")
  lines.push(
    "  Schema.Record(Schema.String, Schema.suspend((): Schema.Codec<JsonValue, JsonValue> => JsonValue)),",
  )
  lines.push("])")
  lines.push("")
  lines.push("export const Int = Schema.Finite.check(Schema.isInt())")
  lines.push(
    "export const NonNegativeInt = Schema.Finite.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0))",
  )
  lines.push("")
  for (const name of topoSort(doc.$defs)) {
    const schema = doc.$defs[name]
    if (schema === undefined) continue
    const comment = isObjectSchema(schema) ? docComment(schema.description, "") : ""
    const expr = emitExpr(schema, name, "")
    lines.push(`${comment}export const ${name} = ${expr}`)
    lines.push(`export type ${name} = typeof ${name}.Type`)
    lines.push("")
  }
  lines.push(`export const PROTOCOL_ROOTS = [${doc["x-roots"].map(quote).join(", ")}] as const`)
  lines.push("")
  return lines.join("\n")
}
