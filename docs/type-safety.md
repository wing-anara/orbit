# Type safety

Orbit has one Rust source of truth for every message that crosses a process boundary. TypeScript code never hand-writes those shapes. This document describes the generation chain, the conformance fixtures, the shared schema hash, database introspection, and the strict Effect tooling.

Related documents: [protocol.md](protocol.md), [schema-evolution.md](schema-evolution.md), [queries.md](queries.md), [configuring-a-new-app.md](configuring-a-new-app.md).

## The generation chain

The chain has four steps. Each step is checked in CI.

1. Rust types in `crates/orbit-protocol` derive `schemars::JsonSchema`. The crate exports these roots: `SyncSchema`, `IntrospectedSchema`, `CdcBatch`, `CdcBatchAck`, `SourceTransaction`, `FillRequest`, `FillPollResponse`, `FillChunk`, `FillResult`, and `EngineError`.
2. The binary `orbit-protocol-schema` writes one JSON Schema document (draft 2020-12) to `schema/protocol.schema.json`. The document also carries `x-internal-protocol-version`, `x-sync-schema-format-version`, and `x-roots`.
3. `tools/codegen` reads that document and writes `packages/protocol/src/generated/protocol.gen.ts`. The output is Effect `Schema` code. Each definition becomes `export const Name = Schema...` and `export type Name = typeof Name.Type`.
4. The rest of the TypeScript workspace imports `@orbit/protocol`, which re-exports the generated file.

Regenerate both artifacts with these commands:

```sh
cargo run -p orbit-protocol --bin orbit-protocol-schema -- schema/protocol.schema.json
pnpm codegen
```

CI fails when either artifact is stale:

- The Rust job runs the exporter to a temporary file and runs `diff -u` against `schema/protocol.schema.json`. The unit test `checked_in_schema_is_current` in `crates/orbit-protocol/src/lib.rs` does the same check.
- The TypeScript job runs `pnpm codegen:check`. The command regenerates the file in memory and exits with status 1 when the text differs.

The generator is strict on purpose. It throws `CodegenError` for every construct it does not understand: a schema without a type, a type union other than `T | null`, an object with both `properties` and `additionalProperties`, or a recursive definition. A Rust type that cannot be represented faithfully fails generation instead of widening to `unknown`.

The generated file also defines `JsonValue`, `Int`, `NonNegativeInt`, `INTERNAL_PROTOCOL_VERSION`, `SYNC_SCHEMA_FORMAT_VERSION`, and `PROTOCOL_ROOTS`. The hand-written client protocol in `packages/protocol/src/client-protocol.ts` reuses `JsonValue`, `EngineError`, `TraceContext`, and `NonNegativeInt` from the generated file. The two halves cannot drift.

## Conformance fixtures

The Rust test `crates/orbit-protocol/tests/fixtures.rs` writes one JSON fixture per root type into `schema/fixtures/`. Enum roots (`CdcBatchAck`, `FillChunk`, `FillResult`, `EngineError`) are written as arrays that cover every variant. The test asserts that the checked-in files match the current Rust output. Regenerate them with:

```sh
UPDATE_FIXTURES=1 cargo test -p orbit-protocol --test fixtures
```

The TypeScript test `packages/protocol/test/conformance.test.ts` decodes every fixture with the generated Effect Schema and re-encodes it. The re-encoded value must equal the file. The test also asserts that `PROTOCOL_ROOTS` equals the fixture list, and that unknown variants such as `{ status: "maybe" }` are rejected. A Rust type change without regeneration fails here.

## The shared schema hash

The compiled sync schema carries `schema_hash`. Every runtime compares this value: the distributor, the Durable Object, and the browser. Rust and TypeScript compute it with the same algorithm:

1. Clone the artifact and set `schema_hash` to the empty string.
2. Serialize it as canonical JSON: sorted object keys, no whitespace.
3. Take the SHA-256 digest and render it as lowercase hex.

Rust implements this in `SyncSchema::compute_hash` and `canonical_json` in `crates/orbit-protocol/src/schema.rs`. TypeScript implements it in `computeSchemaHash` and `canonicalJson` in `packages/schema/src/compile.ts`.

The fixture `schema/fixtures/SyncSchema.hash` holds the hash of the Rust sample schema. The TypeScript test in `packages/schema/test/compile.test.ts` compiles the same definition, compares its hash with the fixture, and compares the artifact with `schema/fixtures/SyncSchema.json`. A divergence in canonicalization fails the test.

`compileSyncSchema` also round-trips the artifact through the generated `SyncSchema` codec. The artifact is therefore exactly what Rust accepts. On the Rust side, `orbit-server` calls `SyncSchema::validate` on load, which recomputes the hash and rejects a mismatch.

## Database introspection

The sync configuration is written against the real database schema. The command below reads `information_schema` through Vitess and writes a TypeScript module:

```sh
orbit-server schema introspect --keyspace orbit --out orbit/schema.introspected.ts
```

When the output path ends in `.ts`, the file exports `introspected` with `as const`. When the path has another extension, the command writes plain JSON. Use `--table` (repeatable) to limit the tables.

The `as const` literal gives `defineSyncSchema` exact table names, column names, nullability, and value kinds. The types in `packages/schema/src/define.ts` derive everything else:

- `TableNamesOf<I>` and `ColumnNamesOf<I, N>` restrict `partitionBy`, `columns`, and relation column lists.
- `KindToType<K>` maps a value kind to a TypeScript type. `bigint` and `decimal` are strings.
- `CellType<Col>` adds `| null` for nullable columns.
- `RowOf<D, N>` is the row type of a synced table. It contains the selected columns plus the primary key and the partition column.
- `ColumnsOf<D, N>`, `RelationNamesOf<D, N>`, `RelationTarget<D, N, R>`, and `RelationKindOf<D, N, R>` feed the query builder.

An application compiles its definition into `orbit.schema.json` with `compileSyncSchema` and commits the artifact. Add a CI check that recompiles the artifact and fails when the committed file is stale.

## Strict Effect tooling

The workspace uses `@effect/tsgo`. The root `postinstall` script runs `effect-tsgo patch`. Type checking runs with `tsc --noEmit` per package.

`tsconfig.base.json` enables `strict`, `exactOptionalPropertyTypes`, `noUncheckedIndexedAccess`, `noImplicitOverride`, `noImplicitReturns`, `noFallthroughCasesInSwitch`, `noPropertyAccessFromIndexSignature`, and `useUnknownInCatchVariables`. It also loads the `@effect/language-service` plugin with `diagnostics: true`. These diagnostics are errors:

- `floatingEffect`
- `missingStarInYieldEffectGen`
- `leakingRequirements`
- `outdatedApi`

`unnecessaryPipe` is a warning.

Linting runs `oxlint --type-aware .`. The configuration in `.oxlintrc.json` extends `@effect/tsgo/oxlint-presets/recommended.json`. These rules are errors: `no-explicit-any`, `no-non-null-assertion`, `no-unsafe-type-assertion`, `no-floating-promises`, `no-misused-promises`, `switch-exhaustiveness-check`, `import/no-cycle`, and `no-console`. Test files, end-to-end tests, and generated files have relaxed rules. Generated files, `schema.introspected.ts`, and `worker-configuration.d.ts` are ignored.

The CI job for TypeScript runs, in order: `pnpm codegen:check`, `pnpm typecheck`, `pnpm lint`, `pnpm format`, and `pnpm test`.

## Runtime validation

Static types stop at the process boundary. Every message is decoded at runtime:

- The Durable Object decodes each WebSocket message with `ClientMessage` and each CDC batch with `CdcBatch`.
- The browser decodes each server message with `ServerMessage`. A message that fails validation becomes a fatal `invalid_message` error, never a silent drop.
- Row images are decoded with a per-table codec built by `rowCodecFor` in `packages/schema/src/runtime.ts`. Unknown columns are an error (`onExcessProperty: "error"`).
- The SQLite worker RPC in `packages/client/src/driver.ts` validates requests and responses on both sides.
