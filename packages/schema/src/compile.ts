/**
 * Compiles a definition into the shared `SyncSchema` artifact and computes the same hash as the
 * Rust crate (SHA-256 over canonical JSON with `schema_hash` blank).
 */

import { Data, Effect, Schema } from "effect"
import {
  SYNC_SCHEMA_FORMAT_VERSION,
  SyncSchema,
  type ColumnSchema,
  type RelationSchema,
  type TableSchema,
} from "@orbit/protocol"

import type { DerivedRule, IntrospectedShape, SyncSchemaDefinition } from "./define.ts"

export class SchemaCompileError extends Data.TaggedError("SchemaCompileError")<{
  readonly problems: ReadonlyArray<string>
}> {
  override get message(): string {
    return `sync schema is invalid:\n${this.problems.map((p) => `  - ${p}`).join("\n")}`
  }
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value)

/** Canonical JSON: sorted object keys, no whitespace. Mirrors `orbit_protocol::schema::canonical_json`. */
export const canonicalJson = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`
  if (isRecord(value)) {
    const entries = Object.entries(value).toSorted(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`).join(",")}}`
  }
  return JSON.stringify(value)
}

const sha256Hex = (text: string): Effect.Effect<string> =>
  Effect.promise(async () => {
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text))
    return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("")
  })

/** Hash of an artifact, ignoring its `schema_hash` field. */
export const computeSchemaHash = (artifact: SyncSchema): Effect.Effect<string> =>
  sha256Hex(canonicalJson({ ...artifact, schema_hash: "" }))

/** Structural checks that need no database access. Returns every problem found. */
const structuralProblems = <I extends IntrospectedShape, T>(
  def: SyncSchemaDefinition<I, T>,
): ReadonlyArray<string> => {
  const problems: Array<string> = []
  const cfg = def.config
  const introspectedTables = new Map(cfg.introspected.tables.map((t) => [t.name, t] as const))
  const tables = Object.entries(cfg.tables) as ReadonlyArray<
    readonly [
      string,
      (
        | {
            partitionBy: string
            partitionVia?: string
            columns?: ReadonlyArray<string>
            derived?: Record<string, { from: string; rule: DerivedRule }>
            relations?: Record<
              string,
              {
                kind: "one" | "many"
                to: string
                from: ReadonlyArray<string>
                toColumns: ReadonlyArray<string>
              }
            >
          }
        | undefined
      ),
    ]
  >
  if (tables.length === 0) problems.push("no tables are configured")
  for (const [name, tcfg] of tables) {
    if (tcfg === undefined) continue
    const it = introspectedTables.get(name)
    if (it === undefined) {
      problems.push(`table ${name} is not in the introspected schema`)
      continue
    }
    const columns = new Map(it.columns.map((c) => [c.name, c] as const))
    if (it.primary_key.length === 0)
      problems.push(`table ${name} has no primary key; every synced table needs one`)
    const pcol = columns.get(tcfg.partitionBy)
    if (pcol === undefined)
      problems.push(`table ${name}: partition column ${tcfg.partitionBy} does not exist`)
    else if (tcfg.partitionVia === undefined) {
      if (pcol.kind !== cfg.partition.kind)
        problems.push(
          `table ${name}: partition column ${tcfg.partitionBy} has kind ${pcol.kind}, expected ${cfg.partition.kind}`,
        )
    } else {
      const parentCfg = cfg.tables[tcfg.partitionVia as keyof typeof cfg.tables]
      const parent = introspectedTables.get(tcfg.partitionVia)
      if (parentCfg === undefined || parent === undefined)
        problems.push(`table ${name}: partitionVia ${tcfg.partitionVia} is not a synced table`)
      else if (parentCfg.partitionVia !== undefined)
        problems.push(
          `table ${name}: partitionVia ${tcfg.partitionVia} is itself derived; only one level is supported`,
        )
      else if (parent.primary_key.length !== 1)
        problems.push(
          `table ${name}: partitionVia ${tcfg.partitionVia} must have a single-column primary key`,
        )
      else {
        const parentKey = parent.columns.find((c) => c.name === parent.primary_key[0])
        if (parentKey !== undefined && parentKey.kind !== pcol.kind)
          problems.push(
            `table ${name}: partition column ${tcfg.partitionBy} has kind ${pcol.kind}, but ${tcfg.partitionVia}.${parentKey.name} has kind ${parentKey.kind}`,
          )
      }
    }
    for (const c of tcfg.columns ?? [])
      if (!columns.has(c)) problems.push(`table ${name}: column ${c} does not exist`)
    for (const [dname, d] of Object.entries(tcfg.derived ?? {})) {
      if (columns.has(dname))
        problems.push(`table ${name}: derived column ${dname} has the name of a source column`)
      if (!columns.has(d.from))
        problems.push(`table ${name}: derived column ${dname} reads unknown column ${d.from}`)
      if (it.primary_key.includes(dname) || dname === tcfg.partitionBy)
        problems.push(`table ${name}: derived column ${dname} cannot be a key column`)
    }
    for (const pk of it.primary_key) {
      const col = columns.get(pk)
      if (col?.kind === "json" || col?.kind === "float")
        problems.push(`table ${name}: primary key column ${pk} has unsupported kind ${col.kind}`)
    }
    for (const [rname, rel] of Object.entries(tcfg.relations ?? {})) {
      const target =
        cfg.tables[rel.to as keyof typeof cfg.tables] === undefined
          ? undefined
          : introspectedTables.get(rel.to)
      if (target === undefined) {
        problems.push(
          `table ${name}: relation ${rname} targets ${rel.to}, which is not a synced table`,
        )
        continue
      }
      if (rel.from.length === 0 || rel.from.length !== rel.toColumns.length)
        problems.push(`table ${name}: relation ${rname} has mismatched column lists`)
      for (const c of rel.from)
        if (!columns.has(c))
          problems.push(`table ${name}: relation ${rname} uses unknown column ${c}`)
      for (const c of rel.toColumns)
        if (!target.columns.some((tc) => tc.name === c))
          problems.push(
            `table ${name}: relation ${rname} uses unknown target column ${rel.to}.${c}`,
          )
      const pkSide =
        rel.kind === "one"
          ? { table: rel.to, cols: rel.toColumns, pk: target.primary_key }
          : { table: name, cols: rel.from, pk: it.primary_key }
      if (
        pkSide.cols.length !== pkSide.pk.length ||
        pkSide.cols.some((c, i) => c !== pkSide.pk[i])
      ) {
        problems.push(
          `table ${name}: relation ${rname} must reference the full primary key of ${pkSide.table} (${pkSide.pk.join(", ")})`,
        )
      }
    }
  }
  return problems
}

const toArtifact = <I extends IntrospectedShape, T>(
  def: SyncSchemaDefinition<I, T>,
): SyncSchema => {
  const cfg = def.config
  const synced = new Set(Object.keys(cfg.tables))
  const tables: Array<TableSchema> = []
  for (const it of cfg.introspected.tables) {
    const tcfg = (
      cfg.tables as Record<
        string,
        | {
            partitionBy: string
            partitionVia?: string
            columns?: ReadonlyArray<string>
            derived?: Record<string, { from: string; rule: DerivedRule }>
            relations?: Record<
              string,
              {
                kind: "one" | "many"
                to: string
                from: ReadonlyArray<string>
                toColumns: ReadonlyArray<string>
              }
            >
          }
        | undefined
      >
    )[it.name]
    if (tcfg === undefined || !synced.has(it.name)) continue
    const always = new Set([...it.primary_key, tcfg.partitionBy])
    const selected =
      tcfg.columns === undefined
        ? new Set(it.columns.map((c) => c.name))
        : new Set([...tcfg.columns, ...always])
    const columns: Array<ColumnSchema> = it.columns
      .filter((c) => selected.has(c.name))
      .map((c) => ({
        name: c.name,
        kind: c.kind,
        nullable: c.nullable,
        source_type: c.column_type,
        ...(c.enum_values === undefined ? {} : { enum_values: [...c.enum_values] }),
      }))
    // Derived columns come last, as plain non-nullable bools with the rule the engine applies.
    for (const [dname, d] of Object.entries(tcfg.derived ?? {}))
      columns.push({
        name: dname,
        kind: "bool",
        nullable: false,
        source_type: "derived",
        derived: { from: d.from, rule: d.rule },
      })
    const relations: Array<RelationSchema> = Object.entries(tcfg.relations ?? {}).map(
      ([rname, rel]) => ({
        name: rname,
        kind: rel.kind,
        target_table: rel.to,
        from_columns: [...rel.from],
        to_columns: [...rel.toColumns],
      }),
    )
    tables.push({
      name: it.name,
      primary_key: [...it.primary_key],
      partition_column: tcfg.partitionBy,
      // Present only for derived partitions, so artifacts without them keep their hash.
      ...(tcfg.partitionVia === undefined ? {} : { partition_parent: tcfg.partitionVia }),
      columns,
      relations,
    })
  }
  // Order: tables referenced by `one` relations first, so parents precede children.
  const order = topoOrder(tables)
  return {
    format_version: SYNC_SCHEMA_FORMAT_VERSION,
    schema_hash: "",
    app: cfg.app,
    keyspace: cfg.introspected.keyspace,
    partition: {
      name: cfg.partition.name,
      key_kind: cfg.partition.kind,
      placement: { strategy: "one_per_partition", version: cfg.placementVersion ?? 1 },
    },
    tables: order,
  }
}

const topoOrder = (tables: ReadonlyArray<TableSchema>): Array<TableSchema> => {
  const byName = new Map(tables.map((t) => [t.name, t] as const))
  const out: Array<TableSchema> = []
  const seen = new Set<string>()
  const visit = (t: TableSchema, stack: ReadonlySet<string>): void => {
    if (seen.has(t.name)) return
    if (stack.has(t.name)) {
      // Self or cyclic references (a folder tree) are fine; they just cannot dictate order.
      return
    }
    const next = new Set(stack).add(t.name)
    for (const r of t.relations) {
      if (r.kind === "one") {
        const target = byName.get(r.target_table)
        if (target !== undefined && target.name !== t.name) visit(target, next)
      }
    }
    seen.add(t.name)
    out.push(t)
  }
  for (const t of tables.toSorted((a, b) => (a.name < b.name ? -1 : 1))) visit(t, new Set())
  return out
}

/**
 * Compiles a definition into a validated artifact with its hash. Fails with every problem found.
 */
export const compileSyncSchema = <I extends IntrospectedShape, T>(
  def: SyncSchemaDefinition<I, T>,
): Effect.Effect<SyncSchema, SchemaCompileError> =>
  Effect.gen(function* () {
    const problems = structuralProblems(def)
    if (problems.length > 0) return yield* new SchemaCompileError({ problems })
    const draft = toArtifact(def)
    const schema_hash = yield* computeSchemaHash(draft)
    const artifact = { ...draft, schema_hash }
    // Round-trip through the generated codec so the artifact is exactly what Rust accepts.
    const encoded = yield* Schema.encodeEffect(SyncSchema)(artifact).pipe(
      Effect.mapError((e) => new SchemaCompileError({ problems: [String(e)] })),
    )
    return yield* Schema.decodeEffect(SyncSchema)(encoded).pipe(
      Effect.mapError((e) => new SchemaCompileError({ problems: [String(e)] })),
    )
  })
