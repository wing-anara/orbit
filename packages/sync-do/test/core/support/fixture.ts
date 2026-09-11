import * as fs from "node:fs"
import * as path from "node:path"
import { fileURLToPath } from "node:url"

import { Schema } from "effect"
import {
  INTERNAL_PROTOCOL_VERSION,
  SyncSchema,
  type CdcBatch,
  type PartitionTransaction,
  type RowChange,
} from "@orbit/protocol"
import type { RowImage } from "@orbit/protocol/client"

import { SyncEngine, type EngineDeps } from "../../../src/core/engine.ts"
import { nodeDriver } from "./node-driver.ts"

const here = path.dirname(fileURLToPath(import.meta.url))

export const fixtureSchema = (): SyncSchema => {
  const text = fs.readFileSync(
    path.resolve(here, "../../../../../schema/fixtures/SyncSchema.json"),
    "utf8",
  )
  return Schema.decodeUnknownSync(SyncSchema)(JSON.parse(text))
}

export const U1 = "a2523813-adbe-11f1-b19c-0a2250a7ed6c"

export const chatbot = (id: string, over: Partial<RowImage> = {}): RowImage => ({
  id,
  organizationId: "org_1",
  groupId: null,
  type: "DOCUMENT",
  displayOrder: null,
  contents: null,
  createdAt: "2026-01-01 00:00:00",
  score: null,
  big: null,
  price: null,
  blob: null,
  day: null,
  at: null,
  ...over,
})

export const organization = (id: string, over: Partial<RowImage> = {}): RowImage => ({
  id,
  name: "Acme",
  created_at: "2026-01-01 00:00:00",
  hipaa_enabled: false,
  ...over,
})

export const insert = (
  table: string,
  row: RowImage,
  keyColumns: ReadonlyArray<string> = ["id"],
): RowChange => ({ table, op: "insert", key: keyColumns.map((c) => row[c] ?? null), after: row })
export const update = (
  table: string,
  before: RowImage,
  after: RowImage,
  keyColumns: ReadonlyArray<string> = ["id"],
): RowChange => ({
  table,
  op: "update",
  key: keyColumns.map((c) => after[c] ?? null),
  before,
  after,
})
export const remove = (
  table: string,
  before: RowImage,
  keyColumns: ReadonlyArray<string> = ["id"],
): RowChange => ({ table, op: "delete", key: keyColumns.map((c) => before[c] ?? null), before })

export class TestClock {
  now = 1_700_000_000_000
  tick(): number {
    this.now += 1
    return this.now
  }
}

export const makeEngine = (
  partition = "org_1",
  schema = fixtureSchema(),
): {
  readonly engine: SyncEngine
  readonly deps: EngineDeps
  readonly driver: ReturnType<typeof nodeDriver>
} => {
  const driver = nodeDriver()
  const clock = new TestClock()
  let ids = 0
  const deps: EngineDeps = {
    driver,
    schema,
    partition,
    now: () => clock.tick(),
    newId: () => `id${++ids}`,
  }
  const engine = new SyncEngine(deps)
  engine.init()
  return { engine, deps, driver }
}

export const txn = (
  seq: number,
  changes: ReadonlyArray<RowChange>,
  gno = seq,
): PartitionTransaction => ({
  seq,
  keyspace: "ks",
  shard: "0",
  gtid: `${U1}:${gno}`,
  position: `MySQL56/${U1}:1-${gno}`,
  commit_timestamp: 1_789_000_000 + seq,
  changes: [...changes],
  trace: {},
})

export const batch = (
  schema: SyncSchema,
  partition: string,
  transactions: ReadonlyArray<PartitionTransaction>,
  epoch = 0,
): CdcBatch => ({
  protocol_version: INTERNAL_PROTOCOL_VERSION,
  schema_hash: schema.schema_hash,
  stream_epoch: epoch,
  partition,
  transactions: [...transactions],
  delivery_id: `d${transactions[0]?.seq ?? 0}`,
})
