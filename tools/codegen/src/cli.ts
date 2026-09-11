import * as fs from "node:fs"
import * as path from "node:path"
import { fileURLToPath } from "node:url"

import { Schema } from "effect"

import { Document, generate } from "./generate.ts"

const here = path.dirname(fileURLToPath(import.meta.url))
const repoRoot = path.resolve(here, "../../..")
const schemaPath = path.join(repoRoot, "schema/protocol.schema.json")
const outPath = path.join(repoRoot, "packages/protocol/src/generated/protocol.gen.ts")

/** Reads and decodes the exporter's document. The shape is fixed by the Rust exporter. */
const readDocument = (): Document =>
  Schema.decodeSync(Schema.fromJsonString(Document))(fs.readFileSync(schemaPath, "utf8"))

const main = (): void => {
  const mode = process.argv[2]
  const generated = generate(readDocument())
  if (mode === "generate") {
    fs.mkdirSync(path.dirname(outPath), { recursive: true })
    fs.writeFileSync(outPath, generated)
    process.stdout.write(`wrote ${path.relative(repoRoot, outPath)}\n`)
    return
  }
  if (mode === "check") {
    const current = fs.existsSync(outPath) ? fs.readFileSync(outPath, "utf8") : ""
    if (current !== generated) {
      process.stderr.write(`${path.relative(repoRoot, outPath)} is stale. Run \`pnpm codegen\`.\n`)
      process.exit(1)
    }
    process.stdout.write("generated protocol is up to date\n")
    return
  }
  process.stderr.write("usage: cli.ts generate|check\n")
  process.exit(2)
}

main()
