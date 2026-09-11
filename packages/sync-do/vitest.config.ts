import { cloudflareTest } from "@cloudflare/vitest-plugin"
import { defineConfig } from "vitest/config"

/**
 * Two projects:
 * * `core`: the engine core (apply, IVM, fills) on Node's built-in SQLite. Fast; used for the
 *   randomized incremental-vs-recompute tests.
 * * `workers`: the real Durable Object in workerd with SQLite storage, WebSockets and alarms.
 */
export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "core",
          include: ["test/core/**/*.test.ts"],
        },
      },
      {
        plugins: [cloudflareTest({ wrangler: { configPath: "./wrangler.jsonc" } })],
        test: {
          name: "workers",
          include: ["test/workers/**/*.test.ts"],
        },
      },
    ],
  },
})
