import path from "node:path"
import {
  cloudflareTest,
  readD1Migrations,
} from "@cloudflare/vitest-pool-workers"
import { defineConfig } from "vitest/config"

export default defineConfig(async () => {
  const migrations = await readD1Migrations(
    path.join(import.meta.dirname, "migrations", "d1"),
  )

  return {
    plugins: [
      cloudflareTest({
        miniflare: {
          d1Databases: ["WORKSPACE_DB"],
          bindings: { TEST_MIGRATIONS: migrations },
        },
      }),
    ],
    test: {
      include: ["tests/workers/**/*.worker.ts"],
      setupFiles: ["./tests/workers/setup-d1.ts"],
    },
  }
})
