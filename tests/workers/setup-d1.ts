/// <reference types="./env.d.ts" />

import { applyD1Migrations } from "cloudflare:test"
import { env } from "cloudflare:workers"

await applyD1Migrations(
  env.WORKSPACE_DB,
  env.TEST_MIGRATIONS.map((migration) => ({
    name: migration.name,
    queries: [...migration.queries],
  })),
)
