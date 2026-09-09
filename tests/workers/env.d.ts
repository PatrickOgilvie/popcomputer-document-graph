/// <reference types="@cloudflare/workers-types" />
/// <reference types="@cloudflare/vitest-pool-workers/types" />

declare global {
  namespace Cloudflare {
    interface Env {
      readonly WORKSPACE_DB: D1Database
      readonly TEST_MIGRATIONS: ReadonlyArray<{
        readonly name: string
        readonly queries: ReadonlyArray<string>
      }>
    }
  }
}

export {}
