import { describe, expect, test } from "bun:test"
import type { Client, Pool, PoolClient } from "pg"
import {
  postgresDocumentGraph,
  postgresTransactionClient,
  type PostgresDocumentGraphConfig,
  type PostgresQueryClient,
} from "../src/postgres.js"

const makeMinimalClient = (): PostgresQueryClient =>
  postgresTransactionClient({
    query: () => Promise.resolve({ rows: [] }),
  })

const configWithTransaction = (
  transaction: Client | PoolClient | PostgresQueryClient,
): PostgresDocumentGraphConfig => ({ transaction })

describe("PostgresDocumentGraphConfig structural clients", () => {
  test("accepts an opted-in structural transaction client", () => {
    const config: PostgresDocumentGraphConfig = {
      transaction: makeMinimalClient(),
    }
    expect(config.schema).toBeUndefined()
  })

  test("keeps accepting pg clients structurally", () => {
    const pgLike = {
      query: (text: string, values?: ReadonlyArray<unknown>) =>
        Promise.resolve({ text, values, rows: [] }),
    }
    expect(
      configWithTransaction(postgresTransactionClient(pgLike)).pool,
    ).toBeUndefined()
  })

  test("rejects raw query objects and pools as transaction clients", () => {
    const rawQueryClient = {
      query: () => Promise.resolve({ rows: [] }),
    }
    const rejectRawQueryClient = () => {
      const invalid: PostgresDocumentGraphConfig = {
        // @ts-expect-error Structural clients must explicitly opt in.
        transaction: rawQueryClient,
      }
      return invalid
    }
    const rejectPool = (pool: Pool) => {
      // @ts-expect-error Pool queries are not pinned to one transaction.
      const invalid: PostgresDocumentGraphConfig = { transaction: pool }
      return invalid
    }

    expect(rejectRawQueryClient).toBeDefined()
    expect(rejectPool).toBeDefined()
  })

  test("composes through the public postgres entry point", () => {
    const live = postgresDocumentGraph({ transaction: makeMinimalClient() })
    expect(live).toBeDefined()
  })
})
