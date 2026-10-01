import { describe, expect, test } from "bun:test"
import { readFile } from "node:fs/promises"
import { Effect, Option, Result } from "effect"
import { Pool } from "pg"
import { ProjectionPublicationCoordinator } from "../src/indexing/projection-publication.js"
import { postgresProjectionPublicationCoordinator } from "../src/storage/postgres/projection-publication.js"
import {
  describePublicationCoordinatorConformance,
  key,
  replacementIntent,
  type PublicationCoordinatorHarness,
} from "./support/projection-publication-coordinator-suite.js"

const databaseUrl = Bun.env.TEST_DATABASE_URL

const runIntegrationTests =
  Bun.env.RUN_DOCUMENT_GRAPH_POSTGRES_TESTS === "true" &&
  databaseUrl !== undefined

interface PostgresHarness extends PublicationCoordinatorHarness {
  readonly pool: Pool
  readonly schema: string
}

const makeHarness = async (): Promise<PostgresHarness> => {
  const pool = new Pool({ connectionString: databaseUrl, max: 8 })
  const schema = `publication_${crypto.randomUUID().replaceAll("-", "")}`

  const migration = await readFile(
    new URL("../migrations/postgres/0006_projection_publications.sql", import.meta.url),
    "utf8",
  )

  await pool.query(`CREATE SCHEMA "${schema}"`)
  await pool.query(migration.replaceAll('"honertia_document_graph"', `"${schema}"`))

  return {
    pool,
    schema,
    run: (effect, options = {}) => Effect.runPromise(effect.pipe(
      Effect.provide(postgresProjectionPublicationCoordinator({
        pool,
        schema,
        indexGeneration: options.indexGeneration ?? "schema-v1",
        publicationLeaseMilliseconds: 1_000,
        retainedPublicationHistory: options.retainedPublicationHistory,
      })),
    )),
    close: async () => {
      try {
        await pool.query(`DROP SCHEMA "${schema}" CASCADE`)
      } finally {
        await pool.end()
      }
    },
  }
}

describePublicationCoordinatorConformance("PostgreSQL", makeHarness, {
  skip: !runIntegrationTests,
})

const postgresTest = runIntegrationTests ? test : test.skip

const count = async (
  harness: PostgresHarness,
  table: string,
  where: string,
  values: ReadonlyArray<unknown>,
): Promise<number> => {
  const result = await harness.pool.query<{ readonly count: string }>(
    `SELECT count(*)::text AS count FROM "${harness.schema}"."${table}" WHERE ${where}`,
    [...values],
  )

  return Number(result.rows[0]?.count)
}

describe("PostgreSQL publication journal", () => {
  postgresTest("lets exactly one of two concurrent writers take a projection", async () => {
    const harness = await makeHarness()

    try {
      const intents = ["a", "b", "c", "d"].map((digest) => replacementIntent({
        mutation: `replace-race-${digest}`,
        digestCharacter: digest,
        token: `token-race-${digest}`,
      }))

      const results = await harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator

        return yield* Effect.all(
          intents.map((intent) => coordinator.beginPublication(intent).pipe(Effect.result)),
          { concurrency: "unbounded" },
        )
      }))

      const leases = results.filter((result) =>
        Result.isSuccess(result) && result.success._tag === "Publish")

      expect(leases).toHaveLength(1)
      expect(results.filter(Result.isFailure).map((result) => result.failure))
        .toEqual(Array.from({ length: 3 }, () => expect.objectContaining({
          reason: "publication_in_progress",
        })))
      expect(await count(harness, "projection_mutations", "true", [])).toBe(1)
    } finally {
      await harness.close()
    }
  })

  postgresTest("keeps rejected staging and retired journal entries out of storage", async () => {
    const harness = await makeHarness()

    try {
      const first = replacementIntent({ mutation: "replace-kept", digestCharacter: "1", token: "token-kept" })
      const rejected = replacementIntent({ mutation: "replace-rejected", digestCharacter: "2", token: "token-rejected" })

      const second = replacementIntent({
        mutation: "replace-latest",
        digestCharacter: "3",
        token: "token-latest",
        expectedToken: "token-kept",
      })

      const lookup = await harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const begun = yield* coordinator.beginPublication(first)
        if (begun._tag !== "Publish") return yield* Effect.die("No lease")
        yield* coordinator.beginPublication(rejected).pipe(Effect.result)
        yield* coordinator.finalizePublication(begun.lease)
        const next = yield* coordinator.beginPublication(second)
        if (next._tag !== "Publish") return yield* Effect.die("No lease")
        yield* coordinator.finalizePublication(next.lease)
        const [head] = yield* coordinator.loadHeads([key])

        return head
      }), { retainedPublicationHistory: 1 })

      expect(Option.getOrThrow(lookup?.head ?? Option.none()).active)
        .toEqual({ _tag: "Revision", token: second.snapshot.token })
      expect(await count(harness, "projection_mutations", "mutation_id = $1", [rejected.mutationId])).toBe(0)
      expect(await count(harness, "projection_mutations", "mutation_id = $1", [first.mutationId])).toBe(0)
      expect(await count(harness, "projection_mutation_chunks", "mutation_id = $1", [first.mutationId])).toBe(0)
      expect(await count(harness, "projection_publications", "true", [])).toBe(1)
    } finally {
      await harness.close()
    }
  })
})
