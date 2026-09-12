import { describe, expect, test } from "bun:test"
import { readFile } from "node:fs/promises"
import { Deferred, Effect, Schema } from "effect"
import { Pool, type PoolClient } from "pg"
import { makeDocumentKey } from "../src/document/document-identity.js"
import { GraphNeighbourLimitSchema } from "../src/graph/graph-relation.js"
import { GraphTopologyStore } from "../src/graph/graph-topology.js"
import {
  postgresDocumentGraph,
  postgresTransactionClient,
  type PostgresQueryClient,
} from "../src/postgres.js"

const databaseUrl = Bun.env.TEST_DATABASE_URL ??
  Bun.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE

const runIntegrationTests = Bun.env.RUN_DOCUMENT_GRAPH_POSTGRES_TESTS === "true" &&
  databaseUrl !== undefined

const graph = "topology-concurrency"

const key = (id: string) => makeDocumentKey({
  graph,
  documentKind: "Article",
  encodedId: id,
})

const replacement = (id: string, targets: ReadonlyArray<string>) => ({
  graph,
  sourceDocumentKey: key(id),
  source: { graph, kind: "Article", id },
  relations: [{
    id: "cites",
    version: "v1",
    targetDocumentKind: "Article",
    targets: targets.map((target) => ({
      documentKey: key(target),
      reference: { graph, kind: "Article", id: target },
    })),
  }],
})

const replace = (
  transaction: PoolClient | PostgresQueryClient,
  schema: string,
  id: string,
  targets: ReadonlyArray<string>,
) => Effect.runPromise(GraphTopologyStore.pipe(
  Effect.flatMap((store) => store.replaceDocumentTopology(replacement(id, targets))),
  Effect.provide(postgresDocumentGraph({ transaction, schema })),
))

const related = (pool: Pool, schema: string, id: string) =>
  Effect.runPromise(GraphTopologyStore.pipe(
    Effect.flatMap((store) => store.findRelatedNodes({
      graph,
      documentKeys: [key(id)],
      documentKind: "Article",
      direction: "outgoing",
      relation: "cites",
      relationVersion: "v1",
      relatedDocumentKind: "Article",
      limit: Schema.decodeSync(GraphNeighbourLimitSchema)(10),
    }).pipe(Effect.map((groups) => groups.flatMap((group) => group.nodes)))),
    Effect.provide(postgresDocumentGraph({ pool, schema })),
  ))

const withDatabase = async (
  run: (pool: Pool, schema: string) => Promise<void>,
) => {
  const pool = new Pool({ connectionString: databaseUrl, max: 4 })
  const schema = `topology_${crypto.randomUUID().replaceAll("-", "")}`

  try {
    for (const file of [
      "0001_initial.sql",
      "0002_mutation_locks.sql",
      "0003_graph_topology.sql", "0004_native_vector_eligibility.sql",
    ]) {
      const sql = await readFile(
        new URL(`../migrations/postgres/${file}`, import.meta.url), "utf8",
      )

      await pool.query(sql.replaceAll('"honertia_document_graph"', `"${schema}"`))
    }

    await run(pool, schema)
  } finally {
    await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
    await pool.end()
  }
}

describe("PostgreSQL concurrent topology", () => {
  test.skipIf(!runIntegrationTests)("orphan cleanup preserves a target held by a concurrent publisher", async () => {
    await withDatabase(async (pool, schema) => {
      const removing = await pool.connect()
      const adding = await pool.connect()
      const reachedCleanup = Deferred.makeUnsafe<void>()
      const continueCleanup = Deferred.makeUnsafe<void>()

      try {
        await removing.query("BEGIN")
        await replace(removing, schema, "A", ["B"])
        await removing.query("COMMIT")
        await removing.query("BEGIN")
        await removing.query("SET LOCAL lock_timeout = 1000")
        await adding.query("BEGIN")

        const controlled = postgresTransactionClient({
          query: async (sql, values) => {
            if (sql.includes("AS node")) {
              Effect.runSync(Deferred.succeed(reachedCleanup, undefined))
              await Effect.runPromise(Deferred.await(continueCleanup))
            }

            return removing.query(sql, values === undefined ? undefined : [...values])
          },
        })

        const removal = replace(controlled, schema, "A", [])
        // Observe both failures and success while the publisher keeps B locked.
        const removalOutcome = Promise.allSettled([removal])
        await Effect.runPromise(Deferred.await(reachedCleanup))
        await replace(adding, schema, "C", ["B"])
        Effect.runSync(Deferred.succeed(continueCleanup, undefined))
        const [outcome] = await removalOutcome
        expect(outcome?.status).toBe("fulfilled")
        await removing.query("COMMIT")
        await adding.query("COMMIT")
        expect((await related(pool, schema, "C")).map((node) => node.reference.id))
          .toEqual(["B"])
        expect(await related(pool, schema, "A")).toEqual([])
      } finally {
        Effect.runSync(Deferred.succeed(continueCleanup, undefined))
        await removing.query("ROLLBACK")
        await adding.query("ROLLBACK")
        removing.release()
        adding.release()
      }
    })
  }, 15_000)

  test.skipIf(!runIntegrationTests)("reciprocal publications acquire node locks in the same order", async () => {
    await withDatabase(async (pool, schema) => {
      const first = await pool.connect()
      const second = await pool.connect()
      const [low, high] = ["A", "B"].sort((left, right) => key(left).localeCompare(key(right)))

      if (low === undefined || high === undefined) throw new Error("Missing topology fixtures")

      try {
        await first.query("BEGIN")
        await replace(first, schema, low, [])
        await replace(first, schema, high, [])
        await first.query("COMMIT")
        await first.query("BEGIN")
        await second.query("BEGIN")
        await first.query("SET LOCAL lock_timeout = 1000")
        await second.query("SET LOCAL statement_timeout = 5000")
        await first.query(`SELECT 1 FROM "${schema}".graph_nodes
          WHERE graph_id = $1 AND document_key = $2 FOR UPDATE`, [graph, key(low)])
        const pidRow = (await second.query("SELECT pg_backend_pid() AS pid")).rows[0]
        const { pid } = Schema.decodeUnknownSync(Schema.Struct({ pid: Schema.Number }))(pidRow)
        const secondOutcome = Promise.allSettled([replace(second, schema, high, [low])])
        // Synchronize on an actual database lock wait, never an assumed delay.
        let blocked = false

        for (let attempt = 0; attempt < 100 && !blocked; attempt += 1) {
          const row = (await pool.query(
            "SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked", [pid],
          )).rows[0]

          blocked = Schema.decodeUnknownSync(Schema.Struct({ blocked: Schema.Boolean }))(row).blocked
        }

        expect(blocked).toBe(true)
        await replace(first, schema, low, [high])
        await first.query("COMMIT")
        const [outcome] = await secondOutcome
        expect(outcome?.status).toBe("fulfilled")
        await second.query("COMMIT")
        expect((await related(pool, schema, low)).map((node) => node.reference.id)).toEqual([high])
        expect((await related(pool, schema, high)).map((node) => node.reference.id)).toEqual([low])
      } finally {
        await first.query("ROLLBACK")
        await second.query("ROLLBACK")
        first.release()
        second.release()
      }
    })
  }, 15_000)
})
