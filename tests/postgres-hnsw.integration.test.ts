import { expect, test } from "bun:test"
import { readFile } from "node:fs/promises"
import { Client } from "pg"
import { Effect, Schema } from "effect"
import {
  postgresDocumentGraph,
  postgresTransactionClient,
  postgresVectorIndexSql,
  type PostgresApproximateVectorSearch,
} from "../src/postgres.js"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import { makeChunkId, makeDocumentKey } from "../src/document/document-identity.js"
import {
  makeGraphSearchScope,
  ProjectionSearchStore,
  SearchResultCountSchema,
  type GraphSearchScope,
} from "../src/retrieval/graph-retrieval.js"

const databaseUrl = Bun.env.TEST_DATABASE_URL

const enabled = Bun.env.RUN_DOCUMENT_GRAPH_PGVECTOR_TESTS === "true" && databaseUrl !== undefined

const dimensions = 64

/** Deterministic unit vectors so recall is reproducible. */
const unitVectors = (count: number, seed: number): Array<Array<number>> => {
  let state = seed

  const next = () => {
    state = (state * 1_103_515_245 + 12_345) % 2_147_483_648

    return state / 2_147_483_648 - 0.5
  }

  return Array.from({ length: count }, () => {
    const vector = Array.from({ length: dimensions }, next)
    const norm = Math.hypot(...vector)

    return vector.map((component) => component / norm)
  })
}

// Requires pgvector 0.8+ (halfvec and iterative index scans) on the server.
// The extension, real migrations, index and fixtures all roll back.
test.skipIf(!enabled)("approximate search uses the HNSW index, preserves scope and matches exhaustive results", async () => {
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 5_000 })
  await client.connect()

  try {
    await client.query("BEGIN")
    await client.query("SET LOCAL statement_timeout = 60000")
    const schema = `hnsw_${crypto.randomUUID().replaceAll("-", "")}`
    const vectorSchema = `vectors_${crypto.randomUUID().replaceAll("-", "")}`
    await client.query(`CREATE SCHEMA "${vectorSchema}"`)
    await client.query(`CREATE EXTENSION vector WITH SCHEMA "${vectorSchema}"`)

    for (const file of ["0001_initial.sql", "0002_mutation_locks.sql", "0003_graph_topology.sql", "0004_native_vector_eligibility.sql", "0005_native_halfvec_eligibility.sql"]) {
      const migration = await readFile(new URL(`../migrations/postgres/${file}`, import.meta.url), "utf8")
      await client.query(migration.replaceAll('"honertia_document_graph"', `"${schema}"`))
    }

    // 1,600 evidence and 400 profile chunks in the searched graph, plus a second
    // graph whose chunks duplicate the first evidence vectors exactly.
    const fixtures = [
      ...unitVectors(1_600, 7).map((vector, index) => ({ graph: "hnsw-test", projection: "evidence", id: index, vector })),
      ...unitVectors(400, 11).map((vector, index) => ({ graph: "hnsw-test", projection: "profile", id: 10_000 + index, vector })),
      ...unitVectors(200, 7).map((vector, index) => ({ graph: "other-graph", projection: "evidence", id: 20_000 + index, vector })),
    ]

    for (const fixture of fixtures) {
      const documentKey = makeDocumentKey({ graph: fixture.graph, documentKind: "Work", encodedId: fixture.id })
      await client.query(`INSERT INTO "${schema}".projected_revisions
        (document_key, projection_id, graph_id, document_kind, encoded_document_id,
         projection_version, revision_hash, embedding_profile_id, embedding_profile_version, embedding_dimensions)
        VALUES ($1, $2, $3, 'Work', $4, 'v1', $5, 'hnsw-profile', 'v1', $6)`,
      [documentKey, fixture.projection, fixture.graph, JSON.stringify(fixture.id), "a".repeat(64), dimensions])
      await client.query(`INSERT INTO "${schema}".projected_chunks
        (chunk_id, document_key, projection_id, ordinal, section_key, section_index, section_part,
         content_hash, content, embedding_content, text_content, has_metadata, embedding_dimensions, embedding)
        VALUES ($1, $2, $3, 0, 'body', 0, 0, $4, 'Evidence', 'Evidence', 'Evidence', false, $5, $6)`,
      [makeChunkId({ documentKey, projection: fixture.projection, sectionKey: "body", sectionPart: 0 }), documentKey, fixture.projection, "b".repeat(64), dimensions, fixture.vector])
    }

    const index = "projected_chunks_embedding_hnsw"
    await client.query(postgresVectorIndexSql({ index, dimensions, schema, vectorSchema, concurrently: false }))

    // A native-eligible vector outside halfvec's range is still writable once the index exists.
    const outOfRange = makeDocumentKey({ graph: "hnsw-test", documentKind: "Work", encodedId: "out-of-range" })
    await client.query(`INSERT INTO "${schema}".projected_revisions
      (document_key, projection_id, graph_id, document_kind, encoded_document_id,
       projection_version, revision_hash, embedding_profile_id, embedding_profile_version, embedding_dimensions)
      VALUES ($1, 'evidence', 'hnsw-test', 'Work', '"out-of-range"', 'v1', $2, 'hnsw-profile', 'v1', $3)`,
    [outOfRange, "a".repeat(64), dimensions])
    await client.query(`INSERT INTO "${schema}".projected_chunks
      (chunk_id, document_key, projection_id, ordinal, section_key, section_index, section_part,
       content_hash, content, embedding_content, text_content, has_metadata, embedding_dimensions, embedding)
      VALUES ($1, $2, 'evidence', 0, 'body', 0, 0, $3, 'Evidence', 'Evidence', 'Evidence', false, $4, $5)`,
    [makeChunkId({ documentKey: outOfRange, projection: "evidence", sectionKey: "body", sectionPart: 0 }), outOfRange, "b".repeat(64), dimensions, Array.from({ length: dimensions }, () => 70_000)])

    // At fixture scale the planner would rightly prefer a sequential scan; production scale
    // prefers the index on its own. Disabling it here makes these assertions exercise HNSW.
    await client.query("ANALYZE")
    await client.query("SET LOCAL enable_seqscan = off")

    const statements: Array<{ readonly text: string; readonly values: ReadonlyArray<unknown> }> = []

    const transaction = postgresTransactionClient({ query: (text, values) => {
      statements.push({ text, values: values ?? [] })

      return client.query(text, values === undefined ? undefined : [...values])
    } })

    const approximate: PostgresApproximateVectorSearch = { mode: "approximate", index, dimensions }
    const candidates = Schema.decodeSync(SearchResultCountSchema)(24)

    const search = (vectorSearch: "float64" | PostgresApproximateVectorSearch, vector: ReadonlyArray<number>, scope: GraphSearchScope) =>
      Effect.runPromise(ProjectionSearchStore.pipe(
        Effect.flatMap((store) => store.searchCandidates({
          vector,
          embeddingProfile: defineEmbeddingProfile({ id: "hnsw-profile", version: "v1", dimensions }),
          scope,
          candidates,
        })),
        Effect.provide(postgresDocumentGraph({ transaction, schema, vectorSearch, searchTimeoutMilliseconds: 30_000 })),
      ))

    const everything = makeGraphSearchScope("hnsw-test", {})
    const profiles = makeGraphSearchScope("hnsw-test", { includeProjections: ["profile"] })
    let overlap = 0
    let compared = 0

    for (const query of unitVectors(20, 97)) {
      const exhaustive = await search("float64", query, everything)
      statements.length = 0
      const indexed = await search(approximate, query, everything)
      expect(statements.some((statement) => statement.text.includes("nearest AS MATERIALIZED"))).toBe(true)
      expect(indexed).toHaveLength(24)
      expect(indexed.every((row) => row.reference.graph === "hnsw-test")).toBe(true)

      // Shared candidates carry the exhaustive float64 scores exactly.
      for (const row of indexed) {
        const reference = exhaustive.find((candidate) => candidate.chunkId === row.chunkId)
        if (reference !== undefined) expect(row.score).toBe(reference.score)
      }

      const expected = new Set(exhaustive.map((row) => row.chunkId))
      overlap += indexed.filter((row) => expected.has(row.chunkId)).length
      compared += exhaustive.length

      // The profile scope keeps 20% of the index; iterative scanning must still fill the request.
      const scoped = await search(approximate, query, profiles)
      expect(scoped).toHaveLength(24)
      expect(scoped.every((row) => row.projection.id === "profile")).toBe(true)
    }

    expect(overlap / compared).toBeGreaterThanOrEqual(0.95)

    // The statement the adapter sends must be served by the HNSW index.
    const statement = statements.find((item) => item.text.includes("nearest AS MATERIALIZED"))
    if (statement === undefined) throw new Error("No approximate statement recorded")
    await client.query("SAVEPOINT explain_plan; SET LOCAL hnsw.iterative_scan = relaxed_order")
    const explained = await client.query(`EXPLAIN (FORMAT JSON) ${statement.text}`, [...statement.values])
    await client.query("ROLLBACK TO SAVEPOINT explain_plan; RELEASE SAVEPOINT explain_plan")
    const plan = JSON.stringify(explained.rows[0]?.["QUERY PLAN"])
    expect(plan).toContain(`"Index Name":"${index}"`)
    expect(plan).toContain(`"Node Type":"Index Scan"`)

    // Settings are transaction-local to the adapter's savepoint.
    expect((await client.query("SELECT current_setting('hnsw.iterative_scan') AS value")).rows[0]?.value).toBe("off")
  } finally {
    try { await client.query("ROLLBACK") } finally { await client.end() }
  }
}, 120_000)
