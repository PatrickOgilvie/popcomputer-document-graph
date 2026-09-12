import { expect, test } from "bun:test"
import { readFile } from "node:fs/promises"
import { Client } from "pg"
import { Effect, Schema } from "effect"
import { makeProjectionIndexStoreConformanceFixture } from "../src/conformance/projection-index-conformance.js"
import { ProjectionIndexStore } from "../src/indexing/projection-index.js"
import { postgresDocumentGraph, postgresTransactionClient } from "../src/postgres.js"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import { makeChunkId, makeDocumentKey } from "../src/document/document-identity.js"
import {
  documentKeys,
  makeGraphSearchScope,
  ProjectionSearchStore,
  SearchResultCountSchema,
} from "../src/retrieval/graph-retrieval.js"

const databaseUrl = Bun.env.TEST_DATABASE_URL

const enabled = Bun.env.RUN_DOCUMENT_GRAPH_PGVECTOR_TESTS === "true" && databaseUrl !== undefined

// Requires pgvector's extension files on the server. CREATE EXTENSION, the
// deliberately quoted namespace, real migrations and fixture writes roll back.
test.skipIf(!enabled)("automatic pgvector scoring preserves scopes and falls back for extreme and oversized vectors", async () => {
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 5_000 })
  await client.connect()

  try {
    await client.query("BEGIN")
    await client.query("SET LOCAL statement_timeout = 10000")
    const schema = `native_${crypto.randomUUID().replaceAll("-", "")}`
    const namespace = 'vector-"kernels'
    const quotedNamespace = '"vector-""kernels"'
    await client.query(`CREATE SCHEMA ${quotedNamespace}`)
    await client.query(`CREATE EXTENSION vector WITH SCHEMA ${quotedNamespace}`)

    for (const file of ["0001_initial.sql", "0002_mutation_locks.sql", "0003_graph_topology.sql", "0004_native_vector_eligibility.sql"]) {
      const migration = await readFile(new URL(`../migrations/postgres/${file}`, import.meta.url), "utf8")
      await client.query(migration.replaceAll('"honertia_document_graph"', `"${schema}"`))
    }

    const vectors = [
      [1, 2, 3], [2, 4, 6], [-1, -2, -3], [0, 0, 0],
      [1e40, 2e40, 3e40], [1e-40, 2e-40, 3e-40],
      [1, 2, 3], [1, 2, 3],
      Array.from({ length: 16_001 }, (_, index) => index % 3 + 1),
    ]

    const keys = vectors.map((_, index) => makeDocumentKey({ graph: "native-test", documentKind: "Work", encodedId: index }))
    const chunks = keys.map((documentKey) => makeChunkId({ documentKey, projection: "evidence", sectionKey: "body", sectionPart: 0 }))

    for (const [index, vector] of vectors.entries()) {
      await client.query(`INSERT INTO "${schema}".projected_revisions
        (document_key, projection_id, graph_id, document_kind, encoded_document_id,
         projection_version, revision_hash, embedding_profile_id, embedding_profile_version, embedding_dimensions)
        VALUES ($1, 'evidence', $2, 'Work', $3, $4, $5, 'native-profile', 'v1', $6)`,
      [keys[index], index === 6 ? "excluded" : "native-test", JSON.stringify(index), index === 7 ? "retired" : "v1", "a".repeat(64), vector.length])
      await client.query(`INSERT INTO "${schema}".projected_chunks
        (chunk_id, document_key, projection_id, ordinal, section_key, section_index, section_part,
         content_hash, content, embedding_content, text_content, has_metadata, embedding_dimensions, embedding)
        VALUES ($1, $2, 'evidence', 0, 'body', 0, 0, $3, 'Evidence', 'Evidence', 'Evidence', false, $4, $5)`,
      [chunks[index], keys[index], "b".repeat(64), vector.length, vector])
    }

    const eligibility = await client.query(`SELECT r.encoded_document_id AS id, c.embedding_native_eligible AS eligible
      FROM "${schema}".projected_chunks c JOIN "${schema}".projected_revisions r USING (document_key, projection_id)
      ORDER BY r.encoded_document_id::integer`)

    expect(eligibility.rows.map((row) => row.eligible)).toEqual([true, true, true, false, false, false, true, true, false])
    const registered = [{ documentKind: "Work", projection: "evidence", projectionVersion: "v1" }]
    const scope = makeGraphSearchScope("native-test", {}, registered)
    const calls: Array<string> = []

    const transaction = postgresTransactionClient({ query: (text, values) => {
      calls.push(text)

      return client.query(text, [...(values ?? [])])
    } })

    const search = (vectorSearch: "auto" | "float64", vector: ReadonlyArray<number>, target = scope) =>
      Effect.runPromise(ProjectionSearchStore.pipe(
        Effect.flatMap((store) => store.searchCandidates({
          vector,
          embeddingProfile: defineEmbeddingProfile({ id: "native-profile", version: "v1", dimensions: vector.length }),
          scope: target,
          candidates: Schema.decodeSync(SearchResultCountSchema)(10),
        })),
        Effect.provide(postgresDocumentGraph({ transaction, schema, vectorSearch })),
      ))

    for (const query of [[1, 2, 3], [-1, 2, -3]]) {
      const reference = await search("float64", query)
      const native = await search("auto", query)
      expect(native.map((row) => row.chunkId).sort()).toEqual(reference.map((row) => row.chunkId).sort())
      expect(native).toHaveLength(5)

      for (const row of native) {
        const original = reference.find((candidate) => candidate.chunkId === row.chunkId)
        expect(original).toBeDefined()
        expect(row.score).toBeCloseTo(original?.score ?? NaN, 6)
      }

      const tied = native.filter((row) => row.chunkId === chunks[0] || row.chunkId === chunks[1])
      expect(tied[0]?.score).toBe(tied[1]?.score)
      expect(tied.map((row) => row.chunkId)).toEqual(chunks.slice(0, 2).sort())
    }

    expect(calls.some((text) => text.includes(`${quotedNamespace}.cosine_distance`))).toBe(true)
    const selected = keys[2]

    if (selected === undefined) throw new Error("Missing fixture key")
    const limited = await search("auto", [1, 2, 3], makeGraphSearchScope("native-test", { target: documentKeys([selected]) }, registered))
    expect(limited.map((row) => row.documentKey)).toEqual([selected])
    expect(await search("auto", [0, 0, 0])).toEqual([])
    const oversized = vectors[8]

    if (oversized === undefined) throw new Error("Missing oversized vector fixture")

    for (const query of [[1e40, 2e40, 3e40], [1e-40, 2e-40, 3e-40], oversized]) {
      const expected = await search("float64", query)
      calls.length = 0
      expect(await search("auto", query)).toEqual(expected)
      expect(calls).toHaveLength(1)
      expect(calls[0]).not.toContain("cosine_distance")
    }

    // The adapter discovers the namespace instead of relying on search_path.
    expect((await client.query("SHOW search_path")).rows[0].search_path).not.toContain(namespace)
  } finally {
    try { await client.query("ROLLBACK") } finally { await client.end() }
  }
}, 30_000)

test.skipIf(!enabled)("automatic pgvector selection falls back when the application role lacks extension privileges", async () => {
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 5_000 })
  await client.connect()

  try {
    await client.query("BEGIN")
    await client.query("SET LOCAL statement_timeout = 10000")
    const identifier = crypto.randomUUID().replaceAll("-", "")
    const schema = `restricted_${identifier}`
    const namespace = `private_vectors_${identifier}`
    const role = `vector_reader_${identifier}`
    await client.query(`CREATE SCHEMA "${namespace}"`)
    await client.query(`CREATE EXTENSION vector WITH SCHEMA "${namespace}"`)

    for (const file of ["0001_initial.sql", "0002_mutation_locks.sql", "0003_graph_topology.sql", "0004_native_vector_eligibility.sql"]) {
      const migration = await readFile(new URL(`../migrations/postgres/${file}`, import.meta.url), "utf8")
      await client.query(migration.replaceAll('"honertia_document_graph"', `"${schema}"`))
    }

    const fixture = makeProjectionIndexStoreConformanceFixture()
    await Effect.runPromise(ProjectionIndexStore.pipe(
      Effect.flatMap((store) => store.replaceRevision(fixture.initial)),
      Effect.provide(postgresDocumentGraph({ transaction: client, schema })),
    ))
    await client.query(`CREATE ROLE "${role}" NOLOGIN`)
    await client.query(`GRANT USAGE ON SCHEMA "${schema}" TO "${role}"`)
    await client.query(`GRANT SELECT ON ALL TABLES IN SCHEMA "${schema}" TO "${role}"`)
    const calls: Array<string> = []

    const transaction = postgresTransactionClient({ query: (text, values) => {
      calls.push(text)

      return client.query(text, [...(values ?? [])])
    } })

    const search = (vectorSearch: "auto" | "float64") => Effect.runPromise(ProjectionSearchStore.pipe(
      Effect.flatMap((store) => store.searchCandidates({
        vector: fixture.initial.embeddings[0]?.vector ?? [],
        embeddingProfile: fixture.initial.embeddingProfile,
        scope: makeGraphSearchScope(fixture.initial.encodedTarget.graph, {}),
        candidates: Schema.decodeSync(SearchResultCountSchema)(10),
      })),
      Effect.provide(postgresDocumentGraph({ transaction, schema, vectorSearch })),
    ))

    await client.query(`SET LOCAL ROLE "${role}"`)
    const reference = await search("float64")
    expect(reference.length).toBeGreaterThan(0)
    calls.length = 0
    expect(await search("auto")).toEqual(reference)
    expect(calls.some((text) => text.includes(`"${namespace}".cosine_distance`))).toBe(false)

    await client.query("RESET ROLE")
    await client.query(`GRANT USAGE ON SCHEMA "${namespace}" TO "${role}"`)
    const cosine = `"${namespace}".cosine_distance("${namespace}".vector, "${namespace}".vector)`
    await client.query(`REVOKE EXECUTE ON FUNCTION ${cosine} FROM PUBLIC`)
    await client.query(`SET LOCAL ROLE "${role}"`)
    calls.length = 0
    expect(await search("auto")).toEqual(reference)
    expect(calls.some((text) => text.includes(`"${namespace}".cosine_distance`))).toBe(false)

    await client.query("RESET ROLE")
    await client.query(`GRANT EXECUTE ON FUNCTION ${cosine} TO "${role}"`)
    await client.query(`SET LOCAL ROLE "${role}"`)
    calls.length = 0
    const native = await search("auto")
    expect(native.map((row) => row.chunkId)).toEqual(reference.map((row) => row.chunkId))
    expect(calls.some((text) => text.includes(`"${namespace}".cosine_distance`))).toBe(true)
  } finally {
    try { await client.query("ROLLBACK") } finally { await client.end() }
  }
}, 30_000)
