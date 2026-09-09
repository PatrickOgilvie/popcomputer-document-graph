import { describe, expect, test } from "bun:test"
import { Client } from "pg"
import { Effect, Schema } from "effect"
import { postgresDocumentGraph } from "../src/postgres.js"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import { DocumentKeySchema } from "../src/document/document-identity.js"
import { parseTextSearchPolicy } from "../src/document/text-search-policy.js"
import {
  makeGraphSearchScope,
  documentKeys,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
  SearchResultCountSchema,
} from "../src/retrieval/graph-retrieval.js"

const databaseUrl = Bun.env.TEST_DATABASE_URL ??
  Bun.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE

/** Fixtures live only in this connection's temporary schema and are rolled back. */
const withFixtures = async (run: (client: Client) => Promise<void>) => {
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 5_000 })
  await client.connect()
  try {
    await client.query("BEGIN")
    await client.query("SET LOCAL statement_timeout = 5000")
    await run(client)
  } finally {
    try {
      await client.query("ROLLBACK")
    } finally {
      await client.end()
    }
  }
}

const runIntegrationTests = Bun.env.RUN_DOCUMENT_GRAPH_POSTGRES_TESTS === "true" && databaseUrl !== undefined

test.skipIf(!runIntegrationTests)("PostgreSQL filters mixed versioned and wildcard catalog entries before limiting both channels", async () => {
  await withFixtures(async (client) => {
    await client.query(`
      CREATE TEMP TABLE projected_revisions (document_key char(64), projection_id text,
        graph_id text, document_kind text, encoded_document_id jsonb,
        projection_version text, revision_hash text, embedding_profile_id text,
        embedding_profile_version text, embedding_dimensions integer);
      CREATE TEMP TABLE projected_chunks (chunk_id char(64), document_key char(64),
        projection_id text, section_key text, section_part integer, content text,
        has_metadata boolean, metadata jsonb, embedding double precision[],
        text_context text, text_label text, text_content text, text_search_simple tsvector);
    `)
    for (const [index, kind, projection, version, vector] of [
      [1, "Work", "evidence", "v1", [1, 0]],
      [2, "Work", "evidence", "v2", [1, 1]],
      [3, "Other", "archive", "legacy", [0, 1]],
    ] as const) {
      const id = String(index).repeat(64)
      await client.query(`INSERT INTO projected_revisions VALUES
        ($1, $2, 'catalog-test', $3, 'null', $4, $5, 'catalog-profile', 'v1', 2)`,
      [id, projection, kind, version, "a".repeat(64)])
      await client.query(`INSERT INTO projected_chunks VALUES
        ($1, $1, $2, 'body', 0, 'evidence', false, null, $3,
         null, null, 'evidence', to_tsvector('simple', 'evidence'))`,
      [id, projection, [...vector]])
    }
    const registered = [
      { documentKind: "Work", projection: "evidence", projectionVersion: "v2" },
      { documentKind: "Other", projection: "archive" },
    ]
    const policy = parseTextSearchPolicy({ language: "simple" })
    if (policy === "disabled") throw new Error("Unexpected disabled policy")
    const selected = Schema.decodeSync(DocumentKeySchema)("2".repeat(64))
    for (const target of [undefined, documentKeys([selected])]) {
      const scope = makeGraphSearchScope("catalog-test", { target }, registered)
      const result = await Effect.runPromise(Effect.gen(function*() {
        const semantic = yield* ProjectionSearchStore
        const text = yield* ProjectionTextSearchStore
        const candidates = Schema.decodeSync(SearchResultCountSchema)(2)
        return {
          semantic: yield* semantic.searchCandidates({
            scope,
            candidates,
            vector: [1, 0],
            embeddingProfile: defineEmbeddingProfile({ id: "catalog-profile", version: "v1", dimensions: 2 }),
          }),
          text: yield* text.searchTextCandidates({ scope, candidates, policy, query: "evidence" }),
        }
      }).pipe(Effect.provide(postgresDocumentGraph({ vectorSearch: "float64", transaction: client, schema: "pg_temp" }))))
      const expected = target === undefined ? ["2".repeat(64), "3".repeat(64)] : [selected]
      expect(result.semantic.map((candidate) => String(candidate.documentKey))).toEqual(expected)
      expect(result.text.map((candidate) => String(candidate.documentKey))).toEqual(expected)
    }
  })
})

for (const dimensions of [3, 1024]) {
  describe("PostgreSQL cosine scoring", () => {
    test.skipIf(!runIntegrationTests)(`preserves cosine scores, scope, zero vectors and ties at ${dimensions} dimensions`, async () => {
      await withFixtures(async (client) => {
        await client.query(`
          CREATE TEMP TABLE projected_revisions (document_key text, projection_id text,
            graph_id text, document_kind text, encoded_document_id jsonb,
            projection_version text, revision_hash text, embedding_profile_id text,
            embedding_profile_version text, embedding_dimensions integer);
          CREATE TEMP TABLE projected_chunks (chunk_id text, document_key text,
            projection_id text, section_key text, section_part integer, content text,
            has_metadata boolean, metadata jsonb, embedding double precision[]);
        `)
        const expand = (values: ReadonlyArray<number>) => Array.from(
          { length: dimensions }, (_, index) => values[index % values.length] ?? 0,
        )
        const vectors = [[1, 2, 3], [2, 4, 6], [-1, -2, -3], [2, -1, 0], [0, 0, 0], [1, 2, 3]].map(expand)
        for (const [index, vector] of vectors.entries()) {
          const id = (index + 1).toString(16).repeat(64)
          await client.query(`INSERT INTO projected_revisions VALUES
            ($1, 'evidence', $2, 'Work', $3, 'v1', $4, 'test-profile', 'v1', $5)`,
          [id, index === 5 ? "other-graph" : "test-graph", JSON.stringify({ id: index }), "a".repeat(64), dimensions])
          await client.query(`INSERT INTO projected_chunks VALUES
            ($1, $1, 'evidence', 'body', 0, 'Evidence', false, null, $2)`, [id, vector])
        }
        const storage = postgresDocumentGraph({ vectorSearch: "float64", transaction: client, schema: "pg_temp" })
        const search = (vector: ReadonlyArray<number>, graph = "test-graph") => Effect.runPromise(ProjectionSearchStore.pipe(
          Effect.flatMap((store) => store.searchCandidates({
            vector,
            embeddingProfile: defineEmbeddingProfile({
              id: "test-profile", version: "v1", dimensions,
            }),
            scope: makeGraphSearchScope(graph, { include: ["Work"], includeProjections: ["evidence"] }),
            candidates: Schema.decodeSync(SearchResultCountSchema)(4),
          })),
          Effect.provide(storage),
        ))
        expect(await search(expand([1e308]), "empty-graph")).toEqual([])
        for (const query of [[1, 2, 3], [-1, 2, -3], [0.25, -0.5, 1], [0, 0, 0]].map(expand)) {
          const results = await search(query)
          const expected = vectors.slice(0, 5).flatMap((vector, index) => {
            const dot = vector.reduce((sum, value, i) => sum + value * (query[i] ?? 0), 0)
            const norm = Math.hypot(...vector) * Math.hypot(...query)
            return norm === 0 ? [] : [{ chunkId: (index + 1).toString(16).repeat(64), score: dot / norm }]
          }).sort((a, b) => b.score - a.score || a.chunkId.localeCompare(b.chunkId))
          expect(results.map(({ chunkId }) => String(chunkId))).toEqual(expected.map(({ chunkId }) => chunkId))
          for (const [index, result] of results.entries()) {
            const candidate = expected[index]
            if (candidate === undefined) throw new Error("Unexpected search candidate")
            expect(result.score).toBeCloseTo(candidate.score, 12)
          }
        }
      })
    })
  })
}

test.skipIf(!runIntegrationTests)("PostgreSQL cached text vectors preserve attributed and plain-text weights in both languages", async () => {
  await withFixtures(async (client) => {
    await client.query(`
      CREATE TEMP TABLE projected_revisions (document_key char(64), projection_id text,
        graph_id text, document_kind text, encoded_document_id jsonb,
        projection_version text, revision_hash text);
      CREATE TEMP TABLE projected_chunks (chunk_id char(64), document_key char(64),
        projection_id text, section_key text, section_part integer, content text,
        has_metadata boolean, metadata jsonb, text_context text, text_label text, text_content text,
        text_search_simple tsvector GENERATED ALWAYS AS (to_tsvector('simple', coalesce(text_context, '') || ' ' || coalesce(text_label, '') || ' ' || text_content)) STORED,
        text_search_english tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(text_context, '') || ' ' || coalesce(text_label, '') || ' ' || text_content)) STORED);
    `)
    for (const [index, context, label, content] of [
      [1, null, null, "Healthcare brands and healthcare campaigns"],
      [2, "Healthcare clients", "Brand launch", "Healthcare campaign"],
      [3, "", "", "Brands and campaigns"],
    ] as const) {
      const id = String(index).repeat(64)
      await client.query("INSERT INTO projected_revisions VALUES ($1, 'evidence', 'text-test', 'Work', 'null', 'v1', $2)", [id, "a".repeat(64)])
      await client.query(`INSERT INTO projected_chunks
        (chunk_id, document_key, projection_id, section_key, section_part, content,
         has_metadata, text_context, text_label, text_content)
        VALUES ($1, $1, 'evidence', 'body', 0, $2, false, $3, $4, $2)`, [id, content, context, label])
    }
    for (const language of ["english", "simple"] as const) {
      for (const weights of [{ context: 0, label: 0, content: 3 }, { context: 1, label: 5, content: 1 }, { context: 1, label: 1, content: 0 }]) {
        const policy = parseTextSearchPolicy({ language, weights })
        if (policy === "disabled") throw new Error("Unexpected disabled text policy")
        for (const query of ["healthcare", "brands OR campaign", '"healthcare campaign"']) {
          const results = await Effect.runPromise(ProjectionTextSearchStore.pipe(
            Effect.flatMap((store) => store.searchTextCandidates({ query, policy,
              scope: makeGraphSearchScope("text-test", {}), candidates: Schema.decodeSync(SearchResultCountSchema)(10) })),
            Effect.provide(postgresDocumentGraph({ transaction: client, schema: "pg_temp" })),
          ))
          const reference = await client.query(`WITH ranked AS (
            SELECT chunk_id,
              $2 * ts_rank_cd(to_tsvector($1::regconfig, coalesce(text_context, '')), websearch_to_tsquery($1::regconfig, $5)) +
              $3 * ts_rank_cd(to_tsvector($1::regconfig, coalesce(text_label, '')), websearch_to_tsquery($1::regconfig, $5)) +
              $4 * ts_rank_cd(to_tsvector($1::regconfig, text_content), websearch_to_tsquery($1::regconfig, $5)) AS score
            FROM projected_chunks
          ) SELECT chunk_id, score FROM ranked WHERE score > 0 ORDER BY score DESC, chunk_id ASC`,
          [language, weights.context, weights.label, weights.content, query])
          expect(results.map((row) => ({ chunk_id: row.chunkId, score: row.score }))).toEqual(reference.rows)
        }
      }
    }
  })
})
