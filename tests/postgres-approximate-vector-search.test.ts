import { describe, expect, test } from "bun:test"
import { Pool } from "pg"
import { Effect, Result, Schema } from "effect"
import { makeDocumentKey } from "../src/document/document-identity.js"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import { parseTextSearchPolicy } from "../src/document/text-search-policy.js"
import {
  postgresDocumentGraph,
  postgresTransactionClient,
  postgresVectorIndexSql,
  type PostgresDocumentGraphConfig,
} from "../src/postgres.js"
import {
  documentKeys,
  makeGraphSearchScope,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
  SearchResultCountSchema,
  type SemanticCandidateRequest,
} from "../src/retrieval/graph-retrieval.js"

const approximate = { mode: "approximate", index: "chunks_embedding_hnsw", dimensions: 3 } as const

const request = {
  vector: [0.2, 0.4, 0.6],
  embeddingProfile: defineEmbeddingProfile({ id: "approximate-test", version: "v1", dimensions: 3 }),
  scope: makeGraphSearchScope("approximate-test", {}),
  candidates: Schema.decodeSync(SearchResultCountSchema)(6),
}

/** Answers discovery like a server with pgvector in `public` and the index in the given state. */
const recordingTransaction = (indexReady: boolean) => {
  const queries: Array<{ readonly text: string; readonly values: ReadonlyArray<unknown> | undefined }> = []

  const transaction = postgresTransactionClient({ query: (text, values) => {
    queries.push({ text, values })
    if (text.includes("pg_catalog.pg_extension")) return Promise.resolve({ rows: [{ namespace: "public" }] })
    if (text.includes("pg_catalog.pg_index")) return Promise.resolve({ rows: [{ ready: indexReady }] })

    return Promise.resolve({ rows: [] })
  } })

  return { queries, transaction }
}

const search = (config: PostgresDocumentGraphConfig, input: SemanticCandidateRequest = request) =>
  Effect.runPromise(Effect.gen(function*() {
    const store = yield* ProjectionSearchStore

    return yield* store.searchCandidates(input)
  }).pipe(Effect.provide(postgresDocumentGraph(config))))

const searchStatement = (queries: ReadonlyArray<{ readonly text: string }>) =>
  queries.find((query) => query.text.includes("query_norm AS MATERIALIZED"))?.text ?? ""

describe("postgresVectorIndexSql", () => {
  test("builds a concurrent partial halfvec expression index by default", () => {
    expect(postgresVectorIndexSql({ index: "chunks_embedding_hnsw", dimensions: 1024, schema: "popcomputer_document_graph" })).toBe(
      `CREATE INDEX CONCURRENTLY "chunks_embedding_hnsw"
  ON "popcomputer_document_graph"."projected_chunks"
  USING hnsw (("embedding"::"public".halfvec(1024)) "public".halfvec_cosine_ops)
  WITH (m = 16, ef_construction = 64)
  WHERE "embedding_dimensions" = 1024 AND "popcomputer_document_graph"."native_halfvec_eligible"("embedding")`,
    )
  })

  test("a vector index covers native-eligible rows and can build inside a transaction", () => {
    const sql = postgresVectorIndexSql({ ...approximate, representation: "vector", vectorSchema: "extensions", concurrently: false, m: 24, efConstruction: 96 })

    expect(sql).toStartWith(`CREATE INDEX "chunks_embedding_hnsw"`)
    expect(sql).toContain(`("embedding"::"extensions".vector(3)) "extensions".vector_cosine_ops`)
    expect(sql).toContain("WITH (m = 24, ef_construction = 96)")
    expect(sql).toContain(`WHERE "embedding_dimensions" = 3 AND "embedding_native_eligible"`)
  })

  test("rejects shapes HNSW cannot build and unsafe identifiers", () => {
    expect(() => postgresVectorIndexSql({ index: "i", dimensions: 2_001, representation: "vector" })).toThrow(RangeError)
    expect(() => postgresVectorIndexSql({ index: "i", dimensions: 4_001 })).toThrow()
    expect(() => postgresVectorIndexSql({ index: "i", dimensions: 3, m: 40, efConstruction: 64 })).toThrow(RangeError)
    expect(() => postgresVectorIndexSql({ index: 'i"; DROP TABLE x; --', dimensions: 3 })).toThrow()
  })
})

describe("approximate semantic search", () => {
  test("orders by the index expression inside a savepoint that reverts its HNSW settings", async () => {
    const { queries, transaction } = recordingTransaction(true)
    await search({ transaction, vectorSearch: approximate })
    const texts = queries.map((query) => query.text)
    const statement = searchStatement(queries)

    // The ORDER BY expression and partial predicate must match the DDL for the planner to use the index.
    expect(statement).toContain(`ORDER BY (c."embedding"::"public".halfvec(3))`)
    expect(statement).toContain(`OPERATOR("public".<=>) ($1::double precision[]::"public".halfvec(3))`)
    expect(statement).toContain(`WHERE c."embedding_dimensions" = 3 AND "honertia_document_graph"."native_halfvec_eligible"(c."embedding")`)
    expect(statement).toContain("SELECT TRUE")
    // Candidates are rescored in float64, not pgvector's float32.
    expect(statement).toContain("FROM unnest(scoped.embedding")
    expect(statement).not.toContain("cosine_distance")

    const opened = texts.findIndex((text) => text.startsWith("SAVEPOINT honertia_document_graph_read"))
    expect(texts[opened]).toBe(
      "SAVEPOINT honertia_document_graph_read; SET LOCAL hnsw.iterative_scan = relaxed_order; SET LOCAL hnsw.ef_search = 100; SET LOCAL hnsw.max_scan_tuples = 20000",
    )
    expect(texts[opened + 1]).toBe(statement)
    expect(texts[opened + 2]).toBe("ROLLBACK TO SAVEPOINT honertia_document_graph_read; RELEASE SAVEPOINT honertia_document_graph_read")

    // Fetch size is candidates × overfetch; the final LIMIT is the requested count.
    const values = queries[opened + 1]?.values ?? []
    expect(values.slice(-2)).toEqual([24, 6])
  })

  test("raises ef_search to the fetch size and applies the statement timeout", async () => {
    const { queries, transaction } = recordingTransaction(true)
    await search({ transaction, vectorSearch: { ...approximate, overfetch: 20 }, searchTimeoutMilliseconds: 3_000 })

    expect(queries.map((query) => query.text)).toContain(
      "SAVEPOINT honertia_document_graph_read; SET LOCAL hnsw.iterative_scan = relaxed_order; SET LOCAL hnsw.ef_search = 120; SET LOCAL hnsw.max_scan_tuples = 20000; SET LOCAL statement_timeout = 3000",
    )
  })

  test.each([
    { name: "the index is invalid or still building", indexReady: false, input: request },
    { name: "the scope names documents", indexReady: true, input: { ...request, scope: makeGraphSearchScope("approximate-test", { target: documentKeys([makeDocumentKey({ graph: "approximate-test", documentKind: "Work", encodedId: 1 })]) }) } },
    { name: "the profile dimensions differ", indexReady: true, input: { ...request, vector: [0.1, 0.2, 0.3, 0.4], embeddingProfile: defineEmbeddingProfile({ id: "approximate-test", version: "v1", dimensions: 4 }) } },
    { name: "the query overflows halfvec", indexReady: true, input: { ...request, vector: [70_000, 1, 1] } },
    { name: "the query collapses to zero in halfvec", indexReady: true, input: { ...request, vector: [1e-9, 1e-9, 1e-9] } },
  ])("uses exact search when $name", async ({ indexReady, input }) => {
    const { queries, transaction } = recordingTransaction(indexReady)
    await search({ transaction, vectorSearch: approximate }, input)
    const statement = searchStatement(queries)

    expect(statement).toContain("FROM unnest(scoped.embedding")
    expect(statement).not.toContain("nearest AS MATERIALIZED")
    expect(queries.some((query) => query.text.includes("hnsw."))).toBe(false)
  })

  test("checks index readiness once per cached discovery and not in exhaustive modes", async () => {
    const approximateRun = recordingTransaction(true)
    await Effect.runPromise(Effect.gen(function*() {
      const store = yield* ProjectionSearchStore
      yield* Effect.all([store.searchCandidates(request), store.searchCandidates(request)], { concurrency: 2 })
    }).pipe(Effect.provide(postgresDocumentGraph({ transaction: approximateRun.transaction, vectorSearch: approximate }))))
    expect(approximateRun.queries.filter((query) => query.text.includes("pg_catalog.pg_index"))).toHaveLength(1)
    expect(approximateRun.queries.find((query) => query.text.includes("pg_catalog.pg_index"))?.values).toEqual(["honertia_document_graph", "chunks_embedding_hnsw"])

    const autoRun = recordingTransaction(true)
    await search({ transaction: autoRun.transaction })
    expect(autoRun.queries.some((query) => query.text.includes("pg_catalog.pg_index"))).toBe(false)
  })

  test("rejects invalid approximate settings when the layer is built", async () => {
    const { transaction } = recordingTransaction(true)
    const attempt = (vectorSearch: NonNullable<PostgresDocumentGraphConfig["vectorSearch"]>) => Effect.runPromise(Effect.gen(function*() {
      const store = yield* ProjectionSearchStore

      return yield* store.searchCandidates(request)
    }).pipe(Effect.provide(postgresDocumentGraph({ transaction, vectorSearch })), Effect.result))

    await expect(attempt({ ...approximate, representation: "vector", dimensions: 2_001 })).rejects.toThrow(RangeError)
    await expect(attempt({ ...approximate, index: "bad name" })).rejects.toThrow()
    await expect(attempt({ ...approximate, overfetch: 0 })).rejects.toThrow()
  })
})

describe("search statement timeout", () => {
  test("wraps semantic and text searches without changing untimed searches", async () => {
    const { queries, transaction } = recordingTransaction(false)
    const textPolicy = parseTextSearchPolicy(undefined)
    if (textPolicy === "disabled") throw new Error("The default text policy unexpectedly disabled search")

    await Effect.runPromise(Effect.gen(function*() {
      const semantic = yield* ProjectionSearchStore
      const text = yield* ProjectionTextSearchStore
      yield* semantic.searchCandidates(request)
      yield* text.searchTextCandidates({ query: "packaging", policy: textPolicy, scope: request.scope, candidates: request.candidates })
    }).pipe(Effect.provide(postgresDocumentGraph({ transaction, searchTimeoutMilliseconds: 7_000 }))))

    expect(queries.filter((query) => query.text === "SAVEPOINT honertia_document_graph_read; SET LOCAL statement_timeout = 7000")).toHaveLength(2)
    expect(queries.filter((query) => query.text.startsWith("ROLLBACK TO SAVEPOINT honertia_document_graph_read"))).toHaveLength(2)

    const untimed = recordingTransaction(false)
    await search({ transaction: untimed.transaction })
    expect(untimed.queries.some((query) => query.text.includes("SAVEPOINT"))).toBe(false)
  })

  test("pool mode runs each timed search in its own read-only transaction and always releases", async () => {
    const log: Array<string> = []
    let fail = false

    const client = {
      query: (text: string) => {
        log.push(text)
        if (fail && text.includes("query_norm AS MATERIALIZED")) return Promise.reject(new Error("canceling statement due to statement timeout"))

        return Promise.resolve({ rows: [] })
      },
      release: () => { log.push("release") },
    }
    // An unconnected pg Pool whose connect() hands out the recording client.
    const pool = Object.assign(new Pool(), { connect: () => Promise.resolve(client) })
    const config: PostgresDocumentGraphConfig = { pool, vectorSearch: "float64", searchTimeoutMilliseconds: 7_000 }

    await search(config)
    expect(log[0]).toBe("BEGIN READ ONLY; SET LOCAL statement_timeout = 7000")
    expect(log.slice(-2)).toEqual(["COMMIT", "release"])

    log.length = 0
    fail = true
    const failed = await Effect.runPromise(Effect.gen(function*() {
      const store = yield* ProjectionSearchStore

      return yield* store.searchCandidates(request)
    }).pipe(Effect.provide(postgresDocumentGraph(config)), Effect.result))

    expect(Result.isFailure(failed)).toBe(true)
    if (Result.isFailure(failed)) expect(failed.failure._tag).toBe("ProjectionSearchStoreFailed")
    expect(log.slice(-2)).toEqual(["ROLLBACK", "release"])
  })
})
