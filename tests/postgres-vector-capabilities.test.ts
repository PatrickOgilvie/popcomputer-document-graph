import { expect, test } from "bun:test"
import { Effect, Result, Schema } from "effect"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import { postgresDocumentGraph, postgresTransactionClient } from "../src/postgres.js"
import {
  makeGraphSearchScope,
  noDocuments,
  ProjectionSearchStore,
  SearchResultCountSchema,
} from "../src/retrieval/graph-retrieval.js"

const request = {
  vector: [1, 0],
  embeddingProfile: defineEmbeddingProfile({ id: "capability-test", version: "v1", dimensions: 2 }),
  scope: makeGraphSearchScope("capability-test", {}),
  candidates: Schema.decodeSync(SearchResultCountSchema)(1),
}

test("concurrent searches share capability discovery and absent pgvector uses float64", async () => {
  const queries: Array<string> = []
  const transaction = postgresTransactionClient({ query: (text) => {
    queries.push(text)
    return Promise.resolve({ rows: [] })
  } })
  const results = await Effect.runPromise(Effect.gen(function*() {
    const store = yield* ProjectionSearchStore
    const empty = yield* store.searchCandidates({ ...request, scope: makeGraphSearchScope("capability-test", { target: noDocuments() }) })
    expect(queries).toHaveLength(0)
    const results = yield* Effect.all([store.searchCandidates(request), store.searchCandidates(request)], { concurrency: 2 })
    return { empty, results }
  }).pipe(Effect.provide(postgresDocumentGraph({ transaction }))))
  expect(results).toEqual({ empty: [], results: [[], []] })
  expect(queries.filter((query) => query.includes("pg_catalog.pg_extension"))).toHaveLength(1)
  expect(queries.filter((query) => query.includes("FROM unnest(scoped.embedding"))).toHaveLength(2)
})

test("a transient discovery failure is typed and does not poison later searches", async () => {
  let probes = 0
  const transaction = postgresTransactionClient({ query: (text) => {
    if (text.includes("pg_catalog.pg_extension")) {
      probes += 1
      if (probes === 1) return Promise.reject(new Error("temporarily unavailable"))
    }
    return Promise.resolve({ rows: [] })
  } })
  const result = await Effect.runPromise(Effect.gen(function*() {
    const store = yield* ProjectionSearchStore
    const first = yield* store.searchCandidates(request).pipe(Effect.result)
    const second = yield* store.searchCandidates(request)
    return { first, second }
  }).pipe(Effect.provide(postgresDocumentGraph({ transaction }))))
  expect(Result.isFailure(result.first)).toBe(true)
  if (Result.isFailure(result.first)) expect(result.first.failure._tag).toBe("ProjectionSearchStoreFailed")
  expect(result.second).toEqual([])
  expect(probes).toBe(2)
})
