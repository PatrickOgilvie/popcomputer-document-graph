import { describe, expect, test } from "bun:test"
import { Effect, Schema } from "effect"
import { makeDocumentKey } from "../src/document/document-identity.js"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import { makeRequestCoalescer } from "../src/storage/postgres/coalesce.js"
import { postgresDocumentGraph, postgresTransactionClient } from "../src/postgres.js"
import { GraphTopologyStore } from "../src/graph/graph-topology.js"
import { GraphNeighbourLimitSchema } from "../src/graph/graph-relation.js"
import {
  makeGraphSearchScope,
  ProjectionSearchStore,
  SearchResultCountSchema,
} from "../src/retrieval/graph-retrieval.js"

describe("makeRequestCoalescer", () => {
  test("answers requests that share a key with one call, in request order", async () => {
    const calls: Array<ReadonlyArray<number>> = []
    const coalescer = makeRequestCoalescer<number, number>({
      windowMilliseconds: 5,
      maximumBatch: 16,
      run: async (_key, requests) => {
        calls.push(requests)
        return requests.map((request) => request * 10)
      },
    })

    const results = await Promise.all([coalescer.submit("a", 1), coalescer.submit("b", 2), coalescer.submit("a", 3)])

    expect(results).toEqual([10, 20, 30])
    expect(calls).toEqual([[1, 3], [2]])
  })

  test("flushes a full batch without waiting for its window", async () => {
    const calls: Array<ReadonlyArray<number>> = []
    const coalescer = makeRequestCoalescer<number, number>({
      windowMilliseconds: 10_000,
      maximumBatch: 2,
      run: async (_key, requests) => {
        calls.push(requests)
        return requests
      },
    })

    expect(await Promise.all([coalescer.submit("a", 1), coalescer.submit("a", 2)])).toEqual([1, 2])
    expect(calls).toEqual([[1, 2]])
  })

  test("fails every request in a batch that fails or answers short", async () => {
    const failing = makeRequestCoalescer<number, number>({
      windowMilliseconds: 1,
      maximumBatch: 16,
      run: () => Promise.reject(new Error("statement timeout")),
    })
    const short = makeRequestCoalescer<number, number>({
      windowMilliseconds: 1,
      maximumBatch: 16,
      run: async () => [1],
    })

    const failed = await Promise.allSettled([failing.submit("a", 1), failing.submit("a", 2)])
    const shorted = await Promise.allSettled([short.submit("a", 1), short.submit("a", 2)])

    expect(failed.map(({ status }) => status)).toEqual(["rejected", "rejected"])
    expect(shorted.map(({ status }) => status)).toEqual(["rejected", "rejected"])
  })
})

describe("coalesced PostgreSQL searches", () => {
  const profile = defineEmbeddingProfile({ id: "coalesce-test", version: "v1", dimensions: 3 })
  const scope = makeGraphSearchScope("coalesce-test", {})
  const candidates = Schema.decodeSync(SearchResultCountSchema)(2)
  const documentKey = makeDocumentKey({ graph: "coalesce-test", documentKind: "Work", encodedId: 1 })
  const chunkId = (suffix: string) => suffix.repeat(64)

  const row = (ordinal: number, score: number, suffix: string) => ({
    request_ordinal: ordinal, score, chunk_id: chunkId(suffix), document_key: documentKey,
    graph_id: "coalesce-test", document_kind: "Work", encoded_document_id: 1,
    projection_id: "evidence", projection_version: "v1", revision_hash: "a".repeat(64),
    section_key: "body", section_part: 0, content: "Evidence", has_metadata: false, metadata: null,
  })

  test("concurrent searches with one scope share a statement and each gets its own ranked rows", async () => {
    const statements: Array<string> = []
    const transaction = postgresTransactionClient({ query: (text) => {
      if (text.includes("pg_catalog.pg_extension")) return Promise.resolve({ rows: [] })
      if (text.includes("query_norm")) {
        statements.push(text)
        // Branch rows arrive interleaved and out of order; each branch is re-sorted.
        return Promise.resolve({ rows: [row(1, 0.4, "c"), row(0, 0.2, "b"), row(1, 0.9, "d"), row(0, 0.7, "a")] })
      }
      return Promise.resolve({ rows: [] })
    } })

    const results = await Effect.runPromise(Effect.gen(function*() {
      const store = yield* ProjectionSearchStore
      return yield* Effect.all([
        store.searchCandidates({ vector: [1, 0, 0], embeddingProfile: profile, scope, candidates }),
        store.searchCandidates({ vector: [0, 1, 0], embeddingProfile: profile, scope, candidates }),
      ], { concurrency: "unbounded" })
    }).pipe(Effect.provide(postgresDocumentGraph({ transaction, coalesceSearches: { windowMilliseconds: 5 } }))))

    expect(statements).toHaveLength(1)
    expect(statements[0]).toContain("query_norm_1")
    expect(statements[0]).toContain("UNION ALL")
    expect(results.map((candidates) => candidates.map(({ score }) => score))).toEqual([[0.7, 0.2], [0.9, 0.4]])
  })

  test("topology reads for one relation merge their keys and each caller gets its own groups", async () => {
    const requested: Array<ReadonlyArray<string>> = []
    const transaction = postgresTransactionClient({ query: (_text, values) => {
      requested.push(Schema.decodeUnknownSync(Schema.Array(Schema.String))(values?.[1] ?? []))
      return Promise.resolve({ rows: [] })
    } })
    const keyFor = (encodedId: number) => makeDocumentKey({ graph: "coalesce-test", documentKind: "Work", encodedId })
    const read = (keys: ReadonlyArray<ReturnType<typeof keyFor>>) => Effect.gen(function*() {
      const topology = yield* GraphTopologyStore
      return yield* topology.findRelatedNodes({
        graph: "coalesce-test", documentKeys: keys, documentKind: "Work", direction: "outgoing",
        relation: "deliveredBy", relationVersion: "v1", relatedDocumentKind: "Agency",
        limit: Schema.decodeSync(GraphNeighbourLimitSchema)(6),
      })
    })

    const [first, second] = await Effect.runPromise(Effect.all([read([keyFor(1), keyFor(2)]), read([keyFor(3)])], { concurrency: "unbounded" })
      .pipe(Effect.provide(postgresDocumentGraph({ transaction, coalesceSearches: { windowMilliseconds: 5 } }))))

    expect(requested).toEqual([[keyFor(1), keyFor(2), keyFor(3)]])
    expect(first?.map(({ documentKey }) => documentKey)).toEqual([keyFor(1), keyFor(2)])
    expect(second?.map(({ documentKey }) => documentKey)).toEqual([keyFor(3)])
  })
})
