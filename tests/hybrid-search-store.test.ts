import { describe, expect, test } from "bun:test"
import { Effect, Result, Schema } from "effect"
import {
  defineEmbeddingProfile,
  EmbeddingProvider,
  InvalidSearchOutput,
  makeChunkId,
  makeDocumentKey,
  makeGraphSearchScope,
  ProjectionHybridSearchStore,
  ProjectionRevisionHashSchema,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
  type EmbeddingProviderService,
  type HybridCandidateRequest,
  type ProjectionHybridSearchStoreService,
  type ProjectionSearchStoreService,
  type ProjectionTextSearchStoreService,
  type SemanticSearchCandidate,
} from "../src/adapter.js"
import { parseTextSearchPolicy } from "../src/document/text-search-policy.js"
import {
  searchGraphHybrid,
  semantic,
  text,
} from "../src/retrieval/graph-retrieval.js"

const embeddingProfile = defineEmbeddingProfile({
  id: "test:hybrid-batch",
  version: "v1",
  dimensions: 2,
})

const textPolicy = parseTextSearchPolicy({ language: "english" })

if (textPolicy === "disabled") {
  throw new Error("The hybrid test requires enabled text search")
}

const scope = makeGraphSearchScope(
  "hybrid-batch-test",
  {},
  [
    {
      documentKind: "Article",
      projection: "sections",
      projectionVersion: "v1",
    },
  ],
)

const reference = {
  graph: "hybrid-batch-test",
  kind: "Article",
  id: "article-1",
} as const

const documentKey = makeDocumentKey({
  graph: reference.graph,
  documentKind: reference.kind,
  encodedId: reference.id,
})

const chunkId = makeChunkId({
  documentKey,
  projection: "sections",
  sectionKey: "body",
  sectionPart: 0,
})

const revisionHash = Schema.decodeSync(ProjectionRevisionHashSchema)(
  "a".repeat(64),
)

const candidate = (score: number): SemanticSearchCandidate => ({
  score,
  chunkId,
  documentKey,
  reference,
  projection: { id: "sections", version: "v1" },
  revisionHash,
  sectionKey: "body",
  sectionPart: 0,
  content: "A batched hybrid candidate.",
  metadata: { visibility: "public" },
})

const makeFallbackStores = () => {
  const calls = { semantic: 0, text: 0 }

  const semanticStore: ProjectionSearchStoreService = {
    searchCandidates: () => {
      calls.semantic += 1

      return Effect.succeed([])
    },
  }

  const textStore: ProjectionTextSearchStoreService = {
    searchTextCandidates: () => {
      calls.text += 1

      return Effect.succeed([])
    },
  }

  return { calls, semanticStore, textStore }
}

const makeEmbeddingProvider = () => {
  const queries: Array<string> = []

  const service: EmbeddingProviderService = {
    profile: embeddingProfile,
    embedDocuments: () => Effect.succeed([]),
    embedQuery: (query) => {
      queries.push(query)

      return Effect.succeed([0.25, 0.75])
    },
  }

  return { queries, service }
}

describe("optional batched hybrid candidate retrieval", () => {
  test("prefers one batched request and preserves both channel signals", async () => {
    const requests: Array<HybridCandidateRequest> = []

    const hybridStore: ProjectionHybridSearchStoreService = {
      searchHybridCandidates: (request) => {
        requests.push(request)

        return Effect.succeed({
          semantic: [candidate(0.91)],
          text: [candidate(7.5)],
        })
      },
    }

    const embeddings = makeEmbeddingProvider()
    const fallback = makeFallbackStores()

    const semanticStrategy = semantic({
      candidates: 11,
      results: 1,
      weight: 2,
    })

    const textStrategy = text({
      policy: textPolicy,
      candidates: 7,
      results: 1,
      weight: 3,
    })

    const hits = await Effect.runPromise(
      searchGraphHybrid({
        query: "hybrid query",
        scope,
        route: {
          _tag: "Projection",
          sourceKind: "Article",
          projection: "sections",
        },
        semantic: semanticStrategy,
        text: textStrategy,
        results: semanticStrategy.results,
        rankConstant: semanticStrategy.rankConstant,
      }).pipe(
        Effect.provideService(EmbeddingProvider, embeddings.service),
        Effect.provideService(ProjectionSearchStore, fallback.semanticStore),
        Effect.provideService(ProjectionTextSearchStore, fallback.textStore),
        Effect.provideService(ProjectionHybridSearchStore, hybridStore),
      ),
    )

    expect(requests).toEqual([
      {
        query: "hybrid query",
        vector: [0.25, 0.75],
        embeddingProfile,
        textPolicy,
        scope,
        semanticCandidates: semanticStrategy.candidates,
        textCandidates: textStrategy.candidates,
      },
    ])
    expect(embeddings.queries).toEqual(["hybrid query"])
    expect(fallback.calls).toEqual({ semantic: 0, text: 0 })
    expect(hits).toHaveLength(1)
    expect(hits[0]?.signals).toEqual([
      expect.objectContaining({
        stream: expect.objectContaining({ channel: "semantic" }),
        rank: 1,
        score: 0.91,
        weight: 2,
      }),
      expect.objectContaining({
        stream: expect.objectContaining({ channel: "text" }),
        rank: 1,
        score: 7.5,
        weight: 3,
      }),
    ])
  })

  test("validates each channel returned by the batched adapter", async () => {
    const hybridStore: ProjectionHybridSearchStoreService = {
      searchHybridCandidates: () =>
        Effect.succeed({
          semantic: [candidate(0.91)],
          text: [candidate(0)],
        }),
    }

    const embeddings = makeEmbeddingProvider()
    const fallback = makeFallbackStores()
    const semanticStrategy = semantic({ candidates: 1, results: 1 })

    const textStrategy = text({
      policy: textPolicy,
      candidates: 1,
      results: 1,
    })

    const result = await Effect.runPromise(
      searchGraphHybrid({
        query: "hybrid query",
        scope,
        route: {
          _tag: "Projection",
          sourceKind: "Article",
          projection: "sections",
        },
        semantic: semanticStrategy,
        text: textStrategy,
        results: semanticStrategy.results,
        rankConstant: semanticStrategy.rankConstant,
      }).pipe(
        Effect.provideService(EmbeddingProvider, embeddings.service),
        Effect.provideService(ProjectionSearchStore, fallback.semanticStore),
        Effect.provideService(ProjectionTextSearchStore, fallback.textStore),
        Effect.provideService(ProjectionHybridSearchStore, hybridStore),
        Effect.result,
      ),
    )

    expect(result).toEqual(
      Result.fail(
        new InvalidSearchOutput({
          channel: "text",
          reason: "invalid_score",
        }),
      ),
    )
  })
})
