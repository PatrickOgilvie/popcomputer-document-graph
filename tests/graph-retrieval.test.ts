import { describe, expect, test } from "bun:test"
import { Effect, Layer, Result, Schema } from "effect"
import {
  defineDocument,
  defineDocumentGraph,
  defineEmbeddingProfile,
  EmbeddingProvider,
  GroundingHydrationFailed,
  GroundingHydrator,
  hydrateGrounding,
  InvalidEmbeddingOutput,
  InvalidSearchOutput,
  InvalidSearchQuery,
  makeGraphSearchScope,
  metadataEquals,
  metadataOneOf,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
  searchGraph,
  searchGraphHybrid,
  sectionChunking,
  semantic,
  text,
  type EmbeddingProviderService,
  type GraphSearchScopeInput,
  type GroundingHydratorService,
  type ProjectionSearchStoreService,
  type ProjectionTextSearchStoreService,
  type SearchGraphInput,
  type SearchHit,
  type SemanticSearchCandidate,
} from "../src/adapter.js"
import { parseTextSearchPolicy } from "../src/document/text-search-policy.js"

const ArticleId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("RetrievalArticleId"),
)
const NoteId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("RetrievalNoteId"),
)
const SearchMetadata = Schema.Struct({
  visibility: Schema.Literals(["public", "private"]),
})
const ArticleValue = Schema.Struct({
  id: ArticleId,
  title: Schema.Trimmed.check(Schema.isNonEmpty()),
  sections: Schema.NonEmptyArray(
    Schema.Struct({
      id: Schema.Trimmed.check(Schema.isNonEmpty()),
      text: Schema.Trimmed.check(Schema.isNonEmpty()),
      visibility: SearchMetadata.fields.visibility,
    }),
  ),
})
const NoteValue = Schema.Struct({
  id: NoteId,
  text: Schema.Trimmed.check(Schema.isNonEmpty()),
})

const ArticleDocument = defineDocument({
  id: ArticleId,
  value: ArticleValue,
  identify: (article) => article.id,
}).vectorise({
  id: "sections",
  version: "v1",
  metadata: SearchMetadata,
  select: (article) => {
    const [first, ...rest] = article.sections
    return {
      context: article.title,
      sections: [
        {
          key: first.id,
          content: first.text,
          metadata: { visibility: first.visibility },
        },
        ...rest.map((section) => ({
          key: section.id,
          content: section.text,
          metadata: { visibility: section.visibility },
        })),
      ],
    }
  },
  chunking: sectionChunking({ maximumCharacters: 128 }),
})

const PrivateNoteDocument = defineDocument({
  id: NoteId,
  value: NoteValue,
  identify: (note) => note.id,
}).vectorise({
  id: "note",
  version: "v1",
  select: (note) => ({
    sections: [{ key: "body", content: note.text }],
  }),
  chunking: sectionChunking({ maximumCharacters: 128 }),
})

const retrievalGraph = defineDocumentGraph({
  id: "retrieval-test",
  documents: {
    Article: ArticleDocument,
    PrivateNote: PrivateNoteDocument,
  },
})
const ArticleSections = retrievalGraph
  .document("Article")
  .projection("sections")
const retrievalScope = (
  input: GraphSearchScopeInput<
    "Article" | "PrivateNote",
    "sections" | "note"
  > = {},
) =>
  makeGraphSearchScope("retrieval-test", input, [
    { documentKind: "Article", projection: "sections" },
    { documentKind: "PrivateNote", projection: "note" },
  ])

const articleId = Schema.decodeSync(ArticleId)(
  "55555555-5555-4555-8555-555555555555",
)
const embeddingProfile = defineEmbeddingProfile({
  id: "test:retrieval",
  version: "v1",
  dimensions: 2,
})

const projectArticle = () => {
  const value = Schema.decodeSync(ArticleValue)({
    id: articleId,
    title: "Distribution guide",
    sections: [
      {
        id: "challenge",
        text: "National distribution was difficult.",
        visibility: "public",
      },
      {
        id: "outcome",
        text: "The work reached national retailers.",
        visibility: "public",
      },
    ],
  })

  return ArticleSections.project(value)
}

const makeCandidate = (
  revision: Effect.Success<ReturnType<typeof projectArticle>>,
  index: number,
  score: number,
): SemanticSearchCandidate => {
  const chunk = revision.chunks[index]
  if (chunk === undefined) {
    throw new Error(`Missing retrieval fixture chunk ${index}`)
  }

  return {
    score,
    chunkId: chunk.chunkId,
    documentKey: revision.documentKey,
    reference: revision.encodedTarget,
    projection: revision.projection,
    revisionHash: revision.revisionHash,
    sectionKey: chunk.sectionKey,
    sectionPart: chunk.sectionPart,
    content: chunk.content,
    metadata: chunk.metadata,
  }
}

const makeSemanticHit = (
  candidate: SemanticSearchCandidate,
): SearchHit => ({
  rank: 1,
  score: 1 / 61,
  signals: [
    {
      stream: {
        route: {
          _tag: "Projection",
          sourceKind: candidate.reference.kind,
          projection: candidate.projection.id,
        },
        channel: "semantic",
      },
      rank: 1,
      score: candidate.score,
      weight: 1,
      contribution: 1 / 61,
    },
  ],
  chunkId: candidate.chunkId,
  documentKey: candidate.documentKey,
  reference: candidate.reference,
  projection: candidate.projection,
  revisionHash: candidate.revisionHash,
  sectionKey: candidate.sectionKey,
  sectionPart: candidate.sectionPart,
  content: candidate.content,
  metadata: candidate.metadata,
})

const makeEmbeddingService = (
  queryVector: ReadonlyArray<number> = [0.25, 0.75],
) => {
  const queries: Array<string> = []
  const service: EmbeddingProviderService = {
    profile: embeddingProfile,
    embedDocuments: (requests) =>
      Effect.succeed(
        requests.map((request) => ({
          contentHash: request.contentHash,
          vector: [request.content.length, 1],
        })),
      ),
    embedQuery: (query) => {
      queries.push(query)
      return Effect.succeed(queryVector)
    },
  }

  return { service, queries }
}

const makeSearchStore = (
  candidates: ReadonlyArray<SemanticSearchCandidate>,
) => {
  const requests: Array<
    Parameters<ProjectionSearchStoreService["searchCandidates"]>[0]
  > = []
  const service: ProjectionSearchStoreService = {
    searchCandidates: (request) => {
      requests.push(request)
      return Effect.succeed(candidates)
    },
  }

  return { service, requests }
}

const makeTextSearchStore = (
  candidates: ReadonlyArray<SemanticSearchCandidate>,
): ProjectionTextSearchStoreService => ({
  searchTextCandidates: () => Effect.succeed(candidates),
})

const runSearch = (
  input: SearchGraphInput,
  embeddings: EmbeddingProviderService,
  store: ProjectionSearchStoreService,
) =>
  Effect.runPromise(
    searchGraph(input).pipe(
      Effect.provide(Layer.succeed(EmbeddingProvider, embeddings)),
      Effect.provide(Layer.succeed(ProjectionSearchStore, store)),
    ),
  )

describe("graph retrieval", () => {
  test("bounds metadata set-membership scopes at configuration time", () => {
    const values: [number, ...Array<number>] = [0]
    for (let value = 1; value <= 100; value += 1) {
      values.push(value)
    }

    expect(() => metadataOneOf("visibility", values)).toThrow(
      "at most 100 values",
    )
  })

  test("returns limited focal hits from a graph-scoped semantic search", async () => {
    const revision = await Effect.runPromise(projectArticle())
    const embeddings = makeEmbeddingService()
    const store = makeSearchStore([
      makeCandidate(revision, 0, 0.9),
      makeCandidate(revision, 1, 0.8),
    ])
    const scope = retrievalScope({
      exclude: ["PrivateNote"],
      includeProjections: ["sections"],
      where: [metadataEquals("visibility", "public")],
    })
    const strategy = semantic({ candidates: 20, results: 1 })

    const hits = await runSearch(
      {
        query: "Why was national distribution difficult?",
        scope,
        strategy,
      },
      embeddings.service,
      store.service,
    )

    expect(hits).toEqual([
      makeSemanticHit(makeCandidate(revision, 0, 0.9)),
    ])
    expect(hits[0]?.content).toBe(
      "National distribution was difficult.",
    )
    expect(revision.chunks[0].embeddingContent).toBe(
      "Distribution guide\n\nNational distribution was difficult.",
    )
    expect(embeddings.queries).toEqual([
      "Why was national distribution difficult?",
    ])
    expect(store.requests[0]).toEqual({
      vector: [0.25, 0.75],
      embeddingProfile,
      scope,
      candidates: strategy.candidates,
    })
  })

  test("rejects a candidate whose key disagrees with its reference", async () => {
    const revision = await Effect.runPromise(projectArticle())
    const embeddings = makeEmbeddingService()
    const candidate = makeCandidate(revision, 0, 0.9)
    const store = makeSearchStore([
      {
        ...candidate,
        reference: {
          ...candidate.reference,
          kind: "PrivateNote",
        },
      },
    ])

    const result = await Effect.runPromise(
      searchGraph({
        query: "distribution",
        scope: retrievalScope({ exclude: ["PrivateNote"] }),
        strategy: semantic(),
      }).pipe(
        Effect.provide(
          Layer.succeed(EmbeddingProvider, embeddings.service),
        ),
        Effect.provide(
          Layer.succeed(ProjectionSearchStore, store.service),
        ),
        Effect.result,
      ),
    )

    expect(result).toEqual(
      Result.fail(
        new InvalidSearchOutput({
          channel: "semantic",
          reason: "invalid_identity",
        }),
      ),
    )
  })

  test("rejects a chunk whose ID disagrees with its section part", async () => {
    const revision = await Effect.runPromise(projectArticle())
    const embeddings = makeEmbeddingService()
    const candidate = makeCandidate(revision, 0, 0.9)
    const store = makeSearchStore([
      { ...candidate, sectionPart: candidate.sectionPart + 1 },
    ])

    const result = await Effect.runPromise(
      searchGraph({
        query: "distribution",
        scope: retrievalScope(),
        strategy: semantic(),
      }).pipe(
        Effect.provide(
          Layer.succeed(EmbeddingProvider, embeddings.service),
        ),
        Effect.provide(
          Layer.succeed(ProjectionSearchStore, store.service),
        ),
        Effect.result,
      ),
    )

    expect(result).toEqual(
      Result.fail(
        new InvalidSearchOutput({
          channel: "semantic",
          reason: "invalid_identity",
        }),
      ),
    )
  })

  test("rejects conflicting same-chunk payloads before hybrid fusion", async () => {
    const revision = await Effect.runPromise(projectArticle())
    const embeddings = makeEmbeddingService()
    const candidate = makeCandidate(revision, 0, 0.9)
    const semanticStore = makeSearchStore([candidate])
    const textStore = makeTextSearchStore([
      { ...candidate, score: 3, content: "Conflicting stored content." },
    ])
    const semanticStrategy = semantic({ candidates: 5, results: 5 })
    const textPolicy = parseTextSearchPolicy(undefined)
    if (textPolicy === "disabled") {
      throw new Error("The default text policy unexpectedly disabled search")
    }
    const textStrategy = text({
      policy: textPolicy,
      candidates: 5,
      results: 5,
    })

    const result = await Effect.runPromise(
      searchGraphHybrid({
        query: "distribution",
        scope: retrievalScope(),
        route: {
          _tag: "Projection",
          sourceKind: "Article",
          projection: "sections",
        },
        semantic: semanticStrategy,
        text: textStrategy,
        results: semantic({ candidates: 1, results: 1 }).results,
        rankConstant: semanticStrategy.rankConstant,
      }).pipe(
        Effect.provide(
          Layer.succeed(EmbeddingProvider, embeddings.service),
        ),
        Effect.provide(
          Layer.succeed(ProjectionSearchStore, semanticStore.service),
        ),
        Effect.provide(
          Layer.succeed(ProjectionTextSearchStore, textStore),
        ),
        Effect.result,
      ),
    )

    expect(result).toEqual(
      Result.fail(
        new InvalidSearchOutput({
          channel: "fusion",
          reason: "conflicting_chunk",
        }),
      ),
    )
  })

  test("rejects a candidate outside the metadata scope", async () => {
    const revision = await Effect.runPromise(projectArticle())
    const embeddings = makeEmbeddingService()
    const candidate = makeCandidate(revision, 0, 0.9)
    const store = makeSearchStore([
      {
        ...candidate,
        metadata: { visibility: "private" },
      },
    ])

    const result = await Effect.runPromise(
      searchGraph({
        query: "distribution",
        scope: retrievalScope({
          where: [metadataEquals("visibility", "public")],
        }),
        strategy: semantic(),
      }).pipe(
        Effect.provide(
          Layer.succeed(EmbeddingProvider, embeddings.service),
        ),
        Effect.provide(
          Layer.succeed(ProjectionSearchStore, store.service),
        ),
        Effect.result,
      ),
    )

    expect(result).toEqual(
      Result.fail(
        new InvalidSearchOutput({
          channel: "semantic",
          reason: "out_of_scope",
        }),
      ),
    )
  })

  test("rejects an empty query before calling either adapter", async () => {
    const embeddings = makeEmbeddingService()
    const store = makeSearchStore([])

    const result = await Effect.runPromise(
      searchGraph({
        query: "   ",
        scope: retrievalScope(),
        strategy: semantic(),
      }).pipe(
        Effect.provide(
          Layer.succeed(EmbeddingProvider, embeddings.service),
        ),
        Effect.provide(
          Layer.succeed(ProjectionSearchStore, store.service),
        ),
        Effect.result,
      ),
    )

    expect(result).toEqual(
      Result.fail(new InvalidSearchQuery({ reason: "empty" })),
    )
    expect(embeddings.queries).toEqual([])
    expect(store.requests).toEqual([])
  })

  test("rejects a query vector outside the provider profile", async () => {
    const embeddings = makeEmbeddingService([0.5])
    const store = makeSearchStore([])

    const result = await Effect.runPromise(
      searchGraph({
        query: "distribution",
        scope: retrievalScope(),
        strategy: semantic(),
      }).pipe(
        Effect.provide(
          Layer.succeed(EmbeddingProvider, embeddings.service),
        ),
        Effect.provide(
          Layer.succeed(ProjectionSearchStore, store.service),
        ),
        Effect.result,
      ),
    )

    expect(result).toEqual(
      Result.fail(
        new InvalidEmbeddingOutput({
          profile: embeddingProfile.id,
          reason: "wrong_dimensions",
        }),
      ),
    )
    expect(store.requests).toEqual([])
  })

  test("hydrates a compact hit through an application-owned source adapter", async () => {
    const revision = await Effect.runPromise(projectArticle())
    const candidate = makeCandidate(revision, 0, 0.9)
    const hit = makeSemanticHit(candidate)
    const requests: Array<
      Parameters<GroundingHydratorService["hydrate"]>[0]
    > = []
    const hydrator: GroundingHydratorService = {
      hydrate: (request) => {
        requests.push(request)
        return Effect.succeed({
          content:
            "National distribution was difficult. Supporting detail.",
          metadata: request.hit.metadata,
        })
      },
    }

    const material = await Effect.runPromise(
      hydrateGrounding(hit).pipe(
        Effect.provide(Layer.succeed(GroundingHydrator, hydrator)),
      ),
    )

    expect(material.content).toContain("Supporting detail")
    expect(material.reference).toEqual(hit.reference)
    expect(material.sectionKey).toBe(hit.sectionKey)
    expect(requests).toEqual([{ hit, level: "section" }])
  })

  test("owns attribution for document-level grounding", async () => {
    const revision = await Effect.runPromise(projectArticle())
    const hit = makeSemanticHit(makeCandidate(revision, 0, 0.9))
    const hydrator: GroundingHydratorService = {
      hydrate: () =>
        Effect.succeed({
          content: "Complete attributed document material.",
          metadata: { visibility: "public" },
        }),
    }

    const material = await Effect.runPromise(
      hydrateGrounding(hit, { level: "document" }).pipe(
        Effect.provide(Layer.succeed(GroundingHydrator, hydrator)),
      ),
    )

    expect(material.reference).toEqual(hit.reference)
    expect(material.sectionKey).toBeUndefined()
  })

  test("rejects empty grounding payloads", async () => {
    const revision = await Effect.runPromise(projectArticle())
    const hit = makeSemanticHit(makeCandidate(revision, 0, 0.9))
    const hydrator: GroundingHydratorService = {
      hydrate: () =>
        Effect.succeed({ content: "   ", metadata: undefined }),
    }

    const result = await Effect.runPromise(
      hydrateGrounding(hit).pipe(
        Effect.provide(Layer.succeed(GroundingHydrator, hydrator)),
        Effect.result,
      ),
    )

    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) {
      expect(result.failure._tag).toBe("DocumentGraphUnavailable")
      expect(result.failure.operation).toBe("hydrate")
      expect(result.failure.reason).toBe("invalid_adapter_output")
      expect(result.failure.cause).toBeInstanceOf(GroundingHydrationFailed)
    }
  })
})

if (import.meta.url === "") {
  // @ts-expect-error Search scopes only accept document kinds in this graph.
  retrievalScope({ exclude: ["UnknownDocument"] })
}
