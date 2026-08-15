import { describe, expect, test } from "bun:test"
import { Effect, Layer, Option, Result, Schema, Tracer } from "effect"
import {
  defineDocument,
  defineDocumentGraph,
  defineEmbeddingProfile,
  EmbeddingProvider,
  EmbeddingProviderFailed,
  GraphRelationStore,
  IndexRevisionTokenSchema,
  makeGraphSearchScope,
  ProjectionIndexConflict,
  ProjectionIndexStore,
  ProjectionIndexStoreFailed,
  ProjectionSearchStore,
  ProjectionSearchStoreFailed,
  ProjectionTextSearchStore,
  ProjectionTextSearchStoreFailed,
  semantic,
  toDocumentGraphErrorTelemetry,
  type EmbeddingProviderService,
  type GraphRelationStoreService,
  type GraphSearchScopeInput,
  type ProjectionIndexStoreService,
  type ProjectionSearchStoreService,
  type ProjectionTextSearchStoreService,
  type ReplaceProjectedRevision,
} from "../src/adapter.js"
import { makeChunkId } from "../src/document/document-identity.js"

const GuideId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("GraphOperationGuideId"),
)
const Guide = Schema.Struct({
  id: GuideId,
  title: Schema.Trimmed.check(Schema.isNonEmpty()),
  content: Schema.Trimmed.check(Schema.isNonEmpty()),
})
const GuideMetadata = Schema.Struct({
  kind: Schema.Literal("guide"),
})

const GuideDocument = defineDocument(Guide, { id: "id" }).vectorise({
  id: "guide-content",
  version: "v1",
  metadata: GuideMetadata,
  select: (guide) => ({
    context: guide.title,
    sections: [
      {
        key: "body",
        content: guide.content,
        metadata: { kind: "guide" as const },
      },
    ],
  }),
})

const graph = defineDocumentGraph({
  id: "graph-operations-test",
  documents: { Guide: GuideDocument },
})
const textDisabledGraph = defineDocumentGraph({
  id: "text-disabled-test",
  documents: {
    Guide: defineDocument(Guide, { id: "id" }).vectorise({
      id: "guide-content",
      version: "v1",
      text: "disabled",
      select: (value) => ({
        sections: [{ key: "body", content: value.content }],
      }),
    }),
  },
})
const GuideNode = graph.document("Guide")
const GuideContent = GuideNode.projection("guide-content")
const DisabledGuideContent = textDisabledGraph
  .document("Guide")
  .projection("guide-content")
const guideScope = (
  input: GraphSearchScopeInput<"Guide", "guide-content">,
) =>
  makeGraphSearchScope("graph-operations-test", input, [
    { documentKind: "Guide", projection: "guide-content" },
  ])

const guideId = Schema.decodeSync(GuideId)(
  "77777777-7777-4777-8777-777777777777",
)
const guide = Schema.decodeSync(Guide)({
  id: guideId,
  title: "National distribution",
  content: "The campaign reached retailers across the country.",
})
const profile = defineEmbeddingProfile({
  id: "test:graph-operations",
  version: "v1",
  dimensions: 2,
})
const token = Schema.decodeSync(IndexRevisionTokenSchema)("revision-1")

const makeServices = (input: {
  readonly projectionVersion?: string
  readonly semanticDelayMs?: number
  readonly textDelayMs?: number
} = {}) => {
  const embeddedDocuments: Array<string> = []
  const queries: Array<string> = []
  const searchRequests: Array<
    Parameters<ProjectionSearchStoreService["searchCandidates"]>[0]
  > = []
  const textSearchRequests: Array<
    Parameters<
      ProjectionTextSearchStoreService["searchTextCandidates"]
    >[0]
  > = []
  let replacement: ReplaceProjectedRevision | undefined

  const embeddings: EmbeddingProviderService = {
    profile,
    embedDocuments: (requests) => {
      embeddedDocuments.push(...requests.map((request) => request.content))
      return Effect.succeed(
        requests.map((request) => ({
          contentHash: request.contentHash,
          vector: [request.content.length, 1],
        })),
      )
    },
    embedQuery: (query) => {
      queries.push(query)
      return Effect.succeed([0.25, 0.75])
    },
  }

  const indexStore: ProjectionIndexStoreService = {
    loadRevision: () => Effect.succeed(Option.none()),
    replaceRevision: (next) => {
      replacement = next
      return Effect.succeed({
        token,
        inserted: next.chunks.length,
        updated: 0,
        deleted: 0,
      })
    },
    deleteRevision: () =>
      Effect.succeed({ deletedRevisions: 1, deletedChunks: 1 }),
    pruneGraph: () =>
      Effect.succeed({ deletedRevisions: 2, deletedChunks: 3 }),
  }

  const searchStore: ProjectionSearchStoreService = {
    searchCandidates: (request) => {
      searchRequests.push(request)
      const storedRevision = replacement
      if (storedRevision === undefined) {
        return Effect.succeed([])
      }

      const result = Effect.succeed(
        storedRevision.chunks.map((chunk, index) => ({
          score: 1 - index / 10,
          chunkId: chunk.chunkId,
          documentKey: storedRevision.key.documentKey,
          reference: storedRevision.encodedTarget,
          projection: {
            id: storedRevision.key.projection,
            version:
              input.projectionVersion ?? storedRevision.projectionVersion,
          },
          revisionHash: storedRevision.revisionHash,
          sectionKey: chunk.sectionKey,
          sectionPart: chunk.sectionPart,
          content: chunk.content,
          metadata: chunk.metadata,
        })),
      )
      return input.semanticDelayMs === undefined
        ? result
        : Effect.delay(result, `${input.semanticDelayMs} millis`)
    },
  }
  const textSearchStore: ProjectionTextSearchStoreService = {
    searchTextCandidates: (request) => {
      textSearchRequests.push(request)
      const storedRevision = replacement
      if (storedRevision === undefined) {
        return Effect.succeed([])
      }

      const result = Effect.succeed(
        storedRevision.chunks.map((chunk, index) => ({
          score: 10 - index,
          chunkId: chunk.chunkId,
          documentKey: storedRevision.key.documentKey,
          reference: storedRevision.encodedTarget,
          projection: {
            id: storedRevision.key.projection,
            version:
              input.projectionVersion ?? storedRevision.projectionVersion,
          },
          revisionHash: storedRevision.revisionHash,
          sectionKey: chunk.sectionKey,
          sectionPart: chunk.sectionPart,
          content: chunk.content,
          metadata: chunk.metadata,
        })),
      )
      return input.textDelayMs === undefined
        ? result
        : Effect.delay(result, `${input.textDelayMs} millis`)
    },
  }
  const relationStore: GraphRelationStoreService = {
    replaceOutgoing: () =>
      Effect.succeed({ inserted: 0, retained: 0, deleted: 0 }),
    deleteNode: () => Effect.succeed({ deleted: 0 }),
    pruneRelations: () => Effect.succeed({ deleted: 0 }),
    findOutgoing: () => Effect.succeed([]),
    findIncoming: () => Effect.succeed([]),
  }

  return {
    embeddedDocuments,
    embeddings,
    indexStore,
    queries,
    relationStore,
    searchStore,
    searchRequests,
    textSearchRequests,
    textSearchStore,
    layer: Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, embeddings),
      Layer.succeed(ProjectionIndexStore, indexStore),
      Layer.succeed(ProjectionSearchStore, searchStore),
      Layer.succeed(ProjectionTextSearchStore, textSearchStore),
      Layer.succeed(GraphRelationStore, relationStore),
    ),
  }
}

describe("document graph operations", () => {
  test("parses document identity once for a whole-document index", async () => {
    let identitySelections = 0
    const document = defineDocument({
      id: GuideId,
      value: Guide,
      identify: (value) => {
        identitySelections += 1
        return value.id
      },
    })
      .vectorise({
        id: "summary",
        version: "v1",
        select: (value) => ({
          sections: [{ key: "summary", content: value.title }],
        }),
      })
      .vectorise({
        id: "body",
        version: "v1",
        select: (value) => ({
          sections: [{ key: "body", content: value.content }],
        }),
      })
    const multiProjectionGraph = defineDocumentGraph({
      id: "parse-once-test",
      documents: { Guide: document },
    })
    const services = makeServices()

    const indexed = await Effect.runPromise(
      multiProjectionGraph
        .document("Guide")
        .index(guide)
        .pipe(Effect.provide(services.layer)),
    )

    expect(indexed.projections).toHaveLength(2)
    expect(identitySelections).toBe(1)
  })

  test("indexes and searches through the graph-bound API", async () => {
    const services = makeServices()

    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const indexed = yield* GuideNode.index(guide)
        const hits = yield* GuideContent.search(
          "Where did the campaign reach?",
        )
        return { indexed, hits }
      }).pipe(Effect.provide(services.layer)),
    )

    expect(result.indexed.projections[0]?.result._tag).toBe("Committed")
    expect(result.hits).toHaveLength(1)
    expect(result.hits[0]?.content).toBe(guide.content)
    expect(result.hits[0]?.reference).toEqual({
      graph: graph.id,
      kind: "Guide",
      id: guide.id,
    })
    expect(result.hits[0]?.metadata.kind).toBe("guide")
    expect(services.embeddedDocuments).toEqual([
      `${guide.title}\n\n${guide.content}`,
    ])
    expect(services.queries).toEqual(["Where did the campaign reach?"])
    expect(services.searchRequests[0]?.scope).toEqual(
      guideScope({
        include: ["Guide"],
        includeProjections: ["guide-content"],
      }),
    )
    expect(services.searchRequests[0]?.candidates).toBe(
      semantic().candidates,
    )
    expect(services.textSearchRequests[0]?.candidates).toBe(
      semantic().candidates,
    )
    expect(
      result.hits[0]?.signals.map((signal) => signal.stream.channel),
    ).toEqual(["semantic", "text"])
  })

  test("keeps hybrid results stable when channels finish in either order", async () => {
    const searchWithDelays = async (
      semanticDelayMs: number,
      textDelayMs: number,
    ) => {
      const services = makeServices({ semanticDelayMs, textDelayMs })
      return Effect.runPromise(
        Effect.gen(function*() {
          yield* GuideNode.index(guide)
          return yield* GuideContent.search("national distribution")
        }).pipe(Effect.provide(services.layer)),
      )
    }

    const semanticLast = await searchWithDelays(10, 0)
    const textLast = await searchWithDelays(0, 10)

    expect(semanticLast).toEqual(textLast)
    expect(
      semanticLast[0]?.signals.map((signal) => signal.stream.channel),
    ).toEqual(["semantic", "text"])
  })

  test("turns simple limits into a projection-bound search", async () => {
    const services = makeServices()
    const strategy = semantic({ candidates: 25, results: 5 })

    await Effect.runPromise(
      GuideContent
        .search("distribution", {
          strategy: "semantic",
          where: { kind: "guide" },
          candidates: { semantic: 25 },
          limit: 5,
        })
        .pipe(Effect.provide(services.layer)),
    )

    expect(services.searchRequests[0]?.scope).toEqual(
      guideScope({
        include: ["Guide"],
        includeProjections: ["guide-content"],
        where: [
          {
            _tag: "Equals",
            key: "kind",
            value: "guide",
          },
        ],
      }),
    )
    expect(services.searchRequests[0]?.candidates).toBe(
      strategy.candidates,
    )
  })

  test("returns invalid runtime search options through the typed channel", async () => {
    const services = makeServices()
    const results = await Effect.runPromise(
      Effect.all({
        graph: graph.search("distribution", { limit: 0 }).pipe(
          Effect.result,
        ),
        semantic: GuideContent.search("distribution", {
          strategy: "semantic",
          candidates: { semantic: 1 },
          limit: 2,
        }).pipe(Effect.result),
        text: GuideContent.search("distribution", {
          strategy: "text",
          candidates: { text: 0 },
        }).pipe(Effect.result),
      }).pipe(Effect.provide(services.layer)),
    )

    for (const result of Object.values(results)) {
      expect(Result.isFailure(result)).toBe(true)
      if (Result.isFailure(result)) {
        expect(result.failure._tag).toBe("InvalidSearchQuery")
        expect(result.failure.reason).toBe("invalid_options")
      }
    }
  })

  test("raises graph-wide candidates to the requested result limit", async () => {
    const services = makeServices()

    await Effect.runPromise(
      graph.search("distribution", {
        candidates: 1,
        limit: 5,
      }).pipe(Effect.provide(services.layer)),
    )

    expect(Number(services.searchRequests[0]?.candidates)).toBe(5)
  })

  test("sends text search the complete projection scope before its bound", async () => {
    const revision = await Effect.runPromise(GuideContent.project(guide))
    const chunk = revision.chunks[0]
    const requests: Array<
      Parameters<
        ProjectionTextSearchStoreService["searchTextCandidates"]
      >[0]
    > = []
    const textStore: ProjectionTextSearchStoreService = {
      searchTextCandidates: (request) => {
        requests.push(request)
        return Effect.succeed([
          {
            score: 3,
            chunkId: chunk.chunkId,
            documentKey: revision.documentKey,
            reference: revision.encodedTarget,
            projection: revision.projection,
            revisionHash: revision.revisionHash,
            sectionKey: chunk.sectionKey,
            sectionPart: chunk.sectionPart,
            content: chunk.content,
            metadata: chunk.metadata,
          },
        ])
      },
    }

    const hits = await Effect.runPromise(
      GuideContent.search("distribution", {
        strategy: "text",
        where: { kind: "guide" },
        candidates: { text: 7 },
        limit: 3,
      }).pipe(
        Effect.provide(
          Layer.succeed(ProjectionTextSearchStore, textStore),
        ),
      ),
    )

    expect(hits).toHaveLength(1)
    expect(requests).toHaveLength(1)
    expect(requests[0]?.scope).toEqual(
      guideScope({
        include: ["Guide"],
        includeProjections: ["guide-content"],
        where: [
          { _tag: "Equals", key: "kind", value: "guide" },
        ],
      }),
    )
    expect(Number(requests[0]?.candidates)).toBe(7)
    expect(requests[0]?.policy.language).toBe("english")
    expect(Number(requests[0]?.policy.weights.context)).toBe(2)
  })

  test("rejects text search when the projection disables it", async () => {
    const textStore: ProjectionTextSearchStoreService = {
      searchTextCandidates: () =>
        Effect.die(new Error("A disabled text projection reached storage")),
    }

    const result = await Effect.runPromise(
      DisabledGuideContent.search("distribution", {
        strategy: "text",
      }).pipe(
        Effect.provide(
          Layer.succeed(ProjectionTextSearchStore, textStore),
        ),
        Effect.result,
      ),
    )

    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) {
      expect(result.failure._tag).toBe("InvalidSearchQuery")
      expect(result.failure.reason).toBe("text_disabled")
    }
  })

  test("normalizes invalid text candidates through the public handle", async () => {
    const revision = await Effect.runPromise(GuideContent.project(guide))
    const chunk = revision.chunks[0]
    const candidate = {
      score: 3,
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
    const otherCandidate = {
      ...candidate,
      chunkId: makeChunkId({
        documentKey: candidate.documentKey,
        projection: candidate.projection.id,
        sectionKey: candidate.sectionKey,
        sectionPart: candidate.sectionPart + 1,
      }),
      sectionPart: candidate.sectionPart + 1,
    }
    const outOfScopeProjection = "other-projection"
    const cases = [
      {
        reason: "invalid_score" as const,
        candidates: [{ ...candidate, score: Number.NaN }],
      },
      {
        reason: "invalid_score" as const,
        candidates: [{ ...candidate, score: 0 }],
      },
      {
        reason: "duplicate_chunk" as const,
        candidates: [candidate, candidate],
      },
      {
        reason: "not_ranked" as const,
        candidates: [
          candidate,
          { ...otherCandidate, score: 4 },
        ],
      },
      {
        reason: "out_of_scope" as const,
        candidates: [
          {
            ...candidate,
            chunkId: makeChunkId({
              documentKey: candidate.documentKey,
              projection: outOfScopeProjection,
              sectionKey: candidate.sectionKey,
              sectionPart: candidate.sectionPart,
            }),
            projection: {
              ...candidate.projection,
              id: outOfScopeProjection,
            },
          },
        ],
      },
    ]

    for (const fixture of cases) {
      const textStore: ProjectionTextSearchStoreService = {
        searchTextCandidates: () => Effect.succeed(fixture.candidates),
      }
      const result = await Effect.runPromise(
        GuideContent.search("distribution", {
          strategy: "text",
          candidates: { text: 2 },
          limit: 2,
        }).pipe(
          Effect.provide(
            Layer.succeed(ProjectionTextSearchStore, textStore),
          ),
          Effect.result,
        ),
      )

      expect(Result.isFailure(result)).toBe(true)
      if (
        Result.isFailure(result) &&
        result.failure._tag === "DocumentGraphUnavailable"
      ) {
        expect(result.failure.reason).toBe("invalid_stored_data")
        expect(result.failure.cause).toMatchObject({
          _tag: "InvalidSearchOutput",
          channel: "text",
          reason: fixture.reason,
        })
      }
    }
  })

  test("rejects stale projection versions returned by storage", async () => {
    const services = makeServices({ projectionVersion: "stale" })

    const result = await Effect.runPromise(
      Effect.gen(function*() {
        yield* GuideNode.index(guide)
        return yield* GuideContent.search("distribution").pipe(
          Effect.result,
        )
      }).pipe(Effect.provide(services.layer)),
    )

    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) {
      expect(result.failure._tag).toBe("DocumentGraphUnavailable")
      if (result.failure._tag === "DocumentGraphUnavailable") {
        expect(result.failure.operation).toBe("search")
        expect(result.failure.reason).toBe("invalid_stored_data")
        expect(result.failure.cause).toMatchObject({
          _tag: "InvalidSearchOutput",
          channel: "semantic",
          reason: "out_of_scope",
        })
      }
    }
  })

  test("exposes one safe operational error for embedding failures", async () => {
    const services = makeServices()
    const providerFailure = new EmbeddingProviderFailed({
      profile: profile.id,
      reason: "unavailable",
      cause: new Error("provider credentials must stay internal"),
    })
    const failingEmbeddings: EmbeddingProviderService = {
      ...services.embeddings,
      embedDocuments: () => Effect.fail(providerFailure),
    }

    const result = await Effect.runPromise(
      GuideNode.index(guide).pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeed(EmbeddingProvider, failingEmbeddings),
            Layer.succeed(ProjectionIndexStore, services.indexStore),
            Layer.succeed(GraphRelationStore, services.relationStore),
          ),
        ),
        Effect.result,
      ),
    )

    expect(Result.isFailure(result)).toBe(true)
    if (
      Result.isFailure(result) &&
      result.failure._tag === "DocumentGraphUnavailable"
    ) {
      expect(result.failure.operation).toBe("index")
      expect(result.failure.reason).toBe("embedding_failed")
      expect(result.failure.cause).toMatchObject({
        _tag: "EmbeddingProviderFailed",
        profile: profile.id,
        reason: "unavailable",
      })
      expect(toDocumentGraphErrorTelemetry(result.failure)).toEqual({
        errorTag: "DocumentGraphUnavailable",
        errorOperation: "index",
        errorReason: "embedding_failed",
      })
      expect("cause" in toDocumentGraphErrorTelemetry(result.failure)).toBe(
        false,
      )
    }
  })

  test("keeps invalid caller queries precise", async () => {
    const services = makeServices()

    const result = await Effect.runPromise(
      graph.search("   ").pipe(
        Effect.provide(services.layer),
        Effect.result,
      ),
    )

    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) {
      expect(result.failure._tag).toBe("InvalidSearchQuery")
      expect(result.failure.reason).toBe("empty")
    }
  })

  test("normalizes search storage failures at the graph boundary", async () => {
    const services = makeServices()
    const storageFailure = new ProjectionSearchStoreFailed({
      reason: "unavailable",
      cause: new Error("database connection details"),
    })
    const failingSearchStore: ProjectionSearchStoreService = {
      searchCandidates: () => Effect.fail(storageFailure),
    }

    const result = await Effect.runPromise(
      graph.search("distribution").pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeed(EmbeddingProvider, services.embeddings),
            Layer.succeed(ProjectionSearchStore, failingSearchStore),
          ),
        ),
        Effect.result,
      ),
    )

    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) {
      expect(result.failure._tag).toBe("DocumentGraphUnavailable")
      if (result.failure._tag === "DocumentGraphUnavailable") {
        expect(result.failure.operation).toBe("search")
        expect(result.failure.reason).toBe("storage_failed")
        expect(result.failure.cause).toMatchObject({
          _tag: "ProjectionSearchStoreFailed",
          reason: "unavailable",
        })
      }
    }
  })

  test("normalizes text storage failures without requiring embeddings", async () => {
    const storageFailure = new ProjectionTextSearchStoreFailed({
      reason: "unavailable",
      cause: new Error("text index connection details"),
    })
    const failingTextStore: ProjectionTextSearchStoreService = {
      searchTextCandidates: () => Effect.fail(storageFailure),
    }

    const result = await Effect.runPromise(
      GuideContent.search("distribution", {
        strategy: "text",
      }).pipe(
        Effect.provide(
          Layer.succeed(ProjectionTextSearchStore, failingTextStore),
        ),
        Effect.result,
      ),
    )

    expect(Result.isFailure(result)).toBe(true)
    if (
      Result.isFailure(result) &&
      result.failure._tag === "DocumentGraphUnavailable"
    ) {
      expect(result.failure.operation).toBe("search")
      expect(result.failure.reason).toBe("storage_failed")
      expect(result.failure.cause).toMatchObject({
        _tag: "ProjectionTextSearchStoreFailed",
        reason: "unavailable",
      })
    }
  })

  test("keeps optimistic indexing conflicts precise", async () => {
    const services = makeServices()
    const conflictingStore: ProjectionIndexStoreService = {
      ...services.indexStore,
      replaceRevision: (replacement) =>
        Effect.fail(
          new ProjectionIndexConflict({
            documentKey: replacement.key.documentKey,
            projection: replacement.key.projection,
          }),
        ),
    }

    const result = await Effect.runPromise(
      GuideNode.index(guide).pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeed(EmbeddingProvider, services.embeddings),
            Layer.succeed(ProjectionIndexStore, conflictingStore),
            Layer.succeed(GraphRelationStore, services.relationStore),
          ),
        ),
        Effect.result,
      ),
    )

    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) {
      expect(result.failure._tag).toBe("ProjectionIndexConflict")
    }
  })

  test("normalizes removal and reconciliation storage failures", async () => {
    const services = makeServices()
    const removeFailure = new ProjectionIndexStoreFailed({
      operation: "delete_revision",
      reason: "unavailable",
      cause: new Error("remove connection details"),
    })
    const reconcileFailure = new ProjectionIndexStoreFailed({
      operation: "prune_graph",
      reason: "invalid_stored_state",
      cause: new Error("invalid row details"),
    })
    const failingStore: ProjectionIndexStoreService = {
      ...services.indexStore,
      deleteRevision: () => Effect.fail(removeFailure),
      pruneGraph: () => Effect.fail(reconcileFailure),
    }
    const layer = Layer.mergeAll(
      Layer.succeed(ProjectionIndexStore, failingStore),
      Layer.succeed(GraphRelationStore, services.relationStore),
    )

    const result = await Effect.runPromise(
      Effect.all({
        remove: GuideNode.remove(guide.id).pipe(Effect.result),
        reconcile: graph.reconcileIndex().pipe(Effect.result),
      }).pipe(Effect.provide(layer)),
    )

    expect(Result.isFailure(result.remove)).toBe(true)
    if (
      Result.isFailure(result.remove) &&
      result.remove.failure._tag === "DocumentGraphUnavailable"
    ) {
      expect(result.remove.failure.operation).toBe("remove")
      expect(result.remove.failure.reason).toBe("storage_failed")
    }

    expect(Result.isFailure(result.reconcile)).toBe(true)
    if (Result.isFailure(result.reconcile)) {
      expect(result.reconcile.failure.operation).toBe("reconcile_index")
      expect(result.reconcile.failure.reason).toBe("invalid_stored_data")
    }
  })

  test("traces operations without recording search content", async () => {
    const services = makeServices()
    const observed: Array<{
      readonly name: string
      readonly attributes: ReadonlyMap<string, unknown>
    }> = []
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options)
        observed.push({
          name: options.name,
          attributes: span.attributes,
        })
        return span
      },
    })

    await Effect.runPromise(
      GuideContent.search("private campaign wording").pipe(
        Effect.provide(services.layer),
        Effect.provide(Layer.succeed(Tracer.Tracer, tracer)),
      ),
    )

    const span = observed.find(
      (candidate) =>
        candidate.name === "honertia.document_graph.search",
    )
    expect(Object.fromEntries(span?.attributes ?? [])).toEqual({
      "document_graph.graph": "graph-operations-test",
      "document_graph.document_kind": "Guide",
      "document_graph.projection": "guide-content",
      "document_graph.search.candidates": 100,
      "document_graph.search.limit": 10,
      "document_graph.search.strategy": "hybrid",
    })
    expect(
      JSON.stringify(Object.fromEntries(span?.attributes ?? [])),
    ).not.toContain("private campaign wording")
    expect(
      JSON.stringify(Object.fromEntries(span?.attributes ?? [])),
    ).not.toContain(guide.content)
  })
})

if (import.meta.url === "") {
  // @ts-expect-error Indexing only accepts projections owned by Guide.
  graph.index("Guide", "unknown-projection", guide)

  // @ts-expect-error Search scopes only accept registered projections.
  guideScope({ includeProjections: ["unknown-projection"] })

  // @ts-expect-error Handles only accept projections owned by Guide.
  GuideNode.projection("unknown-projection")

  // @ts-expect-error Metadata values are inferred from GuideMetadata.
  GuideContent.search("query", { where: { kind: "article" } })

  // @ts-expect-error Unknown metadata keys are rejected.
  GuideContent.search("query", { where: { visibility: "public" } })

  GuideContent.search("query", {
    where: (filter) =>
      filter.all(
        filter.eq("kind", "guide"),
        filter.not(filter.oneOf("kind", ["guide"])),
      ),
  })

  GuideContent.search("query", {
    where: (filter) =>
      // @ts-expect-error Builder keys are inferred from GuideMetadata.
      filter.eq("visibility", "public"),
  })

  GuideContent.search("query", {
    where: (filter) =>
      // @ts-expect-error Builder values preserve schema literals.
      filter.eq("kind", "article"),
  })
}
