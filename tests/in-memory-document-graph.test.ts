import { describe, expect, test } from "bun:test"
import { Effect, Either, Layer, Option, Schema } from "effect"
import {
  defineDocument,
  defineDocumentGraph,
  defineEmbeddingProfile,
  EmbeddingProvider,
  GraphRelationStore,
  GraphRelationStoreFailed,
  ProjectionIndexConflict,
  ProjectionIndexStore,
  ProjectionIndexStoreFailed,
  sectionChunking,
  type EmbeddingProviderService,
  type ProjectedChunkRecord,
} from "../src/adapter.js"
import { inMemoryDocumentGraph } from "../src/in-memory.js"

const ArticleId = Schema.UUID.pipe(Schema.brand("InMemoryArticleId"))
const Visibility = Schema.Literal("public", "private")
const Article = Schema.Struct({
  id: ArticleId,
  title: Schema.NonEmptyTrimmedString,
  sections: Schema.NonEmptyArray(
    Schema.Struct({
      id: Schema.NonEmptyTrimmedString,
      content: Schema.NonEmptyTrimmedString,
      visibility: Visibility,
    }),
  ),
})
const ArticleMetadata = Schema.Struct({ visibility: Visibility })

const ArticleDocument = defineDocument({
  id: ArticleId,
  value: Article,
  identify: (article) => article.id,
}).vectorise({
  id: "article-content",
  version: "v1",
  metadata: ArticleMetadata,
  select: (article) => {
    const [first, ...rest] = article.sections
    return {
      context: article.title,
      sections: [
        {
          key: first.id,
          content: first.content,
          metadata: { visibility: first.visibility },
        },
        ...rest.map((section) => ({
          key: section.id,
          content: section.content,
          metadata: { visibility: section.visibility },
        })),
      ],
    }
  },
  chunking: sectionChunking({ maximumCharacters: 256 }),
})

const graph = defineDocumentGraph({
  id: "in-memory-test",
  documents: { Article: ArticleDocument },
})
const ArticleNode = graph.document("Article")
const ArticleContent = ArticleNode.projection("article-content")

const articleId = Schema.decodeSync(ArticleId)(
  "88888888-8888-4888-8888-888888888888",
)
const profile = defineEmbeddingProfile({
  id: "test:in-memory",
  version: "v1",
  dimensions: 2,
})

const makeArticle = (
  sections: Schema.Schema.Type<typeof Article>["sections"],
) =>
  Schema.decodeSync(Article)({
    id: articleId,
    title: "Distribution report",
    sections,
  })

const makeEmbeddings = () => {
  const batches: Array<ReadonlyArray<string>> = []
  const service: EmbeddingProviderService = {
    profile,
    embedDocuments: (requests) => {
      batches.push(requests.map((request) => request.content))
      return Effect.succeed(
        requests.map((request) => ({
          contentHash: request.contentHash,
          vector: request.content.includes("Confidential")
            ? [1, 0]
            : [0.8, 0.2],
        })),
      )
    },
    embedQuery: () => Effect.succeed([1, 0]),
  }

  return { batches, service }
}

const projectChunkRecord = (
  chunk: Effect.Effect.Success<
    ReturnType<typeof ArticleContent.project>
  >["chunks"][number],
): ProjectedChunkRecord => ({
  chunkId: chunk.chunkId,
  contentHash: chunk.contentHash,
  ordinal: chunk.ordinal,
  sectionKey: chunk.sectionKey,
  sectionIndex: chunk.sectionIndex,
  sectionPart: chunk.sectionPart,
  content: chunk.content,
  embeddingContent: chunk.embeddingContent,
  text: chunk.text,
  metadata: chunk.metadata,
})

describe("inMemoryDocumentGraph", () => {
  test("runs scoped text search without an embedding provider", async () => {
    const embeddings = makeEmbeddings()
    const storage = inMemoryDocumentGraph()
    const article = makeArticle([
      {
        id: "private",
        content: "Confidential national launch details.",
        visibility: "private",
      },
      {
        id: "public",
        content: "Public national distribution results.",
        visibility: "public",
      },
    ])

    const hits = await Effect.runPromise(
      Effect.gen(function*() {
        yield* ArticleContent.index(article).pipe(
          Effect.provideService(EmbeddingProvider, embeddings.service),
        )

        return yield* ArticleContent.search("confidential national", {
          strategy: "text",
          where: { visibility: "public" },
          candidates: { text: 1 },
          limit: 1,
        })
      }).pipe(Effect.provide(storage)),
    )

    expect(hits.map((hit) => hit.sectionKey)).toEqual(["public"])
    expect(hits[0]?.content).toBe(
      "Public national distribution results.",
    )
  })

  test("applies composable Boolean filters before text candidate limiting", async () => {
    const embeddings = makeEmbeddings()
    const storage = inMemoryDocumentGraph()
    const article = makeArticle([
      {
        id: "private",
        content: "Confidential national launch details.",
        visibility: "private",
      },
      {
        id: "public",
        content: "Public national distribution results.",
        visibility: "public",
      },
    ])

    const result = await Effect.runPromise(
      Effect.gen(function*() {
        yield* ArticleContent.index(article).pipe(
          Effect.provideService(EmbeddingProvider, embeddings.service),
        )
        const eitherVisibility = yield* ArticleContent.search("national", {
          strategy: "text",
          where: (filter) =>
            filter.any(
              filter.eq("visibility", "public"),
              filter.eq("visibility", "private"),
            ),
          candidates: { text: 2 },
          limit: 2,
        })
        const publicOnly = yield* ArticleContent.search("national", {
          strategy: "text",
          where: (filter) =>
            filter.all(
              filter.any(
                filter.eq("visibility", "public"),
                filter.eq("visibility", "private"),
              ),
              filter.not(filter.eq("visibility", "private")),
            ),
          candidates: { text: 1 },
          limit: 1,
        })
        return { eitherVisibility, publicOnly }
      }).pipe(Effect.provide(storage)),
    )

    expect(
      result.eitherVisibility.map((hit) => hit.sectionKey).sort(),
    ).toEqual(["private", "public"])
    expect(result.publicOnly.map((hit) => hit.sectionKey)).toEqual([
      "public",
    ])
  })

  test("prefilters retrieval and atomically removes stale chunks", async () => {
    const embeddings = makeEmbeddings()
    const storage = inMemoryDocumentGraph()
    const live = Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, embeddings.service),
      storage,
    )
    const initial = makeArticle([
      {
        id: "private",
        content: "Confidential national launch details.",
        visibility: "private",
      },
      {
        id: "public",
        content: "Public national distribution results.",
        visibility: "public",
      },
    ])
    const updated = makeArticle([
      {
        id: "public",
        content: "Public national distribution results.",
        visibility: "public",
      },
    ])
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const firstIndex = yield* ArticleContent.index(initial)
        const publicHits = yield* ArticleContent.search(
          "national distribution",
          {
            strategy: "semantic",
            where: { visibility: "public" },
            candidates: { semantic: 1 },
            limit: 1,
          },
        )
        const allHits = yield* graph.search("national distribution")
        const secondIndex = yield* ArticleContent.index(updated)
        const afterUpdate = yield* graph.search("national distribution")
        const removed = yield* ArticleNode.remove(articleId)
        const afterRemove = yield* graph.search("national distribution")
        const removedAgain = yield* ArticleNode.remove(articleId)

        return {
          firstIndex,
          publicHits,
          allHits,
          secondIndex,
          afterUpdate,
          removed,
          afterRemove,
          removedAgain,
        }
      }).pipe(Effect.provide(live)),
    )

    expect(result.firstIndex).toMatchObject({
      _tag: "Committed",
      embeddedContent: 2,
      inserted: 2,
      deleted: 0,
    })
    expect(result.publicHits.map((hit) => hit.sectionKey)).toEqual([
      "public",
    ])
    expect(result.allHits.map((hit) => hit.sectionKey)).toEqual([
      "private",
      "public",
    ])
    expect(result.secondIndex).toMatchObject({
      _tag: "Committed",
      embeddedContent: 0,
      reusedChunks: 1,
      inserted: 0,
      updated: 1,
      deleted: 1,
    })
    expect(result.afterUpdate.map((hit) => hit.sectionKey)).toEqual([
      "public",
    ])
    expect(result.removed).toEqual({
      deletedRevisions: 1,
      deletedChunks: 1,
      deletedRelations: 0,
    })
    expect(result.afterRemove).toEqual([])
    expect(result.removedAgain).toEqual({
      deletedRevisions: 0,
      deletedChunks: 0,
      deletedRelations: 0,
    })
    expect(embeddings.batches).toHaveLength(1)
  })

  test("hides and prunes projections removed from the graph schema", async () => {
    const embeddings = makeEmbeddings()
    const live = Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, embeddings.service),
      inMemoryDocumentGraph(),
    )
    const article = makeArticle([
      {
        id: "public",
        content: "Public national distribution results.",
        visibility: "public",
      },
    ])
    const revision = await Effect.runPromise(
      ArticleContent.project(article),
    )

    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const store = yield* ProjectionIndexStore
        const [firstChunk, ...remainingChunks] = revision.chunks
        yield* store.replaceRevision({
          key: {
            documentKey: revision.documentKey,
            projection: "retired-projection",
          },
          expectedToken: Option.none(),
          encodedTarget: revision.encodedTarget,
          projectionVersion: "v1",
          revisionHash: revision.revisionHash,
          embeddingProfile: profile,
          chunks: [
            projectChunkRecord(firstChunk),
            ...remainingChunks.map(projectChunkRecord),
          ],
          embeddings: revision.chunks.map((chunk) => ({
            contentHash: chunk.contentHash,
            vector: [1, 0],
          })),
        })

        const hiddenBeforePrune = yield* graph.search("national")
        const pruned = yield* graph.reconcileIndex()
        const storedAfterPrune = yield* store.loadRevision({
          documentKey: revision.documentKey,
          projection: "retired-projection",
        })
        return { hiddenBeforePrune, pruned, storedAfterPrune }
      }).pipe(Effect.provide(live)),
    )

    expect(result.hiddenBeforePrune).toEqual([])
    expect(result.pruned).toEqual({
      deletedRevisions: 1,
      deletedChunks: revision.chunks.length,
      deletedRelations: 0,
    })
    expect(Option.isNone(result.storedAfterPrune)).toBe(true)
  })

  test("rejects replacement with a stale optimistic token", async () => {
    const embeddings = makeEmbeddings()
    const live = Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, embeddings.service),
      inMemoryDocumentGraph(),
    )
    const article = makeArticle([
      {
        id: "public",
        content: "Public national distribution results.",
        visibility: "public",
      },
    ])
    const revision = await Effect.runPromise(
      ArticleContent.project(article),
    )
    const [firstChunk, ...remainingChunks] = revision.chunks
    const chunks: readonly [
      ProjectedChunkRecord,
      ...ReadonlyArray<ProjectedChunkRecord>,
    ] = [
      projectChunkRecord(firstChunk),
      ...remainingChunks.map(projectChunkRecord),
    ]

    const conflict = await Effect.runPromise(
      Effect.gen(function*() {
        yield* ArticleContent.index(article)
        const store = yield* ProjectionIndexStore
        return yield* store
          .replaceRevision({
            key: {
              documentKey: revision.documentKey,
              projection: revision.projection.id,
            },
            expectedToken: Option.none(),
            encodedTarget: revision.encodedTarget,
            projectionVersion: revision.projection.version,
            revisionHash: revision.revisionHash,
            embeddingProfile: profile,
            chunks,
            embeddings: [],
          })
          .pipe(Effect.either)
      }).pipe(Effect.provide(live)),
    )

    expect(conflict).toEqual(
      Either.left(
        new ProjectionIndexConflict({
          documentKey: revision.documentKey,
          projection: revision.projection.id,
        }),
      ),
    )
  })

  test("rejects a new revision without complete vector material", async () => {
    const article = makeArticle([
      {
        id: "public",
        content: "Public national distribution results.",
        visibility: "public",
      },
    ])
    const revision = await Effect.runPromise(
      ArticleContent.project(article),
    )
    const [firstChunk, ...remainingChunks] = revision.chunks
    const chunks: readonly [
      ProjectedChunkRecord,
      ...ReadonlyArray<ProjectedChunkRecord>,
    ] = [
      projectChunkRecord(firstChunk),
      ...remainingChunks.map(projectChunkRecord),
    ]

    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const store = yield* ProjectionIndexStore
        return yield* store
          .replaceRevision({
            key: {
              documentKey: revision.documentKey,
              projection: revision.projection.id,
            },
            expectedToken: Option.none(),
            encodedTarget: revision.encodedTarget,
            projectionVersion: revision.projection.version,
            revisionHash: revision.revisionHash,
            embeddingProfile: profile,
            chunks,
            embeddings: [],
          })
          .pipe(Effect.either)
      }).pipe(Effect.provide(inMemoryDocumentGraph())),
    )

    expect(result).toEqual(
      Either.left(
        new ProjectionIndexStoreFailed({
          operation: "replace_revision",
          reason: "invalid_stored_state",
          cause: "missing_embedding",
        }),
      ),
    )
  })

  test("enforces shared replacement contracts before mutating storage", async () => {
    const article = makeArticle([
      {
        id: "first",
        content: "First public distribution result.",
        visibility: "public",
      },
      {
        id: "second",
        content: "Second public distribution result.",
        visibility: "public",
      },
    ])
    const revision = await Effect.runPromise(
      ArticleContent.project(article),
    )
    const [firstChunk, secondChunk] = revision.chunks
    if (secondChunk === undefined) {
      throw new Error("The two-section fixture must project two chunks")
    }
    const first = projectChunkRecord(firstChunk)
    const second = projectChunkRecord(secondChunk)
    const replacement = {
      key: {
        documentKey: revision.documentKey,
        projection: revision.projection.id,
      },
      expectedToken: Option.none(),
      encodedTarget: revision.encodedTarget,
      projectionVersion: revision.projection.version,
      revisionHash: revision.revisionHash,
      embeddingProfile: profile,
      embeddings: [],
    } as const

    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const projections = yield* ProjectionIndexStore
        const relations = yield* GraphRelationStore
        const duplicateOrdinal = yield* projections
          .replaceRevision({
            ...replacement,
            chunks: [first, { ...second, ordinal: first.ordinal }],
          })
          .pipe(Effect.either)
        const blankContent = yield* projections
          .replaceRevision({
            ...replacement,
            chunks: [{ ...first, content: " " }],
          })
          .pipe(Effect.either)
        const mismatchedSource = yield* relations
          .replaceOutgoing({
            graph: revision.encodedTarget.graph,
            sourceDocumentKey: revision.documentKey,
            source: {
              ...revision.encodedTarget,
              graph: "another-graph",
            },
            relations: [],
          })
          .pipe(Effect.either)

        return { duplicateOrdinal, blankContent, mismatchedSource }
      }).pipe(Effect.provide(inMemoryDocumentGraph())),
    )

    expect(result.duplicateOrdinal).toEqual(
      Either.left(
        new ProjectionIndexStoreFailed({
          operation: "replace_revision",
          reason: "invalid_stored_state",
          cause: "duplicate_ordinal",
        }),
      ),
    )
    expect(result.blankContent).toEqual(
      Either.left(
        new ProjectionIndexStoreFailed({
          operation: "replace_revision",
          reason: "invalid_stored_state",
          cause: "blank_content",
        }),
      ),
    )
    expect(result.mismatchedSource).toEqual(
      Either.left(
        new GraphRelationStoreFailed({
          operation: "replace_outgoing",
          reason: "invalid_stored_state",
          cause: "source_graph_mismatch",
        }),
      ),
    )
  })

  test("keeps state isolated between separately created Layers", async () => {
    const embeddings = makeEmbeddings()
    const firstLive = Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, embeddings.service),
      inMemoryDocumentGraph(),
    )
    const secondLive = Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, embeddings.service),
      inMemoryDocumentGraph(),
    )
    const article = makeArticle([
      {
        id: "public",
        content: "Public national distribution results.",
        visibility: "public",
      },
    ])

    await Effect.runPromise(
      ArticleContent.index(article).pipe(Effect.provide(firstLive)),
    )
    const secondHits = await Effect.runPromise(
      graph
        .search("national distribution")
        .pipe(Effect.provide(secondLive)),
    )

    expect(secondHits).toEqual([])
  })
})
