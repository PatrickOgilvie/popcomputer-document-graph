import { describe, expect, test } from "bun:test"
import { Effect, Layer, Result, Schema } from "effect"
import {
  defineDocument,
  defineDocumentGraph,
  defineEmbeddingProfile,
  EmbeddingProvider,
  GraphRelationStore,
  prepareGraphMutation,
  ProjectionIndexStore,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
  replayPreparedGraphMutation,
  sectionChunking,
  type EmbeddingProviderService,
  type GraphRelationStoreService,
  type ProjectionIndexStoreService,
  type ProjectionSearchStoreService,
  type ProjectionTextSearchStoreService,
} from "../src/adapter.js"
import type { DocumentGraphStorageService } from "../src/storage/document-graph-storage.js"
import { inMemoryDocumentGraph } from "../src/in-memory.js"

const ArticleId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("PreparedArticleId"),
)
const Article = Schema.Struct({
  id: ArticleId,
  title: Schema.Trimmed.check(Schema.isNonEmpty()),
  sections: Schema.NonEmptyArray(
    Schema.Struct({
      id: Schema.Trimmed.check(Schema.isNonEmpty()),
      label: Schema.Trimmed.check(Schema.isNonEmpty()),
      text: Schema.Trimmed.check(Schema.isNonEmpty()),
    }),
  ),
})
const articleDocument = <const Version extends string>(version: Version) =>
  defineDocument({
    id: ArticleId,
    value: Article,
    identify: (article) => article.id,
  }).vectorise({
    id: "article-content",
    version,
    select: (article) => ({
      context: article.title,
      sections: article.sections.map((section) => ({
        key: section.id,
        label: section.label,
        content: section.text,
      })),
    }),
    chunking: sectionChunking({ maximumCharacters: 256 }),
  })

const ArticleDocument = articleDocument("v1")
const ArticleDocumentV2 = articleDocument("v2")
const graph = defineDocumentGraph({
  id: "prepared-mutations",
  documents: { Article: ArticleDocument },
})
const graphV2 = defineDocumentGraph({
  id: "prepared-mutations",
  documents: { Article: ArticleDocumentV2 },
})
const ArticleNode = graph.document("Article")
const ArticleContent = ArticleNode.projection("article-content")
const ArticleContentV2 = graphV2
  .document("Article")
  .projection("article-content")

const profile = defineEmbeddingProfile({
  id: "test:prepared",
  version: "v1",
  dimensions: 1,
})
const embeddings: EmbeddingProviderService = {
  profile,
  embedDocuments: (requests) =>
    Effect.succeed(
      requests.map((request) => ({
        contentHash: request.contentHash,
        vector: [1],
      })),
    ),
  embedQuery: () => Effect.succeed([1]),
}

const article = (seed: string, text: string) =>
  Schema.decodeSync(Article)({
    id: `${seed}-1111-4111-8111-000000000000`,
    title: "Prepared article",
    sections: [{ id: "body", label: "body", text }],
  })

/**
 * Real in-memory storage whose mutation operations die, proving capture
 * performs no storage writes while ordinary reads still delegate.
 */
const makeWriteGuardedStorageLayer = () =>
  Effect.map(makeFreshStorage(), (storage) => ({
    ...storage,
    replaceRevision: () =>
      Effect.die("replaceRevision must not run during capture"),
    deleteRevision: () =>
      Effect.die("deleteRevision must not run during capture"),
    pruneGraph: () =>
      Effect.die("pruneGraph must not run during capture"),
    replaceOutgoing: () =>
      Effect.die("replaceOutgoing must not run during capture"),
    deleteNode: () =>
      Effect.die("deleteNode must not run during capture"),
    pruneRelations: () =>
      Effect.die("pruneRelations must not run during capture"),
  }))

/** Resolve one complete fresh in-memory storage as a service value. */
const makeFreshStorage = (): Effect.Effect<DocumentGraphStorageService> => Effect.gen(
  function*() {
    const projection: ProjectionIndexStoreService = yield* ProjectionIndexStore
    const relations: GraphRelationStoreService = yield* GraphRelationStore
    const semantic: ProjectionSearchStoreService = yield* ProjectionSearchStore
    const text: ProjectionTextSearchStoreService =
      yield* ProjectionTextSearchStore
    return { ...projection, ...semantic, ...text, ...relations }
  },
).pipe(Effect.provide(inMemoryDocumentGraph()))

/** Provide an explicit storage service value to indexing/replay effects. */
const storageLayer = (storage: DocumentGraphStorageService) =>
  Layer.mergeAll(
    Layer.succeed(ProjectionIndexStore, storage),
    Layer.succeed(GraphRelationStore, storage),
    Layer.succeed(ProjectionSearchStore, storage),
    Layer.succeed(ProjectionTextSearchStore, storage),
  )

describe("prepared graph mutations", () => {
  test("captures, orders, copies, and deeply freezes replacements", async () => {
    const retainedVector = [1]
    const aliasingEmbeddings: EmbeddingProviderService = {
      ...embeddings,
      embedDocuments: (requests) =>
        Effect.succeed(
          requests.map((request) => ({
            contentHash: request.contentHash,
            vector: retainedVector,
          })),
        ),
    }
    const live = Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, aliasingEmbeddings),
      inMemoryDocumentGraph(),
    )

    const captured = await Effect.runPromise(
      prepareGraphMutation(
        ArticleNode.index(article("aaaaaaaa", "Captured body.")),
      ).pipe(
        Effect.provide(live),
      ),
    )
    const prepared = captured.mutation

    retainedVector[0] = 9

    expect(prepared.operations.map((operation) => operation._tag)).toEqual([
      "ReplaceProjectedRevision",
      "ReplaceOutgoingGraphRelations",
    ])
    expect(captured.result.projections).toHaveLength(1)
    expect(Object.isFrozen(prepared)).toBe(true)
    expect(Object.isFrozen(prepared.operations)).toBe(true)
    expect(
      prepared.operations.every((operation) => Object.isFrozen(operation)),
    ).toBe(true)

    const [projection, relations] = prepared.operations
    if (projection?._tag !== "ReplaceProjectedRevision") {
      throw new Error("expected a projection replacement")
    }
    if (relations?._tag !== "ReplaceOutgoingGraphRelations") {
      throw new Error("expected a relation replacement")
    }
    expect(projection.input.embeddings[0]?.vector).toEqual([1])
    expect(Object.isFrozen(projection.input)).toBe(true)
    expect(Object.isFrozen(projection.input.key)).toBe(true)
    expect(Object.isFrozen(projection.input.encodedTarget)).toBe(true)
    expect(Object.isFrozen(projection.input.chunks)).toBe(true)
    expect(Object.isFrozen(projection.input.chunks[0])).toBe(true)
    expect(Object.isFrozen(projection.input.chunks[0].text)).toBe(true)
    expect(Object.isFrozen(projection.input.embeddings)).toBe(true)
    expect(Object.isFrozen(projection.input.embeddings[0]?.vector)).toBe(true)
    expect(Object.isFrozen(relations.input)).toBe(true)
    expect(Object.isFrozen(relations.input.source)).toBe(true)
    expect(Object.isFrozen(relations.input.relations)).toBe(true)
  })

  test("replay into fresh storage equals direct indexing", async () => {
    const live = Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, embeddings),
      inMemoryDocumentGraph(),
    )

    const { prepared, direct } = await Effect.runPromise(
      Effect.gen(function*() {
        const captured = yield* prepareGraphMutation(
          ArticleContent.index(article("cccccccc", "Replay body.")),
        )

        const directStorage = yield* makeFreshStorage()
        yield* ArticleContent.index(article("cccccccc", "Replay body.")).pipe(
          Effect.provide(
            Layer.merge(Layer.succeed(EmbeddingProvider, embeddings), storageLayer(directStorage)),
          ),
        )

        const operation = captured.mutation.operations[0]
        if (operation?._tag !== "ReplaceProjectedRevision") {
          throw new Error("expected a projection replacement")
        }
        const [lookup] = yield* directStorage.loadRevisions([operation.input.key])
        return {
          operation,
          prepared: captured.mutation,
          direct: lookup?.revision._tag === "Some"
            ? lookup.revision.value
            : undefined,
        }
      }).pipe(Effect.provide(live)),
    )

    const operation = prepared.operations[0]
    if (operation?._tag !== "ReplaceProjectedRevision") {
      throw new Error("expected a projection replacement")
    }

    const { report, replayed } = await Effect.runPromise(
      Effect.gen(function*() {
        const storage = yield* makeFreshStorage()
        const replay = yield* replayPreparedGraphMutation(prepared, storage)
        const [lookup] = yield* storage.loadRevisions([operation.input.key])
        return {
          report: replay,
          replayed: lookup?.revision._tag === "Some"
            ? lookup.revision.value
            : undefined,
        }
      }),
    )
    expect(report).toEqual({ replacedRevisions: 1, replacedRelationSets: 0 })

    expect(direct).toBeDefined()
    expect(replayed).toBeDefined()
    if (direct !== undefined && replayed !== undefined) {
      expect(replayed.revisionHash).toBe(direct.revisionHash)
      expect(replayed.chunks).toEqual(direct.chunks)
    }
  })

  test("capture performs no storage writes", async () => {
    const writeGuarded = await Effect.runPromise(makeWriteGuardedStorageLayer())
    const live = Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, embeddings),
      Layer.succeed(ProjectionIndexStore, writeGuarded),
      Layer.succeed(GraphRelationStore, writeGuarded),
    )

    const prepared = await Effect.runPromise(
      prepareGraphMutation(
        ArticleContent.index(article("dddddddd", "Captured body.")),
      ).pipe(
        Effect.map(({ mutation }) => mutation),
        Effect.provide(live),
      ),
    )

    expect(prepared.operations).toHaveLength(1)
  })

  test("delete and prune operations fail without reaching live storage", async () => {
    const writeGuarded = await Effect.runPromise(makeWriteGuardedStorageLayer())
    const live = Layer.mergeAll(
      Layer.succeed(ProjectionIndexStore, writeGuarded),
      Layer.succeed(GraphRelationStore, writeGuarded),
    )
    const revision = await Effect.runPromise(
      ArticleContent.project(article("abababab", "Guarded body.")),
    )

    const outcomes = await Effect.runPromise(
      Effect.gen(function*() {
        return [
          yield* Effect.result(prepareGraphMutation(
            Effect.gen(function*() {
              const projection = yield* ProjectionIndexStore
              return yield* projection.deleteRevision({
              documentKey: revision.documentKey,
              projection: revision.projection.id,
              })
            }),
          )),
          yield* Effect.result(prepareGraphMutation(
            Effect.gen(function*() {
              const projection = yield* ProjectionIndexStore
              return yield* projection.pruneGraph({
                graph: graph.id,
                registered: [],
              })
            }),
          )),
          yield* Effect.result(prepareGraphMutation(
            Effect.gen(function*() {
              const relations = yield* GraphRelationStore
              return yield* relations.deleteNode({
                graph: graph.id,
                documentKey: revision.documentKey,
              })
            }),
          )),
          yield* Effect.result(prepareGraphMutation(
            Effect.gen(function*() {
              const relations = yield* GraphRelationStore
              return yield* relations.pruneRelations({
                graph: graph.id,
                registered: [],
              })
            }),
          )),
        ]
      }).pipe(Effect.provide(live)),
    )

    expect(outcomes.map((outcome) => outcome._tag)).toEqual([
      "Failure",
      "Failure",
      "Failure",
      "Failure",
    ])
  })

  test("different versions targeting one projection are duplicate", async () => {
    const live = Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, embeddings),
      inMemoryDocumentGraph(),
    )

    const outcome = await Effect.runPromise(
      Effect.result(prepareGraphMutation(Effect.gen(function*() {
        yield* ArticleContent.index(article("eeeeeeee", "First pass."))
        yield* ArticleContentV2.index(article("eeeeeeee", "Second pass."))
      }))).pipe(Effect.provide(live)),
    )

    expect(Result.isFailure(outcome)).toBe(true)
    if (Result.isFailure(outcome)) {
      expect(outcome.failure._tag).toBe("DuplicatePreparedMutation")
      if (outcome.failure._tag === "DuplicatePreparedMutation") {
        expect(outcome.failure.identity).toContain("article-content")
      }
    }
  })

  test("replay performs no embedding calls", async () => {
    const live = Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, embeddings),
      inMemoryDocumentGraph(),
    )

    const prepared = await Effect.runPromise(
      Effect.gen(function*() {
        const captured = yield* prepareGraphMutation(
          ArticleContent.index(article("ffffffff", "No network.")),
        )
        return captured.mutation
      }).pipe(Effect.provide(live)),
    )

    const report = await Effect.runPromise(
      Effect.gen(function*() {
        const storage = yield* makeFreshStorage()
        return yield* replayPreparedGraphMutation(prepared, storage)
      }),
    )
    expect(report.replacedRevisions).toBe(1)
  })
})
