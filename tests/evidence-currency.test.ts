import { describe, expect, test } from "bun:test"
import { Effect, Layer, Option, Schema } from "effect"
import {
  defineDocument,
  defineDocumentGraph,
  defineEmbeddingProfile,
  EmbeddingProvider,
  evidenceReferenceFromHit,
  makeChunkId,
  ProjectionIndexStore,
  sectionChunking,
  verifyEvidenceCurrency,
  type EmbeddingProviderService,
  type EvidenceReference,
} from "../src/adapter.js"
import type { ProjectionIndexStoreService } from "../src/indexing/projection-index.js"
import { inMemoryDocumentGraph } from "../src/in-memory.js"

const ArticleId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("CurrencyArticleId"),
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
  id: "evidence-currency",
  documents: { Article: ArticleDocument },
})
const graphV2 = defineDocumentGraph({
  id: "evidence-currency",
  documents: { Article: ArticleDocumentV2 },
})
const ArticleNode = graph.document("Article")
const ArticleContent = ArticleNode.projection("article-content")
const ArticleContentV2 = graphV2
  .document("Article")
  .projection("article-content")

const profile = defineEmbeddingProfile({
  id: "test:currency",
  version: "v1",
  dimensions: 1,
})
const profileV2 = defineEmbeddingProfile({
  id: "test:currency",
  version: "v2",
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
const embeddingsV2: EmbeddingProviderService = {
  ...embeddings,
  profile: profileV2,
}

const liveLayer = () =>
  Layer.mergeAll(
    Layer.succeed(EmbeddingProvider, embeddings),
    inMemoryDocumentGraph(),
  )

const article = (text: string) =>
  Schema.decodeSync(Article)({
    id: "11111111-1111-4111-8111-000000000000",
    title: "Currency article",
    sections: [{ id: "body", label: "body", text }],
  })

/** Reference built exactly the way an application carries a search hit forward. */
const referenceOf = (
  revision: Awaited<ReturnType<typeof runProjection>>,
): EvidenceReference => ({
  documentKey: revision.documentKey,
  projectionId: revision.projection.id,
  projectionVersion: revision.projection.version,
  revisionHash: revision.revisionHash,
})

const otherArticle = (text: string) =>
  Schema.decodeSync(Article)({
    id: "22222222-2222-4222-8222-000000000000",
    title: "Currency article",
    sections: [{ id: "body", label: "body", text }],
  })

const runProjection = (value: Schema.Schema.Type<typeof Article>) =>
  Effect.runPromise(ArticleContent.project(value))

const dyingStore: ProjectionIndexStoreService = {
  loadRevisions: () => Effect.die("storage must not be read"),
  replaceRevision: () => Effect.die("storage must not be read"),
  deleteRevision: () => Effect.die("storage must not be read"),
  pruneGraph: () => Effect.die("storage must not be read"),
}

describe("verifyEvidenceCurrency", () => {
  test("reports Current before any re-index and Stale afterwards", async () => {
    const live = liveLayer()
    const original = article("Original body text.")
    await Effect.runPromise(
      ArticleContent.index(original).pipe(Effect.provide(live)),
    )
    const firstReference = referenceOf(await runProjection(original))

    const beforeReindex = await Effect.runPromise(
      verifyEvidenceCurrency([firstReference]).pipe(Effect.provide(live)),
    )
    expect(beforeReindex).toEqual(["Current"])

    const replacement = article("Replacement body text.")
    await Effect.runPromise(
      ArticleContent.index(replacement).pipe(Effect.provide(live)),
    )
    const secondReference = referenceOf(
      await runProjection(article("Replacement body text.")),
    )

    const afterReindex = await Effect.runPromise(
      verifyEvidenceCurrency([firstReference, secondReference]).pipe(
        Effect.provide(live),
      ),
    )
    expect(afterReindex).toEqual(["Stale", "Current"])
  })

  test("reports Missing after the revision is deleted or unknown", async () => {
    const live = liveLayer()
    const doomed = article("Doomed body.")
    await Effect.runPromise(
      ArticleContent.index(doomed).pipe(Effect.provide(live)),
    )
    const reference = referenceOf(await runProjection(doomed))
    const neverIndexed = referenceOf(
      await runProjection(otherArticle("Never indexed body.")),
    )

    await Effect.runPromise(
      Effect.gen(function*() {
        const store = yield* ProjectionIndexStore
        yield* store.deleteRevision({
          documentKey: reference.documentKey,
          projection: reference.projectionId,
        })
      }).pipe(Effect.provide(live)),
    )

    const currencies = await Effect.runPromise(
      verifyEvidenceCurrency([
        reference,
        neverIndexed,
        { ...reference, projectionId: "no-such-projection" },
      ]).pipe(Effect.provide(live)),
    )
    expect(currencies).toEqual(["Missing", "Missing", "Missing"])
  })

  test("reports Stale when the projection version changes", async () => {
    const storage = inMemoryDocumentGraph()
    const liveV1 = Layer.merge(
      Layer.succeed(EmbeddingProvider, embeddings),
      storage,
    )
    const liveV2 = Layer.merge(
      Layer.succeed(EmbeddingProvider, embeddings),
      storage,
    )
    const value = article("Versioned body.")

    await Effect.runPromise(
      ArticleContent.index(value).pipe(Effect.provide(liveV1)),
    )
    const reference = referenceOf(await runProjection(value))
    await Effect.runPromise(
      ArticleContentV2.index(value).pipe(Effect.provide(liveV2)),
    )

    const currencies = await Effect.runPromise(
      verifyEvidenceCurrency([reference]).pipe(Effect.provide(storage)),
    )
    expect(currencies).toEqual(["Stale"])
  })

  test("keeps identical projected evidence current after re-embedding", async () => {
    const storage = inMemoryDocumentGraph()
    const liveV1 = Layer.merge(
      Layer.succeed(EmbeddingProvider, embeddings),
      storage,
    )
    const liveV2 = Layer.merge(
      Layer.succeed(EmbeddingProvider, embeddingsV2),
      storage,
    )
    const value = article("Re-embedded body.")

    await Effect.runPromise(
      ArticleContent.index(value).pipe(Effect.provide(liveV1)),
    )
    const reference = referenceOf(await runProjection(value))
    await Effect.runPromise(
      ArticleContent.index(value).pipe(Effect.provide(liveV2)),
    )

    const currencies = await Effect.runPromise(
      verifyEvidenceCurrency([reference]).pipe(Effect.provide(storage)),
    )
    expect(currencies).toEqual(["Current"])
  })

  test("empty input touches no storage", async () => {
    const failingReads = Layer.succeed(
      ProjectionIndexStore,
      dyingStore,
    )

    const currencies = await Effect.runPromise(
      verifyEvidenceCurrency([]).pipe(Effect.provide(failingReads)),
    )
    expect(currencies).toEqual([])
  })

  test("deduplicates revision keys into one ordered batch read", async () => {
    const reference = referenceOf(await runProjection(article("Batch body.")))
    const batches: Array<ReadonlyArray<unknown>> = []
    const store: ProjectionIndexStoreService = {
      ...dyingStore,
      loadRevisions: (keys) => {
        batches.push(keys)
        return Effect.succeed(
          keys.map((key) => ({ key, revision: Option.none() })),
        )
      },
    }

    const currencies = await Effect.runPromise(
      verifyEvidenceCurrency([reference, reference]).pipe(
        Effect.provide(Layer.succeed(ProjectionIndexStore, store)),
      ),
    )

    expect(currencies).toEqual(["Missing", "Missing"])
    expect(batches).toHaveLength(1)
    expect(batches[0]).toHaveLength(1)
  })

  test("builds references from search hits", async () => {
    const reference = referenceOf(await runProjection(article("Hit body.")))
    const hit = {
      rank: 1,
      score: 1,
      signals: [
        {
          stream: {
            route: { _tag: "Projection", sourceKind: "Article", projection: "article-content" },
            channel: "semantic",
          },
          rank: 1,
          score: 1,
          weight: 1,
          contribution: 1,
        },
      ] as const,
      chunkId: makeChunkId({
        documentKey: reference.documentKey,
        projection: reference.projectionId,
        sectionKey: "body",
        sectionPart: 0,
      }),
      documentKey: reference.documentKey,
      reference: { graph: graph.id, kind: "Article", id: "11111111-1111-4111-8111-000000000000" },
      projection: {
        id: reference.projectionId,
        version: reference.projectionVersion,
      },
      revisionHash: reference.revisionHash,
      sectionKey: "body",
      sectionPart: 0,
      content: "Hit body.",
      metadata: undefined,
    }

    expect(evidenceReferenceFromHit(hit)).toEqual(reference)
  })
})
