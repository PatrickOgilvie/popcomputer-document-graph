/**
 * All-or-nothing graph publication with prepared mutations.
 *
 * Capture runs the ordinary indexing program without writing any storage:
 * chunking, embedding calls, and planning execute normally. Freezing produces
 * an immutable, deterministically ordered operation set that replays into any
 * storage, so publication can commit graph rows together with application
 * rows inside one short transaction that never performs a network call.
 */
import { Effect, Layer, Schema } from "effect"

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
  type DocumentGraphStorageService,
  type EmbeddingProviderService,
} from "../src/adapter.js"
import { inMemoryDocumentGraph } from "../src/in-memory.js"

const InsightId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("InsightId"),
)
const Insight = Schema.Struct({
  id: InsightId,
  title: Schema.Trimmed.check(Schema.isNonEmpty()),
  body: Schema.Trimmed.check(Schema.isNonEmpty()),
})

const InsightDocument = defineDocument({
  id: InsightId,
  value: Insight,
  identify: (insight) => insight.id,
}).vectorise({
  id: "insight-content",
  version: "v1",
  select: (insight) => ({
    context: insight.title,
    sections: [{ key: "body", label: "body", content: insight.body }],
  }),
  chunking: sectionChunking({ maximumCharacters: 256 }),
})

const graph = defineDocumentGraph({
  id: "atomic-publication",
  documents: { Insight: InsightDocument },
})
const InsightContent = graph.document("Insight").projection("insight-content")

const profile = defineEmbeddingProfile({
  id: "example:publication",
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

/** Resolve one independent storage instance to replay into. */
const freshStorage: Effect.Effect<DocumentGraphStorageService> =
  Effect.gen(function* () {
    const projection = yield* ProjectionIndexStore
    const relations = yield* GraphRelationStore
    const semantic = yield* ProjectionSearchStore
    const text = yield* ProjectionTextSearchStore
    return { ...projection, ...semantic, ...text, ...relations }
  }).pipe(Effect.provide(inMemoryDocumentGraph()))

const program = Effect.gen(function* () {
  const insight = Schema.decodeSync(Insight)({
    id: "11111111-1111-4111-8111-000000000000",
    title: "Capture before writing",
    body: "Resolve embeddings first, then publish atomically.",
  })

  // 1. Capture: no storage writes; embeddings resolve here.
  const { mutation } = yield* prepareGraphMutation(
    InsightContent.index(insight),
  )

  // 3. Replay inside the application's own write transaction. In PostgreSQL
  // this is `postgresDocumentGraph({ transaction: client })`; here a fresh
  // in-memory storage stands in for it.
  const target = yield* freshStorage
  const report = yield* replayPreparedGraphMutation(mutation, target)

  // 4. Mark application state indexed, then COMMIT - all or nothing.
  console.log(
    `published ${report.replacedRevisions} revision(s), ` +
      `${report.replacedRelationSets} relation set(s)`,
  )
})

await Effect.runPromise(
  program.pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.succeed(EmbeddingProvider, embeddings),
        inMemoryDocumentGraph(),
      ),
    ),
  ),
)
