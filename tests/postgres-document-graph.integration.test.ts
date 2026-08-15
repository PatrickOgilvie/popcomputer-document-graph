import { describe, expect, test } from "bun:test"
import { readFile } from "node:fs/promises"
import {
  defineDocument,
  defineDocumentGraph,
  defineEmbeddingProfile,
  EmbeddingProvider,
  ProjectionIndexStore,
  sectionChunking,
  type EmbeddingProviderService,
} from "../src/adapter.js"
import { Effect, Result, Layer, Option, Schema } from "effect"
import { Pool } from "pg"
import { postgresDocumentGraph } from "../src/postgres.js"
import { verifyDocumentGraphStorageConformance } from "../src/testing.js"

const databaseUrl =
  Bun.env.TEST_DATABASE_URL ??
  Bun.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE
const runIntegrationTests =
  Bun.env.RUN_DOCUMENT_GRAPH_POSTGRES_TESTS === "true" &&
  databaseUrl !== undefined

const ArticleId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("PostgresArticleId"),
)
const Visibility = Schema.Literals(["public", "private"])
const Article = Schema.Struct({
  id: ArticleId,
  title: Schema.Trimmed.check(Schema.isNonEmpty()),
  relatedIds: Schema.Array(ArticleId),
  sections: Schema.NonEmptyArray(
    Schema.Struct({
      id: Schema.Trimmed.check(Schema.isNonEmpty()),
      label: Schema.Trimmed.check(Schema.isNonEmpty()),
      text: Schema.Trimmed.check(Schema.isNonEmpty()),
      visibility: Visibility,
    }),
  ),
})
const ArticleDocument = defineDocument({
  id: ArticleId,
  value: Article,
  identify: (article) => article.id,
}).vectorise({
  id: "article-content",
  version: "v1",
  metadata: Schema.Struct({ visibility: Visibility }),
  text: {
    language: "english",
    weights: { context: 1, label: 10, content: 1 },
  },
  select: (article) => {
    const [first, ...rest] = article.sections
    return {
      context: article.title,
      sections: [
        {
          key: first.id,
          label: first.label,
          content: first.text,
          metadata: { visibility: first.visibility },
        },
        ...rest.map((section) => ({
          key: section.id,
          label: section.label,
          content: section.text,
          metadata: { visibility: section.visibility },
        })),
      ],
    }
  },
  chunking: sectionChunking({ maximumCharacters: 256 }),
})
const graph = defineDocumentGraph({
  id: "postgres-contract",
  documents: { Article: ArticleDocument },
  relations: (relation) => ({
    cites: relation({
      from: "Article",
      to: "Article",
      version: "v1",
      select: (article) => article.relatedIds,
    }),
  }),
})
const ArticleNode = graph.document("Article")
const ArticleContent = ArticleNode.projection("article-content")
const profile = defineEmbeddingProfile({
  id: "test:postgres",
  version: "v1",
  dimensions: 2,
})

const migrateInSchema = async (
  pool: Pool,
  schema: string,
): Promise<void> => {
  const migration = await readFile(
    new URL("../migrations/postgres/0001_initial.sql", import.meta.url),
    "utf8",
  )
  await pool.query(
    migration.replaceAll('"honertia_document_graph"', `"${schema}"`),
  )
}

describe("postgresDocumentGraph", () => {
  if (!runIntegrationTests || databaseUrl === undefined) {
    test.skip(
      "reuses vectors, replaces complete revisions, and searches within scope",
      () => undefined,
    )
    return
  }

  test(
    "reuses vectors, replaces complete revisions, and searches within scope",
    async () => {
      const schema = `document_graph_${crypto.randomUUID().replaceAll("-", "")}`
      const pool = new Pool({ connectionString: databaseUrl, max: 4 })
      const embeddedBatches: Array<ReadonlyArray<string>> = []
      const embeddings: EmbeddingProviderService = {
        profile,
        embedDocuments: (requests) => {
          embeddedBatches.push(requests.map((request) => request.content))
          return Effect.succeed(
            requests.map((request) => ({
              contentHash: request.contentHash,
              vector: request.content.includes("Secret")
                ? [1, 0]
                : [0.8, 0.2],
            })),
          )
        },
        embedQuery: () => Effect.succeed([1, 0]),
      }
      const articleId = Schema.decodeSync(ArticleId)(
        "99999999-9999-4999-8999-999999999999",
      )
      const article = (
        sections: Schema.Schema.Type<typeof Article>["sections"],
        relatedIds: Schema.Schema.Type<typeof Article>["relatedIds"] = [],
      ) =>
        Schema.decodeSync(Article)({
          id: articleId,
          title: "Launch report",
          relatedIds,
          sections,
        })

      try {
        await migrateInSchema(pool, schema)
        const live = Layer.mergeAll(
          Layer.succeed(EmbeddingProvider, embeddings),
          postgresDocumentGraph({ pool, schema }),
        )
        const initial = article([
          {
            id: "secret",
            label: "confidential",
            text: "Secret national launch details.",
            visibility: "private",
          },
          {
            id: "results",
            label: "launch",
            text: "Published national distribution results.",
            visibility: "public",
          },
        ])
        const metadataOnly = article([
          {
            id: "secret",
            label: "confidential",
            text: "Secret national launch details.",
            visibility: "public",
          },
          {
            id: "results",
            label: "launch",
            text: "Published national distribution results.",
            visibility: "public",
          },
        ])
        const withoutStaleSection = article([
          {
            id: "results",
            label: "launch",
            text: "Published national distribution results.",
            visibility: "public",
          },
        ])

        const result = await Effect.runPromise(
          Effect.gen(function*() {
            const conformance =
              yield* verifyDocumentGraphStorageConformance()
            const first = yield* ArticleContent.index(initial)
            const publicBefore = yield* ArticleContent.search("national", {
              strategy: "semantic",
              where: { visibility: "public" },
              candidates: { semantic: 1 },
              limit: 1,
            })
            const textPublicBefore = yield* ArticleContent.search(
              "national",
              {
                strategy: "text",
                where: { visibility: "public" },
                candidates: { text: 1 },
                limit: 1,
              },
            )
            const weightedText = yield* ArticleContent.search("launch", {
              strategy: "text",
              candidates: { text: 2 },
              limit: 2,
            })
            const booleanText = yield* ArticleContent.search("national", {
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
            const emptyLexemes = yield* ArticleContent.search("the and", {
              strategy: "text",
            })
            const second = yield* ArticleContent.index(metadataOnly)
            const publicAfter = yield* ArticleContent.search("national", {
              strategy: "semantic",
              where: { visibility: "public" },
              candidates: { semantic: 2 },
              limit: 2,
            })
            const staleRevision = yield* ArticleContent.project(metadataOnly)
            const store = yield* ProjectionIndexStore
            const staleSnapshot = yield* store.loadRevision({
              documentKey: staleRevision.documentKey,
              projection: staleRevision.projection.id,
            })
            const third = yield* ArticleContent.index(withoutStaleSection)
            const finalHits = yield* graph.search("national")
            const [staleFirst, ...staleRest] = staleRevision.chunks
            const chunkRecord = (
              chunk: typeof staleFirst,
            ) => ({
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
            const staleWrite = yield* store.replaceRevision({
              key: {
                documentKey: staleRevision.documentKey,
                projection: staleRevision.projection.id,
              },
              expectedToken: Option.map(
                staleSnapshot,
                (snapshot) => snapshot.token,
              ),
              encodedTarget: staleRevision.encodedTarget,
              projectionVersion: staleRevision.projection.version,
              revisionHash: staleRevision.revisionHash,
              embeddingProfile: profile,
              chunks: [
                chunkRecord(staleFirst),
                ...staleRest.map(chunkRecord),
              ],
              embeddings: [],
            }).pipe(Effect.result)
            return {
              conformance,
              first,
              publicBefore,
              textPublicBefore,
              weightedText,
              booleanText,
              emptyLexemes,
              second,
              publicAfter,
              third,
              finalHits,
              staleWrite,
            }
          }).pipe(Effect.provide(live)),
        )

        expect(result.conformance.projectionIndex.capability).toBe(
          "projection_index",
        )
        expect(result.conformance.graphRelations.capability).toBe(
          "graph_relations",
        )
        expect(result.conformance.retrieval.map((report) => report.channel)).toEqual([
          "semantic",
          "text",
        ])

        expect(result.first).toMatchObject({
          _tag: "Committed",
          embeddedContent: 2,
          inserted: 2,
          deleted: 0,
        })
        expect(result.publicBefore.map((hit) => hit.sectionKey)).toEqual([
          "results",
        ])
        expect(result.textPublicBefore.map((hit) => hit.sectionKey)).toEqual([
          "results",
        ])
        expect(result.weightedText.map((hit) => hit.sectionKey)).toEqual([
          "results",
          "secret",
        ])
        expect(result.booleanText.map((hit) => hit.sectionKey)).toEqual([
          "results",
        ])
        expect(result.emptyLexemes).toEqual([])
        expect(result.second).toMatchObject({
          _tag: "Committed",
          embeddedContent: 0,
          reusedChunks: 2,
          inserted: 0,
          updated: 2,
          deleted: 0,
        })
        expect(result.publicAfter.map((hit) => hit.sectionKey)).toEqual([
          "secret",
          "results",
        ])
        expect(result.third).toMatchObject({
          _tag: "Committed",
          embeddedContent: 0,
          reusedChunks: 1,
          inserted: 0,
          updated: 1,
          deleted: 1,
        })
        expect(result.finalHits.map((hit) => hit.sectionKey)).toEqual([
          "results",
        ])
        expect(Result.isFailure(result.staleWrite)).toBe(true)
        if (Result.isFailure(result.staleWrite)) {
          expect(result.staleWrite.failure._tag).toBe("ProjectionIndexConflict")
        }
        expect(embeddedBatches).toHaveLength(1)

        const persisted = await pool.query<{
          count: string
          metadata: unknown
          dimensions: number
          text_context: string | null
          text_label: string | null
          text_content: string
        }>(
          `SELECT count(*) OVER ()::text AS count,
                  c.metadata,
                  cardinality(c.embedding) AS dimensions,
                  c.text_context, c.text_label, c.text_content
           FROM "${schema}"."projected_chunks" AS c
           INNER JOIN "${schema}"."projected_revisions" AS r
             ON r.document_key = c.document_key
            AND r.projection_id = c.projection_id
           WHERE r.graph_id = $1`,
          [graph.id],
        )
        expect(persisted.rows).toEqual([
          {
            count: "1",
            metadata: { visibility: "public" },
            dimensions: 2,
            text_context: "Launch report",
            text_label: "launch",
            text_content: "Published national distribution results.",
          },
        ])

        let rejectedWrongDimensions = false
        try {
          await pool.query(
            `UPDATE "${schema}"."projected_chunks"
             SET embedding = ARRAY[1.0]::double precision[]`,
          )
        } catch {
          rejectedWrongDimensions = true
        }
        expect(rejectedWrongDimensions).toBe(true)

        const transaction = await pool.connect()
        try {
          await transaction.query("BEGIN")
          const transactionLive = Layer.mergeAll(
            Layer.succeed(EmbeddingProvider, embeddings),
            postgresDocumentGraph({ transaction, schema }),
          )
          const removedInsideTransaction = await Effect.runPromise(
            ArticleNode.remove(articleId).pipe(
              Effect.provide(transactionLive),
            ),
          )
          expect(removedInsideTransaction).toEqual({
            deletedRevisions: 1,
            deletedChunks: 1,
            deletedRelations: 0,
          })
          await transaction.query("ROLLBACK")
        } finally {
          transaction.release()
        }

        const afterRollback = await Effect.runPromise(
          graph.search("national").pipe(Effect.provide(live)),
        )
        expect(afterRollback.map((hit) => hit.sectionKey)).toEqual([
          "results",
        ])

        const invalidStoredTransaction = await pool.connect()
        try {
          await invalidStoredTransaction.query("BEGIN")
          await invalidStoredTransaction.query(
            `ALTER TABLE "${schema}"."projected_chunks"
             DROP CONSTRAINT "projected_chunks_content_not_empty"`,
          )
          await invalidStoredTransaction.query(
            `UPDATE "${schema}"."projected_chunks" AS c
             SET content = ''
             FROM "${schema}"."projected_revisions" AS r
             WHERE r.document_key = c.document_key
               AND r.projection_id = c.projection_id
               AND r.graph_id = $1`,
            [graph.id],
          )
          const invalidStoredLive = Layer.mergeAll(
            Layer.succeed(EmbeddingProvider, embeddings),
            postgresDocumentGraph({
              transaction: invalidStoredTransaction,
              schema,
            }),
          )
          const invalidStoredResult = await Effect.runPromise(
            ArticleContent.search("national", {
              strategy: "text",
            }).pipe(
              Effect.provide(invalidStoredLive),
              Effect.result,
            ),
          )
          expect(Result.isFailure(invalidStoredResult)).toBe(true)
          if (Result.isFailure(invalidStoredResult)) {
            expect(invalidStoredResult.failure._tag).toBe(
              "DocumentGraphUnavailable",
            )
            if (
              invalidStoredResult.failure._tag === "DocumentGraphUnavailable"
            ) {
              expect(invalidStoredResult.failure.reason).toBe(
                "invalid_stored_data",
              )
              const cause = invalidStoredResult.failure.cause
              expect(cause).toBeInstanceOf(Object)
              const causeTag = cause instanceof Object
                ? Object.getOwnPropertyDescriptor(cause, "_tag")?.value
                : undefined
              expect(causeTag).toBe("ProjectionTextSearchStoreFailed")
              const storedCause = cause instanceof Object
                ? Object.getOwnPropertyDescriptor(cause, "cause")?.value
                : undefined
              expect(storedCause).toBeInstanceOf(Object)
              const rowKind = storedCause instanceof Object
                ? Object.getOwnPropertyDescriptor(storedCause, "rowKind")?.value
                : undefined
              expect(rowKind).toBe("text candidate")
              const issues = storedCause instanceof Object
                ? Object.getOwnPropertyDescriptor(storedCause, "issues")?.value
                : undefined
              expect(Array.isArray(issues)).toBe(true)
              expect(
                Array.isArray(issues) &&
                  issues.some(
                    (issue) =>
                      issue instanceof Object &&
                      Object.getOwnPropertyDescriptor(issue, "path")?.value ===
                        "content",
                  ),
              ).toBe(true)
            }
          }
        } finally {
          try {
            await invalidStoredTransaction.query("ROLLBACK")
          } finally {
            invalidStoredTransaction.release()
          }
        }

        const retiredRevision = await Effect.runPromise(
          ArticleContent.project(withoutStaleSection),
        )
        const [retiredFirst, ...retiredRest] = retiredRevision.chunks
        const retiredChunk = (chunk: typeof retiredFirst) => ({
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
        const lifecycle = await Effect.runPromise(
          Effect.gen(function*() {
            const removed = yield* ArticleNode.remove(articleId)
            const removedAgain = yield* ArticleNode.remove(articleId)
            const store = yield* ProjectionIndexStore
            yield* store.replaceRevision({
              key: {
                documentKey: retiredRevision.documentKey,
                projection: "retired-projection",
              },
              expectedToken: Option.none(),
              encodedTarget: retiredRevision.encodedTarget,
              projectionVersion: "v1",
              revisionHash: retiredRevision.revisionHash,
              embeddingProfile: profile,
              chunks: [
                retiredChunk(retiredFirst),
                ...retiredRest.map(retiredChunk),
              ],
              embeddings: retiredRevision.chunks.map((chunk) => ({
                contentHash: chunk.contentHash,
                vector: [0.8, 0.2],
              })),
            })
            const hidden = yield* graph.search("national")
            const pruned = yield* graph.reconcileIndex()
            return { removed, removedAgain, hidden, pruned }
          }).pipe(Effect.provide(live)),
        )

        expect(lifecycle).toEqual({
          removed: {
            deletedRevisions: 1,
            deletedChunks: 1,
            deletedRelations: 0,
          },
          removedAgain: {
            deletedRevisions: 0,
            deletedChunks: 0,
            deletedRelations: 0,
          },
          hidden: [],
          pruned: {
            deletedRevisions: 1,
            deletedChunks: 1,
            deletedRelations: 0,
          },
        })

        const relatedId = Schema.decodeSync(ArticleId)(
          "77777777-7777-4777-8777-777777777777",
        )
        const relationLifecycle = await Effect.runPromise(
          Effect.gen(function*() {
            const indexed = yield* ArticleNode.index(
              article(withoutStaleSection.sections, [relatedId]),
            )
            const neighbours = yield* ArticleNode.neighbours(articleId, {
              via: "cites",
            })
            const incoming = yield* ArticleNode.neighbours(relatedId, {
              via: "cites",
              direction: "incoming",
            })
            const replaced = yield* ArticleNode.index(
              article(withoutStaleSection.sections),
            )
            const afterReplacement = yield* ArticleNode.neighbours(
              articleId,
              { via: "cites" },
            )
            const incomingAfterReplacement =
              yield* ArticleNode.neighbours(relatedId, {
                via: "cites",
                direction: "incoming",
              })
            return {
              indexed,
              neighbours,
              incoming,
              replaced,
              afterReplacement,
              incomingAfterReplacement,
            }
          }).pipe(Effect.provide(live)),
        )
        expect(relationLifecycle.indexed.relations.inserted).toBe(1)
        expect(relationLifecycle.neighbours).toEqual([
          { graph: graph.id, kind: "Article", id: relatedId },
        ])
        expect(relationLifecycle.incoming).toEqual([
          { graph: graph.id, kind: "Article", id: articleId },
        ])
        expect(relationLifecycle.replaced.relations.deleted).toBe(1)
        expect(relationLifecycle.afterReplacement).toEqual([])
        expect(relationLifecycle.incomingAfterReplacement).toEqual([])
      } finally {
        try {
          await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
        } finally {
          await pool.end()
        }
      }
    },
    20_000,
  )
})
