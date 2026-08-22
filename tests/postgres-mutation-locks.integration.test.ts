import { describe, expect, test } from "bun:test"
import { readFile } from "node:fs/promises"
import {
  defineDocument,
  defineDocumentGraph,
  defineEmbeddingProfile,
  EmbeddingProvider,
  GraphRelationStore,
  ProjectionIndexStore,
  sectionChunking,
  type EmbeddingProviderService,
  type ReplaceProjectedRevision,
} from "../src/adapter.js"
import { Effect, Layer, Option, Result, Schema } from "effect"
import { Pool } from "pg"
import {
  postgresDocumentGraph,
  postgresTransactionClient,
} from "../src/postgres.js"

const databaseUrl =
  Bun.env.TEST_DATABASE_URL ??
  Bun.env.CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE
const runIntegrationTests =
  Bun.env.RUN_DOCUMENT_GRAPH_POSTGRES_TESTS === "true" &&
  databaseUrl !== undefined

const ArticleId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("LocksArticleId"),
)
const Article = Schema.Struct({
  id: ArticleId,
  title: Schema.Trimmed.check(Schema.isNonEmpty()),
  relatedIds: Schema.Array(ArticleId),
  sections: Schema.NonEmptyArray(
    Schema.Struct({
      id: Schema.Trimmed.check(Schema.isNonEmpty()),
      label: Schema.Trimmed.check(Schema.isNonEmpty()),
      text: Schema.Trimmed.check(Schema.isNonEmpty()),
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
  select: (article) => {
    const [first, ...rest] = article.sections
    return {
      context: article.title,
      sections: [
        {
          key: first.id,
          label: first.label,
          content: first.text,
        },
        ...rest.map((section) => ({
          key: section.id,
          label: section.label,
          content: section.text,
        })),
      ],
    }
  },
  chunking: sectionChunking({ maximumCharacters: 256 }),
})
const graph = defineDocumentGraph({
  id: "postgres-mutation-locks",
  documents: { Article: ArticleDocument },
})
const ArticleNode = graph.document("Article")
const ArticleContent = ArticleNode.projection("article-content")
const profile = defineEmbeddingProfile({
  id: "test:mutation-locks",
  version: "v1",
  dimensions: 2,
})

const article = (
  id: string,
  sectionText: string,
  relatedIds: Array<string> = [],
) =>
  Schema.decodeSync(Article)({
    id,
    title: "Lock report",
    relatedIds,
    sections: [{ id: "body", label: "body", text: sectionText }],
  })

const embeddings: EmbeddingProviderService = {
  profile,
  embedDocuments: (requests) =>
    Effect.succeed(
      requests.map((request) => ({
        contentHash: request.contentHash,
        vector: [0.8, 0.2],
      })),
    ),
  embedQuery: () => Effect.succeed([1, 0]),
}

interface RecordedQuery {
  readonly text: string
  readonly values: ReadonlyArray<unknown>
}

/**
 * Minimal structural client proving `{ query }` objects compose with the
 * transaction configuration while recording every emitted statement.
 */
const makeRecordingClient = () => {
  const queries: Array<RecordedQuery> = []
  const client = postgresTransactionClient({
    query: (text, values = []) => {
      queries.push({ text, values })
      if (text.includes("RETURNING revision_token")) {
        return Promise.resolve({ rows: [{ revision_token: "7" }] })
      }
      if (text.includes("AS revision_count")) {
        return Promise.resolve({
          rows: [{ revision_count: "0", chunk_count: "0" }],
        })
      }
      if (text.includes("AS deleted_count")) {
        return Promise.resolve({ rows: [{ deleted_count: "0" }] })
      }
      return Promise.resolve({ rows: [] })
    },
  })
  return { client, queries }
}

const replacementOf = (
  revision: Awaited<
    ReturnType<typeof runProjection>
  >,
): ReplaceProjectedRevision => {
  const [firstChunk, ...restChunks] = revision.chunks
  const chunkRecord = (chunk: typeof firstChunk) => ({
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
  return {
    key: {
      documentKey: revision.documentKey,
      projection: revision.projection.id,
    },
    expectedToken: Option.none(),
    encodedTarget: revision.encodedTarget,
    projectionVersion: revision.projection.version,
    revisionHash: revision.revisionHash,
    embeddingProfile: profile,
    chunks: [
      chunkRecord(firstChunk),
      ...restChunks.map(chunkRecord),
    ],
    embeddings: revision.chunks.map((chunk) => ({
      contentHash: chunk.contentHash,
      vector: [0.8, 0.2],
    })),
  }
}

const runProjection = (value: Schema.Schema.Type<typeof Article>) =>
  Effect.runPromise(ArticleContent.project(value))

const expectNoAdvisoryFunctions = (queries: ReadonlyArray<RecordedQuery>) => {
  expect(
    queries.some((query) => /pg_advisory|hashtextextended/iu.test(query.text)),
  ).toBe(false)
}

const findLockQueries = (
  queries: ReadonlyArray<RecordedQuery>,
  kind: string,
): RecordedQuery[] =>
  queries.filter(
    (query) => query.text.includes("mutation_locks") && query.values[0] === kind,
  )

describe("postgresDocumentGraph mutation locking", () => {
  test("emits exact-key row locks instead of advisory functions", async () => {
    const sourceId = "11111111-1111-4111-8111-111111111111"
    const relatedId = "22222222-2222-4222-8222-222222222222"
    const revision = await runProjection(
      article(sourceId, "Locked body text.", [relatedId]),
    )
    const replacement = replacementOf(revision)
    const relatedRevision = await runProjection(
      article(relatedId, "Related body text."),
    )

    const { client, queries } = makeRecordingClient()
    const live = Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, embeddings),
      postgresDocumentGraph({ transaction: client }),
    )
    const relationReplacement = {
      graph: graph.id,
      sourceDocumentKey: revision.documentKey,
      source: { graph: graph.id, kind: "Article", id: sourceId },
      relations: [
        {
          id: "cites",
          version: "v1",
          targetDocumentKind: "Article",
          targets: [
            {
              documentKey: relatedRevision.documentKey,
              reference: {
                graph: graph.id,
                kind: "Article",
                id: relatedId,
              },
            },
          ],
        },
      ],
    }

    await Effect.runPromise(
      Effect.gen(function*() {
        const store = yield* ProjectionIndexStore
        const relations = yield* GraphRelationStore
        yield* store.replaceRevision(replacement)
        yield* ArticleNode.remove(Schema.decodeSync(ArticleId)(sourceId))
        yield* relations.replaceOutgoing(relationReplacement)
      }).pipe(Effect.provide(live)),
    )

    expectNoAdvisoryFunctions(queries)

    const projectionLocks = findLockQueries(queries, "projection")
    expect(projectionLocks).toHaveLength(4)
    for (const lock of projectionLocks) {
      expect(lock.values).toEqual([
        "projection",
        revision.documentKey,
        revision.projection.id,
      ])
    }
    expect(
      projectionLocks.filter((lock) => lock.text.startsWith("INSERT")),
    ).toHaveLength(2)
    expect(
      projectionLocks.filter((lock) => lock.text.includes("FOR UPDATE")),
    ).toHaveLength(2)

    const firstLockInsert = queries.findIndex(
      (query) =>
        query.text.startsWith("INSERT") && query.text.includes("mutation_locks"),
    )
    const revisionRead = queries.findIndex(
      (query) =>
        query.text.includes("projected_revisions") &&
        query.text.includes("FOR UPDATE"),
    )
    expect(firstLockInsert).toBeGreaterThanOrEqual(0)
    expect(revisionRead).toBeGreaterThan(firstLockInsert)

    const relationsLocks = findLockQueries(queries, "relations")
    expect(relationsLocks).toHaveLength(2)
    for (const lock of relationsLocks) {
      expect(lock.values).toEqual([
        "relations",
        graph.id,
        revision.documentKey,
      ])
    }
  })
})

describe("postgresDocumentGraph concurrent mutation locking", () => {
  if (!runIntegrationTests || databaseUrl === undefined) {
    test.skip(
      "serializes concurrent replacements of the same document projection",
      () => undefined,
    )
    test.skip(
      "replaces different documents concurrently without false sharing",
      () => undefined,
    )
    test.skip(
      "runs projection and relations replacements concurrently for one document",
      () => undefined,
    )
    return
  }

  const migrateInSchema = async (
    pool: Pool,
    schema: string,
  ): Promise<void> => {
    for (const file of ["0001_initial.sql", "0002_mutation_locks.sql"]) {
      const migration = await readFile(
        new URL(`../migrations/postgres/${file}`, import.meta.url),
        "utf8",
      )
      await pool.query(
        migration.replaceAll('"honertia_document_graph"', `"${schema}"`),
      )
    }
  }

  const liveFor = (pool: Pool, schema: string) =>
    Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, embeddings),
      postgresDocumentGraph({ pool, schema }),
    )

  test(
    "serializes concurrent replacements of the same document projection",
    async () => {
      const schema = `document_graph_${crypto.randomUUID().replaceAll("-", "")}`
      const pool = new Pool({ connectionString: databaseUrl, max: 4 })
      try {
        await migrateInSchema(pool, schema)
        const live = liveFor(pool, schema)
        const documentId = "33333333-3333-4333-8333-333333333333"
        const firstRevision = await runProjection(
          article(documentId, "First writer body."),
        )
        const secondRevision = await runProjection(
          article(documentId, "Second writer body."),
        )
        const firstReplacement = replacementOf(firstRevision)
        const secondReplacement = replacementOf(secondRevision)

        const [firstOutcome, secondOutcome] = await Promise.all([
          Effect.runPromise(
            Effect.gen(function*() {
              const store = yield* ProjectionIndexStore
              return yield* Effect.result(store.replaceRevision(firstReplacement))
            }).pipe(Effect.provide(live)),
          ),
          Effect.runPromise(
            Effect.gen(function*() {
              const store = yield* ProjectionIndexStore
              return yield* Effect.result(store.replaceRevision(secondReplacement))
            }).pipe(Effect.provide(live)),
          ),
        ])

        const firstIsSuccess = Result.isSuccess(firstOutcome)
        expect(firstIsSuccess).not.toBe(Result.isSuccess(secondOutcome))
        const failure = firstIsSuccess ? secondOutcome : firstOutcome
        if (Result.isFailure(failure)) {
          expect(failure.failure._tag).toBe("ProjectionIndexConflict")
        }

        const winner = firstIsSuccess ? firstReplacement : secondReplacement
        const finalState = await Effect.runPromise(
          Effect.gen(function*() {
            const store = yield* ProjectionIndexStore
            const [lookup] = yield* store.loadRevisions([{
              documentKey: winner.key.documentKey,
              projection: winner.key.projection,
            }])
            return lookup?.revision ?? Option.none()
          }).pipe(Effect.provide(live)),
        )
        expect(Option.isSome(finalState)).toBe(true)
        if (Option.isSome(finalState)) {
          expect(finalState.value.revisionHash).toBe(winner.revisionHash)
          expect(finalState.value.chunks.map((chunk) => chunk.contentHash))
            .toEqual(winner.chunks.map((chunk) => chunk.contentHash))
        }
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

  test(
    "replaces different documents concurrently without false sharing",
    async () => {
      const schema = `document_graph_${crypto.randomUUID().replaceAll("-", "")}`
      const pool = new Pool({ connectionString: databaseUrl, max: 4 })
      try {
        await migrateInSchema(pool, schema)
        const firstId = "44444444-4444-4444-8444-444444444444"
        const secondId = "55555555-5555-4555-8555-555555555555"

        const results = await Promise.all([
          Effect.runPromise(
            ArticleContent.index(article(firstId, "Concurrent body one.")).pipe(
              Effect.provide(liveFor(pool, schema)),
            ),
          ),
          Effect.runPromise(
            ArticleContent.index(article(secondId, "Concurrent body two.")).pipe(
              Effect.provide(liveFor(pool, schema)),
            ),
          ),
        ])

        expect(results.map((result) => result._tag)).toEqual([
          "Committed",
          "Committed",
        ])
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

  test(
    "runs projection and relations replacements concurrently for one document",
    async () => {
      const schema = `document_graph_${crypto.randomUUID().replaceAll("-", "")}`
      const pool = new Pool({ connectionString: databaseUrl, max: 4 })
      try {
        await migrateInSchema(pool, schema)
        const live = liveFor(pool, schema)
        const sourceId = "66666666-6666-4666-8666-666666666666"
        const relatedId = "77777777-7777-4777-8777-777777777777"
        const revision = await runProjection(
          article(sourceId, "Shared document body.", [relatedId]),
        )
        const relatedRevision = await runProjection(
          article(relatedId, "Related document body."),
        )

        const [projectionOutcome, relationsOutcome] = await Promise.all([
          Effect.runPromise(
            Effect.gen(function*() {
              const store = yield* ProjectionIndexStore
              return yield* Effect.result(store.replaceRevision(
                replacementOf(revision),
              ))
            }).pipe(Effect.provide(live)),
          ),
          Effect.runPromise(
            Effect.gen(function*() {
              const relations = yield* GraphRelationStore
              return yield* Effect.result(relations.replaceOutgoing({
                graph: graph.id,
                sourceDocumentKey: revision.documentKey,
                source: { graph: graph.id, kind: "Article", id: sourceId },
                relations: [
                  {
                    id: "cites",
                    version: "v1",
                    targetDocumentKind: "Article",
                    targets: [
                      {
                        documentKey: relatedRevision.documentKey,
                        reference: {
                          graph: graph.id,
                          kind: "Article",
                          id: relatedId,
                        },
                      },
                    ],
                  },
                ],
              }))
            }).pipe(Effect.provide(live)),
          ),
        ])

        expect(Result.isSuccess(projectionOutcome)).toBe(true)
        expect(Result.isSuccess(relationsOutcome)).toBe(true)
        if (Result.isSuccess(relationsOutcome)) {
          expect(relationsOutcome.success).toEqual({
            inserted: 1,
            retained: 0,
            deleted: 0,
          })
        }
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
