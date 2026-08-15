import { describe, expect, test } from "bun:test"
import {
  Array as EffectArray,
  Effect,
  Layer,
  Option,
  Result,
  Schema,
} from "effect"
import {
  defineDocument,
  defineDocumentGraph,
  defineEmbeddingProfile,
  EmbeddingProvider,
  IndexRevisionTokenSchema,
  indexProjectedRevision,
  InvalidEmbeddingOutput,
  ProjectionIndexConflict,
  ProjectionIndexStore,
  ProjectionIndexStoreFailed,
  sectionChunking,
  type ContentHash,
  type EmbeddedContent,
  type EmbeddingProfile,
  type EmbeddingProviderService,
  type EmbeddingRequest,
  type IndexedRevisionSnapshot,
  type IndexRevisionToken,
  type ProjectedChunkRecord,
  type ProjectionIndexStoreService,
  type ReplaceProjectedRevision,
} from "../src/adapter.js"

const ArticleId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("ArticleId"),
)
const ArticleValue = Schema.Struct({
  id: ArticleId,
  sections: Schema.NonEmptyArray(
    Schema.Struct({
      id: Schema.Trimmed.check(Schema.isNonEmpty()),
      text: Schema.Trimmed.check(Schema.isNonEmpty()),
      visibility: Schema.Literals(["public", "private"]),
    }),
  ),
})
const ArticleMetadata = Schema.Struct({
  visibility: Schema.Literals(["public", "private"]),
})
const ArticleDocument = defineDocument({
  id: ArticleId,
  value: ArticleValue,
  identify: (article) => article.id,
}).vectorise({
  id: "article-sections",
  version: "v1",
  metadata: ArticleMetadata,
  select: (article) => {
    const [first, ...rest] = article.sections
    return {
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
const articleGraph = defineDocumentGraph({
  id: "articles",
  documents: { Article: ArticleDocument },
})
const ArticleSections = articleGraph
  .document("Article")
  .projection("article-sections")
const articleId = Schema.decodeSync(ArticleId)(
  "44444444-4444-4444-8444-444444444444",
)

const projectArticle = (
  sections: readonly [
    {
      readonly id: string
      readonly text: string
      readonly visibility: "public" | "private"
    },
    ...ReadonlyArray<{
      readonly id: string
      readonly text: string
      readonly visibility: "public" | "private"
    }>,
  ],
) => {
  const value = Schema.decodeSync(ArticleValue)({
    id: articleId,
    sections,
  })

  return ArticleSections.project(value)
}

interface StoredChunk extends ProjectedChunkRecord {
  readonly vector: ReadonlyArray<number>
}

interface StoredRevision {
  readonly token: IndexRevisionToken
  readonly replacement: ReplaceProjectedRevision
  readonly chunks: ReadonlyArray<StoredChunk>
  readonly snapshotChunks: IndexedRevisionSnapshot["chunks"]
}

const sameProfile = (
  left: EmbeddingProfile,
  right: EmbeddingProfile,
): boolean =>
  left.id === right.id &&
  left.version === right.version &&
  left.dimensions === right.dimensions

const makeRecordingStore = () => {
  let revision: StoredRevision | undefined
  let tokenSequence = 0
  const commits: Array<ReplaceProjectedRevision> = []

  const loadRevision: ProjectionIndexStoreService["loadRevision"] = () =>
    Effect.succeed(
      revision === undefined
        ? Option.none()
        : Option.some({
            token: revision.token,
            revisionHash: revision.replacement.revisionHash,
            embeddingProfile:
              revision.replacement.embeddingProfile,
            chunks: revision.snapshotChunks,
          }),
    )

  const replaceRevision: ProjectionIndexStoreService["replaceRevision"] =
    (replacement) => {
      const expectedMatches =
        revision === undefined
          ? Option.isNone(replacement.expectedToken)
          : Option.isSome(replacement.expectedToken) &&
            replacement.expectedToken.value === revision.token

      if (!expectedMatches) {
        return Effect.fail(
          new ProjectionIndexConflict({
            documentKey: replacement.key.documentKey,
            projection: replacement.key.projection,
          }),
        )
      }

      const suppliedVectors = new Map<
        ContentHash,
        ReadonlyArray<number>
      >(
        replacement.embeddings.map((embedded) => [
          embedded.contentHash,
          embedded.vector,
        ]),
      )
      const reusableVectors = new Map<
        ContentHash,
        ReadonlyArray<number>
      >()

      if (
        revision !== undefined &&
        sameProfile(
          revision.replacement.embeddingProfile,
          replacement.embeddingProfile,
        )
      ) {
        for (const chunk of revision.chunks) {
          reusableVectors.set(chunk.contentHash, chunk.vector)
        }
      }

      const nextChunks: Array<StoredChunk> = []
      for (const chunk of replacement.chunks) {
        const vector =
          suppliedVectors.get(chunk.contentHash) ??
          reusableVectors.get(chunk.contentHash)

        if (vector === undefined) {
          return Effect.fail(
            new ProjectionIndexStoreFailed({
              operation: "replace_revision",
              reason: "invalid_stored_state",
              cause: "No supplied or reusable vector",
            }),
          )
        }

        nextChunks.push({ ...chunk, vector })
      }

      const previousIds = new Set(
        revision?.chunks.map((chunk) => chunk.chunkId) ?? [],
      )
      const nextIds = new Set(
        nextChunks.map((chunk) => chunk.chunkId),
      )
      const inserted = nextChunks.filter(
        (chunk) => !previousIds.has(chunk.chunkId),
      ).length
      const updated = nextChunks.length - inserted
      const deleted = Array.from(previousIds).filter(
        (chunkId) => !nextIds.has(chunkId),
      ).length

      tokenSequence += 1
      const token = Schema.decodeSync(IndexRevisionTokenSchema)(
        `revision-${tokenSequence}`,
      )
      commits.push(replacement)
      revision = {
        token,
        replacement,
        chunks: nextChunks,
        snapshotChunks: EffectArray.map(
          replacement.chunks,
          (chunk) => ({
            chunkId: chunk.chunkId,
            contentHash: chunk.contentHash,
          }),
        ),
      }

      return Effect.succeed({ token, inserted, updated, deleted })
    }

  const service: ProjectionIndexStoreService = {
    loadRevision,
    replaceRevision,
    deleteRevision: () =>
      Effect.succeed({ deletedRevisions: 0, deletedChunks: 0 }),
    pruneGraph: () =>
      Effect.succeed({ deletedRevisions: 0, deletedChunks: 0 }),
  }

  return {
    service,
    commits,
    current: (): StoredRevision | undefined => revision,
    retainOnlyFirstSnapshotChunk: () => {
      if (revision === undefined) return
      const [first] = revision.snapshotChunks
      revision = { ...revision, snapshotChunks: [first] }
    },
    duplicateFirstSnapshotChunk: () => {
      if (revision === undefined) return
      const [first, second, ...rest] = revision.snapshotChunks
      if (second === undefined) return
      revision = {
        ...revision,
        snapshotChunks: [first, first, ...rest],
      }
    },
  }
}

const makeRecordingEmbeddings = (
  profile: EmbeddingProfile,
  vectorFor: (request: EmbeddingRequest) => ReadonlyArray<number> = (
    request,
  ) => [request.content.length, 1],
) => {
  const batches: Array<
    readonly [EmbeddingRequest, ...ReadonlyArray<EmbeddingRequest>]
  > = []
  const service: EmbeddingProviderService = {
    profile,
    embedDocuments: (requests) => {
      batches.push(requests)
      return Effect.succeed(
        requests.map(
          (request): EmbeddedContent => ({
            contentHash: request.contentHash,
            vector: vectorFor(request),
          }),
        ),
      )
    },
    embedQuery: (query) => Effect.succeed([query.length, 1]),
  }

  return { service, batches }
}

const runIndex = <Revision extends Parameters<
  typeof indexProjectedRevision
>[0]>(
  revision: Revision,
  embeddings: EmbeddingProviderService,
  store: ProjectionIndexStoreService,
) =>
  Effect.runPromise(
    indexProjectedRevision(revision).pipe(
      Effect.provide(
        Layer.succeed(EmbeddingProvider, embeddings),
      ),
      Effect.provide(
        Layer.succeed(ProjectionIndexStore, store),
      ),
    ),
  )

const defaultProfile = defineEmbeddingProfile({
  id: "test:embedding",
  version: "v1",
  dimensions: 2,
})

describe("indexProjectedRevision", () => {
  test("embeds a new complete revision and skips an identical replay", async () => {
    const store = makeRecordingStore()
    const embeddings = makeRecordingEmbeddings(defaultProfile)
    const revision = await Effect.runPromise(
      projectArticle([
        { id: "summary", text: "A summary.", visibility: "public" },
        { id: "detail", text: "A detail.", visibility: "private" },
      ]),
    )

    const first = await runIndex(
      revision,
      embeddings.service,
      store.service,
    )
    const replay = await runIndex(
      revision,
      embeddings.service,
      store.service,
    )

    expect(first).toMatchObject({
      _tag: "Committed",
      embeddedContent: 2,
      reusedChunks: 0,
      inserted: 2,
      updated: 0,
      deleted: 0,
    })
    expect(replay).toEqual({
      _tag: "Unchanged",
      revisionHash: revision.revisionHash,
    })
    expect(embeddings.batches).toHaveLength(1)
    expect(store.commits).toHaveLength(1)
  })

  test("repairs an incomplete snapshot instead of reporting it unchanged", async () => {
    const store = makeRecordingStore()
    const embeddings = makeRecordingEmbeddings(defaultProfile)
    const revision = await Effect.runPromise(
      projectArticle([
        { id: "summary", text: "A summary.", visibility: "public" },
        { id: "detail", text: "A detail.", visibility: "private" },
      ]),
    )

    await runIndex(revision, embeddings.service, store.service)
    store.retainOnlyFirstSnapshotChunk()
    const repaired = await runIndex(
      revision,
      embeddings.service,
      store.service,
    )
    const replay = await runIndex(
      revision,
      embeddings.service,
      store.service,
    )

    expect(repaired).toMatchObject({
      _tag: "Committed",
      embeddedContent: 1,
      reusedChunks: 1,
      inserted: 0,
      updated: 2,
      deleted: 0,
    })
    expect(replay).toEqual({
      _tag: "Unchanged",
      revisionHash: revision.revisionHash,
    })
    expect(embeddings.batches).toHaveLength(2)
    expect(store.commits).toHaveLength(2)
  })

  test("repairs duplicate snapshot identities instead of trusting their hash", async () => {
    const store = makeRecordingStore()
    const embeddings = makeRecordingEmbeddings(defaultProfile)
    const revision = await Effect.runPromise(
      projectArticle([
        { id: "summary", text: "A summary.", visibility: "public" },
        { id: "detail", text: "A detail.", visibility: "private" },
      ]),
    )

    await runIndex(revision, embeddings.service, store.service)
    store.duplicateFirstSnapshotChunk()
    const repaired = await runIndex(
      revision,
      embeddings.service,
      store.service,
    )

    expect(repaired).toMatchObject({
      _tag: "Committed",
      embeddedContent: 1,
      reusedChunks: 1,
    })
    expect(store.commits).toHaveLength(2)
  })

  test("updates metadata without requesting another embedding", async () => {
    const store = makeRecordingStore()
    const embeddings = makeRecordingEmbeddings(defaultProfile)
    const before = await Effect.runPromise(
      projectArticle([
        { id: "body", text: "Same text.", visibility: "private" },
      ]),
    )
    const after = await Effect.runPromise(
      projectArticle([
        { id: "body", text: "Same text.", visibility: "public" },
      ]),
    )

    await runIndex(before, embeddings.service, store.service)
    const previousVector = store.current()?.chunks[0]?.vector
    const result = await runIndex(
      after,
      embeddings.service,
      store.service,
    )

    expect(result).toMatchObject({
      _tag: "Committed",
      embeddedContent: 0,
      reusedChunks: 1,
      inserted: 0,
      updated: 1,
      deleted: 0,
    })
    expect(embeddings.batches).toHaveLength(1)
    expect(store.current()?.chunks[0]).toMatchObject({
      metadata: { visibility: "public" },
      vector: previousVector,
    })
  })

  test("re-embeds changed content at the same logical chunk ID", async () => {
    const store = makeRecordingStore()
    const embeddings = makeRecordingEmbeddings(defaultProfile)
    const before = await Effect.runPromise(
      projectArticle([
        { id: "body", text: "Original text.", visibility: "public" },
      ]),
    )
    const after = await Effect.runPromise(
      projectArticle([
        { id: "body", text: "Replacement text.", visibility: "public" },
      ]),
    )

    await runIndex(before, embeddings.service, store.service)
    const result = await runIndex(
      after,
      embeddings.service,
      store.service,
    )

    expect(before.chunks[0].chunkId).toBe(after.chunks[0].chunkId)
    expect(result).toMatchObject({
      _tag: "Committed",
      embeddedContent: 1,
      reusedChunks: 0,
      inserted: 0,
      updated: 1,
      deleted: 0,
    })
    expect(embeddings.batches).toHaveLength(2)
  })

  test("deletes stale chunk IDs while reusing retained content", async () => {
    const store = makeRecordingStore()
    const embeddings = makeRecordingEmbeddings(defaultProfile)
    const before = await Effect.runPromise(
      projectArticle([
        { id: "keep", text: "Keep this.", visibility: "public" },
        { id: "remove", text: "Remove this.", visibility: "public" },
      ]),
    )
    const after = await Effect.runPromise(
      projectArticle([
        { id: "keep", text: "Keep this.", visibility: "public" },
      ]),
    )

    await runIndex(before, embeddings.service, store.service)
    const result = await runIndex(
      after,
      embeddings.service,
      store.service,
    )

    expect(result).toMatchObject({
      _tag: "Committed",
      embeddedContent: 0,
      reusedChunks: 1,
      inserted: 0,
      updated: 1,
      deleted: 1,
    })
    expect(store.current()?.chunks.map((chunk) => chunk.sectionKey)).toEqual([
      "keep",
    ])
  })

  test("re-embeds unchanged content when the embedding profile changes", async () => {
    const store = makeRecordingStore()
    const firstEmbeddings = makeRecordingEmbeddings(defaultProfile)
    const nextEmbeddings = makeRecordingEmbeddings(
      defineEmbeddingProfile({
        id: "test:embedding",
        version: "v2",
        dimensions: 2,
      }),
    )
    const revision = await Effect.runPromise(
      projectArticle([
        { id: "body", text: "Same text.", visibility: "public" },
      ]),
    )

    await runIndex(revision, firstEmbeddings.service, store.service)
    const result = await runIndex(
      revision,
      nextEmbeddings.service,
      store.service,
    )

    expect(result).toMatchObject({
      _tag: "Committed",
      embeddedContent: 1,
      reusedChunks: 0,
      inserted: 0,
      updated: 1,
      deleted: 0,
    })
    expect(nextEmbeddings.batches).toHaveLength(1)
  })

  test("rejects vectors that do not match the declared dimensions", async () => {
    const store = makeRecordingStore()
    const embeddings = makeRecordingEmbeddings(
      defaultProfile,
      () => [1],
    )
    const revision = await Effect.runPromise(
      projectArticle([
        { id: "body", text: "Some text.", visibility: "public" },
      ]),
    )

    const result = await Effect.runPromise(
      indexProjectedRevision(revision).pipe(
        Effect.provide(
          Layer.succeed(EmbeddingProvider, embeddings.service),
        ),
        Effect.provide(
          Layer.succeed(ProjectionIndexStore, store.service),
        ),
        Effect.result,
      ),
    )

    expect(result).toEqual(
      Result.fail(
        new InvalidEmbeddingOutput({
          profile: defaultProfile.id,
          reason: "wrong_dimensions",
        }),
      ),
    )
    expect(store.commits).toHaveLength(0)
  })

  test("returns an optimistic conflict instead of overwriting another writer", async () => {
    const store = makeRecordingStore()
    const embeddings = makeRecordingEmbeddings(defaultProfile)
    const revision = await Effect.runPromise(
      projectArticle([
        { id: "body", text: "Some text.", visibility: "public" },
      ]),
    )
    const conflictingStore: ProjectionIndexStoreService = {
      ...store.service,
      replaceRevision: (replacement) =>
        Effect.fail(
          new ProjectionIndexConflict({
            documentKey: replacement.key.documentKey,
            projection: replacement.key.projection,
          }),
        ),
    }

    const result = await Effect.runPromise(
      indexProjectedRevision(revision).pipe(
        Effect.provide(
          Layer.succeed(EmbeddingProvider, embeddings.service),
        ),
        Effect.provide(
          Layer.succeed(ProjectionIndexStore, conflictingStore),
        ),
        Effect.result,
      ),
    )

    expect(result).toEqual(
      Result.fail(
        new ProjectionIndexConflict({
          documentKey: revision.documentKey,
          projection: revision.projection.id,
        }),
      ),
    )
    expect(store.commits).toHaveLength(0)
  })
})
