import {
  Array as EffectArray,
  Context,
  Effect,
  Option,
  Result,
  Schema,
} from "effect"
import type { JsonValue } from "../document/json-value.js"
import type { EncodedDocumentReference } from "../document/document-instance.js"
import type {
  ChunkId,
  ContentHash,
  DocumentKey,
  ProjectionRevisionHash,
} from "../document/document-identity.js"
import type { ProjectedText } from "../document/text-search-policy.js"
import {
  EmbeddingProvider,
  InvalidEmbeddingOutput,
  type EmbeddedContent,
  type EmbeddingProfile,
  type EmbeddingProviderFailed,
  type EmbeddingRequest,
} from "./embedding-provider.js"

/** Opaque optimistic-concurrency token owned by an index store. */
export const IndexRevisionTokenSchema = Schema.Trimmed.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(500),
).pipe(
  Schema.brand("IndexRevisionToken"),
)

/** Opaque optimistic-concurrency token owned by an index store. */
export type IndexRevisionToken = Schema.Schema.Type<
  typeof IndexRevisionTokenSchema
>

/** Logical key for one stored document projection. */
export interface ProjectionIndexKey {
  readonly documentKey: DocumentKey
  readonly projection: string
}

/** Minimal persisted chunk state required for delta planning. */
export interface IndexedChunkSummary {
  readonly chunkId: ChunkId
  readonly contentHash: ContentHash
}

/** Current persisted state returned before planning a replacement. */
export interface IndexedRevisionSnapshot {
  readonly token: IndexRevisionToken
  readonly revisionHash: ProjectionRevisionHash
  readonly embeddingProfile: EmbeddingProfile
  readonly chunks: readonly [
    IndexedChunkSummary,
    ...ReadonlyArray<IndexedChunkSummary>,
  ]
}

/** Complete storage record for one attributed retrieval chunk. */
export interface ProjectedChunkRecord {
  readonly chunkId: ChunkId
  readonly contentHash: ContentHash
  readonly ordinal: number
  readonly sectionKey: string
  readonly sectionIndex: number
  readonly sectionPart: number
  readonly content: string
  readonly embeddingContent: string
  readonly text: ProjectedText
  readonly metadata: JsonValue | undefined
}

/** New vectors supplied while atomically replacing a projection revision. */
export interface ProjectionChunkEmbedding extends EmbeddedContent {}

/** Atomic replacement command sent to a projection index store. */
export interface ReplaceProjectedRevision {
  readonly key: ProjectionIndexKey
  readonly expectedToken: Option.Option<IndexRevisionToken>
  readonly encodedTarget: EncodedDocumentReference
  readonly projectionVersion: string
  readonly revisionHash: ProjectionRevisionHash
  readonly embeddingProfile: EmbeddingProfile
  readonly chunks: readonly [
    ProjectedChunkRecord,
    ...ReadonlyArray<ProjectedChunkRecord>,
  ]
  readonly embeddings: ReadonlyArray<ProjectionChunkEmbedding>
}

/** Structural reason a complete projection replacement is invalid. */
export type ProjectedRevisionReplacementIssue =
  | "duplicate_chunk_id"
  | "duplicate_ordinal"
  | "duplicate_section_part"
  | "invalid_chunk_position"
  | "blank_content"
  | "duplicate_embedding"
  | "unexpected_embedding"
  | "invalid_embedding"
  | "missing_embedding"

/** Validated vector material and chunk identities for one replacement. */
export interface ProjectedRevisionReplacementPlan {
  readonly chunkIds: ReadonlySet<ChunkId>
  readonly vectors: ReadonlyMap<ContentHash, ReadonlyArray<number>>
}

/** Whether two embedding profiles describe the same reusable vector space. */
export const embeddingProfilesEqual = (
  left: EmbeddingProfile,
  right: EmbeddingProfile,
): boolean =>
  left.id === right.id &&
  left.version === right.version &&
  left.dimensions === right.dimensions

/** Whether a vector satisfies one embedding profile's numeric contract. */
export const isValidEmbeddingVector = (
  vector: ReadonlyArray<number>,
  dimensions: number,
): boolean =>
  vector.length === dimensions &&
  vector.every((component) => Number.isFinite(component))

/**
 * Validate one complete revision replacement and resolve all vector material.
 *
 * Reusable vectors must already have been scoped to the replacement's current
 * revision and embedding profile by the calling adapter.
 */
export const planProjectedRevisionReplacement = (
  replacement: ReplaceProjectedRevision,
  reusableVectors: ReadonlyMap<ContentHash, ReadonlyArray<number>>,
): Result.Result<
  ProjectedRevisionReplacementPlan,
  ProjectedRevisionReplacementIssue
> => {
  const chunkIds = new Set<ChunkId>()
  const ordinals = new Set<number>()
  const sectionParts = new Set<string>()
  const expectedContent = new Set<ContentHash>()

  for (const chunk of replacement.chunks) {
    const sectionPart = `${chunk.sectionKey}\u0000${chunk.sectionPart}`
    if (chunkIds.has(chunk.chunkId)) {
      return Result.fail("duplicate_chunk_id")
    }
    if (ordinals.has(chunk.ordinal)) {
      return Result.fail("duplicate_ordinal")
    }
    if (sectionParts.has(sectionPart)) {
      return Result.fail("duplicate_section_part")
    }
    if (
      !Number.isInteger(chunk.ordinal) ||
      !Number.isInteger(chunk.sectionIndex) ||
      !Number.isInteger(chunk.sectionPart) ||
      chunk.ordinal < 0 ||
      chunk.sectionIndex < 0 ||
      chunk.sectionPart < 0
    ) {
      return Result.fail("invalid_chunk_position")
    }
    if (
      chunk.content.trim().length === 0 ||
      chunk.embeddingContent.trim().length === 0
    ) {
      return Result.fail("blank_content")
    }

    chunkIds.add(chunk.chunkId)
    ordinals.add(chunk.ordinal)
    sectionParts.add(sectionPart)
    expectedContent.add(chunk.contentHash)
  }

  const vectors = new Map(reusableVectors)
  const supplied = new Set<ContentHash>()
  for (const embedding of replacement.embeddings) {
    if (supplied.has(embedding.contentHash)) {
      return Result.fail("duplicate_embedding")
    }
    if (!expectedContent.has(embedding.contentHash)) {
      return Result.fail("unexpected_embedding")
    }
    if (
      !isValidEmbeddingVector(
        embedding.vector,
        replacement.embeddingProfile.dimensions,
      )
    ) {
      return Result.fail("invalid_embedding")
    }

    supplied.add(embedding.contentHash)
    vectors.set(embedding.contentHash, embedding.vector)
  }

  for (const contentHash of expectedContent) {
    if (!vectors.has(contentHash)) {
      return Result.fail("missing_embedding")
    }
  }

  return Result.succeed({ chunkIds, vectors })
}

/** Observable storage changes made by one atomic revision replacement. */
export interface ProjectionIndexCommit {
  readonly token: IndexRevisionToken
  readonly inserted: number
  readonly updated: number
  readonly deleted: number
}

/** Derive stable commit counts from complete previous and next chunk sets. */
export const countProjectedRevisionReplacement = (
  previous: ReadonlySet<ChunkId>,
  next: ReadonlySet<ChunkId>,
): Omit<ProjectionIndexCommit, "token"> => {
  let updated = 0
  for (const chunkId of next) {
    if (previous.has(chunkId)) updated += 1
  }

  let deleted = 0
  for (const chunkId of previous) {
    if (!next.has(chunkId)) deleted += 1
  }

  return {
    inserted: next.size - updated,
    updated,
    deleted,
  }
}

/** Idempotent result of removing one indexed projection revision. */
export interface ProjectionIndexDeletion {
  readonly deletedRevisions: number
  readonly deletedChunks: number
}

/** One document/projection pair retained during graph reconciliation. */
export interface RegisteredGraphProjection {
  readonly documentKind: string
  readonly projection: string
}

/** Command for pruning stored projections no longer registered by a graph. */
export interface PruneGraphIndex {
  readonly graph: string
  readonly registered: ReadonlyArray<RegisteredGraphProjection>
}

/** Result of reconciling stored revisions with the current graph manifest. */
export interface ProjectionIndexPrune {
  readonly deletedRevisions: number
  readonly deletedChunks: number
}

/** A projection index store could not read or replace persisted state. */
export class ProjectionIndexStoreFailed extends Schema.TaggedError<
  ProjectionIndexStoreFailed
>()("ProjectionIndexStoreFailed", {
  operation: Schema.Literals([
    "load_revision",
    "replace_revision",
    "delete_revision",
    "prune_graph",
  ]),
  reason: Schema.Literals(["unavailable", "invalid_stored_state"]),
  cause: Schema.Unknown,
}) {}

/** Another writer replaced the projection after delta planning began. */
export class ProjectionIndexConflict extends Schema.TaggedError<
  ProjectionIndexConflict
>()("ProjectionIndexConflict", {
  documentKey: Schema.String,
  projection: Schema.String,
}) {}

/** Persistence capability consumed by the indexing workflow. */
export interface ProjectionIndexStoreService {
  /** Load the current revision summary, if the projection is indexed. */
  readonly loadRevision: (
    key: ProjectionIndexKey,
  ) => Effect.Effect<
    Option.Option<IndexedRevisionSnapshot>,
    ProjectionIndexStoreFailed
  >

  /**
   * Atomically replace a complete revision and delete records not present in it.
   *
   * Chunks without a supplied embedding reuse a vector from the expected
   * snapshot with the same embedding profile and content hash.
   */
  readonly replaceRevision: (
    replacement: ReplaceProjectedRevision,
  ) => Effect.Effect<
    ProjectionIndexCommit,
    ProjectionIndexStoreFailed | ProjectionIndexConflict
  >

  /** Idempotently delete one complete projected revision and its chunks. */
  readonly deleteRevision: (
    key: ProjectionIndexKey,
  ) => Effect.Effect<ProjectionIndexDeletion, ProjectionIndexStoreFailed>

  /** Delete revisions whose document/projection pair left the graph schema. */
  readonly pruneGraph: (
    input: PruneGraphIndex,
  ) => Effect.Effect<ProjectionIndexPrune, ProjectionIndexStoreFailed>
}

/** Effect service tag for vector and graph projection persistence. */
export class ProjectionIndexStore extends Context.Service<
  ProjectionIndexStore,
  ProjectionIndexStoreService
>()("@popcomputer/document-graph/ProjectionIndexStore") {}

interface IndexableProjectedChunk {
  readonly chunkId: ChunkId
  readonly contentHash: ContentHash
  readonly ordinal: number
  readonly sectionKey: string
  readonly sectionIndex: number
  readonly sectionPart: number
  readonly content: string
  readonly embeddingContent: string
  readonly text: ProjectedText
  readonly metadata?: JsonValue
}

/** Minimum complete projected-revision shape accepted by the indexer. */
export interface IndexableProjectedRevision {
  readonly encodedTarget: EncodedDocumentReference
  readonly documentKey: DocumentKey
  readonly projection: {
    readonly id: string
    readonly version: string
  }
  readonly revisionHash: ProjectionRevisionHash
  readonly chunks: readonly [
    IndexableProjectedChunk,
    ...ReadonlyArray<IndexableProjectedChunk>,
  ]
}

/** Result of indexing one complete projected revision. */
export type IndexProjectedRevisionResult =
  | {
      readonly _tag: "Unchanged"
      readonly revisionHash: ProjectionRevisionHash
    }
  | {
      readonly _tag: "Committed"
      readonly revisionHash: ProjectionRevisionHash
      readonly embeddedContent: number
      readonly reusedChunks: number
      readonly inserted: number
      readonly updated: number
      readonly deleted: number
    }

/** Expected failures while delta-indexing one projected revision. */
export type IndexProjectedRevisionError =
  | EmbeddingProviderFailed
  | InvalidEmbeddingOutput
  | ProjectionIndexStoreFailed
  | ProjectionIndexConflict

const validateEmbeddingOutput = (
  profile: EmbeddingProfile,
  requests: ReadonlyArray<EmbeddingRequest>,
  output: ReadonlyArray<EmbeddedContent>,
): Effect.Effect<ReadonlyArray<EmbeddedContent>, InvalidEmbeddingOutput> => {
  const requested = new Set(requests.map((item) => item.contentHash))
  const returned = new Set<ContentHash>()

  for (const item of output) {
    if (!requested.has(item.contentHash)) {
      return Effect.fail(
        new InvalidEmbeddingOutput({
          profile: profile.id,
          reason: "unexpected_content",
        }),
      )
    }

    if (returned.has(item.contentHash)) {
      return Effect.fail(
        new InvalidEmbeddingOutput({
          profile: profile.id,
          reason: "duplicate_content",
        }),
      )
    }

    if (item.vector.length !== profile.dimensions) {
      return Effect.fail(
        new InvalidEmbeddingOutput({
          profile: profile.id,
          reason: "wrong_dimensions",
        }),
      )
    }

    if (item.vector.some((component) => !Number.isFinite(component))) {
      return Effect.fail(
        new InvalidEmbeddingOutput({
          profile: profile.id,
          reason: "invalid_number",
        }),
      )
    }

    returned.add(item.contentHash)
  }

  if (returned.size !== requested.size) {
    return Effect.fail(
      new InvalidEmbeddingOutput({
        profile: profile.id,
        reason: "missing_content",
      }),
    )
  }

  return Effect.succeed(output)
}

const projectChunkRecord = (
  chunk: IndexableProjectedChunk,
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

/** Whether persisted state proves it already contains the exact revision. */
const snapshotProvesRevision = (
  snapshot: IndexedRevisionSnapshot,
  revision: IndexableProjectedRevision,
  profile: EmbeddingProfile,
): boolean => {
  if (
    snapshot.revisionHash !== revision.revisionHash ||
    !embeddingProfilesEqual(snapshot.embeddingProfile, profile) ||
    snapshot.chunks.length !== revision.chunks.length
  ) {
    return false
  }

  const storedContentByChunk = new Map<ChunkId, ContentHash>()
  for (const chunk of snapshot.chunks) {
    if (storedContentByChunk.has(chunk.chunkId)) return false
    storedContentByChunk.set(chunk.chunkId, chunk.contentHash)
  }

  return revision.chunks.every(
    (chunk) =>
      storedContentByChunk.get(chunk.chunkId) === chunk.contentHash,
  )
}

/**
 * Delta-index one complete projection revision.
 *
 * Existing vectors are reused by content hash only when the embedding profile
 * also matches. The store atomically replaces records and removes stale chunks.
 */
export const indexProjectedRevision = (
  revision: IndexableProjectedRevision,
): Effect.Effect<
  IndexProjectedRevisionResult,
  IndexProjectedRevisionError,
  EmbeddingProvider | ProjectionIndexStore
> =>
  Effect.gen(function*() {
    const embeddings = yield* EmbeddingProvider
    const store = yield* ProjectionIndexStore
    const key: ProjectionIndexKey = {
      documentKey: revision.documentKey,
      projection: revision.projection.id,
    }
    const current = yield* store.loadRevision(key)

    if (
      Option.isSome(current) &&
      snapshotProvesRevision(
        current.value,
        revision,
        embeddings.profile,
      )
    ) {
      return {
        _tag: "Unchanged" as const,
        revisionHash: revision.revisionHash,
      }
    }

    const reusableContent = new Set<ContentHash>()
    if (
      Option.isSome(current) &&
      embeddingProfilesEqual(
        current.value.embeddingProfile,
        embeddings.profile,
      )
    ) {
      for (const chunk of current.value.chunks) {
        reusableContent.add(chunk.contentHash)
      }
    }

    const embeddingRequests = new Map<ContentHash, EmbeddingRequest>()
    let reusedChunks = 0
    for (const chunk of revision.chunks) {
      if (reusableContent.has(chunk.contentHash)) {
        reusedChunks += 1
        continue
      }

      embeddingRequests.set(chunk.contentHash, {
        contentHash: chunk.contentHash,
        content: chunk.embeddingContent,
      })
    }

    const requests = Array.from(embeddingRequests.values())
    const embeddingEffect = !EffectArray.isReadonlyArrayNonEmpty(requests)
      ? Effect.succeed<ReadonlyArray<EmbeddedContent>>([])
      : embeddings.embedDocuments(requests).pipe(
          Effect.flatMap((output) =>
            validateEmbeddingOutput(
              embeddings.profile,
              requests,
              output,
            ),
          ),
        )
    const embeddedContent = yield* embeddingEffect

    const commit = yield* store.replaceRevision({
      key,
      expectedToken: Option.map(current, (snapshot) => snapshot.token),
      encodedTarget: revision.encodedTarget,
      projectionVersion: revision.projection.version,
      revisionHash: revision.revisionHash,
      embeddingProfile: embeddings.profile,
      chunks: EffectArray.map(revision.chunks, projectChunkRecord),
      embeddings: embeddedContent,
    })

    return {
      _tag: "Committed" as const,
      revisionHash: revision.revisionHash,
      embeddedContent: embeddedContent.length,
      reusedChunks,
      inserted: commit.inserted,
      updated: commit.updated,
      deleted: commit.deleted,
    }
  })
