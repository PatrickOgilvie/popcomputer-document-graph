import { Effect, Option, Result, Schema } from "effect"
import {
  ChunkIdSchema,
  ContentHashSchema,
  DocumentKeySchema,
  ProjectionRevisionHashSchema,
} from "../document/document-identity.js"
import {
  ProjectionIndexConflict,
  ProjectionIndexStore,
  type IndexRevisionToken,
  type IndexedRevisionSnapshot,
  type ProjectionIndexCommit,
  type ProjectionIndexKey,
  type ProjectedChunkRecord,
  type ReplaceProjectedRevision,
} from "../indexing/projection-index.js"
import { defineEmbeddingProfile } from "../indexing/embedding-provider.js"

/** Persistence laws checked against every projection-index adapter. */
export const ProjectionIndexStoreConformanceLawSchema = Schema.Literals([
  "complete_replacement",
  "snapshot_inventory",
  "content_hash_reuse",
  "optimistic_conflict",
  "invalid_replacement_atomicity",
  "stale_chunk_deletion",
  "idempotent_deletion",
  "schema_pruning",
])

/** Persistence law checked against every projection-index adapter. */
export type ProjectionIndexStoreConformanceLaw =
  typeof ProjectionIndexStoreConformanceLawSchema.Type

/** A projection-index adapter violated one storage-independent law. */
export class ProjectionIndexStoreConformanceViolation extends Schema.TaggedError<
  ProjectionIndexStoreConformanceViolation
>()("ProjectionIndexStoreConformanceViolation", {
  law: ProjectionIndexStoreConformanceLawSchema,
}) {}

/** Evidence that one projection-index adapter passed every stable law. */
export interface ProjectionIndexStoreConformanceReport {
  readonly capability: "projection_index"
  readonly verified: ReadonlyArray<ProjectionIndexStoreConformanceLaw>
}

/** Deterministic replacement commands available for adapter-specific support. */
export interface ProjectionIndexStoreConformanceFixture {
  readonly initial: ReplaceProjectedRevision
  readonly metadataOnly: ReplaceProjectedRevision
  readonly reduced: ReplaceProjectedRevision
  readonly retired: ReplaceProjectedRevision
}

const GraphId = "@popcomputer/document-graph/conformance/index"
const DocumentKind = "Included"
const ProjectionId = "evidence"
const ProjectionVersion = "v1"

const profile = defineEmbeddingProfile({
  id: "honertia:index-conformance",
  version: "v1",
  dimensions: 2,
})

const documentKey = Schema.decodeSync(DocumentKeySchema)("a".repeat(64))
const firstChunkId = Schema.decodeSync(ChunkIdSchema)("b".repeat(64))
const secondChunkId = Schema.decodeSync(ChunkIdSchema)("c".repeat(64))
const retiredChunkId = Schema.decodeSync(ChunkIdSchema)("d".repeat(64))
const firstContentHash = Schema.decodeSync(ContentHashSchema)("e".repeat(64))
const secondContentHash = Schema.decodeSync(ContentHashSchema)("f".repeat(64))
const retiredContentHash = Schema.decodeSync(ContentHashSchema)("1".repeat(64))

const encodedTarget = {
  graph: GraphId,
  kind: DocumentKind,
  id: "index-document",
} as const

const initialChunks: readonly [
  ProjectedChunkRecord,
  ProjectedChunkRecord,
] = [
  {
    chunkId: firstChunkId,
    contentHash: firstContentHash,
    ordinal: 0,
    sectionKey: "summary",
    sectionIndex: 0,
    sectionPart: 0,
    content: "First indexed section.",
    embeddingContent: "First indexed section.",
    text: {
      context: "Index conformance",
      label: "Summary",
      content: "First indexed section.",
    },
    metadata: { visibility: "public" },
  },
  {
    chunkId: secondChunkId,
    contentHash: secondContentHash,
    ordinal: 1,
    sectionKey: "detail",
    sectionIndex: 1,
    sectionPart: 0,
    content: "Second indexed section.",
    embeddingContent: "Second indexed section.",
    text: {
      context: "Index conformance",
      label: "Detail",
      content: "Second indexed section.",
    },
    metadata: { visibility: "public" },
  },
]

const initialRevisionHash = Schema.decodeSync(
  ProjectionRevisionHashSchema,
)("2".repeat(64))
const metadataRevisionHash = Schema.decodeSync(
  ProjectionRevisionHashSchema,
)("3".repeat(64))
const reducedRevisionHash = Schema.decodeSync(
  ProjectionRevisionHashSchema,
)("4".repeat(64))
const retiredRevisionHash = Schema.decodeSync(
  ProjectionRevisionHashSchema,
)("5".repeat(64))

/** Create deterministic commands used by the projection-index verifier. */
export const makeProjectionIndexStoreConformanceFixture =
  (): ProjectionIndexStoreConformanceFixture => {
    const initial: ReplaceProjectedRevision = {
      key: { documentKey, projection: ProjectionId },
      expectedToken: Option.none(),
      encodedTarget,
      projectionVersion: ProjectionVersion,
      revisionHash: initialRevisionHash,
      embeddingProfile: profile,
      chunks: initialChunks,
      embeddings: [
        { contentHash: firstContentHash, vector: [1, 0] },
        { contentHash: secondContentHash, vector: [0, 1] },
      ],
    }
    const metadataOnly: ReplaceProjectedRevision = {
      ...initial,
      revisionHash: metadataRevisionHash,
      chunks: [
        {
          ...initialChunks[0],
          metadata: { visibility: "reviewed" },
        },
        initialChunks[1],
      ],
      embeddings: [],
    }
    const reduced: ReplaceProjectedRevision = {
      ...metadataOnly,
      revisionHash: reducedRevisionHash,
      chunks: [metadataOnly.chunks[0]],
    }
    const retired: ReplaceProjectedRevision = {
      key: { documentKey, projection: "retired" },
      expectedToken: Option.none(),
      encodedTarget,
      projectionVersion: ProjectionVersion,
      revisionHash: retiredRevisionHash,
      embeddingProfile: profile,
      chunks: [
        {
          chunkId: retiredChunkId,
          contentHash: retiredContentHash,
          ordinal: 0,
          sectionKey: "retired",
          sectionIndex: 0,
          sectionPart: 0,
          content: "Retired indexed section.",
          embeddingContent: "Retired indexed section.",
          text: {
            context: "Index conformance",
            label: "Retired",
            content: "Retired indexed section.",
          },
          metadata: { visibility: "public" },
        },
      ],
      embeddings: [
        { contentHash: retiredContentHash, vector: [0.5, 0.5] },
      ],
    }

    return { initial, metadataOnly, reduced, retired }
  }

const violation = (
  law: ProjectionIndexStoreConformanceLaw,
): ProjectionIndexStoreConformanceViolation =>
  new ProjectionIndexStoreConformanceViolation({ law })

const withExpectedToken = (
  replacement: ReplaceProjectedRevision,
  token: IndexRevisionToken,
): ReplaceProjectedRevision => ({
  ...replacement,
  expectedToken: Option.some(token),
})

const commitMatches = (
  commit: ProjectionIndexCommit,
  expected: Omit<ProjectionIndexCommit, "token">,
): boolean =>
  commit.inserted === expected.inserted &&
  commit.updated === expected.updated &&
  commit.deleted === expected.deleted

const snapshotMatches = (
  snapshot: IndexedRevisionSnapshot,
  replacement: ReplaceProjectedRevision,
): boolean => {
  if (
    snapshot.revisionHash !== replacement.revisionHash ||
    snapshot.embeddingProfile.id !== replacement.embeddingProfile.id ||
    snapshot.embeddingProfile.version !==
      replacement.embeddingProfile.version ||
    snapshot.embeddingProfile.dimensions !==
      replacement.embeddingProfile.dimensions ||
    snapshot.chunks.length !== replacement.chunks.length
  ) {
    return false
  }

  const stored = new Map(
    snapshot.chunks.map((chunk) => [chunk.chunkId, chunk.contentHash]),
  )
  return (
    stored.size === snapshot.chunks.length &&
    replacement.chunks.every(
      (chunk) => stored.get(chunk.chunkId) === chunk.contentHash,
    )
  )
}

const sameSnapshotState = (
  left: IndexedRevisionSnapshot,
  right: IndexedRevisionSnapshot,
): boolean => {
  if (
    left.token !== right.token ||
    left.revisionHash !== right.revisionHash ||
    left.embeddingProfile.id !== right.embeddingProfile.id ||
    left.embeddingProfile.version !== right.embeddingProfile.version ||
    left.embeddingProfile.dimensions !== right.embeddingProfile.dimensions ||
    left.chunks.length !== right.chunks.length
  ) {
    return false
  }

  const leftInventory = new Map(
    left.chunks.map((chunk) => [chunk.chunkId, chunk.contentHash]),
  )
  const rightInventory = new Map(
    right.chunks.map((chunk) => [chunk.chunkId, chunk.contentHash]),
  )
  return (
    leftInventory.size === left.chunks.length &&
    rightInventory.size === right.chunks.length &&
    right.chunks.every(
      (chunk) => leftInventory.get(chunk.chunkId) === chunk.contentHash,
    )
  )
}

const loadRequiredSnapshot = (
  key: ProjectionIndexKey,
  law: ProjectionIndexStoreConformanceLaw,
) =>
  Effect.gen(function*() {
    const store = yield* ProjectionIndexStore
    const loaded = yield* store.loadRevision(key)
    if (Option.isNone(loaded)) {
      return yield* Effect.fail(violation(law))
    }
    return loaded.value
  })

const verifyRejectedWithoutMutation = (
  before: IndexedRevisionSnapshot,
  replacement: ReplaceProjectedRevision,
  law: ProjectionIndexStoreConformanceLaw,
) =>
  Effect.gen(function*() {
    const store = yield* ProjectionIndexStore
    const rejected = yield* store.replaceRevision(replacement).pipe(Effect.result)
    if (
      !Result.isFailure(rejected) ||
      rejected.failure._tag !== "ProjectionIndexStoreFailed" ||
      rejected.failure.reason !== "invalid_stored_state"
    ) {
      return yield* Effect.fail(violation(law))
    }

    const after = yield* loadRequiredSnapshot(replacement.key, law)
    if (!sameSnapshotState(after, before)) {
      return yield* Effect.fail(violation(law))
    }
  })

const verifiedLaws: ReadonlyArray<ProjectionIndexStoreConformanceLaw> = [
  "complete_replacement",
  "snapshot_inventory",
  "content_hash_reuse",
  "optimistic_conflict",
  "invalid_replacement_atomicity",
  "stale_chunk_deletion",
  "idempotent_deletion",
  "schema_pruning",
]

/** Verify a projection index through its production Effect service interface. */
export const verifyProjectionIndexStoreConformance = () =>
  Effect.gen(function*() {
    const fixture = makeProjectionIndexStoreConformanceFixture()
    const store = yield* ProjectionIndexStore

    yield* store.deleteRevision(fixture.initial.key)
    yield* store.deleteRevision(fixture.retired.key)

    const initialCommit = yield* store.replaceRevision(fixture.initial)
    if (
      !commitMatches(initialCommit, {
        inserted: 2,
        updated: 0,
        deleted: 0,
      })
    ) {
      return yield* Effect.fail(violation("complete_replacement"))
    }
    const initialSnapshot = yield* loadRequiredSnapshot(
      fixture.initial.key,
      "snapshot_inventory",
    )
    if (
      initialSnapshot.token !== initialCommit.token ||
      !snapshotMatches(initialSnapshot, fixture.initial)
    ) {
      return yield* Effect.fail(violation("snapshot_inventory"))
    }

    const metadataCommit = yield* store.replaceRevision(
      withExpectedToken(fixture.metadataOnly, initialSnapshot.token),
    )
    if (
      !commitMatches(metadataCommit, {
        inserted: 0,
        updated: 2,
        deleted: 0,
      })
    ) {
      return yield* Effect.fail(violation("content_hash_reuse"))
    }
    const metadataSnapshot = yield* loadRequiredSnapshot(
      fixture.metadataOnly.key,
      "content_hash_reuse",
    )
    if (!snapshotMatches(metadataSnapshot, fixture.metadataOnly)) {
      return yield* Effect.fail(violation("content_hash_reuse"))
    }

    const staleWrite = yield* store
      .replaceRevision(
        withExpectedToken(fixture.reduced, initialSnapshot.token),
      )
      .pipe(Effect.result)
    if (
      !Result.isFailure(staleWrite) ||
      !(staleWrite.failure instanceof ProjectionIndexConflict)
    ) {
      return yield* Effect.fail(violation("optimistic_conflict"))
    }
    const afterConflict = yield* loadRequiredSnapshot(
      fixture.metadataOnly.key,
      "optimistic_conflict",
    )
    if (
      afterConflict.token !== metadataSnapshot.token ||
      !snapshotMatches(afterConflict, fixture.metadataOnly)
    ) {
      return yield* Effect.fail(violation("optimistic_conflict"))
    }

    yield* verifyRejectedWithoutMutation(
      metadataSnapshot,
      {
        ...fixture.metadataOnly,
        expectedToken: Option.some(metadataSnapshot.token),
        chunks: [
          { ...fixture.metadataOnly.chunks[0], content: " " },
          initialChunks[1],
        ],
      },
      "invalid_replacement_atomicity",
    )

    const reducedCommit = yield* store.replaceRevision(
      withExpectedToken(fixture.reduced, metadataSnapshot.token),
    )
    if (
      !commitMatches(reducedCommit, {
        inserted: 0,
        updated: 1,
        deleted: 1,
      })
    ) {
      return yield* Effect.fail(violation("stale_chunk_deletion"))
    }
    const reducedSnapshot = yield* loadRequiredSnapshot(
      fixture.reduced.key,
      "stale_chunk_deletion",
    )
    if (!snapshotMatches(reducedSnapshot, fixture.reduced)) {
      return yield* Effect.fail(violation("stale_chunk_deletion"))
    }

    const removed = yield* store.deleteRevision(fixture.reduced.key)
    const removedAgain = yield* store.deleteRevision(fixture.reduced.key)
    const afterDelete = yield* store.loadRevision(fixture.reduced.key)
    if (
      removed.deletedRevisions !== 1 ||
      removed.deletedChunks !== 1 ||
      removedAgain.deletedRevisions !== 0 ||
      removedAgain.deletedChunks !== 0 ||
      Option.isSome(afterDelete)
    ) {
      return yield* Effect.fail(violation("idempotent_deletion"))
    }

    yield* store.replaceRevision(fixture.initial)
    yield* store.replaceRevision(fixture.retired)
    const pruned = yield* store.pruneGraph({
      graph: GraphId,
      registered: [
        { documentKind: DocumentKind, projection: ProjectionId },
      ],
    })
    const activeAfterPrune = yield* store.loadRevision(fixture.initial.key)
    const retiredAfterPrune = yield* store.loadRevision(fixture.retired.key)
    if (
      pruned.deletedRevisions !== 1 ||
      pruned.deletedChunks !== 1 ||
      Option.isNone(activeAfterPrune) ||
      !snapshotMatches(activeAfterPrune.value, fixture.initial) ||
      Option.isSome(retiredAfterPrune)
    ) {
      return yield* Effect.fail(violation("schema_pruning"))
    }

    yield* store.deleteRevision(fixture.initial.key)

    return {
      capability: "projection_index" as const,
      verified: verifiedLaws,
    }
  })
