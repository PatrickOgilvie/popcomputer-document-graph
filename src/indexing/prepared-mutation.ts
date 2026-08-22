import { Effect, Layer, Option, Result, Schema } from "effect"
import type { EncodedDocumentReference } from "../document/document-instance.js"
import type { JsonValue } from "../document/json-value.js"
import {
  GraphRelationStore,
  GraphRelationStoreFailed,
  type GraphRelationCommit,
  type GraphRelationStoreService,
  type OutgoingGraphRelationTarget,
  type ReplaceOutgoingGraphRelations,
} from "../graph/graph-relation.js"
import {
  IndexRevisionTokenSchema,
  ProjectionIndexConflict,
  ProjectionIndexStore,
  ProjectionIndexStoreFailed,
  type ProjectionIndexCommit,
  type ProjectionIndexStoreService,
  type ProjectedChunkRecord,
  type ReplaceProjectedRevision,
} from "./projection-index.js"

/**
 * One captured mutation: either a complete projected-revision replacement or
 * one source document's complete outgoing relation replacement.
 */
export type PreparedGraphMutationOperation =
  | {
    readonly _tag: "ReplaceProjectedRevision"
    readonly input: ReplaceProjectedRevision
  }
  | {
    readonly _tag: "ReplaceOutgoingGraphRelations"
    readonly input: ReplaceOutgoingGraphRelations
  }

/**
 * A frozen, deterministically ordered mutation set captured without writing
 * any storage. Operations and their complete input graphs are copied and frozen
 * at freeze time. Embeddings are resolved before capture completes, so
 * replaying a prepared mutation never performs a network call.
 */
export interface PreparedGraphMutation {
  readonly operations: ReadonlyArray<PreparedGraphMutationOperation>
}

/** The original workflow value paired with the storage mutation it planned. */
export interface PreparedGraphMutationResult<A> {
  readonly result: A
  readonly mutation: PreparedGraphMutation
}

/** Minimum persistence authority required to replay a prepared mutation. */
export interface GraphMutationTarget {
  readonly replaceRevision: ProjectionIndexStoreService["replaceRevision"]
  readonly replaceOutgoing: GraphRelationStoreService["replaceOutgoing"]
}

/** Two captured mutations claim the same identity. */
export class DuplicatePreparedMutation extends Schema.TaggedError<
  DuplicatePreparedMutation
>()("DuplicatePreparedMutation", {
  identity: Schema.String,
}) {}

/** Stable identity used for ordering and duplicate detection. */
const projectionIdentity = (input: ReplaceProjectedRevision): string =>
  `projection\u0000${input.key.documentKey}\u0000${input.key.projection}`

/** Stable identity used for ordering and duplicate detection. */
const relationIdentity = (input: ReplaceOutgoingGraphRelations): string =>
  `relations\u0000${input.graph}\u0000${input.sourceDocumentKey}`

const syntheticToken = Schema.decodeSync(IndexRevisionTokenSchema)("prepared")

const syntheticProjectionCommit = (
  replacement: ReplaceProjectedRevision,
): ProjectionIndexCommit => ({
  token: syntheticToken,
  inserted: replacement.chunks.length,
  updated: 0,
  deleted: 0,
})

const syntheticRelationCommit = (
  replacement: ReplaceOutgoingGraphRelations,
): GraphRelationCommit => ({
  inserted: replacement.relations.reduce(
    (count, relation) => count + relation.targets.length,
    0,
  ),
  retained: 0,
  deleted: 0,
})

const JsonPrimitiveSchema = Schema.Union([
  Schema.Null,
  Schema.Boolean,
  Schema.Finite,
  Schema.String,
])

const freezeJsonValue = (value: JsonValue): JsonValue => {
  if (Schema.is(JsonPrimitiveSchema)(value)) {
    return value
  }

  if (Array.isArray(value)) {
    return Object.freeze(value.map(freezeJsonValue))
  }

  const record: Record<string, JsonValue> = {}
  for (const [key, item] of Object.entries(value)) {
    record[key] = freezeJsonValue(item)
  }
  return Object.freeze(record)
}

const freezeReference = (
  reference: EncodedDocumentReference,
): EncodedDocumentReference =>
  Object.freeze({
    graph: reference.graph,
    kind: reference.kind,
    id: freezeJsonValue(reference.id),
  })

const freezeChunk = (chunk: ProjectedChunkRecord): ProjectedChunkRecord =>
  Object.freeze({
    chunkId: chunk.chunkId,
    contentHash: chunk.contentHash,
    ordinal: chunk.ordinal,
    sectionKey: chunk.sectionKey,
    sectionIndex: chunk.sectionIndex,
    sectionPart: chunk.sectionPart,
    content: chunk.content,
    embeddingContent: chunk.embeddingContent,
    text: Object.freeze({
      context: chunk.text.context,
      label: chunk.text.label,
      content: chunk.text.content,
    }),
    metadata:
      chunk.metadata === undefined
        ? undefined
        : freezeJsonValue(chunk.metadata),
  })

const freezeProjectionReplacement = (
  replacement: ReplaceProjectedRevision,
): ReplaceProjectedRevision => {
  const [firstChunk, ...remainingChunks] = replacement.chunks
  const chunks: [ProjectedChunkRecord, ...Array<ProjectedChunkRecord>] = [
    freezeChunk(firstChunk),
    ...remainingChunks.map(freezeChunk),
  ]
  Object.freeze(chunks)
  const expectedToken = Option.isNone(replacement.expectedToken)
    ? Option.none()
    : Option.some(replacement.expectedToken.value)
  if (Option.isSome(expectedToken)) {
    Object.freeze(expectedToken)
  }

  return Object.freeze({
    key: Object.freeze({
      documentKey: replacement.key.documentKey,
      projection: replacement.key.projection,
    }),
    expectedToken,
    encodedTarget: freezeReference(replacement.encodedTarget),
    projectionVersion: replacement.projectionVersion,
    revisionHash: replacement.revisionHash,
    embeddingProfile: Object.freeze({
      id: replacement.embeddingProfile.id,
      version: replacement.embeddingProfile.version,
      dimensions: replacement.embeddingProfile.dimensions,
    }),
    chunks,
    embeddings: Object.freeze(
      replacement.embeddings.map((embedding) =>
        Object.freeze({
          contentHash: embedding.contentHash,
          vector: Object.freeze([...embedding.vector]),
        })
      ),
    ),
  })
}

const freezeRelationTarget = (
  target: OutgoingGraphRelationTarget,
): OutgoingGraphRelationTarget =>
  Object.freeze({
    documentKey: target.documentKey,
    reference: freezeReference(target.reference),
  })

const freezeRelationReplacement = (
  replacement: ReplaceOutgoingGraphRelations,
): ReplaceOutgoingGraphRelations =>
  Object.freeze({
    graph: replacement.graph,
    sourceDocumentKey: replacement.sourceDocumentKey,
    source: freezeReference(replacement.source),
    relations: Object.freeze(
      replacement.relations.map((relation) =>
        Object.freeze({
          id: relation.id,
          version: relation.version,
          targetDocumentKind: relation.targetDocumentKind,
          targets: Object.freeze(relation.targets.map(freezeRelationTarget)),
        })
      ),
    ),
  })

const unsupportedProjectionMutation = (
  operation: "delete_revision" | "prune_graph",
): ProjectionIndexStoreFailed =>
  new ProjectionIndexStoreFailed({
    operation,
    reason: "unavailable",
    cause: "Prepared mutation capture supports replacement operations only",
  })

const unsupportedRelationMutation = (
  operation: "delete_node" | "prune_graph",
): GraphRelationStoreFailed =>
  new GraphRelationStoreFailed({
    operation,
    reason: "unavailable",
    cause: "Prepared mutation capture supports replacement operations only",
  })

const prepareMutation = (
  projections: ReadonlyArray<ReplaceProjectedRevision>,
  relations: ReadonlyArray<ReplaceOutgoingGraphRelations>,
): Result.Result<PreparedGraphMutation, DuplicatePreparedMutation> => {
  const seen = new Set<string>()
  const operations: Array<PreparedGraphMutationOperation> = []

  const sortedProjections = [...projections].sort((left, right) =>
    projectionIdentity(left).localeCompare(projectionIdentity(right))
  )
  for (const input of sortedProjections) {
    const identity = projectionIdentity(input)
    if (seen.has(identity)) {
      return Result.fail(new DuplicatePreparedMutation({ identity }))
    }
    seen.add(identity)
    operations.push(
      Object.freeze({
        _tag: "ReplaceProjectedRevision",
        input: freezeProjectionReplacement(input),
      }),
    )
  }

  const sortedRelations = [...relations].sort((left, right) =>
    relationIdentity(left).localeCompare(relationIdentity(right))
  )
  for (const input of sortedRelations) {
    const identity = relationIdentity(input)
    if (seen.has(identity)) {
      return Result.fail(new DuplicatePreparedMutation({ identity }))
    }
    seen.add(identity)
    operations.push(
      Object.freeze({
        _tag: "ReplaceOutgoingGraphRelations",
        input: freezeRelationReplacement(input),
      }),
    )
  }

  return Result.succeed(
    Object.freeze({ operations: Object.freeze(operations) }),
  )
}

/** Capturing stores plus the frozen mutation they recorded. */
interface MutationCapture {
  /**
   * Layers replacing the live stores during capture. Reads delegate to the
   * live stores; replacements are recorded and acknowledged with synthetic
   * commit counts. Delete and prune operations fail without touching storage.
   */
  readonly layer: Layer.Layer<ProjectionIndexStore | GraphRelationStore>

  /**
   * Freeze the captured operations into a prepared mutation. The first call
   * fixes the result; later calls return the same prepared mutation.
   */
  readonly prepare: () => Result.Result<
    PreparedGraphMutation,
    DuplicatePreparedMutation
  >
}

/**
 * Resolve the live stores and return capturing wrappers for them.
 *
 * Run the application's normal indexing program against `layer` to capture a
 * complete mutation set: chunking, embedding calls, and planning all execute
 * normally, but no storage is written until
 * {@link replayPreparedGraphMutation} runs against a target storage.
 */
const makeMutationCapture: Effect.Effect<
  MutationCapture,
  never,
  ProjectionIndexStore | GraphRelationStore
> = Effect.gen(function*() {
  const liveProjectionStore = yield* ProjectionIndexStore
  const liveRelationStore = yield* GraphRelationStore

  const projections: Array<ReplaceProjectedRevision> = []
  const relations: Array<ReplaceOutgoingGraphRelations> = []
  let prepared:
    | Result.Result<PreparedGraphMutation, DuplicatePreparedMutation>
    | undefined

  const layer = Layer.merge(
    Layer.succeed(
      ProjectionIndexStore,
      ProjectionIndexStore.of({
        ...liveProjectionStore,
        replaceRevision: (replacement) =>
          Effect.sync(() => {
            projections.push(replacement)
            return syntheticProjectionCommit(replacement)
          }),
        deleteRevision: () =>
          Effect.fail(unsupportedProjectionMutation("delete_revision")),
        pruneGraph: () =>
          Effect.fail(unsupportedProjectionMutation("prune_graph")),
      }),
    ),
    Layer.succeed(
      GraphRelationStore,
      GraphRelationStore.of({
        ...liveRelationStore,
        replaceOutgoing: (replacement) =>
          Effect.sync(() => {
            relations.push(replacement)
            return syntheticRelationCommit(replacement)
          }),
        deleteNode: () =>
          Effect.fail(unsupportedRelationMutation("delete_node")),
        pruneRelations: () =>
          Effect.fail(unsupportedRelationMutation("prune_graph")),
      }),
    ),
  )

  return {
    layer,
    prepare: () => {
      if (prepared === undefined) {
        prepared = prepareMutation(projections, relations)
      }
      return prepared
    },
  }
})

/**
 * Run a normal graph workflow while replacing its storage writes with one
 * immutable, replayable mutation. Reads and all non-storage requirements stay
 * live, so callers compose this around the same indexing program they would
 * otherwise execute directly.
 */
export const prepareGraphMutation: <A, E, R>(
  program: Effect.Effect<A, E, R>,
) => Effect.Effect<
  PreparedGraphMutationResult<A>,
  E | DuplicatePreparedMutation,
  R | ProjectionIndexStore | GraphRelationStore
> = Effect.fn("DocumentGraph.prepareMutation")(function*(program) {
  const capturing = yield* makeMutationCapture
  const result = yield* program.pipe(Effect.provide(capturing.layer))
  const mutation = yield* Effect.fromResult(capturing.prepare())
  return { result, mutation }
})

/** Counts produced by replaying one prepared mutation. */
export interface PreparedMutationReplayReport {
  readonly replacedRevisions: number
  readonly replacedRelationSets: number
}

/**
 * Sequentially apply a prepared mutation to target storage.
 *
 * Operations replay in prepared order (all projection replacements before all
 * relation replacements) because storage adapters serialize each operation
 * under one transaction-scoped lock; replaying sequentially keeps one
 * operation in flight per transaction. No embedding or other network call is
 * performed: every vector was resolved before capture.
 */
export const replayPreparedGraphMutation: (
  prepared: PreparedGraphMutation,
  target: GraphMutationTarget,
) => Effect.Effect<
  PreparedMutationReplayReport,
  ProjectionIndexStoreFailed | ProjectionIndexConflict | GraphRelationStoreFailed
> = Effect.fn("DocumentGraph.replayMutation")(function*(prepared, target) {
    let replacedRevisions = 0
    let replacedRelationSets = 0

    for (const operation of prepared.operations) {
      if (operation._tag === "ReplaceProjectedRevision") {
        yield* target.replaceRevision(operation.input)
        replacedRevisions += 1
      } else {
        yield* target.replaceOutgoing(operation.input)
        replacedRelationSets += 1
      }
    }

    return { replacedRevisions, replacedRelationSets }
  })
