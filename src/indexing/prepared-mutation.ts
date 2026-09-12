import { Effect, Layer, Option, Result, Schema } from "effect"
import {
  ChunkIdSchema,
  ContentHashSchema,
  DocumentKeySchema,
  makeChunkId,
  makeContentHash,
  makeDocumentKey,
  ProjectionRevisionHashSchema,
  type ContentHash,
} from "../document/document-identity.js"
import type { EncodedDocumentReference } from "../document/document-instance.js"
import { JsonValueSchema, type JsonValue } from "../document/json-value.js"
import {
  TextSearchLanguageSchema,
  TextSearchWeightSchema,
} from "../document/text-search-policy.js"
import {
  VectorProjectionIdSchema,
  VectorProjectionVersionSchema,
} from "../document/vector-projection.js"
import {
  GraphRelationIdSchema,
  GraphRelationVersionSchema,
  planOutgoingGraphRelationReplacement,
  type GraphRelationCommit,
  type OutgoingGraphRelationTarget,
  type ReplaceOutgoingGraphRelations,
} from "../graph/graph-relation.js"
import {
  GraphTopologyStore,
  GraphTopologyStoreFailed,
  type GraphTopologyStoreService,
} from "../graph/graph-topology.js"
import {
  EmbeddingDimensionsSchema,
  EmbeddingProfileIdSchema,
  EmbeddingProfileVersionSchema,
} from "./embedding-provider.js"
import {
  IndexRevisionTokenSchema,
  planProjectedRevisionReplacement,
  type ProjectionIndexConflict,
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
 * replaying a prepared mutation never recomputes embeddings.
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
  readonly replaceDocumentTopology:
    GraphTopologyStoreService["replaceDocumentTopology"]
}

/** Two captured mutations claim the same identity. */
export class DuplicatePreparedMutation extends Schema.TaggedError<
  DuplicatePreparedMutation
>()("DuplicatePreparedMutation", {
  identity: Schema.String,
}) {}

/** Current persisted representation of a prepared graph mutation. */
export const PreparedGraphMutationArtifactSchemaVersion = 1

const PreparedGraphMutationTextPolicyArtifactSchema = Schema.Union([
  Schema.Literal("disabled"),
  Schema.TaggedStruct("TextSearch", {
    language: TextSearchLanguageSchema,
    weights: Schema.Struct({
      context: TextSearchWeightSchema,
      label: TextSearchWeightSchema,
      content: TextSearchWeightSchema,
    }),
  }),
])

const PreparedGraphMutationReferenceArtifactSchema = Schema.Struct({
  graph: Schema.String,
  kind: Schema.String,
  id: JsonValueSchema,
})

const PreparedGraphMutationMetadataArtifactSchema = Schema.Union([
  Schema.TaggedStruct("None", {}),
  Schema.TaggedStruct("Some", {
    value: JsonValueSchema,
  }),
])

const PreparedGraphMutationChunkArtifactSchema = Schema.Struct({
  chunkId: ChunkIdSchema,
  contentHash: ContentHashSchema,
  ordinal: Schema.Finite,
  sectionKey: Schema.String,
  sectionIndex: Schema.Finite,
  sectionPart: Schema.Finite,
  content: Schema.String,
  embeddingContent: Schema.String,
  text: Schema.Struct({
    context: Schema.NullOr(Schema.String),
    label: Schema.NullOr(Schema.String),
    content: Schema.String,
  }),
  metadata: PreparedGraphMutationMetadataArtifactSchema,
})

const PreparedGraphMutationProjectionArtifactSchema = Schema.TaggedStruct("ReplaceProjectedRevision", {
  input: Schema.Struct({
    key: Schema.Struct({
      documentKey: DocumentKeySchema,
      projection: VectorProjectionIdSchema,
    }),
    expectedToken: Schema.NullOr(IndexRevisionTokenSchema),
    encodedTarget: PreparedGraphMutationReferenceArtifactSchema,
    projectionVersion: VectorProjectionVersionSchema,
    textPolicy: PreparedGraphMutationTextPolicyArtifactSchema,
    revisionHash: ProjectionRevisionHashSchema,
    embeddingProfile: Schema.Struct({
      id: EmbeddingProfileIdSchema,
      version: EmbeddingProfileVersionSchema,
      dimensions: EmbeddingDimensionsSchema,
    }),
    chunks: Schema.NonEmptyArray(
      PreparedGraphMutationChunkArtifactSchema,
    ),
    embeddings: Schema.Array(
      Schema.Struct({
        contentHash: ContentHashSchema,
        vector: Schema.Array(Schema.Finite),
      }),
    ),
  }),
})

const PreparedGraphMutationTopologyArtifactSchema = Schema.TaggedStruct("ReplaceOutgoingGraphRelations", {
  input: Schema.Struct({
    graph: Schema.String,
    sourceDocumentKey: DocumentKeySchema,
    source: PreparedGraphMutationReferenceArtifactSchema,
    relations: Schema.Array(
      Schema.Struct({
        id: GraphRelationIdSchema,
        version: GraphRelationVersionSchema,
        targetDocumentKind: Schema.String,
        targets: Schema.Array(
          Schema.Struct({
            documentKey: DocumentKeySchema,
            reference: PreparedGraphMutationReferenceArtifactSchema,
          }),
        ),
      }),
    ),
  }),
})

/** Strict JSON-safe v1 envelope stored for durable mutation replay. */
export const PreparedGraphMutationArtifactSchema = Schema.Struct({
  schemaVersion: Schema.Literal(
    PreparedGraphMutationArtifactSchemaVersion,
  ),
  operations: Schema.Array(
    Schema.Union([
      PreparedGraphMutationProjectionArtifactSchema,
      PreparedGraphMutationTopologyArtifactSchema,
    ]),
  ),
})

/** Strict JSON-safe v1 envelope stored for durable mutation replay. */
export type PreparedGraphMutationArtifact =
  typeof PreparedGraphMutationArtifactSchema.Type

type PreparedGraphMutationProjectionArtifact =
  typeof PreparedGraphMutationProjectionArtifactSchema.Type

type PreparedGraphMutationTopologyArtifact =
  typeof PreparedGraphMutationTopologyArtifactSchema.Type

type PreparedGraphMutationChunkArtifact =
  typeof PreparedGraphMutationChunkArtifactSchema.Type

type PreparedGraphMutationReferenceArtifact =
  typeof PreparedGraphMutationReferenceArtifactSchema.Type

/** A persisted prepared mutation could not be decoded or safely replayed. */
export class InvalidPreparedGraphMutationArtifact extends Schema.TaggedError<
  InvalidPreparedGraphMutationArtifact
>()("InvalidPreparedGraphMutationArtifact", {
  reason: Schema.Literals([
    "invalid_json",
    "invalid_shape",
    "duplicate_operation",
    "invalid_projection",
    "invalid_topology",
  ]),
  detail: Schema.String,
}) {}

/** Stable identity used for ordering and duplicate detection. */
const projectionIdentity = (input: ReplaceProjectedRevision): string =>
  `projection\u0000${input.key.documentKey}\u0000${input.key.projection}`

/** Stable identity used for ordering and duplicate detection. */
const relationIdentity = (input: ReplaceOutgoingGraphRelations): string =>
  `relations\u0000${input.graph}\u0000${input.sourceDocumentKey}`

const compareIdentity = (left: string, right: string): number =>
  left < right ? -1 : left > right ? 1 : 0

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

const encodeJsonPrimitive = (
  value: null | boolean | number | string,
): string => {
  const encoded = JSON.stringify(value)

  if (encoded === undefined) {
    throw new Error("A JSON primitive unexpectedly failed to encode")
  }

  return encoded
}

/** Recursively sort object keys while preserving meaningful array order. */
const canonicalJsonValue = (value: JsonValue): string => {
  if (Schema.is(JsonPrimitiveSchema)(value)) {
    return encodeJsonPrimitive(value)
  }

  if (Array.isArray(value)) {
    return `[${value.map(canonicalJsonValue).join(",")}]`
  }

  // SAFETY: JsonValue contains only primitives, arrays, and string-keyed records.
  const record = value as Readonly<Record<string, JsonValue>>

  return `{${Object.keys(record)
    .sort()
    .map((key) => {
      const item = record[key]

      if (item === undefined) {
        throw new Error(
          "A parsed JSON object unexpectedly contained undefined",
        )
      }

      return `${encodeJsonPrimitive(key)}:${canonicalJsonValue(item)}`
    })
    .join(",")}}`
}

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
    textPolicy: replacement.textPolicy === "disabled"
      ? "disabled"
      : Object.freeze({
          _tag: "TextSearch" as const,
          language: replacement.textPolicy.language,
          weights: Object.freeze({ ...replacement.textPolicy.weights }),
        }),
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

const referenceToArtifact = (
  reference: EncodedDocumentReference,
): PreparedGraphMutationReferenceArtifact => ({
  graph: reference.graph,
  kind: reference.kind,
  id: reference.id,
})

const chunkToArtifact = (
  chunk: ProjectedChunkRecord,
): PreparedGraphMutationChunkArtifact => ({
  chunkId: chunk.chunkId,
  contentHash: chunk.contentHash,
  ordinal: chunk.ordinal,
  sectionKey: chunk.sectionKey,
  sectionIndex: chunk.sectionIndex,
  sectionPart: chunk.sectionPart,
  content: chunk.content,
  embeddingContent: chunk.embeddingContent,
  text: {
    context: chunk.text.context ?? null,
    label: chunk.text.label ?? null,
    content: chunk.text.content,
  },
  metadata: chunk.metadata === undefined
    ? { _tag: "None" }
    : { _tag: "Some", value: chunk.metadata },
})

const projectionToArtifact = (
  replacement: ReplaceProjectedRevision,
): PreparedGraphMutationProjectionArtifact => {
  const [firstChunk, ...remainingChunks] = replacement.chunks

  return {
    _tag: "ReplaceProjectedRevision",
    input: {
      key: replacement.key,
      expectedToken: Option.getOrNull(replacement.expectedToken),
      encodedTarget: referenceToArtifact(replacement.encodedTarget),
      projectionVersion: replacement.projectionVersion,
      textPolicy: replacement.textPolicy,
      revisionHash: replacement.revisionHash,
      embeddingProfile: replacement.embeddingProfile,
      chunks: [
        chunkToArtifact(firstChunk),
        ...remainingChunks.map(chunkToArtifact),
      ],
      embeddings: replacement.embeddings.map((embedding) => ({
        contentHash: embedding.contentHash,
        vector: embedding.vector,
      })),
    },
  }
}

const topologyToArtifact = (
  replacement: ReplaceOutgoingGraphRelations,
): PreparedGraphMutationTopologyArtifact => ({
  _tag: "ReplaceOutgoingGraphRelations",
  input: {
    graph: replacement.graph,
    sourceDocumentKey: replacement.sourceDocumentKey,
    source: referenceToArtifact(replacement.source),
    relations: replacement.relations.map((relation) => ({
      id: relation.id,
      version: relation.version,
      targetDocumentKind: relation.targetDocumentKind,
      targets: relation.targets.map((target) => ({
        documentKey: target.documentKey,
        reference: referenceToArtifact(target.reference),
      })),
    })),
  },
})

const mutationToArtifact = (
  mutation: PreparedGraphMutation,
): PreparedGraphMutationArtifact => ({
  schemaVersion: PreparedGraphMutationArtifactSchemaVersion,
  operations: mutation.operations.map((operation) =>
    operation._tag === "ReplaceProjectedRevision"
      ? projectionToArtifact(operation.input)
      : topologyToArtifact(operation.input)
  ),
})

const referenceFromArtifact = (
  reference: PreparedGraphMutationReferenceArtifact,
): EncodedDocumentReference => ({
  graph: reference.graph,
  kind: reference.kind,
  id: reference.id,
})

const chunkFromArtifact = (
  chunk: PreparedGraphMutationChunkArtifact,
): ProjectedChunkRecord => ({
  chunkId: chunk.chunkId,
  contentHash: chunk.contentHash,
  ordinal: chunk.ordinal,
  sectionKey: chunk.sectionKey,
  sectionIndex: chunk.sectionIndex,
  sectionPart: chunk.sectionPart,
  content: chunk.content,
  embeddingContent: chunk.embeddingContent,
  text: {
    context: chunk.text.context ?? undefined,
    label: chunk.text.label ?? undefined,
    content: chunk.text.content,
  },
  metadata: chunk.metadata._tag === "None"
    ? undefined
    : chunk.metadata.value,
})

const projectionFromArtifact = (
  artifact: PreparedGraphMutationProjectionArtifact,
): ReplaceProjectedRevision => {
  const [firstChunk, ...remainingChunks] = artifact.input.chunks

  return {
    key: artifact.input.key,
    expectedToken: artifact.input.expectedToken === null
      ? Option.none()
      : Option.some(artifact.input.expectedToken),
    encodedTarget: referenceFromArtifact(artifact.input.encodedTarget),
    projectionVersion: artifact.input.projectionVersion,
    textPolicy: artifact.input.textPolicy,
    revisionHash: artifact.input.revisionHash,
    embeddingProfile: artifact.input.embeddingProfile,
    chunks: [
      chunkFromArtifact(firstChunk),
      ...remainingChunks.map(chunkFromArtifact),
    ],
    embeddings: artifact.input.embeddings.map((embedding) => ({
      contentHash: embedding.contentHash,
      vector: embedding.vector,
    })),
  }
}

const topologyFromArtifact = (
  artifact: PreparedGraphMutationTopologyArtifact,
): ReplaceOutgoingGraphRelations => ({
  graph: artifact.input.graph,
  sourceDocumentKey: artifact.input.sourceDocumentKey,
  source: referenceFromArtifact(artifact.input.source),
  relations: artifact.input.relations.map((relation) => ({
    id: relation.id,
    version: relation.version,
    targetDocumentKind: relation.targetDocumentKind,
    targets: relation.targets.map((target) => ({
      documentKey: target.documentKey,
      reference: referenceFromArtifact(target.reference),
    })),
  })),
})

const invalidPreparedMutationArtifact = (
  reason: InvalidPreparedGraphMutationArtifact["reason"],
  detail: string,
): InvalidPreparedGraphMutationArtifact =>
  new InvalidPreparedGraphMutationArtifact({ reason, detail })

const validateProjectionReplacement = (
  replacement: ReplaceProjectedRevision,
): Result.Result<void, string> => {
  const reusableVectors = new Map<
    ContentHash,
    ReadonlyArray<number>
  >()

  if (Option.isSome(replacement.expectedToken)) {
    for (const chunk of replacement.chunks) {
      reusableVectors.set(chunk.contentHash, [])
    }
  }

  const plan = planProjectedRevisionReplacement(
    replacement,
    reusableVectors,
  )

  if (Result.isFailure(plan)) {
    return Result.fail(plan.failure)
  }

  const expectedDocumentKey = makeDocumentKey({
    graph: replacement.encodedTarget.graph,
    documentKind: replacement.encodedTarget.kind,
    encodedId: replacement.encodedTarget.id,
  })

  if (expectedDocumentKey !== replacement.key.documentKey) {
    return Result.fail("target_key_mismatch")
  }

  if (
    replacement.textPolicy !== "disabled" &&
    replacement.textPolicy.weights.context === 0 &&
    replacement.textPolicy.weights.label === 0 &&
    replacement.textPolicy.weights.content === 0
  ) {
    return Result.fail("invalid_text_policy")
  }

  for (const chunk of replacement.chunks) {
    if (
      makeChunkId({
        documentKey: replacement.key.documentKey,
        projection: replacement.key.projection,
        sectionKey: chunk.sectionKey,
        sectionPart: chunk.sectionPart,
      }) !== chunk.chunkId
    ) {
      return Result.fail("chunk_id_mismatch")
    }

    if (makeContentHash(chunk.embeddingContent) !== chunk.contentHash) {
      return Result.fail("content_hash_mismatch")
    }
  }

  return Result.succeed(undefined)
}

const validateTopologyReplacement = (
  replacement: ReplaceOutgoingGraphRelations,
): Result.Result<void, string> => {
  const plan = planOutgoingGraphRelationReplacement(replacement)

  return Result.isFailure(plan)
    ? Result.fail(plan.failure)
    : Result.succeed(undefined)
}

const unsupportedProjectionMutation = (
  operation: "delete_revision" | "prune_graph",
): ProjectionIndexStoreFailed =>
  new ProjectionIndexStoreFailed({
    operation,
    reason: "unavailable",
    cause: "Prepared mutation capture supports replacement operations only",
  })

const unsupportedTopologyMutation = (
  operation: "delete_node" | "prune_graph",
): GraphTopologyStoreFailed =>
  new GraphTopologyStoreFailed({
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
    compareIdentity(projectionIdentity(left), projectionIdentity(right))
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
    compareIdentity(relationIdentity(left), relationIdentity(right))
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

const decodePreparedGraphMutationArtifactValue = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- This storage boundary immediately decodes the untrusted JSON value with Effect Schema.
  input: unknown,
): Effect.Effect<
  PreparedGraphMutationArtifact,
  InvalidPreparedGraphMutationArtifact
> =>
  Schema.decodeUnknownEffect(PreparedGraphMutationArtifactSchema)(input, {
    onExcessProperty: "error",
  }).pipe(
    Effect.mapError(() =>
      invalidPreparedMutationArtifact("invalid_shape", "schema_rejected")
    ),
  )

const prepareMutationFromArtifact = (
  artifact: PreparedGraphMutationArtifact,
): Effect.Effect<
  PreparedGraphMutation,
  InvalidPreparedGraphMutationArtifact
> =>
  Effect.gen(function*() {
    const projections: Array<ReplaceProjectedRevision> = []
    const topologies: Array<ReplaceOutgoingGraphRelations> = []

    for (const operation of artifact.operations) {
      if (operation._tag === "ReplaceProjectedRevision") {
        const replacement = projectionFromArtifact(operation)
        const validation = validateProjectionReplacement(replacement)

        if (Result.isFailure(validation)) {
          return yield* invalidPreparedMutationArtifact(
            "invalid_projection",
            validation.failure,
          )
        }

        projections.push(replacement)
      } else {
        const replacement = topologyFromArtifact(operation)
        const validation = validateTopologyReplacement(replacement)

        if (Result.isFailure(validation)) {
          return yield* invalidPreparedMutationArtifact(
            "invalid_topology",
            validation.failure,
          )
        }

        topologies.push(replacement)
      }
    }

    const prepared = prepareMutation(projections, topologies)

    if (Result.isFailure(prepared)) {
      return yield* invalidPreparedMutationArtifact(
        "duplicate_operation",
        prepared.failure.identity,
      )
    }

    return prepared.success
  })

/** Encode a prepared mutation as canonical, versioned JSON for durable storage. */
export const encodePreparedGraphMutation: (
  prepared: PreparedGraphMutation,
) => Effect.Effect<string, InvalidPreparedGraphMutationArtifact> = Effect.fn(
  "DocumentGraph.encodePreparedMutation",
)(function*(prepared) {
  const artifact = yield* decodePreparedGraphMutationArtifactValue(
    mutationToArtifact(prepared),
  )

  const normalized = yield* prepareMutationFromArtifact(artifact)
  const canonicalArtifact = mutationToArtifact(normalized)

  const jsonValue = yield* Schema.decodeEffect(JsonValueSchema)(
    canonicalArtifact,
    { onExcessProperty: "error" },
  ).pipe(
    Effect.mapError(() =>
      invalidPreparedMutationArtifact("invalid_shape", "not_json_safe")
    ),
  )

  return canonicalJsonValue(jsonValue)
})

/** Decode, validate, normalize, copy, and deeply freeze persisted mutation JSON. */
export const decodePreparedGraphMutation: (
  encoded: string,
) => Effect.Effect<
  PreparedGraphMutation,
  InvalidPreparedGraphMutationArtifact
> = Effect.fn("DocumentGraph.decodePreparedMutation")(function*(encoded) {
  const parsed: unknown = yield* Effect.try({
    try: () => JSON.parse(encoded),
    catch: () =>
      invalidPreparedMutationArtifact("invalid_json", "parse_failed"),
  })

  const artifact = yield* decodePreparedGraphMutationArtifactValue(parsed)

  return yield* prepareMutationFromArtifact(artifact)
})

/** Capturing stores plus the frozen mutation they recorded. */
interface MutationCapture {
  /**
   * Layers replacing the live stores during capture. Reads delegate to the
   * live stores; replacements are recorded and acknowledged with synthetic
   * commit counts. Delete and prune operations fail without touching storage.
   */
  readonly layer: Layer.Layer<ProjectionIndexStore | GraphTopologyStore>

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
  ProjectionIndexStore | GraphTopologyStore
> = Effect.gen(function*() {
  const liveProjectionStore = yield* ProjectionIndexStore
  const liveTopologyStore = yield* GraphTopologyStore

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
      GraphTopologyStore,
      GraphTopologyStore.of({
        ...liveTopologyStore,
        replaceDocumentTopology: (replacement) =>
          Effect.sync(() => {
            relations.push(replacement)

            return syntheticRelationCommit(replacement)
          }),
        deleteNode: () =>
          Effect.fail(unsupportedTopologyMutation("delete_node")),
        pruneTopology: () =>
          Effect.fail(unsupportedTopologyMutation("prune_graph")),
      }),
    ),
  )

  return {
    layer,
    prepare: () => {
      prepared ??= prepareMutation(projections, relations)

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
  R | ProjectionIndexStore | GraphTopologyStore
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
 * topology replacements) because storage adapters serialize each operation
 * under one transaction-scoped lock; replaying sequentially keeps one
 * operation in flight per transaction. Every vector was resolved before
 * capture, but a remote projection adapter may still publish over the network.
 */
export const replayPreparedGraphMutation: (
  prepared: PreparedGraphMutation,
  target: GraphMutationTarget,
) => Effect.Effect<
  PreparedMutationReplayReport,
  | ProjectionIndexStoreFailed
  | ProjectionIndexConflict
  | GraphTopologyStoreFailed
> = Effect.fn("DocumentGraph.replayMutation")(function*(prepared, target) {
    let replacedRevisions = 0
    let replacedRelationSets = 0

    for (const operation of prepared.operations) {
      if (operation._tag === "ReplaceProjectedRevision") {
        yield* target.replaceRevision(operation.input)
        replacedRevisions += 1
      } else {
        yield* target.replaceDocumentTopology(operation.input)
        replacedRelationSets += 1
      }
    }

    return { replacedRevisions, replacedRelationSets }
  })
