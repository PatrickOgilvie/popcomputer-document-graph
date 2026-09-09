import type {
  Filter,
  NamespaceQueryParams,
  NamespaceWriteParams,
} from "@turbopuffer/turbopuffer"
import { Effect, Option, Result, Schema } from "effect"
import {
  ContentHashSchema,
  type ContentHash,
} from "../../document/document-identity.js"
import {
  JsonValueSchema,
  type JsonValue,
} from "../../document/json-value.js"
import type { EmbeddingProfile } from "../../indexing/embedding-provider.js"
import {
  ProjectionMutationIdSchema,
  ProjectionPayloadDigestSchema,
  ProjectionPublicationCoordinator,
  ProjectionPublicationCoordinatorFailed,
  ProjectionPublicationGenerationSchema,
  ProjectionPublicationIdSchema,
  ProjectionPublicationPlanStale,
  ProjectionPublicationSuperseded,
  sumProjectionPrune,
  type ProjectionPublicationCoordinatorService,
  type ProjectionPublicationIntent,
  type ProjectionPublicationLease,
} from "../../indexing/projection-publication.js"
import {
  countProjectedRevisionReplacement,
  embeddingProfilesEqual,
  IndexRevisionTokenSchema,
  planProjectedRevisionReplacement,
  ProjectionIndexConflict,
  ProjectionIndexStoreFailed,
  type IndexedRevisionSnapshot,
  type ProjectionIndexCommit,
  type ProjectionIndexDeletion,
  type ProjectionIndexStoreService,
  type ProjectedChunkRecord,
  type ReplaceProjectedRevision,
} from "../../indexing/projection-index.js"
import type { TextSearchPolicy } from "../../document/text-search-policy.js"
import type { EncodedDocumentReference } from "../../document/document-instance.js"
import {
  TurbopufferClient,
  type TurbopufferClientService,
} from "./client.js"
import {
  compileTurbopufferSchemaManifest,
  TurbopufferFilterableAttributes,
  TurbopufferMaximumAttributeBytes,
  TurbopufferMaximumDocumentBytes,
  TurbopufferMaximumFilterableValueBytes,
  TurbopufferMaximumQueryRows,
  TurbopufferMaximumWriteBytes,
  type TurbopufferSchemaGeneration,
} from "./config.js"
import {
  InvalidTurbopufferConfiguration,
  InvalidTurbopufferResponse,
  TurbopufferMutationTooLarge,
  type TurbopufferTransportFailed,
} from "./errors.js"
import {
  hashTurbopufferIdentity,
  makeTurbopufferMarkerRowId,
  TurbopufferNamespaceIdentitySchema,
} from "./identity.js"
import {
  turbopufferWorkspacePartitionsEqual,
  validateTurbopufferWorkspacePartition,
  type TurbopufferWorkspacePartition,
} from "./partition.js"
import {
  makeTurbopufferLiveSlotRow,
  makeTurbopufferMarkerRow,
  makeTurbopufferTombstoneRow,
  type TurbopufferPublicationRow,
  type TurbopufferPublicationRowContext,
} from "./row-codec.js"

const QueryEnvelopeSchema = Schema.Struct({
  rows: Schema.Array(Schema.Unknown),
})

const CurrentVectorRowSchema = Schema.Struct({
  content_hash: ContentHashSchema,
  vector: Schema.Array(Schema.Finite),
})

const WriteResponseSchema = Schema.Struct({
  status: Schema.Literal("OK"),
  rows_affected: Schema.Number.pipe(
    Schema.check(Schema.isInt()),
    Schema.check(Schema.isGreaterThanOrEqualTo(0)),
  ),
})

const MarkerResultRowSchema = Schema.Struct({
  row_kind: Schema.Literal("marker"),
  partition_id: TurbopufferNamespaceIdentitySchema,
  publication_id: ProjectionPublicationIdSchema,
  publication_generation: ProjectionPublicationGenerationSchema,
  slot_high_water: Schema.Number.pipe(
    Schema.check(Schema.isInt()),
    Schema.check(Schema.isGreaterThanOrEqualTo(0)),
  ),
})

/** Runtime bounds and immutable vector partition for the TP projection store. */
export interface TurbopufferProjectionIndexConfig {
  /** Canonical workspace, vector-space, and schema partition. */
  readonly partition: TurbopufferWorkspacePartition
  /** Maximum stable slots for one logical revision; defaults to 10,000. */
  readonly maximumSlotsPerRevision?: number | undefined
  /** Maximum serialized atomic request bytes; defaults to 4 MiB. */
  readonly maximumPublicationBytes?: number | undefined
}

interface ResolvedProjectionIndexConfig {
  readonly partition: TurbopufferWorkspacePartition
  readonly embeddingProfile: EmbeddingProfile
  readonly schemaGeneration: TurbopufferSchemaGeneration
  readonly maximumSlotsPerRevision: number
  readonly maximumPublicationBytes: number
}

const resolveConfig = (
  config: TurbopufferProjectionIndexConfig,
): ResolvedProjectionIndexConfig => {
  const maximumSlotsPerRevision = config.maximumSlotsPerRevision ?? 10_000
  const maximumPublicationBytes = config.maximumPublicationBytes ?? 4_194_304
  if (!Number.isSafeInteger(maximumSlotsPerRevision) ||
    maximumSlotsPerRevision < 1 ||
    maximumSlotsPerRevision > TurbopufferMaximumQueryRows) {
    throw new InvalidTurbopufferConfiguration({
      field: "maximum_slots_per_revision",
      reason: "invalid_value",
    })
  }
  if (!Number.isSafeInteger(maximumPublicationBytes) ||
    maximumPublicationBytes < 1_024 ||
    maximumPublicationBytes > TurbopufferMaximumWriteBytes) {
    throw new InvalidTurbopufferConfiguration({
      field: "maximum_publication_bytes",
      reason: "invalid_value",
    })
  }
  const partition = validateTurbopufferWorkspacePartition(config.partition)
  return {
    partition,
    embeddingProfile: partition.embeddingProfile,
    schemaGeneration: partition.schemaGeneration,
    maximumSlotsPerRevision,
    maximumPublicationBytes,
  }
}

const indexFailure = (
  operation: ProjectionIndexStoreFailed["operation"],
  reason: ProjectionIndexStoreFailed["reason"],
  cause: unknown,
): ProjectionIndexStoreFailed =>
  new ProjectionIndexStoreFailed({ operation, reason, cause })

const coordinatorFailure = (
  operation: ProjectionIndexStoreFailed["operation"],
  error: ProjectionPublicationCoordinatorFailed,
): ProjectionIndexStoreFailed =>
  indexFailure(
    operation,
    error.reason === "invalid_stored_state"
      ? "invalid_stored_state"
      : error.reason === "capacity_exceeded"
        ? "capacity_exceeded"
        : error.reason,
    error,
  )

const providerFailure = (
  operation: ProjectionIndexStoreFailed["operation"],
  error: TurbopufferTransportFailed,
): ProjectionIndexStoreFailed =>
  indexFailure(
    operation,
    error.reason === "rejected" ? "invalid_replacement" : "unavailable",
    error,
  )

const decodeEnvelopeRows = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- This provider boundary immediately decodes with QueryEnvelopeSchema.
  response: unknown,
  operation: "query" | "write",
): Effect.Effect<ReadonlyArray<unknown>, InvalidTurbopufferResponse> =>
  Schema.decodeUnknownEffect(QueryEnvelopeSchema)(response).pipe(
    Effect.mapError((cause) =>
      new InvalidTurbopufferResponse({
        operation,
        reason: "invalid_shape",
        cause,
      })),
    Effect.map((decoded) => decoded.rows),
  )

const loadReusableVectors = Effect.fn(
  "TurbopufferProjectionIndex.loadReusableVectors",
)(function*(input: {
  readonly replacement: ReplaceProjectedRevision
  readonly config: ResolvedProjectionIndexConfig
  readonly client: TurbopufferClientService
}) {
  const request: NamespaceQueryParams = {
    filters: ["And", [
      ["row_kind", "Eq", "slot"],
      ["is_live", "Eq", true],
      ["partition_id", "Eq", input.config.partition.identity],
      ["document_key", "Eq", input.replacement.key.documentKey],
      ["projection_id", "Eq", input.replacement.key.projection],
      [
        "embedding_profile_id",
        "Eq",
        input.config.embeddingProfile.id,
      ],
      [
        "embedding_profile_version",
        "Eq",
        input.config.embeddingProfile.version,
      ],
      ["schema_generation", "Eq", input.config.schemaGeneration],
    ]],
    include_attributes: ["content_hash", "vector"],
    limit: input.config.maximumSlotsPerRevision,
    rank_by: ["id", "asc"],
    consistency: { level: "strong" },
  }
  const response = yield* input.client.query(request)
  const rows = yield* decodeEnvelopeRows(response, "query")
  const vectors = new Map<ContentHash, ReadonlyArray<number>>()
  yield* Effect.forEach(rows, (row, index) =>
    Schema.decodeUnknownEffect(CurrentVectorRowSchema)(row).pipe(
      Effect.mapError((cause) =>
        new InvalidTurbopufferResponse({
          operation: "query",
          reason: "invalid_row",
          rowIndex: index,
          cause,
        })),
      Effect.flatMap((decoded) => {
        if (
          decoded.vector.length !== input.config.embeddingProfile.dimensions
        ) {
          return Effect.fail(new InvalidTurbopufferResponse({
            operation: "query",
            reason: "invalid_row",
            rowIndex: index,
            cause: "Reusable vector dimensions do not match the profile",
          }))
        }
        const previous = vectors.get(decoded.content_hash)
        if (
          previous !== undefined &&
          (previous.length !== decoded.vector.length ||
            previous.some((component, ordinal) =>
              component !== decoded.vector[ordinal]))
        ) {
          return Effect.fail(new InvalidTurbopufferResponse({
            operation: "query",
            reason: "invalid_row",
            rowIndex: index,
            cause: "One content hash resolved to inconsistent reusable vectors",
          }))
        }
        vectors.set(decoded.content_hash, decoded.vector)
        return Effect.void
      }),
    ),
  )
  return vectors
})

const chunkPayload = (chunk: ProjectedChunkRecord): JsonValue => ({
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
  hasMetadata: chunk.metadata !== undefined,
  metadata: chunk.metadata ?? null,
})

const textPolicyPayload = (policy: TextSearchPolicy): JsonValue =>
  policy === "disabled"
    ? "disabled"
    : {
        language: policy.language,
        weights: {
          context: policy.weights.context,
          label: policy.weights.label,
          content: policy.weights.content,
        },
      }

const replacementIdentity = (input: {
  readonly replacement: ReplaceProjectedRevision
  readonly slotHighWater: number
  readonly vectors: ReadonlyMap<ContentHash, ReadonlyArray<number>>
  readonly schemaGeneration: TurbopufferSchemaGeneration
  readonly namespace: TurbopufferWorkspacePartition["namespace"]
}) => {
  const logical: JsonValue = [
    "honertia.turbopuffer-mutation",
    1,
    "replace",
    input.namespace,
    input.replacement.key.documentKey,
    input.replacement.key.projection,
    Option.getOrNull(input.replacement.expectedToken),
    input.replacement.encodedTarget.graph,
    input.replacement.encodedTarget.kind,
    input.replacement.encodedTarget.id,
    input.replacement.projectionVersion,
    input.replacement.revisionHash,
    input.replacement.embeddingProfile.id,
    input.replacement.embeddingProfile.version,
    input.replacement.embeddingProfile.dimensions,
    input.schemaGeneration,
    textPolicyPayload(input.replacement.textPolicy),
    input.slotHighWater,
    input.replacement.chunks.map(chunkPayload),
  ]
  const physical: JsonValue = [
    logical,
    input.replacement.chunks.map((chunk) => [
      chunk.contentHash,
      input.vectors.get(chunk.contentHash) ?? null,
    ]),
  ]
  return {
    mutationId: Schema.decodeSync(ProjectionMutationIdSchema)(
      hashTurbopufferIdentity(logical),
    ),
    payloadDigest: Schema.decodeSync(ProjectionPayloadDigestSchema)(
      hashTurbopufferIdentity(physical),
    ),
  }
}

const deletionIdentity = (input: {
  readonly key: ReplaceProjectedRevision["key"]
  readonly expectedToken: Option.Option<IndexedRevisionSnapshot["token"]>
  readonly slotHighWater: number
  readonly config: ResolvedProjectionIndexConfig
  readonly namespace: TurbopufferWorkspacePartition["namespace"]
}) => {
  const logical: JsonValue = [
    "honertia.turbopuffer-mutation",
    1,
    "delete",
    input.namespace,
    input.key.documentKey,
    input.key.projection,
    Option.getOrNull(input.expectedToken),
    input.config.embeddingProfile.id,
    input.config.embeddingProfile.version,
    input.config.embeddingProfile.dimensions,
    input.config.schemaGeneration,
    input.slotHighWater,
  ]
  const digest = hashTurbopufferIdentity([logical, "all-slots-tombstoned"])
  return {
    mutationId: Schema.decodeSync(ProjectionMutationIdSchema)(
      hashTurbopufferIdentity(logical),
    ),
    payloadDigest: Schema.decodeSync(ProjectionPayloadDigestSchema)(digest),
  }
}

interface PublicationMaterial {
  readonly encodedTarget: EncodedDocumentReference
  readonly projectionVersion: string
  readonly textPolicy: TextSearchPolicy
  readonly revisionHash: ReplaceProjectedRevision["revisionHash"]
  readonly chunks: ReadonlyArray<ProjectedChunkRecord>
  readonly vectors: ReadonlyMap<ContentHash, ReadonlyArray<number>>
}

const rowContext = (input: {
  readonly lease: ProjectionPublicationLease
  readonly material: PublicationMaterial | undefined
  readonly config: ResolvedProjectionIndexConfig
}): TurbopufferPublicationRowContext => ({
  graph: input.material?.encodedTarget.graph ?? "__deleted__",
  documentKind: input.material?.encodedTarget.kind ?? "__deleted__",
  key: input.lease.intent.key,
  projectionVersion: input.material?.projectionVersion ?? "deleted",
  partition: input.config.partition,
  publicationId: input.lease.publicationId,
  publicationGeneration: input.lease.generation,
})

const buildPublicationRows = (input: {
  readonly lease: ProjectionPublicationLease
  readonly material: PublicationMaterial | undefined
  readonly config: ResolvedProjectionIndexConfig
}): ReadonlyArray<TurbopufferPublicationRow> => {
  const context = rowContext(input)
  const chunks = new Map(
    (input.material?.chunks ?? []).map((chunk) => [chunk.ordinal, chunk]),
  )
  const rows: Array<TurbopufferPublicationRow> = [
    makeTurbopufferMarkerRow({
      context,
      liveSlotCount: chunks.size,
      slotHighWater: input.lease.slotHighWater,
    }),
  ]
  for (let slotOrdinal = 0; slotOrdinal < input.lease.slotHighWater; slotOrdinal += 1) {
    const chunk = chunks.get(slotOrdinal)
    if (chunk === undefined || input.material === undefined) {
      rows.push(makeTurbopufferTombstoneRow({ context, slotOrdinal }))
      continue
    }
    const vector = input.material.vectors.get(chunk.contentHash)
    if (vector === undefined) {
      throw new Error("A planned publication lost a complete vector")
    }
    rows.push(makeTurbopufferLiveSlotRow({
      context,
      encodedTarget: input.material.encodedTarget,
      revisionHash: input.material.revisionHash,
      textPolicy: input.material.textPolicy,
      slotOrdinal,
      chunk,
      vector,
    }))
  }
  return rows
}

const generationFence: Filter = ["Or", [
  [
    "publication_generation",
    "Lt",
    { $ref_new: "publication_generation" },
  ],
  ["And", [
    [
      "publication_generation",
      "Eq",
      { $ref_new: "publication_generation" },
    ],
    ["publication_id", "Eq", { $ref_new: "publication_id" }],
  ]],
]]

const writeRequest = (
  rows: ReadonlyArray<TurbopufferPublicationRow>,
  config: ResolvedProjectionIndexConfig,
): NamespaceWriteParams => ({
  upsert_rows: rows.map((row) => ({
    ...row,
    vector: [...row.vector],
  })),
  upsert_condition: generationFence,
  return_affected_ids: true,
  distance_metric: "cosine_distance",
  schema: {
    ...compileTurbopufferSchemaManifest(
      config.embeddingProfile.dimensions,
    ).attributes,
  },
})

const JsonObjectSchema = Schema.Record(Schema.String, JsonValueSchema)

interface PublicationMeasurements {
  readonly requestBytes: number
  readonly largestDocumentBytes: number
  readonly largestAttributeBytes: number
  readonly largestFilterableValueBytes: number
}

const jsonBytes = (value: JsonValue): number =>
  new TextEncoder().encode(JSON.stringify(value)).byteLength

const largestScalarBytes = (value: JsonValue): number =>
  Array.isArray(value)
    ? value.reduce(
        (largest, item) => Math.max(largest, jsonBytes(item)),
        0,
      )
    : jsonBytes(value)

const publicationMeasurements = (
  rows: ReadonlyArray<TurbopufferPublicationRow>,
  config: ResolvedProjectionIndexConfig,
): PublicationMeasurements => {
  let largestDocumentBytes = 0
  let largestAttributeBytes = 0
  let largestFilterableValueBytes = 0
  const filterableAttributes = new Set<string>(
    TurbopufferFilterableAttributes,
  )
  for (const row of rows) {
    const encodedDocument = JSON.stringify(row)
    if (encodedDocument === undefined) {
      throw new Error("A Turbopuffer publication row failed to encode")
    }
    const decodedDocument: unknown = JSON.parse(encodedDocument)
    const document = Schema.decodeUnknownSync(JsonObjectSchema)(
      decodedDocument,
    )
    largestDocumentBytes = Math.max(
      largestDocumentBytes,
      new TextEncoder().encode(encodedDocument).byteLength,
    )
    for (const [name, attribute] of Object.entries(document)) {
      largestAttributeBytes = Math.max(
        largestAttributeBytes,
        jsonBytes(attribute),
      )
      if (filterableAttributes.has(name)) {
        largestFilterableValueBytes = Math.max(
          largestFilterableValueBytes,
          largestScalarBytes(attribute),
        )
      }
    }
  }
  return {
    requestBytes: new TextEncoder().encode(
      JSON.stringify(writeRequest(rows, config)),
    ).byteLength,
    largestDocumentBytes,
    largestAttributeBytes,
    largestFilterableValueBytes,
  }
}

const capacityFailure = (
  config: ResolvedProjectionIndexConfig,
  rows: ReadonlyArray<TurbopufferPublicationRow>,
  operation: ProjectionIndexStoreFailed["operation"],
): ProjectionIndexStoreFailed => {
  const measurements = publicationMeasurements(rows, config)
  const limit = measurements.largestFilterableValueBytes >
      TurbopufferMaximumFilterableValueBytes
    ? {
        bytes: measurements.largestFilterableValueBytes,
        maximumBytes: TurbopufferMaximumFilterableValueBytes,
      }
    : measurements.largestAttributeBytes >
      TurbopufferMaximumAttributeBytes
    ? {
        bytes: measurements.largestAttributeBytes,
        maximumBytes: TurbopufferMaximumAttributeBytes,
      }
    : measurements.largestDocumentBytes > TurbopufferMaximumDocumentBytes
      ? {
          bytes: measurements.largestDocumentBytes,
          maximumBytes: TurbopufferMaximumDocumentBytes,
        }
      : {
          bytes: measurements.requestBytes,
          maximumBytes: config.maximumPublicationBytes,
        }
  return indexFailure(
    operation,
    "capacity_exceeded",
    new TurbopufferMutationTooLarge({
      rows: rows.length,
      bytes: limit.bytes,
      maximumBytes: limit.maximumBytes,
    }),
  )
}

const assertPublicationFits = (
  config: ResolvedProjectionIndexConfig,
  rows: ReadonlyArray<TurbopufferPublicationRow>,
  operation: ProjectionIndexStoreFailed["operation"],
): Effect.Effect<void, ProjectionIndexStoreFailed> => {
  const measurements = publicationMeasurements(rows, config)
  if (
    rows.length - 1 > config.maximumSlotsPerRevision ||
    measurements.requestBytes > config.maximumPublicationBytes ||
    measurements.largestDocumentBytes > TurbopufferMaximumDocumentBytes ||
    measurements.largestAttributeBytes > TurbopufferMaximumAttributeBytes ||
    measurements.largestFilterableValueBytes >
      TurbopufferMaximumFilterableValueBytes
  ) {
    return Effect.fail(capacityFailure(config, rows, operation))
  }
  return Effect.void
}

const provisionalLease = (
  intent: ProjectionPublicationIntent,
): ProjectionPublicationLease => ({
  intent,
  publicationId: Schema.decodeSync(ProjectionPublicationIdSchema)(
    "p".repeat(128),
  ),
  generation: Schema.decodeSync(ProjectionPublicationGenerationSchema)(
    Number.MAX_SAFE_INTEGER,
  ),
  slotHighWater: intent.slotHighWater,
})

const decodeWriteResponse = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- This provider boundary immediately decodes with WriteResponseSchema.
  response: unknown,
): Effect.Effect<typeof WriteResponseSchema.Type, InvalidTurbopufferResponse> =>
  Schema.decodeUnknownEffect(WriteResponseSchema)(response).pipe(
    Effect.mapError((cause) =>
      new InvalidTurbopufferResponse({
        operation: "write",
        reason: "invalid_shape",
        cause,
      })),
  )

const finalizeVisiblePublication = Effect.fn(
  "TurbopufferProjectionIndex.finalizeVisiblePublication",
)(function*(input: {
  readonly lease: ProjectionPublicationLease
  readonly coordinator: ProjectionPublicationCoordinatorService
}) {
  return yield* input.coordinator.finalizePublication(input.lease).pipe(
    Effect.mapError((error) =>
      error instanceof ProjectionPublicationSuperseded
        ? indexFailure(
            input.lease.intent._tag === "Replace"
              ? "replace_revision"
              : "delete_revision",
            "invalid_stored_state",
            error,
          )
        : error.reason === "invalid_stored_state"
          ? coordinatorFailure(
              input.lease.intent._tag === "Replace"
                ? "replace_revision"
                : "delete_revision",
              error,
            )
          : indexFailure(
            input.lease.intent._tag === "Replace"
              ? "replace_revision"
              : "delete_revision",
            "publication_in_doubt",
            error,
          )),
  )
})

const reconcilePublicationMarker = Effect.fn(
  "TurbopufferProjectionIndex.reconcileMarker",
)(function*(input: {
  readonly lease: ProjectionPublicationLease
  readonly client: TurbopufferClientService
  readonly coordinator: ProjectionPublicationCoordinatorService
}) {
  const operation = input.lease.intent._tag === "Replace"
    ? "replace_revision"
    : "delete_revision"
  const markerId = makeTurbopufferMarkerRowId({
    partitionIdentity: input.client.partition.identity,
    documentKey: input.lease.intent.key.documentKey,
    projection: input.lease.intent.key.projection,
  })
  const response = yield* input.client.query({
    filters: ["And", [
      ["id", "Eq", markerId],
      ["partition_id", "Eq", input.client.partition.identity],
    ]],
    include_attributes: [
      "row_kind",
      "partition_id",
      "publication_id",
      "publication_generation",
      "slot_high_water",
    ],
    limit: 1,
    rank_by: ["id", "asc"],
    consistency: { level: "strong" },
  }).pipe(
    Effect.mapError((error) =>
      indexFailure(operation, "publication_in_doubt", error)),
  )
  const rows = yield* decodeEnvelopeRows(response, "query").pipe(
    Effect.mapError((error) =>
      indexFailure(operation, "publication_in_doubt", error)),
  )
  const row = rows[0]
  if (row === undefined) {
    return yield* Effect.fail(indexFailure(
      operation,
      "publication_in_doubt",
      "The publication marker is not yet visible",
    ))
  }
  const marker = yield* Schema.decodeUnknownEffect(MarkerResultRowSchema)(row)
    .pipe(Effect.mapError((cause) =>
      indexFailure(operation, "invalid_stored_state", cause)))
  if (marker.partition_id !== input.client.partition.identity) {
    return yield* Effect.fail(indexFailure(
      operation,
      "invalid_stored_state",
      "The publication marker belongs to another workspace partition",
    ))
  }
  if (
    marker.publication_generation === input.lease.generation &&
    marker.publication_id === input.lease.publicationId &&
    marker.slot_high_water === input.lease.slotHighWater
  ) {
    return yield* finalizeVisiblePublication({
      lease: input.lease,
      coordinator: input.coordinator,
    })
  }
  if (
    marker.publication_generation > input.lease.generation ||
    (marker.publication_generation === input.lease.generation &&
      marker.publication_id !== input.lease.publicationId)
  ) {
    yield* input.coordinator.supersedePublication(input.lease).pipe(
      Effect.mapError((error) => coordinatorFailure(operation, error)),
    )
    return yield* Effect.fail(indexFailure(
      operation,
      "invalid_stored_state",
      "A newer or competing publication fenced this lease",
    ))
  }
  return yield* Effect.fail(indexFailure(
    operation,
    "publication_in_doubt",
    "The publication marker does not prove the pending lease visible",
  ))
})

const publishLease = Effect.fn(
  "TurbopufferProjectionIndex.publishLease",
)(function*(input: {
  readonly lease: ProjectionPublicationLease
  readonly material: PublicationMaterial | undefined
  readonly config: ResolvedProjectionIndexConfig
  readonly client: TurbopufferClientService
  readonly coordinator: ProjectionPublicationCoordinatorService
}) {
  const operation = input.lease.intent._tag === "Replace"
    ? "replace_revision"
    : "delete_revision"
  const rows = buildPublicationRows(input)
  yield* assertPublicationFits(input.config, rows, operation)
  const attempt = yield* input.client.write(
    writeRequest(rows, input.config),
  ).pipe(
    Effect.map((response) => ({
      _tag: "ProviderResponse" as const,
      response,
    })),
    Effect.catch((error) => {
      if (
        error.requestOutcome === "definitely_not_applied"
      ) {
        // Duplicate attempts share this lease. Rejection of this request does
        // not prove that another attempt cannot still publish and finalize it.
        return Effect.fail(providerFailure(operation, error))
      }
      return reconcilePublicationMarker({
        lease: input.lease,
        client: input.client,
        coordinator: input.coordinator,
      }).pipe(Effect.map((outcome) => ({
        _tag: "Reconciled" as const,
        outcome,
      })))
    }),
  )
  if (attempt._tag === "Reconciled") {
    return attempt.outcome
  }
  const decoded = yield* decodeWriteResponse(attempt.response).pipe(
    Effect.mapError((error) =>
      indexFailure(operation, "invalid_stored_state", error)),
  )
  if (decoded.rows_affected === rows.length) {
    return yield* finalizeVisiblePublication({
      lease: input.lease,
      coordinator: input.coordinator,
    })
  }
  if (decoded.rows_affected === 0) {
    return yield* reconcilePublicationMarker({
      lease: input.lease,
      client: input.client,
      coordinator: input.coordinator,
    })
  }
  return yield* Effect.fail(indexFailure(
    operation,
    "invalid_stored_state",
    new InvalidTurbopufferResponse({
      operation: "write",
      reason: "partial_write",
      cause: {
        expected: rows.length,
        actual: decoded.rows_affected,
      },
    }),
  ))
})

/** Build the projection-index service backed by D1 coordination and TP rows. */
export const makeTurbopufferProjectionIndexStore = (
  inputConfig: TurbopufferProjectionIndexConfig,
): Effect.Effect<
  ProjectionIndexStoreService,
  InvalidTurbopufferConfiguration,
  ProjectionPublicationCoordinator | TurbopufferClient
> => Effect.gen(function*() {
  const config = yield* Effect.try({
    try: () => resolveConfig(inputConfig),
    catch: (cause) =>
      Schema.is(InvalidTurbopufferConfiguration)(cause)
        ? cause
        : new InvalidTurbopufferConfiguration({
            field: "partition",
            reason: "invalid_value",
          }),
  })
  const coordinator = yield* ProjectionPublicationCoordinator
  const client = yield* TurbopufferClient
  if (
    !turbopufferWorkspacePartitionsEqual(config.partition, client.partition) ||
    coordinator.indexGeneration !== config.partition.d1IndexGeneration
  ) {
    return yield* Effect.fail(new InvalidTurbopufferConfiguration({
      field: "partition",
      reason: "mismatch",
    }))
  }

  const replaceAttempt = (
    replacement: ReplaceProjectedRevision,
    remainingPlanRetries: number,
  ): Effect.Effect<
    ProjectionIndexCommit,
    ProjectionIndexStoreFailed | ProjectionIndexConflict
  > => Effect.gen(function*() {
      if (!embeddingProfilesEqual(
        replacement.embeddingProfile,
        config.embeddingProfile,
      )) {
        return yield* Effect.fail(indexFailure(
          "replace_revision",
          "invalid_replacement",
          "The replacement embedding profile does not match the namespace",
        ))
      }
      const [[lookup], [headLookup]] = yield* Effect.all([
        coordinator.loadRevisions([replacement.key]),
        coordinator.loadHeads([replacement.key]),
      ], { concurrency: "unbounded" }).pipe(
        Effect.mapError((error) =>
          coordinatorFailure("replace_revision", error)),
      )
      const current = lookup?.revision ?? Option.none()
      const suppliedContent = new Set(
        replacement.embeddings.map((embedding) => embedding.contentHash),
      )
      const needsReusableVectors = replacement.chunks.some((chunk) =>
        !suppliedContent.has(chunk.contentHash),
      )
      // A never-published or deleted head has no provider vectors to reuse.
      // Skipping this query is what lets the following write bootstrap a new
      // namespace with its pinned schema.
      // Prepared replay supplies every vector. It must not download the
      // previous revision's potentially large vector inventory unnecessarily.
      const reusableVectors = Option.isNone(current) || !needsReusableVectors
        ? new Map<ContentHash, ReadonlyArray<number>>()
        : yield* loadReusableVectors({ replacement, config, client }).pipe(
            Effect.mapError((error) =>
              error._tag === "TurbopufferTransportFailed"
                ? providerFailure("replace_revision", error)
                : indexFailure(
                    "replace_revision",
                    "invalid_stored_state",
                    error,
                  )),
          )
      const plan = planProjectedRevisionReplacement(
        replacement,
        reusableVectors,
      )
      if (Result.isFailure(plan)) {
        return yield* Effect.fail(indexFailure(
          "replace_revision",
          "invalid_replacement",
          plan.failure,
        ))
      }
      const requiredSlotHighWater = replacement.chunks.reduce(
        (maximum, chunk) => Math.max(maximum, chunk.ordinal + 1),
        0,
      )
      const inheritedSlotHighWater = Option.isSome(headLookup?.head ?? Option.none())
        ? Option.getOrThrow(headLookup?.head ?? Option.none()).slotHighWater
        : 0
      const slotHighWater = Math.max(
        inheritedSlotHighWater,
        requiredSlotHighWater,
      )
      if (slotHighWater > config.maximumSlotsPerRevision) {
        return yield* Effect.fail(indexFailure(
          "replace_revision",
          "capacity_exceeded",
          { slotHighWater, limit: config.maximumSlotsPerRevision },
        ))
      }
      const previous = new Set(
        Option.match(current, {
          onNone: () => [],
          onSome: (snapshot) => snapshot.chunks.map((chunk) => chunk.chunkId),
        }),
      )
      const counts = countProjectedRevisionReplacement(
        previous,
        plan.success.chunkIds,
      )
      const identity = replacementIdentity({
        replacement,
        slotHighWater,
        vectors: plan.success.vectors,
        schemaGeneration: config.schemaGeneration,
        namespace: client.partition.namespace,
      })
      const token = Schema.decodeSync(IndexRevisionTokenSchema)(
        `turbopuffer:${identity.mutationId}`,
      )
      const [firstChunk, ...remainingChunks] = replacement.chunks
      const snapshot: IndexedRevisionSnapshot = {
        token,
        revisionHash: replacement.revisionHash,
        embeddingProfile: replacement.embeddingProfile,
        chunks: [
          {
            chunkId: firstChunk.chunkId,
            contentHash: firstChunk.contentHash,
          },
          ...remainingChunks.map((chunk) => ({
            chunkId: chunk.chunkId,
            contentHash: chunk.contentHash,
          })),
        ],
      }
      const intent: ProjectionPublicationIntent = {
        _tag: "Replace",
        key: replacement.key,
        expectedToken: replacement.expectedToken,
        mutationId: identity.mutationId,
        payloadDigest: identity.payloadDigest,
        snapshot,
        catalog: {
          graph: replacement.encodedTarget.graph,
          documentKind: replacement.encodedTarget.kind,
          projectionVersion: replacement.projectionVersion,
        },
        liveSlotCount: replacement.chunks.length,
        requiredSlotHighWater,
        slotHighWater,
        maximumSlotHighWater: config.maximumSlotsPerRevision,
        commit: { token, ...counts },
      }
      const material: PublicationMaterial = {
        encodedTarget: replacement.encodedTarget,
        projectionVersion: replacement.projectionVersion,
        textPolicy: replacement.textPolicy,
        revisionHash: replacement.revisionHash,
        chunks: replacement.chunks,
        vectors: plan.success.vectors,
      }
      const provisional = provisionalLease(intent)
      yield* assertPublicationFits(
        config,
        buildPublicationRows({ lease: provisional, material, config }),
        "replace_revision",
      )
      yield* assertPublicationFits(
        config,
        buildPublicationRows({ lease: provisional, material: undefined, config }),
        "replace_revision",
      )
      const beginResult = yield* coordinator.beginPublication(intent).pipe(
        Effect.result,
      )
      if (Result.isFailure(beginResult)) {
        const error = beginResult.failure
        if (error instanceof ProjectionPublicationPlanStale) {
          return remainingPlanRetries > 0
            ? yield* replaceAttempt(replacement, remainingPlanRetries - 1)
            : yield* Effect.fail(indexFailure(
                "replace_revision",
                "publication_in_progress",
                error,
              ))
        }
        return yield* Effect.fail(
          error instanceof ProjectionIndexConflict
            ? error
            : coordinatorFailure("replace_revision", error),
        )
      }
      const begun = beginResult.success
      if (begun._tag === "AlreadyCommitted") {
        if (begun.outcome._tag !== "Replaced") {
          return yield* Effect.fail(indexFailure(
            "replace_revision",
            "invalid_stored_state",
            "A replacement mutation resolved to a deletion outcome",
          ))
        }
        return begun.outcome.commit
      }
      const outcome = yield* publishLease({
        lease: begun.lease,
        material,
        config,
        client,
        coordinator,
      })
      if (outcome._tag !== "Replaced") {
        return yield* Effect.fail(indexFailure(
          "replace_revision",
          "invalid_stored_state",
          "A replacement publication finalized as deleted",
        ))
      }
      return outcome.commit
    })

  const replaceRevision: ProjectionIndexStoreService["replaceRevision"] =
    (replacement) => replaceAttempt(replacement, 3)

  const deleteAttempt = (
    key: ReplaceProjectedRevision["key"],
    remainingAttempts: number,
  ): Effect.Effect<ProjectionIndexDeletion, ProjectionIndexStoreFailed> =>
    Effect.gen(function*() {
      const [[lookup], [headLookup]] = yield* Effect.all([
        coordinator.loadRevisions([key]),
        coordinator.loadHeads([key]),
      ], { concurrency: "unbounded" }).pipe(
        Effect.mapError((error) => coordinatorFailure("delete_revision", error)),
      )
      const current = lookup?.revision ?? Option.none()
      const expectedToken = Option.map(current, (snapshot) => snapshot.token)
      const deletion: ProjectionIndexDeletion = Option.match(current, {
        onNone: () => ({ deletedRevisions: 0, deletedChunks: 0 }),
        onSome: (snapshot) => ({
          deletedRevisions: 1,
          deletedChunks: snapshot.chunks.length,
        }),
      })
      const inheritedSlotHighWater = Option.isSome(headLookup?.head ?? Option.none())
        ? Option.getOrThrow(headLookup?.head ?? Option.none()).slotHighWater
        : 0
      if (inheritedSlotHighWater > config.maximumSlotsPerRevision) {
        return yield* Effect.fail(indexFailure(
          "delete_revision",
          "capacity_exceeded",
          { inheritedSlotHighWater, limit: config.maximumSlotsPerRevision },
        ))
      }
      const identity = deletionIdentity({
        key,
        expectedToken,
        slotHighWater: inheritedSlotHighWater,
        config,
        namespace: client.partition.namespace,
      })
      const intent: ProjectionPublicationIntent = {
        _tag: "Delete",
        key,
        expectedToken,
        mutationId: identity.mutationId,
        payloadDigest: identity.payloadDigest,
        deletion,
        slotHighWater: inheritedSlotHighWater,
        maximumSlotHighWater: config.maximumSlotsPerRevision,
      }
      const provisional = provisionalLease(intent)
      yield* assertPublicationFits(
        config,
        buildPublicationRows({ lease: provisional, material: undefined, config }),
        "delete_revision",
      )
      const beginResult = yield* coordinator.beginPublication(intent).pipe(
        Effect.result,
      )
      if (Result.isFailure(beginResult)) {
        const error = beginResult.failure
        if (
          error instanceof ProjectionPublicationPlanStale ||
          error instanceof ProjectionIndexConflict
        ) {
          return remainingAttempts > 0
            ? yield* deleteAttempt(key, remainingAttempts - 1)
            : yield* Effect.fail(indexFailure(
                "delete_revision",
                "publication_in_progress",
                error,
              ))
        }
        return yield* Effect.fail(coordinatorFailure(
          "delete_revision",
          error,
        ))
      }
      const begun = beginResult.success
      if (begun._tag === "AlreadyCommitted") {
        if (begun.outcome._tag !== "Deleted") {
          return yield* Effect.fail(indexFailure(
            "delete_revision",
            "invalid_stored_state",
            "A deletion mutation resolved to a replacement outcome",
          ))
        }
        return begun.outcome.deletion
      }
      const outcome = yield* publishLease({
        lease: begun.lease,
        material: undefined,
        config,
        client,
        coordinator,
      })
      if (outcome._tag !== "Deleted") {
        return yield* Effect.fail(indexFailure(
          "delete_revision",
          "invalid_stored_state",
          "A deletion publication finalized as a replacement",
        ))
      }
      return outcome.deletion
    })

  return {
    loadRevisions: (keys) => coordinator.loadRevisions(keys).pipe(
      Effect.mapError((error) => coordinatorFailure("load_revisions", error)),
    ),
    replaceRevision,
    deleteRevision: (key) => deleteAttempt(key, 3),
    pruneGraph: (input) => Effect.gen(function*() {
      const keys = yield* coordinator.listStaleRevisions(input).pipe(
        Effect.mapError((error) => coordinatorFailure("prune_graph", error)),
      )
      const deletions = yield* Effect.forEach(
        keys,
        (key) => deleteAttempt(key, 3),
        { concurrency: 1 },
      )
      return sumProjectionPrune(deletions)
    }),
  }
})
