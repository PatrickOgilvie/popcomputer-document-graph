import { Effect, Schema } from "effect"
import {
  ChunkIdSchema,
  ContentHashSchema,
  DocumentKeySchema,
  ProjectionRevisionHashSchema,
  type ProjectionRevisionHash,
} from "../../document/document-identity.js"
import { JsonValueSchema, type JsonValue } from "../../document/json-value.js"
import type { EncodedDocumentReference } from "../../document/document-instance.js"
import type { TextSearchPolicy } from "../../document/text-search-policy.js"
import {
  EmbeddingProfileIdSchema,
  EmbeddingProfileVersionSchema,
  type EmbeddingDimensions,
} from "../../indexing/embedding-provider.js"
import {
  ProjectionPublicationGenerationSchema,
  ProjectionPublicationIdSchema,
  type ProjectionPublicationGeneration,
  type ProjectionPublicationId,
} from "../../indexing/projection-publication.js"
import type {
  ProjectedChunkRecord,
  ProjectionIndexKey,
} from "../../indexing/projection-index.js"
import { TurbopufferSchemaGenerationSchema } from "./config.js"
import { InvalidTurbopufferResponse } from "./errors.js"
import {
  canonicalTurbopufferJson,
  makeTurbopufferMarkerRowId,
  makeTurbopufferSlotRowId,
  TurbopufferNamespaceIdentitySchema,
  TurbopufferPhysicalRowIdSchema,
  type TurbopufferProjectionAddress,
} from "./identity.js"
import { encodeTurbopufferMetadataTerms } from "./metadata-terms.js"
import type { TurbopufferWorkspacePartition } from "./partition.js"

const NonNegativeIntegerSchema = Schema.Number.pipe(
  Schema.check(Schema.isInt()),
  Schema.check(Schema.isGreaterThanOrEqualTo(0)),
)

const NonEmptyFiniteVectorSchema = Schema.NonEmptyArray(Schema.Finite)

/** Discriminator retained on every physical provider row. */
export const TurbopufferRowKindSchema = Schema.Literals([
  "marker",
  "slot",
])

/** Discriminator retained on every physical provider row. */
export type TurbopufferRowKind = typeof TurbopufferRowKindSchema.Type

const PublicationIdentityFields = {
  id: TurbopufferPhysicalRowIdSchema,
  vector: NonEmptyFiniteVectorSchema,
  partition_id: TurbopufferNamespaceIdentitySchema,
  graph_id: Schema.String,
  document_kind: Schema.String,
  projection_id: Schema.String,
  projection_version: Schema.String,
  embedding_profile_id: EmbeddingProfileIdSchema,
  embedding_profile_version: EmbeddingProfileVersionSchema,
  schema_generation: TurbopufferSchemaGenerationSchema,
  document_key: DocumentKeySchema,
  publication_id: ProjectionPublicationIdSchema,
  publication_generation: ProjectionPublicationGenerationSchema,
}

/** One publication fence row for a logical document projection. */
export const TurbopufferMarkerRowSchema = Schema.Struct({
  ...PublicationIdentityFields,
  row_kind: Schema.Literal("marker"),
  is_live: Schema.Literal(false),
  live_slot_count: NonNegativeIntegerSchema,
  slot_high_water: NonNegativeIntegerSchema,
})

/** One stable slot cleared by a complete physical publication. */
export const TurbopufferTombstoneRowSchema = Schema.Struct({
  ...PublicationIdentityFields,
  row_kind: Schema.Literal("slot"),
  is_live: Schema.Literal(false),
  slot_ordinal: NonNegativeIntegerSchema,
})

const FullTextFields = {
  fts_en_context: Schema.optional(Schema.String),
  fts_en_label: Schema.optional(Schema.String),
  fts_en_content: Schema.optional(Schema.String),
  fts_simple_context: Schema.optional(Schema.String),
  fts_simple_label: Schema.optional(Schema.String),
  fts_simple_content: Schema.optional(Schema.String),
}

/** One live retrieval slot carrying plaintext, attribution, filters, and vector. */
export const TurbopufferLiveSlotRowSchema = Schema.Struct({
  ...PublicationIdentityFields,
  ...FullTextFields,
  row_kind: Schema.Literal("slot"),
  is_live: Schema.Literal(true),
  slot_ordinal: NonNegativeIntegerSchema,
  revision_hash: ProjectionRevisionHashSchema,
  chunk_id: ChunkIdSchema,
  content_hash: ContentHashSchema,
  section_key: Schema.String,
  section_index: NonNegativeIntegerSchema,
  section_part: NonNegativeIntegerSchema,
  content: Schema.String,
  embedding_content: Schema.String,
  encoded_id_json: Schema.String,
  metadata_json: Schema.optional(Schema.String),
  metadata_terms: Schema.Array(Schema.String),
})

/** Any complete physical row accepted by a Turbopuffer publication write. */
export const TurbopufferPublicationRowSchema = Schema.Union([
  TurbopufferMarkerRowSchema,
  TurbopufferTombstoneRowSchema,
  TurbopufferLiveSlotRowSchema,
])

/** One publication fence row for a logical document projection. */
export type TurbopufferMarkerRow = typeof TurbopufferMarkerRowSchema.Type

/** One stable slot cleared by a complete physical publication. */
export type TurbopufferTombstoneRow =
  typeof TurbopufferTombstoneRowSchema.Type

/** One live retrieval slot carrying plaintext, attribution, filters, and vector. */
export type TurbopufferLiveSlotRow =
  typeof TurbopufferLiveSlotRowSchema.Type

/** Any complete physical row accepted by a Turbopuffer publication write. */
export type TurbopufferPublicationRow =
  typeof TurbopufferPublicationRowSchema.Type

/** Fields shared by all rows in one atomic publication. */
export interface TurbopufferPublicationRowContext {
  readonly graph: string
  readonly documentKind: string
  readonly key: ProjectionIndexKey
  readonly projectionVersion: string
  readonly partition: TurbopufferWorkspacePartition
  readonly publicationId: ProjectionPublicationId
  readonly publicationGeneration: ProjectionPublicationGeneration
}

const projectionAddress = (
  context: TurbopufferPublicationRowContext,
): TurbopufferProjectionAddress => ({
  partitionIdentity: context.partition.identity,
  documentKey: context.key.documentKey,
  projection: context.key.projection,
})

const validateVector = (
  vector: ReadonlyArray<number>,
  dimensions: EmbeddingDimensions,
): readonly [number, ...Array<number>] => {
  if (
    vector.length !== dimensions ||
    vector.some((component) => !Number.isFinite(component))
  ) {
    throw new Error(
      "Turbopuffer row vectors must match the embedding profile dimensions",
    )
  }
  const [first, ...rest] = vector
  if (first === undefined) {
    throw new Error("Turbopuffer row vectors cannot be empty")
  }
  return [first, ...rest]
}

/** Produce the deterministic non-zero vector used by markers and tombstones. */
export const makeTurbopufferDummyVector = (
  dimensions: EmbeddingDimensions,
): readonly [number, ...Array<number>] => {
  const vector = Array.from({ length: dimensions }, () => 0)
  vector[0] = 1
  const [first, ...rest] = vector
  if (first === undefined) {
    throw new Error("Embedding dimensions must be positive")
  }
  return [first, ...rest]
}

const publicationFields = (
  context: TurbopufferPublicationRowContext,
) => ({
  partition_id: context.partition.identity,
  graph_id: context.graph,
  document_kind: context.documentKind,
  projection_id: context.key.projection,
  projection_version: context.projectionVersion,
  embedding_profile_id: context.partition.embeddingProfile.id,
  embedding_profile_version: context.partition.embeddingProfile.version,
  schema_generation: context.partition.schemaGeneration,
  document_key: context.key.documentKey,
  publication_id: context.publicationId,
  publication_generation: context.publicationGeneration,
})

/** Encode the marker that identifies a complete publication generation. */
export const makeTurbopufferMarkerRow = (input: {
  readonly context: TurbopufferPublicationRowContext
  readonly liveSlotCount: number
  readonly slotHighWater: number
}): TurbopufferMarkerRow =>
  Schema.decodeUnknownSync(TurbopufferMarkerRowSchema)(
    {
      id: makeTurbopufferMarkerRowId(projectionAddress(input.context)),
      vector: makeTurbopufferDummyVector(
        input.context.partition.embeddingProfile.dimensions,
      ),
      ...publicationFields(input.context),
      row_kind: "marker",
      is_live: false,
      live_slot_count: input.liveSlotCount,
      slot_high_water: input.slotHighWater,
    },
    { onExcessProperty: "error" },
  )

/** Encode a cleared stable slot for one complete physical publication. */
export const makeTurbopufferTombstoneRow = (input: {
  readonly context: TurbopufferPublicationRowContext
  readonly slotOrdinal: number
}): TurbopufferTombstoneRow =>
  Schema.decodeUnknownSync(TurbopufferTombstoneRowSchema)(
    {
      id: makeTurbopufferSlotRowId(
        projectionAddress(input.context),
        input.slotOrdinal,
      ),
      vector: makeTurbopufferDummyVector(
        input.context.partition.embeddingProfile.dimensions,
      ),
      ...publicationFields(input.context),
      row_kind: "slot",
      is_live: false,
      slot_ordinal: input.slotOrdinal,
    },
    { onExcessProperty: "error" },
  )

interface TurbopufferFullTextRowFields {
  fts_en_context?: string
  fts_en_label?: string
  fts_en_content?: string
  fts_simple_context?: string
  fts_simple_label?: string
  fts_simple_content?: string
}

const encodeFullText = (
  policy: TextSearchPolicy,
  text: ProjectedChunkRecord["text"],
): TurbopufferFullTextRowFields => {
  if (policy === "disabled") return {}

  if (policy.language === "english") {
    const fields: TurbopufferFullTextRowFields = {
      fts_en_content: text.content,
    }
    if (text.context !== undefined) fields.fts_en_context = text.context
    if (text.label !== undefined) fields.fts_en_label = text.label
    return fields
  }

  const fields: TurbopufferFullTextRowFields = {
    fts_simple_content: text.content,
  }
  if (text.context !== undefined) fields.fts_simple_context = text.context
  if (text.label !== undefined) fields.fts_simple_label = text.label
  return fields
}

/** Encode one live stable slot with only its selected FTS language populated. */
export const makeTurbopufferLiveSlotRow = (input: {
  readonly context: TurbopufferPublicationRowContext
  readonly encodedTarget: EncodedDocumentReference
  readonly revisionHash: ProjectionRevisionHash
  readonly textPolicy: TextSearchPolicy
  readonly slotOrdinal: number
  readonly chunk: ProjectedChunkRecord
  readonly vector: ReadonlyArray<number>
}): TurbopufferLiveSlotRow => {
  if (
    input.encodedTarget.graph !== input.context.graph ||
    input.encodedTarget.kind !== input.context.documentKind
  ) {
    throw new Error(
      "Turbopuffer row context must match the encoded document target",
    )
  }

  const metadataJson =
    input.chunk.metadata === undefined
      ? undefined
      : canonicalTurbopufferJson(input.chunk.metadata)

  return Schema.decodeUnknownSync(TurbopufferLiveSlotRowSchema)(
    {
      id: makeTurbopufferSlotRowId(
        projectionAddress(input.context),
        input.slotOrdinal,
      ),
      vector: validateVector(
        input.vector,
        input.context.partition.embeddingProfile.dimensions,
      ),
      ...publicationFields(input.context),
      ...encodeFullText(input.textPolicy, input.chunk.text),
      row_kind: "slot",
      is_live: true,
      slot_ordinal: input.slotOrdinal,
      revision_hash: input.revisionHash,
      chunk_id: input.chunk.chunkId,
      content_hash: input.chunk.contentHash,
      section_key: input.chunk.sectionKey,
      section_index: input.chunk.sectionIndex,
      section_part: input.chunk.sectionPart,
      content: input.chunk.content,
      embedding_content: input.chunk.embeddingContent,
      encoded_id_json: canonicalTurbopufferJson(input.encodedTarget.id),
      metadata_json: metadataJson,
      metadata_terms: encodeTurbopufferMetadataTerms(input.chunk.metadata),
    },
    { onExcessProperty: "error" },
  )
}

/** Exact attributes requested for each retrieval candidate. */
export const TurbopufferSearchResultAttributes = Object.freeze([
  "row_kind",
  "is_live",
  "partition_id",
  "graph_id",
  "document_kind",
  "projection_id",
  "projection_version",
  "document_key",
  "revision_hash",
  "chunk_id",
  "content_hash",
  "section_key",
  "section_part",
  "content",
  "encoded_id_json",
  "metadata_json",
])

/** Runtime codec for one row returned by a semantic or lexical query. */
export const TurbopufferSearchResultRowSchema = Schema.Struct({
  id: TurbopufferPhysicalRowIdSchema,
  $dist: Schema.Finite,
  row_kind: Schema.Literal("slot"),
  is_live: Schema.Literal(true),
  partition_id: TurbopufferNamespaceIdentitySchema,
  graph_id: Schema.String,
  document_kind: Schema.String,
  projection_id: Schema.String,
  projection_version: Schema.String,
  document_key: DocumentKeySchema,
  revision_hash: ProjectionRevisionHashSchema,
  chunk_id: ChunkIdSchema,
  content_hash: ContentHashSchema,
  section_key: Schema.String,
  section_part: NonNegativeIntegerSchema,
  content: Schema.String,
  encoded_id_json: Schema.String,
  metadata_json: Schema.optional(Schema.String),
})

/** Decoded provider row ready to become a storage-neutral search candidate. */
export interface DecodedTurbopufferSearchResultRow {
  readonly providerScore: number
  readonly partitionIdentity: typeof TurbopufferNamespaceIdentitySchema.Type
  readonly chunkId: typeof ChunkIdSchema.Type
  readonly contentHash: typeof ContentHashSchema.Type
  readonly documentKey: typeof DocumentKeySchema.Type
  readonly reference: EncodedDocumentReference
  readonly projection: {
    readonly id: string
    readonly version: string
  }
  readonly revisionHash: typeof ProjectionRevisionHashSchema.Type
  readonly sectionKey: string
  readonly sectionPart: number
  readonly content: string
  readonly metadata: JsonValue | undefined
}

const invalidRow = (
  cause: unknown,
  rowIndex: number | undefined,
): InvalidTurbopufferResponse => {
  if (rowIndex === undefined) {
    return new InvalidTurbopufferResponse({
      operation: "query",
      reason: "invalid_row",
      cause,
    })
  }
  return new InvalidTurbopufferResponse({
    operation: "query",
    reason: "invalid_row",
    rowIndex,
    cause,
  })
}

const decodeJson = (
  encoded: string,
  rowIndex: number | undefined,
): Effect.Effect<JsonValue, InvalidTurbopufferResponse> =>
  Effect.try({
    try: () =>
      Schema.decodeUnknownSync(JsonValueSchema)(JSON.parse(encoded), {
        onExcessProperty: "error",
      }),
    catch: (cause) => invalidRow(cause, rowIndex),
  })

/** Decode one provider result row and reject malformed attributed content. */
export const decodeTurbopufferSearchResultRow = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- This provider response boundary immediately decodes the value with TurbopufferSearchResultRowSchema.
  row: unknown,
  rowIndex?: number,
): Effect.Effect<
  DecodedTurbopufferSearchResultRow,
  InvalidTurbopufferResponse
> =>
  Schema.decodeUnknownEffect(TurbopufferSearchResultRowSchema)(row, {
    onExcessProperty: "error",
  }).pipe(
    Effect.mapError((cause) => invalidRow(cause, rowIndex)),
    Effect.flatMap((decoded) =>
      Effect.all({
        encodedId: decodeJson(decoded.encoded_id_json, rowIndex),
        metadata:
          decoded.metadata_json === undefined
            ? Effect.succeed(undefined)
            : decodeJson(decoded.metadata_json, rowIndex),
      }).pipe(
        Effect.map(({ encodedId, metadata }) => ({
          providerScore: decoded.$dist,
          partitionIdentity: decoded.partition_id,
          chunkId: decoded.chunk_id,
          contentHash: decoded.content_hash,
          documentKey: decoded.document_key,
          reference: {
            graph: decoded.graph_id,
            kind: decoded.document_kind,
            id: encodedId,
          },
          projection: {
            id: decoded.projection_id,
            version: decoded.projection_version,
          },
          revisionHash: decoded.revision_hash,
          sectionKey: decoded.section_key,
          sectionPart: decoded.section_part,
          content: decoded.content,
          metadata,
        })),
      ),
    ),
  )

/** Convert Turbopuffer cosine distance into a higher-is-better score. */
export const scoreTurbopufferCosineDistance = (distance: number): number =>
  1 - distance

/** Preserve Turbopuffer's already higher-is-better BM25 score. */
export const scoreTurbopufferBm25 = (score: number): number => score
