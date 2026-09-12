import type {
  AttributeSchema,
  DistanceMetric,
} from "@turbopuffer/turbopuffer"
import { Schema } from "effect"
import {
  EmbeddingDimensionsSchema,
  type EmbeddingDimensions,
} from "../../indexing/embedding-provider.js"
import { InvalidTurbopufferConfiguration } from "./errors.js"
import type { TurbopufferNamespaceIdentity } from "./identity.js"

/** Maximum dense-vector dimensionality accepted by Turbopuffer. */
export const TurbopufferMaximumVectorDimensions = 10_752

/** Maximum serialized request size accepted by one Turbopuffer write. */
export const TurbopufferMaximumWriteBytes = 512 * 1_024 * 1_024

/** Maximum rows returned by one Turbopuffer query. */
export const TurbopufferMaximumQueryRows = 10_000

/** Maximum serialized document size accepted by Turbopuffer. */
export const TurbopufferMaximumDocumentBytes = 64 * 1_024 * 1_024

/** Maximum serialized value size accepted by one Turbopuffer attribute. */
export const TurbopufferMaximumAttributeBytes = 8 * 1_024 * 1_024

/** Maximum serialized scalar size accepted by a filterable value. */
export const TurbopufferMaximumFilterableValueBytes = 4 * 1_024

/** Embedding dimensionality proven to fit Turbopuffer's dense-vector limit. */
export const TurbopufferVectorDimensionsSchema =
  EmbeddingDimensionsSchema.pipe(
    Schema.check(
      Schema.isLessThanOrEqualTo(TurbopufferMaximumVectorDimensions),
    ),
  )

/** Embedding dimensionality proven to fit Turbopuffer's dense-vector limit. */
export type TurbopufferVectorDimensions =
  typeof TurbopufferVectorDimensionsSchema.Type

/** Parse generic embedding dimensions at the Turbopuffer adapter boundary. */
export const parseTurbopufferVectorDimensions = (
  dimensions: EmbeddingDimensions,
): TurbopufferVectorDimensions => {
  try {
    return Schema.decodeSync(TurbopufferVectorDimensionsSchema)(dimensions)
  } catch {
    throw new InvalidTurbopufferConfiguration({
      field: "vector_dimensions",
      reason: "invalid_value",
    })
  }
}

/** Turbopuffer namespace name, validated against the provider wire contract. */
export const TurbopufferNamespaceSchema = Schema.String.pipe(
  Schema.check(Schema.isLengthBetween(1, 128)),
  Schema.check(Schema.isPattern(/^[A-Za-z0-9._-]+$/)),
  Schema.brand("TurbopufferNamespace"),
)

/** Turbopuffer namespace name, validated against the provider wire contract. */
export type TurbopufferNamespace = typeof TurbopufferNamespaceSchema.Type

/** Immutable schema generation included in every row and retrieval filter. */
export const TurbopufferSchemaGenerationSchema = Schema.Number.pipe(
  Schema.check(
    Schema.makeFilter(Number.isSafeInteger, {
      title: "SafeInteger",
    }),
  ),
  Schema.check(Schema.isGreaterThanOrEqualTo(1)),
  Schema.brand("TurbopufferSchemaGeneration"),
)

/** Immutable schema generation included in every row and retrieval filter. */
export type TurbopufferSchemaGeneration =
  typeof TurbopufferSchemaGenerationSchema.Type

/** Parse an externally assigned namespace without silently normalizing it. */
export const parseTurbopufferNamespace = (
  namespace: string,
): TurbopufferNamespace => {
  try {
    return Schema.decodeSync(TurbopufferNamespaceSchema)(namespace)
  } catch {
    throw new InvalidTurbopufferConfiguration({
      field: "namespace",
      reason: "invalid_value",
    })
  }
}

/** Parse the adapter's immutable namespace schema generation. */
export const parseTurbopufferSchemaGeneration = (
  generation: number,
): TurbopufferSchemaGeneration => {
  try {
    return Schema.decodeSync(TurbopufferSchemaGenerationSchema)(generation)
  } catch {
    throw new InvalidTurbopufferConfiguration({
      field: "schema_generation",
      reason: "invalid_value",
    })
  }
}

/** Build a readable namespace while retaining a collision-resistant identity. */
export const namespaceFromTurbopufferIdentity = (input: {
  readonly prefix?: string | undefined
  readonly identity: TurbopufferNamespaceIdentity
}): TurbopufferNamespace => {
  const prefix = input.prefix ?? "document-graph"
  const maximumPrefixLength = 128 - input.identity.length - 1
  const namespace = `${prefix.slice(0, maximumPrefixLength)}-${input.identity}`

  return parseTurbopufferNamespace(namespace)
}

/** Names of the six pinned full-text attributes in the provider manifest. */
export const TurbopufferFullTextAttributes = Object.freeze({
  english: Object.freeze({
    context: "fts_en_context",
    label: "fts_en_label",
    content: "fts_en_content",
  }),
  simple: Object.freeze({
    context: "fts_simple_context",
    label: "fts_simple_label",
    content: "fts_simple_content",
  }),
})

/** Attributes whose scalar values enter Turbopuffer's inverted index. */
export const TurbopufferFilterableAttributes = Object.freeze([
  "row_kind",
  "is_live",
  "partition_id",
  "graph_id",
  "document_kind",
  "projection_id",
  "projection_version",
  "embedding_profile_id",
  "embedding_profile_version",
  "schema_generation",
  "document_key",
  "publication_id",
  "publication_generation",
  "metadata_terms",
] as const)

const englishFullText = Object.freeze({
  type: "string",
  filterable: false,
  full_text_search: Object.freeze({
    language: "english" as const,
    tokenizer: "word_v4" as const,
  }),
}) satisfies AttributeSchema

const simpleFullText = Object.freeze({
  type: "string",
  filterable: false,
  full_text_search: Object.freeze({
    language: "english" as const,
    tokenizer: "word_v4" as const,
    remove_stopwords: false,
    stemming: false,
  }),
}) satisfies AttributeSchema

/** Provider schema manifest compiled from one embedding vector space. */
export interface TurbopufferSchemaManifest {
  readonly format: "honertia.document-graph/turbopuffer-schema-v1"
  readonly distanceMetric: DistanceMetric
  readonly attributes: Readonly<Record<string, AttributeSchema>>
}

/** Compile the exact schema expected by publication and retrieval adapters. */
export const compileTurbopufferSchemaManifest = (
  dimensions: EmbeddingDimensions,
): TurbopufferSchemaManifest => {
  const providerDimensions = parseTurbopufferVectorDimensions(dimensions)

  return {
    format: "honertia.document-graph/turbopuffer-schema-v1",
    distanceMetric: "cosine_distance",
    attributes: Object.freeze({
      vector: {
        type: `[${providerDimensions}]f32`,
        ann: { distance_metric: "cosine_distance" as const },
      },
      row_kind: { type: "string", filterable: true },
      is_live: { type: "bool", filterable: true },
      partition_id: { type: "string", filterable: true },
      graph_id: { type: "string", filterable: true },
      document_kind: { type: "string", filterable: true },
      projection_id: { type: "string", filterable: true },
      projection_version: { type: "string", filterable: true },
      embedding_profile_id: { type: "string", filterable: true },
      embedding_profile_version: { type: "string", filterable: true },
      schema_generation: { type: "uint", filterable: true },
      document_key: { type: "string", filterable: true },
      publication_id: { type: "string", filterable: true },
      publication_generation: { type: "uint", filterable: true },
      slot_ordinal: { type: "uint", filterable: false },
      live_slot_count: { type: "uint", filterable: false },
      slot_high_water: { type: "uint", filterable: false },
      revision_hash: { type: "string", filterable: false },
      chunk_id: { type: "string", filterable: false },
      content_hash: { type: "string", filterable: false },
      section_key: { type: "string", filterable: false },
      section_index: { type: "uint", filterable: false },
      section_part: { type: "uint", filterable: false },
      content: { type: "string", filterable: false },
      embedding_content: { type: "string", filterable: false },
      encoded_id_json: { type: "string", filterable: false },
      metadata_json: { type: "string", filterable: false },
      metadata_terms: { type: "[]string", filterable: true },
      fts_en_context: englishFullText,
      fts_en_label: englishFullText,
      fts_en_content: englishFullText,
      fts_simple_context: simpleFullText,
      fts_simple_label: simpleFullText,
      fts_simple_content: simpleFullText,
    }) satisfies Readonly<Record<string, AttributeSchema>>,
  }
}
