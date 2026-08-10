export {
  ChunkerIdSchema,
  ChunkerVersionSchema,
  ChunkMaximumCharactersSchema,
  defineChunker,
  sectionChunking,
  type Chunker,
  type ChunkerInput,
  type ChunkerSection,
  type ChunkFragment,
  type ChunkMaximumCharacters,
  type ChunkingInput,
  type ChunkingStrategy,
  type ChunkingStrategyDescriptor,
  type ChunkingStrategyShape,
  type DefineChunkerInput,
  type SectionChunkingInput,
  type SectionChunkingStrategy,
} from "./document/chunking-strategy.js"

export {
  JsonValueSchema,
  type JsonValue,
} from "./document/json-value.js"

export {
  defineDocument,
  type DefineDocumentInput,
  type DocumentDefinition,
  type DocumentDefinitionShape,
} from "./document/document-definition.js"

export {
  defineDocumentGraph,
  type DefineDocumentGraphInput,
  type AnyGraphSearchHit,
  type DocumentGraph,
  type DocumentGraphSearchOptions,
  type DefineGraphRetrievalInput,
  type DirectRetrievalRoute,
  type GraphDocumentHandle,
  type GraphProjectionHandle,
  type GraphRetrievalHandle,
  type GraphRetrievalResult,
  type GraphSearchHit,
  type IndexGraphDocumentError,
  type IndexedGraphProjectionResult,
  type IndexGraphDocumentResult,
  type ProjectionSearchOptions,
  type ProjectionSearchWhere,
  type ProjectionSearchWhereShorthand,
  type LegalRetrievalRoute,
  type RelationRetrievalRoute,
  type RetrievalEvidence,
  type RetrievalRouteOptions,
  type HybridProjectionSearchOptions,
  type HybridRetrievalStrategyInput,
  type RetrievalCandidateBudgets,
  type SemanticProjectionSearchOptions,
  type TextProjectionSearchOptions,
  type RemoveGraphDocumentResult,
  type ReconcileDocumentGraphResult,
  type SchemaSearchOptions,
  type SearchDocumentGraphError,
} from "./graph/document-graph.js"

export {
  InvalidDocumentGraphDefinition,
  InvalidDocumentReference,
  InvalidGraphRelationDefinition,
  InvalidGraphTraversal,
} from "./graph/document-graph-errors.js"

export type { DocumentGraphManifest } from "./graph/document-graph-schema.js"

export type { DocumentReference } from "./document/document-reference.js"

export type {
  MetadataFilter,
  MetadataFilterBuilder,
  MetadataSearchPredicate,
  MetadataSearchValue,
  SearchableMetadataKey,
} from "./retrieval/metadata-filter.js"

export {
  DocumentGraphOperationSchema,
  DocumentGraphUnavailable,
  DocumentGraphUnavailableReasonSchema,
  toDocumentGraphErrorTelemetry,
  type DocumentGraphErrorTelemetry,
  type DocumentGraphOperation,
  type DocumentGraphUnavailableReason,
} from "./graph/document-graph-operation.js"

export type { DocumentGraphServices } from "./graph/document-graph-services.js"

export {
  GraphNeighbourLimitSchema,
  GraphRelationIdSchema,
  GraphRelationVersionSchema,
  InvalidGraphRelationOutput,
  type DefineGraphRelation,
  type GraphNeighbourLimit,
  type GraphRelationCommit,
  type GraphRelationDefinition,
} from "./graph/graph-relation.js"

export {
  DocumentChunkingFailed,
  InvalidVectorProjectionOutput,
  type DocumentProjectionId,
  type ProjectDocumentError,
  type ProjectedChunk,
  type ProjectedRevision,
} from "./document/document-projection.js"

export {
  InvalidDocumentValue,
  type EncodedDocumentReference,
} from "./document/document-instance.js"

export {
  ChunkIdSchema,
  ContentHashSchema,
  DocumentKeySchema,
  InvalidDocumentIdentity,
  ProjectionRevisionHashSchema,
  Sha256HexSchema,
  type ChunkId,
  type ContentHash,
  type DocumentKey,
  type ProjectionRevisionHash,
} from "./document/document-identity.js"

export {
  defineEmbeddingProfile,
  EmbeddingDimensionsSchema,
  EmbeddingProfileIdSchema,
  EmbeddingProfileVersionSchema,
  EmbeddingProvider,
  type EmbeddingDimensions,
  type EmbeddingProfile,
  type EmbeddingProfileId,
  type EmbeddingProfileVersion,
  type EmbeddingProviderService,
  type EmbeddingRequest,
} from "./indexing/embedding-provider.js"

export { ProjectionIndexConflict } from "./indexing/projection-index.js"

export {
  GroundingHydrator,
  hydrateGrounding,
  type GroundingHydrationRequest,
  type GroundingHydratorService,
  type GroundingLevel,
  type GroundingMaterial,
  type GroundingPayload,
} from "./retrieval/grounding.js"

export {
  InvalidSearchQuery,
  ProjectionTextSearchStore,
  type SearchHit,
  type SearchSignal,
  type RetrievalChannel,
  type RetrievalRouteKey,
  type RetrievalStreamKey,
  type RelationRetrievalRouteKey,
} from "./retrieval/graph-retrieval.js"

export {
  ReciprocalRankConstantSchema,
  RetrievalWeightSchema,
  type ReciprocalRankConstant,
  type RetrievalWeight,
} from "./retrieval/rank-fusion.js"

export {
  RetrievalResultLimitSchema,
  type RetrievalResultLimit,
  type RetrievalStrategyInput,
} from "./retrieval/retrieval-strategy.js"

export {
  VectorProjectionIdSchema,
  VectorProjectionVersionSchema,
  type DefineVectorProjectionInput,
  type ProjectedDocument,
  type ProjectedSection,
  type VectorProjection,
  type VectorProjectionShape,
} from "./document/vector-projection.js"

export {
  TextSearchFieldSchema,
  TextSearchLanguageSchema,
  TextSearchWeightSchema,
  type ProjectedText,
  type TextSearchField,
  type TextSearchLanguage,
  type TextSearchPolicy,
  type TextSearchPolicyInput,
  type TextSearchWeight,
} from "./document/text-search-policy.js"
