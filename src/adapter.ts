/**
 * Low-level contracts for authors of storage and retrieval adapters.
 *
 * Applications normally import from `@popcomputer/document-graph` instead.
 */
export * from "./index.js"

export {
  makeDocumentGraphStorage,
  type DocumentGraphStorageService,
} from "./storage/document-graph-storage.js"

export {
  EmbeddingProviderFailed,
  InvalidEmbeddingOutput,
  type EmbeddedContent,
} from "./indexing/embedding-provider.js"

export {
  countProjectedRevisionReplacement,
  embeddingProfilesEqual,
  indexProjectedRevision,
  IndexRevisionTokenSchema,
  isValidEmbeddingVector,
  planProjectedRevisionReplacement,
  ProjectionIndexConflict,
  ProjectionIndexStore,
  ProjectionIndexStoreFailed,
  type IndexedChunkSummary,
  type IndexedRevisionSnapshot,
  type IndexableProjectedRevision,
  type IndexProjectedRevisionError,
  type IndexProjectedRevisionResult,
  type IndexRevisionToken,
  type ProjectedChunkRecord,
  type ProjectionChunkEmbedding,
  type ProjectionIndexCommit,
  type ProjectionIndexDeletion,
  type ProjectionIndexKey,
  type ProjectionIndexPrune,
  type ProjectionRevisionLookup,
  type ProjectedRevisionReplacementIssue,
  type ProjectedRevisionReplacementPlan,
  type ProjectionIndexStoreService,
  type PruneGraphIndex,
  type RegisteredGraphProjection,
  type ReplaceProjectedRevision,
} from "./indexing/projection-index.js"

export {
  makeChunkId,
  makeDocumentKey,
} from "./document/document-identity.js"

export {
  GroundingHydrationFailed,
} from "./retrieval/grounding.js"

export {
  allDocuments,
  documentKeyMatchesGraphSearchTarget,
  documentKeys,
  GraphSearchTargetSchema,
  InvalidSearchOutput,
  InvalidSearchQuery,
  MAX_GRAPH_SEARCH_TARGET_DOCUMENT_KEYS,
  makeGraphSearchScope,
  noDocuments,
  ProjectionHybridSearchStore,
  ProjectionSearchStore,
  ProjectionSearchStoreFailed,
  projectionMatchesGraphSearchScope,
  ProjectionTextSearchStore,
  ProjectionTextSearchStoreFailed,
  SearchResultCountSchema,
  type AllDocuments,
  type CandidateFields,
  type DocumentKeys,
  type GraphSearchTarget,
  type GraphSearchScope,
  type GraphSearchScopeInput,
  type NoDocuments,
  type HybridCandidateRequest,
  type HybridCandidateResult,
  type ProjectionHybridSearchStoreService,
  type ProjectionSearchStoreService,
  type ProjectionTextSearchStoreService,
  type SearchResultCount,
  type SemanticCandidateRequest,
  type SemanticSearchCandidate,
  type TextCandidateRequest,
  type TextSearchCandidate,
} from "./retrieval/graph-retrieval.js"

export {
  InvalidRankFusionInput,
  weightedReciprocalRankFusion,
  type RankFusionItem,
  type RankFusionResult,
  type RankFusionSignal,
  type RankFusionStream,
} from "./retrieval/rank-fusion.js"

export {
  evaluateMetadataFilter,
  makeMetadataFilterBuilder,
  metadataAll,
  metadataAny,
  metadataEquals,
  metadataNot,
  metadataOneOf,
  normalizeMetadataFilter,
  normalizeMetadataFilters,
  type MetadataFilter,
  type MetadataFilterBuilder,
  type MetadataSearchPredicate,
  type MetadataSearchValue,
  type SearchableMetadataKey,
} from "./retrieval/metadata-filter.js"

export {
  countGraphRelationReplacement,
  makeGraphRelationEdgeIdentity,
  planOutgoingGraphRelationReplacement,
  type FindIncomingGraphNeighbours,
  type FindOutgoingGraphNeighbours,
  type OutgoingGraphRelationSet,
  type OutgoingGraphRelationTarget,
  type OutgoingGraphRelationReplacementIssue,
  type OutgoingGraphRelationReplacementPlan,
  type PlannedOutgoingGraphRelation,
  type RegisteredGraphRelation,
  type ReplaceOutgoingGraphRelations,
} from "./graph/graph-relation.js"

export {
  GraphNodePageLimitSchema,
  GraphNodeStateSchema,
  GraphTopologyStore,
  GraphTopologyStoreFailed,
  InvalidGraphTopologyOutput,
  type FindRelatedGraphNodes,
  type RelatedGraphNodeSet,
  type GraphNodePage,
  type GraphNodePageLimit,
  type GraphNodeState,
  type GraphTopologyDeletion,
  type GraphTopologyPrune,
  type GraphTopologyStoreService,
  type ListGraphNodes,
  type PruneGraphTopology,
  type ReplaceDocumentTopology,
  type StoredGraphNode,
} from "./graph/graph-topology.js"
