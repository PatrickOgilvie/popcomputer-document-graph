import { Array as EffectArray, Effect, Schema } from "effect"
import type { DocumentReference } from "../document/document-reference.js"
import type {
  DocumentDefinitionShape,
  DocumentDefinitions,
  DocumentId,
  DocumentKind,
  DocumentValue,
} from "../document/document-definition.js"
import {
  projectDocument,
  projectParsedDocument,
  type DocumentProjectionId,
  type ProjectDocumentError,
  type ProjectedRevision,
} from "../document/document-projection.js"
import {
  encodeDocumentId,
  makeDocumentKey,
  type DocumentKey,
  type InvalidDocumentIdentity,
} from "../document/document-identity.js"
import { parseDocumentInstance } from "../document/document-instance.js"
import {
  makeGraphSearchScope,
  InvalidSearchOutput,
  InvalidSearchQuery,
  prepareSemanticQuery,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
  searchGraph,
  searchGraphHybrid,
  searchGraphHybridWithSemanticQuery,
  searchGraphText,
  searchGraphWithPreparedSemanticQuery,
  SearchResultCountSchema,
  semantic,
  text,
  type GraphSearchScope,
  type GraphSearchScopeInput,
  type SemanticQueryPreparation,
  type SearchGraphError,
  type SearchHit,
  type RetrievalRouteKey,
  type RetrievalStreamKey,
  type SearchResultCount,
  type SearchSignal,
} from "../retrieval/graph-retrieval.js"
import {
  makeMetadataFilterBuilder,
  metadataEquals,
  metadataOneOf,
  normalizeMetadataFilter,
  type MetadataFilter,
  type MetadataFilterBuilder,
  type MetadataSearchValue,
  type SearchableMetadataKey,
} from "../retrieval/metadata-filter.js"
import {
  parseRetrievalStrategy,
  RetrievalResultLimitSchema,
  type RetrievalStrategy,
  type RetrievalStrategyInput,
} from "../retrieval/retrieval-strategy.js"
import {
  ReciprocalRankConstantSchema,
  RetrievalWeightSchema,
  weightedReciprocalRankFusion,
  type RetrievalWeight,
} from "../retrieval/rank-fusion.js"
import {
  indexProjectedRevision,
  type ProjectionIndexConflict,
  ProjectionIndexStore,
  type IndexProjectedRevisionError,
  type IndexProjectedRevisionResult,
  type ProjectionIndexStoreFailed,
} from "../indexing/projection-index.js"
import {
  EmbeddingProvider,
  type EmbeddingProviderFailed,
  type InvalidEmbeddingOutput,
} from "../indexing/embedding-provider.js"
import {
  documentGraphUnavailable,
  traceDocumentGraphOperation,
  type DocumentGraphOperation,
  type DocumentGraphSpanAttributes,
  type DocumentGraphUnavailable,
} from "./document-graph-operation.js"
import {
  GraphNeighbourLimitSchema,
  InvalidGraphNeighbourOutput,
  InvalidGraphRelationOutput,
  projectOutgoingGraphRelationsForInstance,
  GraphRelationStore,
  type GraphRelationCommit,
  type GraphNeighbourLimit,
  type DefineGraphRelation,
  type GraphRelationDefinitions,
  type GraphRelationStoreFailed,
} from "./graph-relation.js"
import type {
  VectorProjection,
  VectorProjectionShape,
} from "../document/vector-projection.js"
import {
  InvalidDocumentReference,
  InvalidGraphTraversal,
} from "./document-graph-errors.js"
import {
  assertUniqueProjectionIds,
  assertValidGraphRelations,
  makeDocumentGraphManifest,
  registeredGraphProjections,
  registeredGraphRelations,
  type DocumentGraphManifest,
} from "./document-graph-schema.js"

type OutgoingRelationId<
  Relations extends GraphRelationDefinitions,
  Kind extends string,
> = {
  [RelationId in keyof Relations & string]: Relations[RelationId] extends {
    readonly from: Kind
  }
    ? RelationId
    : never
}[keyof Relations & string]

type IncomingRelationId<
  Relations extends GraphRelationDefinitions,
  Kind extends string,
> = {
  [RelationId in keyof Relations & string]: Relations[RelationId] extends {
    readonly to: Kind
  }
    ? RelationId
    : never
}[keyof Relations & string]

type RelationTargetKind<
  Relations extends GraphRelationDefinitions,
  RelationId extends keyof Relations & string,
> = Relations[RelationId] extends { readonly to: infer To extends string }
  ? To
  : never

type RelationSourceKind<
  Relations extends GraphRelationDefinitions,
  RelationId extends keyof Relations & string,
> = Relations[RelationId] extends {
  readonly from: infer From extends string
}
  ? From
  : never

type GraphNeighbours<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Relations extends GraphRelationDefinitions,
  Kind extends DocumentKind<Documents>,
> = {
  <RelationId extends OutgoingRelationId<Relations, Kind>>(
    id: DocumentId<Documents, Kind>,
    options: {
      readonly via: RelationId
      readonly direction?: "outgoing"
      /** Defaults to 100 and is bounded to 1,000. */
      readonly limit?: number
    },
  ): Effect.Effect<
    ReadonlyArray<
      DocumentReference<
        GraphId,
        Documents,
        Extract<
          RelationTargetKind<Relations, RelationId>,
          DocumentKind<Documents>
        >
      >
    >,
    | InvalidDocumentIdentity
    | InvalidGraphTraversal
    | DocumentGraphUnavailable,
    GraphRelationStore
  >

  <RelationId extends IncomingRelationId<Relations, Kind>>(
    id: DocumentId<Documents, Kind>,
    options: {
      readonly via: RelationId
      readonly direction: "incoming"
      /** Defaults to 100 and is bounded to 1,000. */
      readonly limit?: number
    },
  ): Effect.Effect<
    ReadonlyArray<
      DocumentReference<
        GraphId,
        Documents,
        Extract<
          RelationSourceKind<Relations, RelationId>,
          DocumentKind<Documents>
        >
      >
    >,
    | InvalidDocumentIdentity
    | InvalidGraphTraversal
    | DocumentGraphUnavailable,
    GraphRelationStore
  >
}

type ProjectionFor<
  Documents extends DocumentDefinitions,
  Kind extends DocumentKind<Documents>,
  ProjectionId extends DocumentProjectionId<Documents, Kind>,
> = Extract<
  Documents[Kind]["projections"][number],
  { readonly id: ProjectionId }
>

type ProjectionMetadata<Projection> =
  Projection extends VectorProjection<
    infer _Value,
    infer Metadata,
    infer _Id,
    infer _Version
  >
    ? Metadata
    : never

type GraphProjectionId<Documents extends DocumentDefinitions> = {
  [Kind in DocumentKind<Documents>]: DocumentProjectionId<Documents, Kind>
}[DocumentKind<Documents>]

// Four concurrent routes with four neighbour reads each cap adapter-neutral
// relation-store pressure at sixteen operations per retrieval.
const GraphRetrievalRouteConcurrency = 4
const GraphNeighbourExpansionConcurrencyPerRoute = 4

/** Optional graph-owned constraints for one semantic search. */
export interface DocumentGraphSearchOptions<
  Documents extends DocumentDefinitions,
> extends SchemaSearchOptions {
  readonly include?: ReadonlyArray<DocumentKind<Documents>>
  readonly exclude?: ReadonlyArray<DocumentKind<Documents>>
  readonly includeProjections?: ReadonlyArray<GraphProjectionId<Documents>>
  readonly excludeProjections?: ReadonlyArray<GraphProjectionId<Documents>>
}

/** Simple result and candidate limits for schema-bound semantic search. */
export interface SchemaSearchOptions {
  /** Defaults to 50 and is always at least the requested result limit. */
  readonly candidates?: number
  /** Defaults to 10. */
  readonly limit?: number
}

type SearchableMetadataValue<Value> =
  Extract<Value, MetadataSearchValue>

/** Typed implicit-AND shorthand for projection metadata filters. */
export type ProjectionSearchWhereShorthand<Metadata> = {
  readonly [Key in SearchableMetadataKey<Metadata>]?:
    | SearchableMetadataValue<Metadata[Key]>
    | {
        readonly oneOf: readonly [
          SearchableMetadataValue<Metadata[Key]>,
          ...ReadonlyArray<SearchableMetadataValue<Metadata[Key]>>,
        ]
      }
}

/** Typed shorthand or composable Boolean filter for projection metadata. */
export type ProjectionSearchWhere<Metadata> =
  | ProjectionSearchWhereShorthand<Metadata>
  | ((builder: MetadataFilterBuilder<Metadata>) => MetadataFilter)

/** Independent candidate budgets for projection retrieval channels. */
export interface RetrievalCandidateBudgets {
  readonly semantic?: number
  readonly text?: number
}

type ProjectionSearchBase<Metadata> = {
  readonly candidates?: RetrievalCandidateBudgets
  readonly limit?: number
} &
  ([Metadata] extends [never]
    ? { readonly where?: never }
    : { readonly where?: ProjectionSearchWhere<Metadata> })

/** Plain configurable hybrid strategy accepted by projection search. */
export type HybridRetrievalStrategyInput = Extract<
  RetrievalStrategyInput,
  { readonly mode: "hybrid" }
>

/** Projection search options selecting the semantic/vector capability. */
export type SemanticProjectionSearchOptions<Metadata> =
  ProjectionSearchBase<Metadata> & {
    readonly strategy: "semantic"
  }

/** Projection search options selecting the text-search capability. */
export type TextProjectionSearchOptions<Metadata> =
  ProjectionSearchBase<Metadata> & {
    readonly strategy: "text"
  }

/** Projection search options selecting package-owned hybrid fusion. */
export type HybridProjectionSearchOptions<Metadata> =
  ProjectionSearchBase<Metadata> & {
    readonly strategy?: "hybrid" | HybridRetrievalStrategyInput
  }

/** Search options inferred from one projection's metadata schema. */
export type ProjectionSearchOptions<Metadata> =
  | SemanticProjectionSearchOptions<Metadata>
  | TextProjectionSearchOptions<Metadata>
  | HybridProjectionSearchOptions<Metadata>

interface GraphSearchHitFields<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Kind extends DocumentKind<Documents>,
  ProjectionId extends DocumentProjectionId<Documents, Kind>,
> extends Omit<SearchHit, "reference" | "projection" | "metadata"> {
  readonly reference: DocumentReference<GraphId, Documents, Kind>
  readonly projection: {
    readonly id: ProjectionId
    readonly version: ProjectionFor<
      Documents,
      Kind,
      ProjectionId
    >["version"]
  }
}

/** Search hit parsed through its document and metadata schemas. */
export type GraphSearchHit<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Kind extends DocumentKind<Documents>,
  ProjectionId extends DocumentProjectionId<Documents, Kind>,
> = GraphSearchHitFields<GraphId, Documents, Kind, ProjectionId> &
  ([ProjectionMetadata<
    ProjectionFor<Documents, Kind, ProjectionId>
  >] extends [never]
    ? { readonly metadata: undefined }
    : {
        readonly metadata: ProjectionMetadata<
          ProjectionFor<Documents, Kind, ProjectionId>
        >
      })

/** Union of every legal search hit declared by a graph schema. */
export type AnyGraphSearchHit<
  GraphId extends string,
  Documents extends DocumentDefinitions,
> = {
  [Kind in DocumentKind<Documents>]: {
    [ProjectionId in DocumentProjectionId<Documents, Kind>]: GraphSearchHit<
      GraphId,
      Documents,
      Kind,
      ProjectionId
    >
  }[DocumentProjectionId<Documents, Kind>]
}[DocumentKind<Documents>]

/** Optional weight and traversal bound for a retrieval route. */
export interface RetrievalRouteOptions {
  readonly weight?: number
  readonly neighboursPerSource?: number
}

/** Search a projection owned by the requested target document kind. */
export interface DirectRetrievalRoute<
  SourceKind extends string = string,
  ProjectionId extends string = string,
> {
  readonly _tag: "Direct"
  readonly sourceKind: SourceKind
  readonly projection: ProjectionId
  readonly weight: RetrievalWeight
}

/** Search source evidence and traverse one outgoing graph relation. */
export interface RelationRetrievalRoute<
  SourceKind extends string = string,
  TargetKind extends string = string,
  ProjectionId extends string = string,
  RelationId extends string = string,
> {
  readonly _tag: "Relation"
  readonly sourceKind: SourceKind
  readonly targetKind: TargetKind
  readonly projection: ProjectionId
  readonly relation: RelationId
  readonly direction: "outgoing"
  readonly weight: RetrievalWeight
  readonly neighboursPerSource: GraphNeighbourLimit
}

/** One source chunk retained to explain a target retrieval result. */
export interface RetrievalEvidence<SourceHit = SearchHit> {
  readonly route: RetrievalRouteKey
  readonly source: SourceHit
  readonly streams: readonly [
    RetrievalStreamKey,
    ...ReadonlyArray<RetrievalStreamKey>,
  ]
}

/** One graph target ranked from direct or related source evidence. */
export interface GraphRetrievalResult<TargetReference, SourceHit = SearchHit> {
  readonly rank: number
  readonly score: number
  readonly target: TargetReference
  readonly signals: readonly [SearchSignal, ...ReadonlyArray<SearchSignal>]
  readonly evidence: readonly [
    RetrievalEvidence<SourceHit>,
    ...ReadonlyArray<RetrievalEvidence<SourceHit>>,
  ]
}

/** One projection result included in a document-wide index operation. */
export interface IndexedGraphProjectionResult<ProjectionId extends string> {
  readonly projection: ProjectionId
  readonly result: IndexProjectedRevisionResult
}

/** Results from indexing every projection registered by one document type. */
export interface IndexGraphDocumentResult<ProjectionId extends string> {
  readonly projections: ReadonlyArray<
    IndexedGraphProjectionResult<ProjectionId>
  >
  readonly relations: GraphRelationCommit
}

/** Operations bound to one registered vector projection. */
export interface GraphProjectionHandle<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Relations extends GraphRelationDefinitions,
  Kind extends DocumentKind<Documents>,
  ProjectionId extends DocumentProjectionId<Documents, Kind>,
> {
  readonly id: ProjectionId
  readonly version: ProjectionFor<Documents, Kind, ProjectionId>["version"]
  readonly project: (
    value: DocumentValue<Documents, Kind>,
  ) => Effect.Effect<
    ProjectedRevision<GraphId, Documents, Kind, ProjectionId>,
    ProjectDocumentError
  >
  readonly index: (
    value: DocumentValue<Documents, Kind>,
  ) => Effect.Effect<
    IndexProjectedRevisionResult,
    IndexGraphDocumentError,
    EmbeddingProvider | ProjectionIndexStore
  >
  readonly search: {
    (
      query: string,
      options: SemanticProjectionSearchOptions<
        ProjectionMetadata<ProjectionFor<Documents, Kind, ProjectionId>>
      >,
    ): Effect.Effect<
      ReadonlyArray<
        GraphSearchHit<GraphId, Documents, Kind, ProjectionId>
      >,
      SearchDocumentGraphError,
      EmbeddingProvider | ProjectionSearchStore
    >
    (
      query: string,
      options: TextProjectionSearchOptions<
        ProjectionMetadata<ProjectionFor<Documents, Kind, ProjectionId>>
      >,
    ): Effect.Effect<
      ReadonlyArray<
        GraphSearchHit<GraphId, Documents, Kind, ProjectionId>
      >,
      SearchDocumentGraphError,
      ProjectionTextSearchStore
    >
    (
      query: string,
      options?: HybridProjectionSearchOptions<
        ProjectionMetadata<ProjectionFor<Documents, Kind, ProjectionId>>
      >,
    ): Effect.Effect<
      ReadonlyArray<
        GraphSearchHit<GraphId, Documents, Kind, ProjectionId>
      >,
      SearchDocumentGraphError,
      | EmbeddingProvider
      | ProjectionSearchStore
      | ProjectionTextSearchStore
    >
  }
  /** Use this projection as direct evidence for its owning document kind. */
  readonly route: (
    options?: Omit<RetrievalRouteOptions, "neighboursPerSource">,
  ) => DirectRetrievalRoute<Kind, ProjectionId>
  /** Traverse one outgoing relation from this projection's source documents. */
  readonly through: <
    RelationId extends OutgoingRelationId<Relations, Kind>,
  >(
    relation: RelationId,
    options?: RetrievalRouteOptions,
  ) => RelationRetrievalRoute<
    Kind,
    Extract<
      RelationTargetKind<Relations, RelationId>,
      DocumentKind<Documents>
    >,
    ProjectionId,
    RelationId
  >
}

/** Operations bound to one registered document type. */
export interface GraphDocumentHandle<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Relations extends GraphRelationDefinitions,
  Kind extends DocumentKind<Documents>,
> {
  readonly kind: Kind
  readonly ref: (
    id: DocumentId<Documents, Kind>,
  ) => DocumentReference<GraphId, Documents, Kind>
  readonly key: (
    id: DocumentId<Documents, Kind>,
  ) => Effect.Effect<DocumentKey, InvalidDocumentIdentity>
  readonly projection: <
    ProjectionId extends DocumentProjectionId<Documents, Kind>,
  >(
    id: ProjectionId,
  ) => GraphProjectionHandle<
    GraphId,
    Documents,
    Relations,
    Kind,
    ProjectionId
  >
  /**
   * Index every projection, then replace the complete outgoing relation set.
   *
   * Steps are independently atomic and idempotent. Without a caller-owned
   * transaction, retry a failed operation to convergence because earlier
   * projection steps may already have committed.
   */
  readonly index: (
    value: DocumentValue<Documents, Kind>,
  ) => Effect.Effect<
    IndexGraphDocumentResult<DocumentProjectionId<Documents, Kind>>,
    IndexGraphDocumentError,
    | EmbeddingProvider
    | ProjectionIndexStore
    | GraphRelationStore
  >
  readonly remove: (
    id: DocumentId<Documents, Kind>,
  ) => Effect.Effect<
    RemoveGraphDocumentResult,
    InvalidDocumentIdentity | DocumentGraphUnavailable,
    ProjectionIndexStore | GraphRelationStore
  >
  /** Traverse one schema-declared relation in its stored direction or reverse. */
  readonly neighbours: GraphNeighbours<
    GraphId,
    Documents,
    Relations,
    Kind
  >
}

type DirectRetrievalInput<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Relations extends GraphRelationDefinitions,
  TargetKind extends DocumentKind<Documents>,
> = {
  [ProjectionId in DocumentProjectionId<Documents, TargetKind>]:
    | DirectRetrievalRoute<TargetKind, ProjectionId>
    | GraphProjectionHandle<
        GraphId,
        Documents,
        Relations,
        TargetKind,
        ProjectionId
      >
}[DocumentProjectionId<Documents, TargetKind>]

type RelationRetrievalInput<
  Documents extends DocumentDefinitions,
  Relations extends GraphRelationDefinitions,
  TargetKind extends DocumentKind<Documents>,
> = {
  [RelationId in keyof Relations & string]: Relations[RelationId] extends {
    readonly from: infer SourceKind extends DocumentKind<Documents>
    readonly to: TargetKind
  }
    ? {
        [ProjectionId in DocumentProjectionId<Documents, SourceKind>]: RelationRetrievalRoute<
          SourceKind,
          TargetKind,
          ProjectionId,
          RelationId
        >
      }[DocumentProjectionId<Documents, SourceKind>]
    : never
}[keyof Relations & string]

/** Direct or one-relation evidence route that terminates at one target kind. */
export type LegalRetrievalRoute<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Relations extends GraphRelationDefinitions,
  TargetKind extends DocumentKind<Documents>,
> =
  | DirectRetrievalInput<GraphId, Documents, Relations, TargetKind>
  | RelationRetrievalInput<Documents, Relations, TargetKind>

/** Schema-checked definition for one reusable graph retrieval plan. */
export interface DefineGraphRetrievalInput<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Relations extends GraphRelationDefinitions,
  TargetKind extends DocumentKind<Documents>,
  Routes extends readonly [
    LegalRetrievalRoute<GraphId, Documents, Relations, TargetKind>,
    ...ReadonlyArray<
      LegalRetrievalRoute<GraphId, Documents, Relations, TargetKind>
    >,
  ],
  Strategy extends RetrievalStrategyInput | undefined,
> {
  readonly target: TargetKind
  readonly routes: Routes
  readonly strategy?: Strategy
  readonly candidates?: RetrievalCandidateBudgets
  readonly maximumEvidencePerTarget?: number
}

type GraphRetrievalSearchServices<
  Strategy extends RetrievalStrategyInput | undefined,
> = Strategy extends "semantic"
  ? EmbeddingProvider | ProjectionSearchStore
  : Strategy extends "text"
    ? ProjectionTextSearchStore
    : EmbeddingProvider | ProjectionSearchStore | ProjectionTextSearchStore

type GraphRetrievalRelationServices<
  Routes extends readonly unknown[],
> = Extract<Routes[number], RelationRetrievalRoute> extends never
  ? never
  : GraphRelationStore

/** Reusable target-oriented retrieval compiled from graph schema routes. */
export interface GraphRetrievalHandle<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  TargetKind extends DocumentKind<Documents>,
  Routes extends readonly unknown[],
  Strategy extends RetrievalStrategyInput | undefined,
> {
  readonly target: TargetKind
  readonly search: (
    query: string,
    options?: {
      readonly limit?: number
      readonly candidates?: RetrievalCandidateBudgets
    },
  ) => Effect.Effect<
    ReadonlyArray<
      GraphRetrievalResult<
        DocumentReference<GraphId, Documents, TargetKind>,
        AnyGraphSearchHit<GraphId, Documents>
      >
    >,
    SearchDocumentGraphError,
    | GraphRetrievalSearchServices<Strategy>
    | GraphRetrievalRelationServices<Routes>
  >
}

/** Expected failures while projecting and indexing one graph document. */
export type IndexGraphDocumentError =
  | ProjectDocumentError
  | InvalidGraphRelationOutput
  | ProjectionIndexConflict
  | DocumentGraphUnavailable

/** Expected public failures while searching a compiled document graph. */
export type SearchDocumentGraphError =
  | InvalidSearchQuery
  | DocumentGraphUnavailable

/** Idempotent result of removing every projection for one graph document. */
export interface RemoveGraphDocumentResult {
  readonly deletedRevisions: number
  readonly deletedChunks: number
  readonly deletedRelations: number
}

/** Result of pruning stale vector projections and graph relations. */
export interface ReconcileDocumentGraphResult {
  readonly deletedRevisions: number
  readonly deletedChunks: number
  readonly deletedRelations: number
}

/** Small application API compiled from one document graph schema. */
export interface DocumentGraph<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Relations extends GraphRelationDefinitions = {},
> {
  readonly id: GraphId
  readonly manifest: DocumentGraphManifest

  /** Bind graph operations to one document type. */
  readonly document: <Kind extends DocumentKind<Documents>>(
    kind: Kind,
  ) => GraphDocumentHandle<GraphId, Documents, Relations, Kind>

  /** Compile reusable direct and one-relation routes into target retrieval. */
  readonly retrieval: <
    TargetKind extends DocumentKind<Documents>,
    const Routes extends readonly [
      LegalRetrievalRoute<
        GraphId,
        Documents,
        Relations,
        NoInfer<TargetKind>
      >,
      ...ReadonlyArray<
        LegalRetrievalRoute<
          GraphId,
          Documents,
          Relations,
          NoInfer<TargetKind>
        >
      >,
    ],
    const Strategy extends RetrievalStrategyInput | undefined = undefined,
  >(
    definition: DefineGraphRetrievalInput<
      GraphId,
      Documents,
      Relations,
      TargetKind,
      Routes,
      Strategy
    >,
  ) => GraphRetrievalHandle<
    GraphId,
    Documents,
    TargetKind,
    Routes,
    Strategy
  >

  /** Parse an unknown persisted or protocol document reference. */
  readonly parseReference: (
    input: unknown,
  ) => Effect.Effect<
    DocumentReference<GraphId, Documents>,
    InvalidDocumentReference
  >

  /** Prune stored projections no longer registered by this graph schema. */
  readonly reconcileIndex: () => Effect.Effect<
    ReconcileDocumentGraphResult,
    DocumentGraphUnavailable,
    ProjectionIndexStore | GraphRelationStore
  >

  /** Search every indexed projection in this graph unless scoped further. */
  readonly search: (
    query: string,
    options?: DocumentGraphSearchOptions<Documents>,
  ) => Effect.Effect<
    ReadonlyArray<AnyGraphSearchHit<GraphId, Documents>>,
    SearchDocumentGraphError,
    EmbeddingProvider | ProjectionSearchStore
  >

}

/** Input for compiling one schema-defined document graph. */
export interface DefineDocumentGraphInput<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Relations extends GraphRelationDefinitions = {},
> {
  readonly id: GraphId
  readonly documents: Documents
  readonly relations?: (
    relation: DefineGraphRelation<Documents>,
  ) => Relations
}

const UnknownReferenceSchema = Schema.Struct({
  graph: Schema.String,
  kind: Schema.String,
  id: Schema.Unknown,
})

const invalidReference = (
  reason: InvalidDocumentReference["reason"],
): InvalidDocumentReference => new InvalidDocumentReference({ reason })

const failDocumentGraphOperation = (
  operation: DocumentGraphOperation,
  reason: DocumentGraphUnavailable["reason"],
) =>
  (cause: unknown): Effect.Effect<never, DocumentGraphUnavailable> =>
    Effect.fail(documentGraphUnavailable(operation, reason, cause))

const failStoredOperation = (operation: DocumentGraphOperation) =>
  (error: {
    readonly reason: "unavailable" | "invalid_stored_state"
  }): Effect.Effect<never, DocumentGraphUnavailable> =>
    failDocumentGraphOperation(
      operation,
      error.reason === "invalid_stored_state"
        ? "invalid_stored_data"
        : "storage_failed",
    )(error)

const exposeIndexOperation = <A, R>(
  effect: Effect.Effect<
    A,
    | ProjectDocumentError
    | InvalidGraphRelationOutput
    | IndexProjectedRevisionError
    | GraphRelationStoreFailed,
    R
  >,
  attributes: DocumentGraphSpanAttributes,
): Effect.Effect<A, IndexGraphDocumentError, R> =>
  effect.pipe(
    Effect.catchTags({
      EmbeddingProviderFailed: failDocumentGraphOperation(
        "index",
        "embedding_failed",
      ),
      InvalidEmbeddingOutput: failDocumentGraphOperation(
        "index",
        "invalid_adapter_output",
      ),
      ProjectionIndexStoreFailed: failStoredOperation("index"),
      GraphRelationStoreFailed: failStoredOperation("index"),
    }),
    traceDocumentGraphOperation("index", attributes),
  )

const exposeSearchOperation = <A, R>(
  effect: Effect.Effect<
    A,
    SearchGraphError | InvalidDocumentReference,
    R
  >,
  attributes: DocumentGraphSpanAttributes,
): Effect.Effect<A, SearchDocumentGraphError, R> =>
  effect.pipe(
    Effect.catchTags({
      EmbeddingProviderFailed: failDocumentGraphOperation(
        "search",
        "embedding_failed",
      ),
      InvalidEmbeddingOutput: failDocumentGraphOperation(
        "search",
        "invalid_adapter_output",
      ),
      ProjectionSearchStoreFailed: failStoredOperation("search"),
      ProjectionTextSearchStoreFailed: failStoredOperation("search"),
      InvalidSearchOutput: failDocumentGraphOperation(
        "search",
        "invalid_stored_data",
      ),
      InvalidDocumentReference: failDocumentGraphOperation(
        "search",
        "invalid_stored_data",
      ),
    }),
    traceDocumentGraphOperation("search", attributes),
  )

const schemaSearchLimits = (input?: SchemaSearchOptions) => {
  const results = Schema.decodeSync(SearchResultCountSchema)(
    input?.limit ?? 10,
  )
  return {
    results,
    candidates: Schema.decodeSync(SearchResultCountSchema)(
      Math.max(input?.candidates ?? 50, results),
    ),
  }
}

const invalidSearchOptions = (): InvalidSearchQuery =>
  new InvalidSearchQuery({ reason: "invalid_options" })

const parseRuntimeSearchOptions = <Value>(
  parse: () => Value,
): Effect.Effect<Value, InvalidSearchQuery> =>
  Effect.try({
    try: parse,
    catch: invalidSearchOptions,
  })

const schemaSearchStrategy = (
  input?: SchemaSearchOptions,
): Effect.Effect<
  ReturnType<typeof semantic>,
  InvalidSearchQuery
> =>
  parseRuntimeSearchOptions(() => semantic(schemaSearchLimits(input)))

const projectionStrategyRequiresText = (
  input: RetrievalStrategyInput | undefined,
): boolean =>
  input === "text" ||
  input === "hybrid" ||
  (typeof input === "object" && input.mode === "hybrid")

interface RuntimeProjectionSearchPlan {
  readonly strategy: RetrievalStrategy
  readonly results: SearchResultCount
  readonly semanticCandidates: SearchResultCount
  readonly textCandidates: SearchResultCount
}

const projectionSearchPlan = (
  input: ProjectionSearchOptions<unknown> | undefined,
  textEnabled: boolean,
): Effect.Effect<RuntimeProjectionSearchPlan, InvalidSearchQuery> =>
  parseRuntimeSearchOptions(() => {
    const strategy = parseRetrievalStrategy(
      input?.strategy,
      textEnabled,
    )
    const publicLimit = Schema.decodeSync(RetrievalResultLimitSchema)(
      input?.limit ?? 10,
    )
    const results = Schema.decodeSync(SearchResultCountSchema)(publicLimit)
    const semanticCandidates = Schema.decodeSync(
      SearchResultCountSchema,
    )(input?.candidates?.semantic ?? 50)
    const textCandidates = Schema.decodeSync(SearchResultCountSchema)(
      input?.candidates?.text ?? 50,
    )

    if (
      strategy._tag === "Semantic" &&
      results > semanticCandidates
    ) {
      throw new Error(
        "Semantic search results cannot exceed semantic candidates",
      )
    }
    if (strategy._tag === "Text" && results > textCandidates) {
      throw new Error("Text search results cannot exceed text candidates")
    }
    if (
      strategy._tag === "Hybrid" &&
      results > semanticCandidates + textCandidates
    ) {
      throw new Error(
        "Hybrid search results cannot exceed combined candidates",
      )
    }

    return { strategy, results, semanticCandidates, textCandidates }
  })

const metadataFilters = <Metadata>(
  where: ProjectionSearchWhere<Metadata> | undefined,
): ReadonlyArray<MetadataFilter> => {
  if (typeof where === "function") {
    return [
      normalizeMetadataFilter(where(makeMetadataFilterBuilder<Metadata>())),
    ]
  }

  return Object.entries(where ?? {}).map(([key, condition]) => {
    if (
      typeof condition === "object" &&
      condition !== null &&
      "oneOf" in condition
    ) {
      return metadataOneOf(
        key,
        condition.oneOf as readonly [
          MetadataSearchValue,
          ...ReadonlyArray<MetadataSearchValue>,
        ],
      )
    }

    return metadataEquals(key, condition as MetadataSearchValue)
  })
}

/** Compile document schemas and vector metadata into one application graph. */
export const defineDocumentGraph = <
  const GraphId extends string,
  const Documents extends DocumentDefinitions,
  const Relations extends GraphRelationDefinitions = {},
>(
  input: DefineDocumentGraphInput<GraphId, Documents, Relations>,
): DocumentGraph<GraphId, Documents, Relations> => {
  const defineRelation: DefineGraphRelation<Documents> = (relation) =>
    relation
  // SAFETY: When no relation callback is provided, Relations uses its empty
  // default. A provided callback constructs every entry through defineRelation.
  const relations = (input.relations === undefined
    ? {}
    : input.relations(defineRelation)) as Relations
  assertUniqueProjectionIds(input.documents)
  assertValidGraphRelations(input.documents, relations)
  const registered = registeredGraphProjections(input.documents)
  const registeredRelations = registeredGraphRelations(relations)

  const ref = <Kind extends DocumentKind<Documents>>(
    kind: Kind,
    id: DocumentId<Documents, Kind>,
  ): DocumentReference<GraphId, Documents, Kind> => {
    const definition: Documents[Kind] | undefined = input.documents[kind]
    if (definition === undefined) {
      throw new Error(`Unknown document kind: ${kind}`)
    }

    const parsedId = Schema.decodeUnknownSync(definition.id)(id)

    // SAFETY: Kind selects this exact document definition and its ID schema
    // parsed the value before the graph-specific reference was constructed.
    return {
      graph: input.id,
      kind,
      id: parsedId,
    } as DocumentReference<GraphId, Documents, Kind>
  }

  const parseReference = (
    candidate: unknown,
  ): Effect.Effect<
    DocumentReference<GraphId, Documents>,
    InvalidDocumentReference
  > =>
    Schema.decodeUnknown(UnknownReferenceSchema)(candidate, {
      onExcessProperty: "error",
    }).pipe(
      Effect.mapError(() => invalidReference("invalid_shape")),
      Effect.flatMap((parsed) => {
        if (parsed.graph !== input.id) {
          return Effect.fail(invalidReference("wrong_graph"))
        }

        const definition: DocumentDefinitionShape | undefined =
          input.documents[parsed.kind]
        if (definition === undefined) {
          return Effect.fail(invalidReference("unknown_document_kind"))
        }

        return Schema.decodeUnknown(definition.id)(parsed.id).pipe(
          Effect.mapError(() => invalidReference("invalid_document_id")),
          Effect.map((id) => {
            // SAFETY: The graph and kind were matched against this definition,
            // and its ID schema parsed the value before constructing the union.
            return {
              graph: input.id,
              kind: parsed.kind,
              id,
            } as DocumentReference<GraphId, Documents>
          }),
        )
      }),
    )

  const key = <Kind extends DocumentKind<Documents>>(
    target: DocumentReference<GraphId, Documents, Kind>,
  ): Effect.Effect<DocumentKey, InvalidDocumentIdentity> => {
    const definition = input.documents[target.kind]
    if (definition === undefined) {
      return Effect.dieMessage(
        `Unknown document kind ${target.kind} for graph ${input.id}`,
      )
    }

    return encodeDocumentId(
      input.id,
      target.kind,
      definition.id,
      target.id,
    ).pipe(
      Effect.map((encodedId) =>
        makeDocumentKey({
          graph: input.id,
          documentKind: target.kind,
          encodedId,
        }),
      ),
    )
  }

  const scope = (
    scopeInput?: GraphSearchScopeInput<
      DocumentKind<Documents>,
      GraphProjectionId<Documents>
    >,
  ): GraphSearchScope<GraphId> =>
    makeGraphSearchScope(input.id, scopeInput ?? {}, registered)

  const parseSearchHit = (
    hit: SearchHit,
    channel: InvalidSearchOutput["channel"],
  ): Effect.Effect<
    AnyGraphSearchHit<GraphId, Documents>,
    InvalidDocumentReference | InvalidSearchOutput
  > =>
    Effect.gen(function*() {
      const reference = yield* parseReference(hit.reference)
      const definition = input.documents[reference.kind]
      const projection = definition?.projections.find(
        (candidate) =>
          candidate.id === hit.projection.id &&
          candidate.version === hit.projection.version,
      )
      if (projection === undefined) {
        return yield* Effect.fail(
          new InvalidSearchOutput({ channel, reason: "out_of_scope" }),
        )
      }

      if (
        projection.metadataSchema === undefined &&
        hit.metadata !== undefined
      ) {
        return yield* Effect.fail(
          new InvalidSearchOutput({ channel, reason: "invalid_metadata" }),
        )
      }

      const metadata =
        projection.metadataSchema === undefined
          ? undefined
          : yield* Schema.decodeUnknown(projection.metadataSchema)(
              hit.metadata,
              { onExcessProperty: "error" },
            ).pipe(
              Effect.mapError(
                () =>
                  new InvalidSearchOutput({
                    channel,
                    reason: "invalid_metadata",
                  }),
              ),
            )

      // SAFETY: The reference, registered projection version, and projection
      // metadata schema all parsed before constructing the graph hit union.
      return { ...hit, reference, metadata } as unknown as AnyGraphSearchHit<
        GraphId,
        Documents
      >
    })

  const searchProjectionRuntime = (inputSearch: {
    readonly query: string
    readonly semanticQuery?: SemanticQueryPreparation | undefined
    readonly documentKind: string
    readonly projection: VectorProjectionShape
    readonly plan: RuntimeProjectionSearchPlan
    readonly where: ReadonlyArray<MetadataFilter>
  }): Effect.Effect<
    ReadonlyArray<AnyGraphSearchHit<GraphId, Documents>>,
    SearchGraphError | InvalidDocumentReference,
    | EmbeddingProvider
    | ProjectionSearchStore
    | ProjectionTextSearchStore
  > =>
    Effect.gen(function*() {
      const textPolicy = inputSearch.projection.text
      if (
        textPolicy === "disabled" &&
        inputSearch.plan.strategy._tag !== "Semantic"
      ) {
        return yield* Effect.fail(
          new InvalidSearchQuery({ reason: "text_disabled" }),
        )
      }

      const candidateCount =
        inputSearch.plan.strategy._tag === "Semantic"
          ? inputSearch.plan.semanticCandidates
          : inputSearch.plan.strategy._tag === "Text"
            ? inputSearch.plan.textCandidates
            : inputSearch.plan.semanticCandidates +
              inputSearch.plan.textCandidates
      const searchScope = makeGraphSearchScope(
        input.id,
        {
          include: [inputSearch.documentKind],
          includeProjections: [inputSearch.projection.id],
          where: inputSearch.where,
        },
        registered,
      )
      yield* Effect.annotateCurrentSpan({
        "document_graph.search.strategy":
          inputSearch.plan.strategy._tag.toLowerCase(),
        "document_graph.search.candidates": candidateCount,
        "document_graph.search.limit": inputSearch.plan.results,
      })

      const searchHits = yield* Effect.gen(function*() {
        switch (inputSearch.plan.strategy._tag) {
          case "Semantic": {
            const strategy = semantic({
              candidates: inputSearch.plan.semanticCandidates,
              results: inputSearch.plan.results,
              weight: inputSearch.plan.strategy.weight,
              rankConstant: inputSearch.plan.strategy.rankConstant,
            })
            return inputSearch.semanticQuery === undefined
              ? yield* searchGraph({
                  query: inputSearch.query,
                  scope: searchScope,
                  strategy,
                })
              : yield* inputSearch.semanticQuery.pipe(
                  Effect.flatMap((query) =>
                    searchGraphWithPreparedSemanticQuery({
                      query,
                      scope: searchScope,
                      strategy,
                    }),
                  ),
                )
          }
          case "Text":
            if (textPolicy === "disabled") {
              return yield* Effect.fail(
                new InvalidSearchQuery({ reason: "text_disabled" }),
              )
            }
            return yield* searchGraphText({
              query: inputSearch.query,
              scope: searchScope,
              strategy: text({
                policy: textPolicy,
                candidates: inputSearch.plan.textCandidates,
                results: inputSearch.plan.results,
                weight: inputSearch.plan.strategy.weight,
                rankConstant: inputSearch.plan.strategy.rankConstant,
              }),
            })
          case "Hybrid": {
            if (textPolicy === "disabled") {
              return yield* Effect.fail(
                new InvalidSearchQuery({ reason: "text_disabled" }),
              )
            }
            const hybridInput = {
              scope: searchScope,
              route: {
                _tag: "Projection",
                sourceKind: inputSearch.documentKind,
                projection: inputSearch.projection.id,
              },
              semantic: semantic({
                candidates: inputSearch.plan.semanticCandidates,
                results: inputSearch.plan.semanticCandidates,
                weight: inputSearch.plan.strategy.weights.semantic,
                rankConstant: inputSearch.plan.strategy.rankConstant,
              }),
              text: text({
                policy: textPolicy,
                candidates: inputSearch.plan.textCandidates,
                results: inputSearch.plan.textCandidates,
                weight: inputSearch.plan.strategy.weights.text,
                rankConstant: inputSearch.plan.strategy.rankConstant,
              }),
              results: inputSearch.plan.results,
              rankConstant: inputSearch.plan.strategy.rankConstant,
            } as const
            return inputSearch.semanticQuery === undefined
              ? yield* searchGraphHybrid({
                  ...hybridInput,
                  query: inputSearch.query,
                })
              : yield* searchGraphHybridWithSemanticQuery({
                  ...hybridInput,
                  query: inputSearch.query,
                  semanticQuery: inputSearch.semanticQuery,
                })
          }
        }
      })

      return yield* Effect.forEach(searchHits, (hit) => {
        const channel = hit.signals[0].stream.channel
        return parseSearchHit(hit, channel).pipe(
          Effect.flatMap((parsed) =>
            parsed.reference.kind !== inputSearch.documentKind ||
            parsed.projection.id !== inputSearch.projection.id ||
            parsed.projection.version !== inputSearch.projection.version
              ? Effect.fail(
                  new InvalidSearchOutput({
                    channel,
                    reason: "out_of_scope",
                  }),
                )
              : Effect.succeed(parsed),
          ),
        )
      })
    })

  type RuntimeNeighbour = {
    readonly documentKey: DocumentKey
    readonly reference: DocumentReference<GraphId, Documents>
  }

  const findRuntimeNeighbours = (inputNeighbour: {
    readonly currentDocumentKey: DocumentKey
    readonly currentDocumentKind: string
    readonly relationId: string
    readonly direction: "outgoing" | "incoming"
    readonly limit: GraphNeighbourLimit
  }): Effect.Effect<
    ReadonlyArray<RuntimeNeighbour>,
    | InvalidDocumentIdentity
    | InvalidDocumentReference
    | InvalidGraphNeighbourOutput
    | GraphRelationStoreFailed,
    GraphRelationStore
  > => {
    const relation = relations[inputNeighbour.relationId]
    const currentKind =
      inputNeighbour.direction === "outgoing"
        ? relation?.from
        : relation?.to
    if (
      relation === undefined ||
      currentKind !== inputNeighbour.currentDocumentKind
    ) {
      return Effect.dieMessage(
        `Unknown ${inputNeighbour.direction} relation ${inputNeighbour.relationId} for ${inputNeighbour.currentDocumentKind}`,
      )
    }

    return Effect.gen(function*() {
      const store = yield* GraphRelationStore
      const stored = yield* (inputNeighbour.direction === "outgoing"
        ? store.findOutgoing({
            graph: input.id,
            sourceDocumentKey: inputNeighbour.currentDocumentKey,
            sourceDocumentKind: inputNeighbour.currentDocumentKind,
            relation: inputNeighbour.relationId,
            relationVersion: relation.version,
            targetDocumentKind: relation.to,
            limit: inputNeighbour.limit,
          })
        : store.findIncoming({
            graph: input.id,
            targetDocumentKey: inputNeighbour.currentDocumentKey,
            targetDocumentKind: inputNeighbour.currentDocumentKind,
            relation: inputNeighbour.relationId,
            relationVersion: relation.version,
            sourceDocumentKind: relation.from,
            limit: inputNeighbour.limit,
          }))
      const neighbourKind =
        inputNeighbour.direction === "outgoing"
          ? relation.to
          : relation.from
      if (stored.length > inputNeighbour.limit) {
        return yield* Effect.fail(
          new InvalidGraphNeighbourOutput({ reason: "too_many" }),
        )
      }

      const seen = new Set<DocumentKey>()
      let previousKey: DocumentKey | undefined
      for (const candidate of stored) {
        if (seen.has(candidate.documentKey)) {
          return yield* Effect.fail(
            new InvalidGraphNeighbourOutput({ reason: "duplicate" }),
          )
        }
        if (
          previousKey !== undefined &&
          String(previousKey).localeCompare(
            String(candidate.documentKey),
          ) > 0
        ) {
          return yield* Effect.fail(
            new InvalidGraphNeighbourOutput({ reason: "not_ordered" }),
          )
        }
        seen.add(candidate.documentKey)
        previousKey = candidate.documentKey
      }

      return yield* Effect.forEach(stored, (candidate) =>
        parseReference(candidate.reference).pipe(
          Effect.flatMap((reference) => {
            if (reference.kind !== neighbourKind) {
              return Effect.fail(
                invalidReference("unknown_document_kind"),
              )
            }

            return key(reference).pipe(
              Effect.flatMap((parsedKey) =>
                parsedKey === candidate.documentKey
                  ? Effect.succeed({
                      documentKey: candidate.documentKey,
                      reference,
                    })
                  : Effect.fail(
                      invalidReference("invalid_document_id"),
                    ),
              ),
            )
          }),
        ),
      )
    })
  }

  const document = <Kind extends DocumentKind<Documents>>(
    kind: Kind,
  ): GraphDocumentHandle<GraphId, Documents, Relations, Kind> => {
    const definition = input.documents[kind]
    if (definition === undefined) {
      throw new Error(`Unknown document kind: ${kind}`)
    }

    const projection = <
      ProjectionId extends DocumentProjectionId<Documents, Kind>,
    >(
      projectionId: ProjectionId,
    ): GraphProjectionHandle<
      GraphId,
      Documents,
      Relations,
      Kind,
      ProjectionId
    > => {
      const registeredProjection = definition.projections.find(
        (candidate) => candidate.id === projectionId,
      )
      if (registeredProjection === undefined) {
        throw new Error(
          `Unknown projection ${projectionId} for document ${kind}`,
        )
      }

      const search = (
        query: string,
        options?: ProjectionSearchOptions<
          ProjectionMetadata<
            ProjectionFor<Documents, Kind, ProjectionId>
          >
        >,
      ) =>
        exposeSearchOperation(
          Effect.gen(function*() {
            const textEnabled = registeredProjection.text !== "disabled"
            if (
              !textEnabled &&
              projectionStrategyRequiresText(options?.strategy)
            ) {
              return yield* Effect.fail(
                new InvalidSearchQuery({ reason: "text_disabled" }),
              )
            }

            const plan = yield* projectionSearchPlan(
              options,
              textEnabled,
            )
            const where = yield* parseRuntimeSearchOptions(() =>
              metadataFilters(options?.where),
            )
            return yield* searchProjectionRuntime({
              query,
              documentKind: kind,
              projection: registeredProjection,
              plan,
              where,
            })
          }),
          {
            "document_graph.graph": input.id,
            "document_graph.document_kind": kind,
            "document_graph.projection": projectionId,
          },
        )

      // SAFETY: projectionId was found in this document definition; its
      // runtime version and operations therefore match ProjectionFor.
      return {
        id: projectionId,
        version: registeredProjection.version,
        project: (value: DocumentValue<Documents, Kind>) =>
          projectDocument(
            input.id,
            input.documents,
            kind,
            projectionId,
            value,
          ),
        index: (value: DocumentValue<Documents, Kind>) =>
          exposeIndexOperation(
            projectDocument(
              input.id,
              input.documents,
              kind,
              projectionId,
              value,
            ).pipe(Effect.flatMap(indexProjectedRevision)),
            {
              "document_graph.graph": input.id,
              "document_graph.document_kind": kind,
              "document_graph.projection": projectionId,
            },
          ),
        search,
        route: (
          options?: Omit<RetrievalRouteOptions, "neighboursPerSource">,
        ) => ({
          _tag: "Direct" as const,
          sourceKind: kind,
          projection: projectionId,
          weight: Schema.decodeSync(RetrievalWeightSchema)(
            options?.weight ?? 1,
          ),
        }),
        through: (
          relationId: keyof Relations & string,
          options?: RetrievalRouteOptions,
        ) => {
          const relation = relations[relationId]
          if (relation === undefined || relation.from !== kind) {
            throw new Error(
              `Unknown outgoing relation ${relationId} for ${kind}`,
            )
          }
          return {
            _tag: "Relation" as const,
            sourceKind: kind,
            targetKind: relation.to,
            projection: projectionId,
            relation: relationId,
            direction: "outgoing" as const,
            weight: Schema.decodeSync(RetrievalWeightSchema)(
              options?.weight ?? 1,
            ),
            neighboursPerSource: Schema.decodeSync(
              GraphNeighbourLimitSchema,
            )(options?.neighboursPerSource ?? 100),
          }
        },
      } as unknown as GraphProjectionHandle<
        GraphId,
        Documents,
        Relations,
        Kind,
        ProjectionId
      >
    }

    return {
      kind,
      ref: (id) => ref(kind, id),
      key: (id) => key(ref(kind, id)),
      projection,
      index: (value) =>
        exposeIndexOperation(
          Effect.gen(function*() {
            const document = yield* parseDocumentInstance({
              graph: input.id,
              documentKind: kind,
              definition,
              value,
            })
            const projectedRevisions = yield* Effect.forEach(
              definition.projections,
              (item) =>
                projectParsedDocument(
                  input.id,
                  input.documents,
                  kind,
                  item.id,
                  document,
                ),
            )
            const replacement =
              yield* projectOutgoingGraphRelationsForInstance({
              graph: input.id,
              documentKind: kind,
              documents: input.documents,
              relations,
              document,
            })
            const projections = yield* Effect.forEach(
              projectedRevisions,
              (revision) =>
                indexProjectedRevision(revision).pipe(
                  Effect.map((result) => ({
                    projection: revision.projection.id,
                    result,
                  })),
                ),
            )
            const relationStore = yield* GraphRelationStore
            const relationCommit = yield* relationStore.replaceOutgoing(
              replacement,
            )

            return { projections, relations: relationCommit }
          }),
          {
            "document_graph.graph": input.id,
            "document_graph.document_kind": kind,
          },
        // SAFETY: Every projected revision was created from this definition's
        // projections, so each ID is DocumentProjectionId<Documents, Kind>.
        ) as Effect.Effect<
          IndexGraphDocumentResult<
            DocumentProjectionId<Documents, Kind>
          >,
          IndexGraphDocumentError,
          | EmbeddingProvider
          | ProjectionIndexStore
          | GraphRelationStore
        >,
      remove: (id) =>
        Effect.gen(function*() {
          const projectionStore = yield* ProjectionIndexStore
          const documentKey = yield* key(ref(kind, id))
          const deletions = yield* Effect.forEach(
            definition.projections,
            (item) =>
              projectionStore.deleteRevision({
                documentKey,
                projection: item.id,
              }),
          )
          const projectionDeletion = deletions.reduce(
            (total, deletion) => ({
              deletedRevisions:
                total.deletedRevisions + deletion.deletedRevisions,
              deletedChunks: total.deletedChunks + deletion.deletedChunks,
            }),
            { deletedRevisions: 0, deletedChunks: 0 },
          )
          const relationStore = yield* GraphRelationStore
          const relationDeletion = yield* relationStore.deleteNode({
            graph: input.id,
            documentKey,
          })

          return {
            ...projectionDeletion,
            deletedRelations: relationDeletion.deleted,
          }
        }).pipe(
          Effect.catchTags({
            ProjectionIndexStoreFailed: failStoredOperation("remove"),
            GraphRelationStoreFailed: failStoredOperation("remove"),
          }),
          traceDocumentGraphOperation("remove", {
            "document_graph.graph": input.id,
            "document_graph.document_kind": kind,
          }),
        ),
      neighbours: ((
        id: DocumentId<Documents, Kind>,
        options: {
          readonly via: keyof Relations & string
          readonly direction?: "outgoing" | "incoming"
          readonly limit?: number
        },
      ) => {
        const relation = relations[options.via]
        const direction = options.direction ?? "outgoing"
        const currentKind =
          direction === "outgoing" ? relation?.from : relation?.to
        if (relation === undefined || currentKind !== kind) {
          return Effect.dieMessage(
            `Unknown ${direction} relation ${options.via} for ${kind}`,
          )
        }

        return Effect.gen(function*() {
          const limit = yield* Schema.decodeUnknown(
            GraphNeighbourLimitSchema,
          )(options.limit ?? 100).pipe(
            Effect.mapError(
              () => new InvalidGraphTraversal({ reason: "invalid_limit" }),
            ),
          )
          const currentDocumentKey = yield* key(ref(kind, id))
          const neighbours = yield* findRuntimeNeighbours({
            currentDocumentKey,
            currentDocumentKind: kind,
            relationId: options.via,
            direction,
            limit,
          })
          return neighbours.map((neighbour) => neighbour.reference)
        }).pipe(
          Effect.catchTags({
            GraphRelationStoreFailed: failStoredOperation("neighbours"),
            InvalidDocumentReference: failDocumentGraphOperation(
              "neighbours",
              "invalid_stored_data",
            ),
            InvalidGraphNeighbourOutput: failDocumentGraphOperation(
              "neighbours",
              "invalid_stored_data",
            ),
          }),
          traceDocumentGraphOperation("neighbours", {
            "document_graph.graph": input.id,
            "document_graph.document_kind": kind,
            "document_graph.relation": options.via,
            "document_graph.neighbours.direction": direction,
            "document_graph.neighbours.limit": options.limit ?? 100,
          }),
        )
        // SAFETY: The implementation validates the selected relation against
        // the current node and checks every parsed reference against the
        // opposite kind before exposing its direction-specific overload.
      }) as unknown as GraphNeighbours<
        GraphId,
        Documents,
        Relations,
        Kind
      >,
    }
  }

  const retrieval = <
    TargetKind extends DocumentKind<Documents>,
    const Routes extends readonly [
      LegalRetrievalRoute<GraphId, Documents, Relations, TargetKind>,
      ...ReadonlyArray<
        LegalRetrievalRoute<GraphId, Documents, Relations, TargetKind>
      >,
    ],
    const Strategy extends RetrievalStrategyInput | undefined = undefined,
  >(
    definition: DefineGraphRetrievalInput<
      GraphId,
      Documents,
      Relations,
      TargetKind,
      Routes,
      Strategy
    >,
  ): GraphRetrievalHandle<
    GraphId,
    Documents,
    TargetKind,
    Routes,
    Strategy
  > => {
    type TargetReference = DocumentReference<
      GraphId,
      Documents,
      TargetKind
    >
    type SourceHit = AnyGraphSearchHit<GraphId, Documents>
    type RuntimeRoute =
      | DirectRetrievalRoute<string, string>
      | RelationRetrievalRoute<string, string, string, string>
    type CompiledRuntimeRoute = {
      readonly route: RuntimeRoute
      readonly projection: VectorProjectionShape
    }
    type Target = {
      readonly key: DocumentKey
      readonly reference: TargetReference
    }
    type ExpandedHit = {
      readonly hit: SourceHit
      readonly targets: ReadonlyArray<Target>
    }

    const compiledRoutes: ReadonlyArray<CompiledRuntimeRoute> =
      definition.routes.map((candidate) => {
        const selected = "_tag" in candidate ? candidate : candidate.route()
        // SAFETY: LegalRetrievalRoute restricts every route to schema-declared
        // document, projection, relation, and target identities. Runtime checks
        // below reconstruct those same declarations before storing the route.
        const route = selected as RuntimeRoute
        if (
          (route._tag === "Direct" &&
            route.sourceKind !== definition.target) ||
          (route._tag === "Relation" &&
            route.targetKind !== definition.target)
        ) {
          throw new Error(
            `Retrieval route ${route.sourceKind}.${route.projection} does not target ${definition.target}`,
          )
        }
        const source = input.documents[route.sourceKind]
        const projection = source?.projections.find(
          (candidateProjection) =>
            candidateProjection.id === route.projection,
        )
        if (source === undefined || projection === undefined) {
          throw new Error(
            `Unknown retrieval projection ${route.sourceKind}.${route.projection}`,
          )
        }
        if (route._tag === "Relation") {
          const relation = relations[route.relation]
          if (
            relation === undefined ||
            relation.from !== route.sourceKind ||
            relation.to !== route.targetKind
          ) {
            throw new Error(
              `Unknown retrieval relation ${route.relation} from ${route.sourceKind} to ${route.targetKind}`,
            )
          }
        }
        return { route, projection }
      })

    const routeKey = (route: RuntimeRoute): RetrievalRouteKey =>
      route._tag === "Direct"
        ? {
            _tag: "Projection",
            sourceKind: route.sourceKind,
            projection: route.projection,
          }
        : {
            _tag: "Relation",
            sourceKind: route.sourceKind,
            projection: route.projection,
            relation: route.relation,
            direction: "outgoing",
            targetKind: route.targetKind,
          }
    const routeId = (route: RuntimeRoute): string => {
      const key = routeKey(route)
      return key._tag === "Projection"
        ? `direct:${key.sourceKind}:${key.projection}`
        : `relation:${key.sourceKind}:${key.projection}:${key.relation}:${key.targetKind}`
    }
    const routeIds = compiledRoutes.map((compiled) =>
      routeId(compiled.route),
    )
    if (new Set(routeIds).size !== routeIds.length) {
      throw new Error("Retrieval routes must have unique structural identities")
    }

    const configuredStrategy = parseRetrievalStrategy(
      definition.strategy,
      true,
    )
    const channelWeights =
      configuredStrategy._tag === "Hybrid"
        ? [
            configuredStrategy.weights.semantic,
            configuredStrategy.weights.text,
          ]
        : [configuredStrategy.weight]
    for (const { route } of compiledRoutes) {
      for (const channelWeight of channelWeights) {
        Schema.decodeSync(RetrievalWeightSchema)(
          route.weight * channelWeight,
        )
      }
    }

    const maximumEvidence = Schema.decodeSync(SearchResultCountSchema)(
      definition.maximumEvidencePerTarget ?? 3,
    )
    const rankConstant = Schema.decodeSync(
      ReciprocalRankConstantSchema,
    )(
      typeof definition.strategy === "object"
        ? definition.strategy.rankConstant ?? 60
        : 60,
    )

    type ExpandedRoute = {
      readonly route: RuntimeRoute
      readonly expanded: ReadonlyArray<ExpandedHit>
    }
    type RetainedEvidence = {
      readonly route: RetrievalRouteKey
      readonly source: SourceHit
      readonly streams: Map<string, RetrievalStreamKey>
    }
    type RuntimeRetrievalStream = {
      readonly id: string
      readonly signal: RetrievalStreamKey
      readonly weight: RetrievalWeight
      readonly items: ReadonlyArray<{
        readonly key: DocumentKey
        readonly value: TargetReference
        readonly score: number
      }>
    }
    type BuiltRetrievalStreams = {
      readonly targetReferences: ReadonlyMap<DocumentKey, TargetReference>
      readonly evidence: ReadonlyMap<
        DocumentKey,
        ReadonlyMap<string, RetainedEvidence>
      >
      readonly streams: ReadonlyArray<RuntimeRetrievalStream>
    }

    const expandRoutes = (
      query: string,
      plan: RuntimeProjectionSearchPlan,
      semanticQuery: SemanticQueryPreparation | undefined,
    ): Effect.Effect<
      ReadonlyArray<ExpandedRoute>,
      SearchDocumentGraphError,
      | EmbeddingProvider
      | ProjectionSearchStore
      | ProjectionTextSearchStore
      | GraphRelationStore
    > =>
      Effect.forEach(
        compiledRoutes,
        (compiled) =>
          Effect.gen(function*() {
            const route = compiled.route
            const hits = yield* exposeSearchOperation(
              searchProjectionRuntime({
                query,
                semanticQuery,
                documentKind: route.sourceKind,
                projection: compiled.projection,
                plan,
                where: [],
              }),
              {
                "document_graph.graph": input.id,
                "document_graph.document_kind": route.sourceKind,
                "document_graph.projection": route.projection,
              },
            )
            if (route._tag === "Direct") {
              return {
                route,
                expanded: hits.map((hit) => ({
                  hit,
                  targets: [
                    {
                      key: hit.documentKey,
                      // SAFETY: Direct routes are compiled only from a
                      // projection owned by the declared target kind.
                      reference: hit.reference as TargetReference,
                    },
                  ],
                })),
              }
            }

            const uniqueSources = new Map<DocumentKey, SourceHit>()
            for (const hit of hits) {
              uniqueSources.set(hit.documentKey, hit)
            }
            const neighbourEntries = yield* Effect.forEach(
              uniqueSources.values(),
              (hit) =>
                findRuntimeNeighbours({
                  currentDocumentKey: hit.documentKey,
                  currentDocumentKind: route.sourceKind,
                  relationId: route.relation,
                  direction: "outgoing",
                  limit: route.neighboursPerSource,
                }).pipe(
                  Effect.map((neighbours) => [
                    hit.documentKey,
                    neighbours.map((neighbour) => ({
                      key: neighbour.documentKey,
                      // SAFETY: Runtime relation validation fixed this route's
                      // target kind to the retrieval target.
                      reference: neighbour.reference as TargetReference,
                    })),
                  ] as const),
                  Effect.catchTags({
                    GraphRelationStoreFailed:
                      failStoredOperation("search"),
                    InvalidDocumentIdentity: failDocumentGraphOperation(
                      "search",
                      "invalid_stored_data",
                    ),
                    InvalidDocumentReference: failDocumentGraphOperation(
                      "search",
                      "invalid_stored_data",
                    ),
                    InvalidGraphNeighbourOutput:
                      failDocumentGraphOperation(
                        "search",
                        "invalid_stored_data",
                      ),
                  }),
                ),
              {
                concurrency: GraphNeighbourExpansionConcurrencyPerRoute,
              },
            )
            const neighboursBySource = new Map(neighbourEntries)
            return {
              route,
              expanded: hits.map((hit) => ({
                hit,
                targets: neighboursBySource.get(hit.documentKey) ?? [],
              })),
            }
          }),
        { concurrency: GraphRetrievalRouteConcurrency },
      )

    const buildStreams = (
      expandedRoutes: ReadonlyArray<ExpandedRoute>,
    ): BuiltRetrievalStreams => {
      const targetReferences = new Map<DocumentKey, TargetReference>()
      const evidence = new Map<
        DocumentKey,
        Map<string, RetainedEvidence>
      >()
      const streams: Array<RuntimeRetrievalStream> = []

      for (const output of expandedRoutes) {
        const keyForRoute = routeKey(output.route)
        for (const channel of ["semantic", "text"] as const) {
          const streamKey: RetrievalStreamKey = {
            route: keyForRoute,
            channel,
          }
          const streamId = `${routeId(output.route)}:${channel}`
          const ranked = output.expanded
            .flatMap((entry) => {
              const signal = entry.hit.signals.find(
                (candidate) => candidate.stream.channel === channel,
              )
              return signal === undefined ? [] : [{ entry, signal }]
            })
            .sort(
              (left, right) =>
                left.signal.rank - right.signal.rank ||
                String(left.entry.hit.chunkId).localeCompare(
                  String(right.entry.hit.chunkId),
                ),
            )
          const firstRanked = ranked[0]
          if (firstRanked === undefined) continue

          const seenTargets = new Set<DocumentKey>()
          const items: Array<
            RuntimeRetrievalStream["items"][number]
          > = []
          for (const { entry, signal } of ranked) {
            for (const target of entry.targets) {
              targetReferences.set(target.key, target.reference)
              let targetEvidence = evidence.get(target.key)
              if (targetEvidence === undefined) {
                targetEvidence = new Map()
                evidence.set(target.key, targetEvidence)
              }
              const evidenceKey = String(entry.hit.chunkId)
              let retained = targetEvidence.get(evidenceKey)
              if (retained === undefined) {
                retained = {
                  route: keyForRoute,
                  source: entry.hit,
                  streams: new Map(),
                }
                targetEvidence.set(evidenceKey, retained)
              }
              retained.streams.set(streamId, streamKey)

              if (!seenTargets.has(target.key)) {
                seenTargets.add(target.key)
                items.push({
                  key: target.key,
                  value: target.reference,
                  score: signal.score,
                })
              }
            }
          }
          if (items.length === 0) continue
          streams.push({
            id: streamId,
            signal: streamKey,
            weight: Schema.decodeSync(RetrievalWeightSchema)(
              output.route.weight * firstRanked.signal.weight,
            ),
            items,
          })
        }
      }

      return { targetReferences, evidence, streams }
    }

    const assembleResults = (
      built: BuiltRetrievalStreams,
      resultLimit: number,
    ): Effect.Effect<
      ReadonlyArray<GraphRetrievalResult<TargetReference, SourceHit>>
    > =>
      weightedReciprocalRankFusion({
        rankConstant,
        streams: built.streams,
      }).pipe(
        Effect.orDie,
        Effect.map((fused) =>
          fused.slice(0, resultLimit).map((result, index) => {
            const target = built.targetReferences.get(result.key)
            const retained = built.evidence.get(result.key)
            if (target === undefined || retained === undefined) {
              throw new Error(
                "A fused graph target unexpectedly lost its evidence",
              )
            }
            const material = Array.from(retained.values())
              .sort(
                (left, right) =>
                  left.source.rank - right.source.rank ||
                  String(left.source.chunkId).localeCompare(
                    String(right.source.chunkId),
                  ),
              )
              .slice(0, maximumEvidence)
              .map((item) => {
                const orderedStreams = Array.from(item.streams.entries())
                  .sort(([left], [right]) => left.localeCompare(right))
                  .map(([, stream]) => stream)
                if (!EffectArray.isNonEmptyReadonlyArray(orderedStreams)) {
                  throw new Error(
                    "Retained graph evidence unexpectedly has no streams",
                  )
                }
                return {
                  route: item.route,
                  source: item.source,
                  streams: orderedStreams,
                }
              })
            if (!EffectArray.isNonEmptyReadonlyArray(material)) {
              throw new Error(
                "A fused graph target unexpectedly has no evidence",
              )
            }
            return {
              rank: index + 1,
              score: result.score,
              target,
              signals: result.signals,
              evidence: material,
            }
          }),
        ),
      )

    return {
      target: definition.target,
      search: (
        query: string,
        options?: {
          readonly limit?: number
          readonly candidates?: RetrievalCandidateBudgets
        },
      ) =>
        Effect.gen(function*() {
          const searchPlan = yield* parseRuntimeSearchOptions(() => {
            const requestedCandidates = {
              ...definition.candidates,
              ...options?.candidates,
            }
            const semanticCandidates = Schema.decodeSync(
              SearchResultCountSchema,
            )(requestedCandidates.semantic ?? 50)
            const textCandidates = Schema.decodeSync(
              SearchResultCountSchema,
            )(requestedCandidates.text ?? 50)
            const routeResults = Schema.decodeSync(
              SearchResultCountSchema,
            )(
              configuredStrategy._tag === "Semantic"
                ? semanticCandidates
                : configuredStrategy._tag === "Text"
                  ? textCandidates
                  : Math.min(
                      10_000,
                      semanticCandidates + textCandidates,
                    ),
            )
            const resultLimit = Schema.decodeSync(
              RetrievalResultLimitSchema,
            )(options?.limit ?? 10)

            return {
              semanticCandidates,
              textCandidates,
              resultLimit,
              routePlan: {
                strategy: configuredStrategy,
                results: routeResults,
                semanticCandidates,
                textCandidates,
              } satisfies RuntimeProjectionSearchPlan,
            }
          })
          yield* Effect.annotateCurrentSpan({
            "document_graph.search.semantic_candidates":
              searchPlan.semanticCandidates,
            "document_graph.search.text_candidates":
              searchPlan.textCandidates,
            "document_graph.search.limit": searchPlan.resultLimit,
          })

          const semanticQuery =
            searchPlan.routePlan.strategy._tag === "Text"
              ? undefined
              : yield* Effect.cached(prepareSemanticQuery(query))
          const expanded = yield* expandRoutes(
            query,
            searchPlan.routePlan,
            semanticQuery,
          )
          return yield* assembleResults(
            buildStreams(expanded),
            searchPlan.resultLimit,
          )
        }).pipe(
          traceDocumentGraphOperation("search", {
            "document_graph.graph": input.id,
            "document_graph.target_kind": definition.target,
            "document_graph.search.route_count": compiledRoutes.length,
            "document_graph.search.strategy":
              definition.strategy === undefined
                ? "hybrid"
                : typeof definition.strategy === "object"
                  ? definition.strategy.mode
                  : definition.strategy,
          }),
        ),
    } as unknown as GraphRetrievalHandle<
      GraphId,
      Documents,
      TargetKind,
      Routes,
      Strategy
    >
  }

  return {
    id: input.id,
    manifest: makeDocumentGraphManifest(input, relations),
    document,
    retrieval,
    parseReference,
    reconcileIndex: () =>
      Effect.gen(function*() {
        const projectionStore = yield* ProjectionIndexStore
        const projectionPrune = yield* projectionStore.pruneGraph({
          graph: input.id,
          registered,
        })
        const relationStore = yield* GraphRelationStore
        const relationPrune = yield* relationStore.pruneRelations({
          graph: input.id,
          registered: registeredRelations,
        })

        return {
          deletedRevisions: projectionPrune.deletedRevisions,
          deletedChunks: projectionPrune.deletedChunks,
          deletedRelations: relationPrune.deleted,
        }
      }).pipe(
        Effect.catchTags({
          ProjectionIndexStoreFailed:
            failStoredOperation("reconcile_index"),
          GraphRelationStoreFailed:
            failStoredOperation("reconcile_index"),
        }),
        traceDocumentGraphOperation("reconcile_index", {
          "document_graph.graph": input.id,
        }),
      ),
    search: (query, options) =>
      exposeSearchOperation(
        Effect.gen(function*() {
          const strategy = yield* schemaSearchStrategy(options)
          const searchScope = yield* parseRuntimeSearchOptions(() => ({
            ...scope({
              ...(options?.include === undefined
                ? {}
                : { include: options.include }),
              ...(options?.exclude === undefined
                ? {}
                : { exclude: options.exclude }),
              ...(options?.includeProjections === undefined
                ? {}
                : { includeProjections: options.includeProjections }),
              ...(options?.excludeProjections === undefined
                ? {}
                : { excludeProjections: options.excludeProjections }),
            }),
            registered,
          }))
          yield* Effect.annotateCurrentSpan({
            "document_graph.search.candidates": strategy.candidates,
            "document_graph.search.limit": strategy.results,
          })

          const hits = yield* searchGraph({
            query,
            scope: searchScope,
            strategy,
          })
          return yield* Effect.forEach(hits, (hit) =>
            parseSearchHit(hit, "semantic"),
          )
        }),
        {
          "document_graph.graph": input.id,
          "document_graph.search.strategy": "semantic",
        },
      ),
  }
}
