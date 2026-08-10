import { Context, Effect, Schema } from "effect"
import type { JsonValue } from "../document/json-value.js"
import type { EncodedDocumentReference } from "../document/document-instance.js"
import {
  makeChunkId,
  makeDocumentKey,
  ProjectionRevisionHashSchema,
  type ChunkId,
  type DocumentKey,
  type ProjectionRevisionHash,
} from "../document/document-identity.js"
import {
  EmbeddingProvider,
  type EmbeddingProviderFailed,
  InvalidEmbeddingOutput,
  type EmbeddingProfile,
} from "../indexing/embedding-provider.js"
import type { TextSearchPolicy } from "../document/text-search-policy.js"
import {
  ReciprocalRankConstantSchema,
  RetrievalWeightSchema,
  weightedReciprocalRankFusion,
  type InvalidRankFusionInput,
  type ReciprocalRankConstant,
  type RetrievalWeight,
} from "./rank-fusion.js"
import {
  evaluateMetadataFilter,
  metadataEquals,
  metadataOneOf,
  normalizeMetadataFilters,
  type MetadataFilter,
  type MetadataSearchPredicate,
  type MetadataSearchValue,
} from "./metadata-filter.js"

export {
  metadataEquals,
  metadataOneOf,
  type MetadataFilter,
  type MetadataSearchPredicate,
  type MetadataSearchValue,
}

/** Application input for a typed graph search scope. */
export interface GraphSearchScopeInput<
  DocumentKind extends string,
  ProjectionId extends string = string,
> {
  readonly include?: ReadonlyArray<DocumentKind>
  readonly exclude?: ReadonlyArray<DocumentKind>
  readonly includeProjections?: ReadonlyArray<ProjectionId>
  readonly excludeProjections?: ReadonlyArray<ProjectionId>
  readonly where?: ReadonlyArray<MetadataFilter>
}

/** Serializable constraints that every retrieval adapter must apply. */
export interface GraphSearchScope<GraphId extends string = string> {
  readonly graph: GraphId
  /**
   * Registered document/projection pairs for schema-bound searches.
   * Undefined keeps the lower-level storage-neutral all-projections behavior.
   */
  readonly registered: ReadonlyArray<{
    readonly documentKind: string
    readonly projection: string
  }> | undefined
  readonly includeDocumentKinds: ReadonlyArray<string>
  readonly excludeDocumentKinds: ReadonlyArray<string>
  readonly includeProjections: ReadonlyArray<string>
  readonly excludeProjections: ReadonlyArray<string>
  readonly where: ReadonlyArray<MetadataFilter>
}

/** Normalize one graph-owned search scope for retrieval adapters. */
export const makeGraphSearchScope = <
  const GraphId extends string,
  DocumentKind extends string,
  ProjectionId extends string,
>(
  graph: GraphId,
  input: GraphSearchScopeInput<DocumentKind, ProjectionId>,
  registered?: ReadonlyArray<{
    readonly documentKind: string
    readonly projection: string
  }>,
): GraphSearchScope<GraphId> => ({
  graph,
  registered: registered?.map((target) => ({ ...target })),
  includeDocumentKinds: [...(input.include ?? [])],
  excludeDocumentKinds: [...(input.exclude ?? [])],
  includeProjections: [...(input.includeProjections ?? [])],
  excludeProjections: [...(input.excludeProjections ?? [])],
  where: normalizeMetadataFilters(input.where ?? []),
})

/** Whether one document projection is included by a normalized graph scope. */
export const projectionMatchesGraphSearchScope = (
  scope: GraphSearchScope,
  target: {
    readonly graph: string
    readonly documentKind: string
    readonly projection: string
  },
): boolean => {
  if (target.graph !== scope.graph) {
    return false
  }

  if (
    scope.registered !== undefined &&
    !scope.registered.some(
      (registered) =>
        registered.documentKind === target.documentKind &&
        registered.projection === target.projection,
    )
  ) {
    return false
  }

  if (
    scope.includeDocumentKinds.length > 0 &&
    !scope.includeDocumentKinds.includes(target.documentKind)
  ) {
    return false
  }
  if (scope.excludeDocumentKinds.includes(target.documentKind)) {
    return false
  }
  if (
    scope.includeProjections.length > 0 &&
    !scope.includeProjections.includes(target.projection)
  ) {
    return false
  }

  return !scope.excludeProjections.includes(target.projection)
}

/** Positive number of candidates or final results in one retrieval channel. */
export const SearchResultCountSchema = Schema.Number.pipe(
  Schema.int(),
  Schema.between(1, 10_000),
  Schema.brand("SearchResultCount"),
)

/** Positive number of candidates or final results in one retrieval channel. */
export type SearchResultCount = Schema.Schema.Type<
  typeof SearchResultCountSchema
>

/** Configured semantic candidate-generation strategy. */
export interface SemanticSearchStrategy {
  readonly _tag: "Semantic"
  readonly candidates: SearchResultCount
  readonly results: SearchResultCount
  readonly weight: RetrievalWeight
  readonly rankConstant: ReciprocalRankConstant
}

/** Configured text candidate-generation strategy. */
export interface TextSearchStrategy {
  readonly _tag: "Text"
  readonly candidates: SearchResultCount
  readonly results: SearchResultCount
  readonly policy: Exclude<TextSearchPolicy, "disabled">
  readonly weight: RetrievalWeight
  readonly rankConstant: ReciprocalRankConstant
}

/** Configure semantic candidate generation and final result count. */
export const semantic = (input: {
  readonly candidates?: number
  readonly results?: number
  readonly weight?: number
  readonly rankConstant?: number
} = {}): SemanticSearchStrategy => {
  const candidates = Schema.decodeSync(SearchResultCountSchema)(
    input.candidates ?? 50,
  )
  const results = Schema.decodeSync(SearchResultCountSchema)(
    input.results ?? 10,
  )

  if (results > candidates) {
    throw new Error(
      "Semantic search results cannot exceed the candidate count",
    )
  }

  return {
    _tag: "Semantic",
    candidates,
    results,
    weight: Schema.decodeSync(RetrievalWeightSchema)(input.weight ?? 1),
    rankConstant: Schema.decodeSync(ReciprocalRankConstantSchema)(
      input.rankConstant ?? 60,
    ),
  }
}

/** Configure text candidate generation for one text-enabled projection. */
export const text = (input: {
  readonly policy: Exclude<TextSearchPolicy, "disabled">
  readonly candidates?: number
  readonly results?: number
  readonly weight?: number
  readonly rankConstant?: number
}): TextSearchStrategy => {
  const candidates = Schema.decodeSync(SearchResultCountSchema)(
    input.candidates ?? 50,
  )
  const results = Schema.decodeSync(SearchResultCountSchema)(
    input.results ?? 10,
  )

  if (results > candidates) {
    throw new Error("Text search results cannot exceed the candidate count")
  }

  return {
    _tag: "Text",
    candidates,
    results,
    policy: input.policy,
    weight: Schema.decodeSync(RetrievalWeightSchema)(input.weight ?? 1),
    rankConstant: Schema.decodeSync(ReciprocalRankConstantSchema)(
      input.rankConstant ?? 60,
    ),
  }
}

/** Request sent to a vector-search storage adapter. */
export interface SemanticCandidateRequest {
  readonly vector: ReadonlyArray<number>
  readonly embeddingProfile: EmbeddingProfile
  readonly scope: GraphSearchScope
  readonly candidates: SearchResultCount
}

/** One ranked storage candidate before the public result limit is applied. */
export interface CandidateFields {
  /** Higher scores must represent better matches. */
  readonly score: number
  readonly chunkId: ChunkId
  readonly documentKey: DocumentKey
  readonly reference: EncodedDocumentReference
  readonly projection: {
    readonly id: string
    readonly version: string
  }
  /** Complete projection revision from which this candidate was loaded. */
  readonly revisionHash: ProjectionRevisionHash
  readonly sectionKey: string
  /** Stable logical position of this chunk within its attributed section. */
  readonly sectionPart: number
  readonly content: string
  readonly metadata: JsonValue | undefined
}

/** One ranked semantic storage candidate. */
export interface SemanticSearchCandidate extends CandidateFields {}

/** Request sent to a text-search storage adapter. */
export interface TextCandidateRequest {
  readonly query: string
  readonly policy: Exclude<TextSearchPolicy, "disabled">
  readonly scope: GraphSearchScope
  readonly candidates: SearchResultCount
}

/** One ranked text storage candidate. */
export interface TextSearchCandidate extends CandidateFields {}

/** A vector-search store could not retrieve semantic candidates. */
export class ProjectionSearchStoreFailed extends Schema.TaggedError<
  ProjectionSearchStoreFailed
>()("ProjectionSearchStoreFailed", {
  reason: Schema.Literal("unavailable", "invalid_stored_state"),
  cause: Schema.Unknown,
}) {}

/** Candidate retrieval capability implemented by a vector-store adapter. */
export interface ProjectionSearchStoreService {
  readonly searchCandidates: (
    request: SemanticCandidateRequest,
  ) => Effect.Effect<
    ReadonlyArray<SemanticSearchCandidate>,
    ProjectionSearchStoreFailed
  >
}

/** Effect service tag for semantic candidate retrieval. */
export class ProjectionSearchStore extends Context.Tag(
  "@popcomputer/document-graph/ProjectionSearchStore",
)<ProjectionSearchStore, ProjectionSearchStoreService>() {}

/** A text-search store could not retrieve lexical candidates. */
export class ProjectionTextSearchStoreFailed extends Schema.TaggedError<
  ProjectionTextSearchStoreFailed
>()("ProjectionTextSearchStoreFailed", {
  reason: Schema.Literal("unavailable", "invalid_stored_state"),
  cause: Schema.Unknown,
}) {}

/** Candidate retrieval capability implemented by a text-search adapter. */
export interface ProjectionTextSearchStoreService {
  readonly searchTextCandidates: (
    request: TextCandidateRequest,
  ) => Effect.Effect<
    ReadonlyArray<TextSearchCandidate>,
    ProjectionTextSearchStoreFailed
  >
}

/** Effect service tag for text candidate retrieval. */
export class ProjectionTextSearchStore extends Context.Tag(
  "@popcomputer/document-graph/ProjectionTextSearchStore",
)<ProjectionTextSearchStore, ProjectionTextSearchStoreService>() {}

/** A retrieval query did not satisfy the public search contract. */
export class InvalidSearchQuery extends Schema.TaggedError<
  InvalidSearchQuery
>()("InvalidSearchQuery", {
  reason: Schema.Literal(
    "empty",
    "too_long",
    "text_disabled",
    "invalid_options",
  ),
}) {}

/** A search adapter returned candidates that violated its contract. */
export class InvalidSearchOutput extends Schema.TaggedError<
  InvalidSearchOutput
>()("InvalidSearchOutput", {
  channel: Schema.Literal("semantic", "text", "fusion"),
  reason: Schema.Literal(
    "too_many_candidates",
    "invalid_score",
    "invalid_content",
    "invalid_identity",
    "duplicate_chunk",
    "conflicting_chunk",
    "out_of_scope",
    "not_ranked",
    "invalid_metadata",
  ),
}) {}

/** Retrieval channels currently supported by projection search. */
export type RetrievalChannel = "semantic" | "text"

/** Structural identity for a projection retrieval route. */
export interface ProjectionRetrievalRouteKey {
  readonly _tag: "Projection"
  readonly sourceKind: string
  readonly projection: string
}

/** Structural identity for retrieval through one declared relation. */
export interface RelationRetrievalRouteKey {
  readonly _tag: "Relation"
  readonly sourceKind: string
  readonly projection: string
  readonly relation: string
  readonly direction: "outgoing"
  readonly targetKind: string
}

/** Structural identity for a direct or relation-backed retrieval route. */
export type RetrievalRouteKey =
  | ProjectionRetrievalRouteKey
  | RelationRetrievalRouteKey

/** Structural identity for one route/channel candidate stream. */
export interface RetrievalStreamKey {
  readonly route: RetrievalRouteKey
  readonly channel: RetrievalChannel
}

/** One explainable channel contribution to a search result. */
export interface SearchSignal {
  readonly stream: RetrievalStreamKey
  readonly rank: number
  /** Raw adapter score, comparable only within its originating stream. */
  readonly score: number
  readonly weight: number
  readonly contribution: number
}

/** One compact, attributed retrieval result ready for hydration. */
export interface SearchHit {
  /** One-based rank after the selected retrieval strategy. */
  readonly rank: number
  /** Higher scores represent better matches within one result set. */
  readonly score: number
  readonly signals: readonly [
    SearchSignal,
    ...ReadonlyArray<SearchSignal>,
  ]
  readonly chunkId: ChunkId
  readonly documentKey: DocumentKey
  readonly reference: EncodedDocumentReference
  readonly projection: {
    readonly id: string
    readonly version: string
  }
  readonly revisionHash: ProjectionRevisionHash
  readonly sectionKey: string
  readonly sectionPart: number
  readonly content: string
  readonly metadata: JsonValue | undefined
}

/** Input for graph-scoped semantic retrieval. */
export interface SearchGraphInput {
  readonly query: string
  readonly scope: GraphSearchScope
  readonly strategy: SemanticSearchStrategy
}

/** Input for graph-scoped text retrieval. */
export interface SearchGraphTextInput {
  readonly query: string
  readonly scope: GraphSearchScope
  readonly strategy: TextSearchStrategy
}

/** Input for concurrent semantic/text projection retrieval and fusion. */
export interface SearchGraphHybridInput {
  readonly query: string
  readonly scope: GraphSearchScope
  readonly route: ProjectionRetrievalRouteKey
  readonly semantic: SemanticSearchStrategy
  readonly text: TextSearchStrategy
  readonly results: SearchResultCount
  readonly rankConstant: ReciprocalRankConstant
}

/** Expected failures produced by graph-scoped semantic retrieval. */
export type SearchGraphError =
  | InvalidSearchQuery
  | EmbeddingProviderFailed
  | InvalidEmbeddingOutput
  | ProjectionSearchStoreFailed
  | ProjectionTextSearchStoreFailed
  | InvalidSearchOutput

const parseSearchQuery = (
  query: string,
): Effect.Effect<string, InvalidSearchQuery> => {
  if (query.trim().length === 0) {
    return Effect.fail(new InvalidSearchQuery({ reason: "empty" }))
  }

  if (query.length > 32_000) {
    return Effect.fail(new InvalidSearchQuery({ reason: "too_long" }))
  }

  return Effect.succeed(query)
}

const validateQueryVector = (
  profile: EmbeddingProfile,
  vector: ReadonlyArray<number>,
): Effect.Effect<ReadonlyArray<number>, InvalidEmbeddingOutput> => {
  if (vector.length !== profile.dimensions) {
    return Effect.fail(
      new InvalidEmbeddingOutput({
        profile: profile.id,
        reason: "wrong_dimensions",
      }),
    )
  }

  if (vector.some((component) => !Number.isFinite(component))) {
    return Effect.fail(
      new InvalidEmbeddingOutput({
        profile: profile.id,
        reason: "invalid_number",
      }),
    )
  }

  return Effect.succeed(vector)
}

const candidateMatchesScope = (
  candidate: CandidateFields,
  scope: GraphSearchScope,
): boolean => {
  if (
    !projectionMatchesGraphSearchScope(scope, {
      graph: candidate.reference.graph,
      documentKind: candidate.reference.kind,
      projection: candidate.projection.id,
    })
  ) {
    return false
  }

  return scope.where.every((filter) =>
    evaluateMetadataFilter(candidate.metadata, filter),
  )
}

const candidateHasValidIdentity = (
  candidate: CandidateFields,
): boolean => {
  if (
    !Schema.is(ProjectionRevisionHashSchema)(candidate.revisionHash) ||
    !Number.isSafeInteger(candidate.sectionPart) ||
    candidate.sectionPart < 0
  ) {
    return false
  }

  const documentKey = makeDocumentKey({
    graph: candidate.reference.graph,
    documentKind: candidate.reference.kind,
    encodedId: candidate.reference.id,
  })
  if (candidate.documentKey !== documentKey) {
    return false
  }

  return (
    candidate.chunkId ===
    makeChunkId({
      documentKey,
      projection: candidate.projection.id,
      sectionKey: candidate.sectionKey,
      sectionPart: candidate.sectionPart,
    })
  )
}

const validateCandidates = <Candidate extends CandidateFields>(
  channel: "semantic" | "text",
  candidates: ReadonlyArray<Candidate>,
  input: {
    readonly scope: GraphSearchScope
    readonly candidates: SearchResultCount
  },
): Effect.Effect<
  ReadonlyArray<Candidate>,
  InvalidSearchOutput
> => {
  if (candidates.length > input.candidates) {
    return Effect.fail(
      new InvalidSearchOutput({ channel, reason: "too_many_candidates" }),
    )
  }

  const seen = new Set<ChunkId>()
  let previousScore: number | undefined
  for (const candidate of candidates) {
    if (
      !Number.isFinite(candidate.score) ||
      (channel === "text" && candidate.score <= 0)
    ) {
      return Effect.fail(
        new InvalidSearchOutput({ channel, reason: "invalid_score" }),
      )
    }

    if (candidate.content.trim().length === 0) {
      return Effect.fail(
        new InvalidSearchOutput({ channel, reason: "invalid_content" }),
      )
    }

    if (!candidateHasValidIdentity(candidate)) {
      return Effect.fail(
        new InvalidSearchOutput({ channel, reason: "invalid_identity" }),
      )
    }

    if (seen.has(candidate.chunkId)) {
      return Effect.fail(
        new InvalidSearchOutput({ channel, reason: "duplicate_chunk" }),
      )
    }

    if (!candidateMatchesScope(candidate, input.scope)) {
      return Effect.fail(
        new InvalidSearchOutput({ channel, reason: "out_of_scope" }),
      )
    }

    if (
      previousScore !== undefined &&
      candidate.score > previousScore
    ) {
      return Effect.fail(
        new InvalidSearchOutput({ channel, reason: "not_ranked" }),
      )
    }

    seen.add(candidate.chunkId)
    previousScore = candidate.score
  }

  return Effect.succeed(candidates)
}

const selectSearchHits = (
  candidates: ReadonlyArray<CandidateFields>,
  strategy: SemanticSearchStrategy | TextSearchStrategy,
): ReadonlyArray<SearchHit> =>
  candidates.slice(0, strategy.results).map((candidate, index) => {
    const rank = index + 1
    const contribution =
      strategy.weight / (strategy.rankConstant + rank)
    const channel =
      strategy._tag === "Semantic" ? "semantic" : "text"

    return {
      rank,
      score: contribution,
      signals: [
        {
          stream: {
            route: {
              _tag: "Projection",
              sourceKind: candidate.reference.kind,
              projection: candidate.projection.id,
            },
            channel,
          },
          rank,
          score: candidate.score,
          weight: strategy.weight,
          contribution,
        },
      ],
      chunkId: candidate.chunkId,
      documentKey: candidate.documentKey,
      reference: candidate.reference,
      projection: candidate.projection,
      revisionHash: candidate.revisionHash,
      sectionKey: candidate.sectionKey,
      sectionPart: candidate.sectionPart,
      content: candidate.content,
      metadata: candidate.metadata,
    }
  })

/** One parsed and embedded query reusable across semantic retrieval routes. */
export interface PreparedSemanticQuery {
  readonly query: string
  readonly vector: ReadonlyArray<number>
  readonly embeddingProfile: EmbeddingProfile
}

/** Parse and embed a semantic query exactly once for a retrieval operation. */
export const prepareSemanticQuery = (
  queryInput: string,
): Effect.Effect<
  PreparedSemanticQuery,
  InvalidSearchQuery | EmbeddingProviderFailed | InvalidEmbeddingOutput,
  EmbeddingProvider
> =>
  Effect.gen(function*() {
    const embeddings = yield* EmbeddingProvider
    const query = yield* parseSearchQuery(queryInput)
    const vector = yield* embeddings.embedQuery(query).pipe(
      Effect.flatMap((output) =>
        validateQueryVector(embeddings.profile, output),
      ),
    )

    return {
      query,
      vector,
      embeddingProfile: embeddings.profile,
    }
  })

/** Execute semantic retrieval with a query prepared by the package runtime. */
export const searchGraphWithPreparedSemanticQuery = (
  input: Omit<SearchGraphInput, "query"> & {
    readonly query: PreparedSemanticQuery
  },
): Effect.Effect<
  ReadonlyArray<SearchHit>,
  ProjectionSearchStoreFailed | InvalidSearchOutput,
  ProjectionSearchStore
> =>
  Effect.gen(function*() {
    const store = yield* ProjectionSearchStore
    const candidates = yield* store.searchCandidates({
      vector: input.query.vector,
      embeddingProfile: input.query.embeddingProfile,
      scope: input.scope,
      candidates: input.strategy.candidates,
    })
    const validated = yield* validateCandidates("semantic", candidates, {
      scope: input.scope,
      candidates: input.strategy.candidates,
    })

    return selectSearchHits(validated, input.strategy)
  })

/**
 * Run semantic candidate retrieval within one graph-owned scope.
 *
 * Storage adapters must apply the scope before ANN limiting. The core validates
 * every returned candidate again so an adapter cannot expose out-of-scope data.
 */
export const searchGraph = (
  input: SearchGraphInput,
): Effect.Effect<
  ReadonlyArray<SearchHit>,
  SearchGraphError,
  EmbeddingProvider | ProjectionSearchStore
> =>
  Effect.gen(function*() {
    const query = yield* prepareSemanticQuery(input.query)
    return yield* searchGraphWithPreparedSemanticQuery({
      query,
      scope: input.scope,
      strategy: input.strategy,
    })
  })

/**
 * Run text candidate retrieval within one graph-owned scope.
 *
 * Storage adapters must apply the complete scope before limiting candidates.
 * The core validates every returned candidate again at the trust boundary.
 */
export const searchGraphText = (
  input: SearchGraphTextInput,
): Effect.Effect<
  ReadonlyArray<SearchHit>,
  InvalidSearchQuery | ProjectionTextSearchStoreFailed | InvalidSearchOutput,
  ProjectionTextSearchStore
> =>
  Effect.gen(function*() {
    const query = yield* parseSearchQuery(input.query)
    return yield* searchGraphTextWithParsedQuery({
      query,
      scope: input.scope,
      strategy: input.strategy,
    })
  })

const searchGraphTextWithParsedQuery = (
  input: SearchGraphTextInput,
): Effect.Effect<
  ReadonlyArray<SearchHit>,
  ProjectionTextSearchStoreFailed | InvalidSearchOutput,
  ProjectionTextSearchStore
> =>
  Effect.gen(function*() {
    const store = yield* ProjectionTextSearchStore
    const candidates = yield* store.searchTextCandidates({
      query: input.query,
      policy: input.strategy.policy,
      scope: input.scope,
      candidates: input.strategy.candidates,
    })
    const validated = yield* validateCandidates("text", candidates, {
      scope: input.scope,
      candidates: input.strategy.candidates,
    })

    return selectSearchHits(validated, input.strategy)
  })

const hybridOutputError = (
  error: InvalidRankFusionInput,
): InvalidSearchOutput =>
  new InvalidSearchOutput({
    channel: error.stream === "text" ? "text" : "semantic",
    reason:
      error.reason === "duplicate_key"
        ? "duplicate_chunk"
        : "invalid_score",
  })

const jsonValuesEqual = (left: JsonValue, right: JsonValue): boolean => {
  if (
    left === null ||
    right === null ||
    typeof left !== "object" ||
    typeof right !== "object"
  ) {
    return Object.is(left, right)
  }

  if (Array.isArray(left)) {
    return (
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => {
        const other = right[index]
        return other !== undefined && jsonValuesEqual(value, other)
      })
    )
  }
  if (Array.isArray(right)) return false

  // SAFETY: Primitive, null, and array branches were eliminated above, so
  // both JSON values are records whose values remain JSON-safe.
  const leftRecord = left as Readonly<Record<string, JsonValue>>
  const rightRecord = right as Readonly<Record<string, JsonValue>>
  const leftKeys = Object.keys(leftRecord).sort()
  const rightKeys = Object.keys(rightRecord).sort()
  if (
    leftKeys.length !== rightKeys.length ||
    leftKeys.some((key, index) => key !== rightKeys[index])
  ) {
    return false
  }

  return leftKeys.every((key) => {
    const leftValue = leftRecord[key]
    const rightValue = rightRecord[key]
    return (
      leftValue !== undefined &&
      rightValue !== undefined &&
      jsonValuesEqual(leftValue, rightValue)
    )
  })
}

const optionalJsonValuesEqual = (
  left: JsonValue | undefined,
  right: JsonValue | undefined,
): boolean =>
  left === undefined || right === undefined
    ? left === right
    : jsonValuesEqual(left, right)

const candidatePayloadsEqual = (
  left: SearchHit,
  right: SearchHit,
): boolean =>
  left.documentKey === right.documentKey &&
  left.reference.graph === right.reference.graph &&
  left.reference.kind === right.reference.kind &&
  jsonValuesEqual(left.reference.id, right.reference.id) &&
  left.projection.id === right.projection.id &&
  left.projection.version === right.projection.version &&
  left.revisionHash === right.revisionHash &&
  left.sectionKey === right.sectionKey &&
  left.sectionPart === right.sectionPart &&
  left.content === right.content &&
  optionalJsonValuesEqual(left.metadata, right.metadata)

const validateHybridCandidateAgreement = (
  semanticHits: ReadonlyArray<SearchHit>,
  textHits: ReadonlyArray<SearchHit>,
): Effect.Effect<void, InvalidSearchOutput> => {
  const textByChunk = new Map(
    textHits.map((hit) => [hit.chunkId, hit] as const),
  )
  for (const semanticHit of semanticHits) {
    const textHit = textByChunk.get(semanticHit.chunkId)
    if (
      textHit !== undefined &&
      !candidatePayloadsEqual(semanticHit, textHit)
    ) {
      return Effect.fail(
        new InvalidSearchOutput({
          channel: "fusion",
          reason: "conflicting_chunk",
        }),
      )
    }
  }

  return Effect.void
}

const fuseHybridSearchHits = (
  input: Omit<SearchGraphHybridInput, "query">,
  semanticHits: ReadonlyArray<SearchHit>,
  textHits: ReadonlyArray<SearchHit>,
): Effect.Effect<
  ReadonlyArray<SearchHit>,
  InvalidSearchOutput
> =>
  Effect.gen(function*() {
    yield* validateHybridCandidateAgreement(semanticHits, textHits)

    const fused = yield* weightedReciprocalRankFusion({
      rankConstant: input.rankConstant,
      streams: [
        {
          id: "semantic",
          signal: { route: input.route, channel: "semantic" as const },
          weight: input.semantic.weight,
          items: semanticHits.map((hit) => ({
            key: hit.chunkId,
            score: hit.signals[0].score,
            value: hit,
          })),
        },
        {
          id: "text",
          signal: { route: input.route, channel: "text" as const },
          weight: input.text.weight,
          items: textHits.map((hit) => ({
            key: hit.chunkId,
            score: hit.signals[0].score,
            value: hit,
          })),
        },
      ],
    }).pipe(Effect.mapError(hybridOutputError))

    return fused.slice(0, input.results).map((result, index) => ({
      rank: index + 1,
      score: result.score,
      signals: result.signals,
      chunkId: result.value.chunkId,
      documentKey: result.value.documentKey,
      reference: result.value.reference,
      projection: result.value.projection,
      revisionHash: result.value.revisionHash,
      sectionKey: result.value.sectionKey,
      sectionPart: result.value.sectionPart,
      content: result.value.content,
      metadata: result.value.metadata,
    }))
  })

/** A semantic query computation shared by every route in one operation. */
export type SemanticQueryPreparation = Effect.Effect<
  PreparedSemanticQuery,
  InvalidSearchQuery | EmbeddingProviderFailed | InvalidEmbeddingOutput,
  EmbeddingProvider
>

/** Execute hybrid retrieval while sharing one semantic query computation. */
export const searchGraphHybridWithSemanticQuery = (
  input: SearchGraphHybridInput & {
    readonly semanticQuery: SemanticQueryPreparation
  },
): Effect.Effect<
  ReadonlyArray<SearchHit>,
  SearchGraphError,
  | EmbeddingProvider
  | ProjectionSearchStore
  | ProjectionTextSearchStore
> =>
  Effect.gen(function*() {
    const [semanticHits, textHits] = yield* Effect.all(
      [
        input.semanticQuery.pipe(
          Effect.flatMap((query) =>
            searchGraphWithPreparedSemanticQuery({
              query,
              scope: input.scope,
              strategy: input.semantic,
            }),
          ),
        ),
        searchGraphText({
          query: input.query,
          scope: input.scope,
          strategy: input.text,
        }),
      ],
      { concurrency: "unbounded" },
    )

    return yield* fuseHybridSearchHits(input, semanticHits, textHits)
  })

/** Run semantic and text retrieval concurrently, then fuse ranks by chunk ID. */
export const searchGraphHybrid = (
  input: SearchGraphHybridInput,
): Effect.Effect<
  ReadonlyArray<SearchHit>,
  SearchGraphError,
  | EmbeddingProvider
  | ProjectionSearchStore
  | ProjectionTextSearchStore
> =>
  searchGraphHybridWithSemanticQuery({
    ...input,
    semanticQuery: prepareSemanticQuery(input.query),
  })
