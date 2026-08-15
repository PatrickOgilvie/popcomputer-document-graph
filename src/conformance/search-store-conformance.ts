import { Effect, Option, Schema } from "effect"
import {
  makeGraphSearchScope,
  metadataEquals,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
  SearchResultCountSchema,
  type CandidateFields,
  type GraphSearchScope,
  type SemanticCandidateRequest,
  type SemanticSearchCandidate,
  type TextCandidateRequest,
  type TextSearchCandidate,
} from "../retrieval/graph-retrieval.js"
import {
  ChunkIdSchema,
  ContentHashSchema,
  DocumentKeySchema,
  ProjectionRevisionHashSchema,
} from "../document/document-identity.js"
import {
  ProjectionIndexStore,
  type ReplaceProjectedRevision,
} from "../indexing/projection-index.js"
import {
  defineEmbeddingProfile,
  type EmbeddingProfile,
} from "../indexing/embedding-provider.js"
import {
  parseTextSearchPolicy,
  type TextSearchPolicy,
} from "../document/text-search-policy.js"
import {
  verifyGraphRelationStoreConformance,
  type GraphRelationStoreConformanceReport,
} from "./graph-relation-conformance.js"
import {
  verifyProjectionIndexStoreConformance,
  type ProjectionIndexStoreConformanceReport,
} from "./projection-index-conformance.js"

export {
  makeProjectionIndexStoreConformanceFixture,
  ProjectionIndexStoreConformanceLawSchema,
  ProjectionIndexStoreConformanceViolation,
  verifyProjectionIndexStoreConformance,
  type ProjectionIndexStoreConformanceFixture,
  type ProjectionIndexStoreConformanceLaw,
  type ProjectionIndexStoreConformanceReport,
} from "./projection-index-conformance.js"

export {
  GraphRelationStoreConformanceLawSchema,
  GraphRelationStoreConformanceViolation,
  makeGraphRelationStoreConformanceFixture,
  verifyGraphRelationStoreConformance,
  type GraphRelationStoreConformanceFixture,
  type GraphRelationStoreConformanceLaw,
  type GraphRelationStoreConformanceReport,
} from "./graph-relation-conformance.js"

/** Retrieval laws checked against every candidate-store adapter. */
export const SearchStoreConformanceLawSchema = Schema.Literals([
  "scope_before_limit",
  "candidate_bound",
  "score_order",
  "unique_chunks",
  "boundary_validity",
  "positive_text_score",
  "stable_ties",
  "repeatability",
])

/** Retrieval law checked against every candidate-store adapter. */
export type SearchStoreConformanceLaw =
  typeof SearchStoreConformanceLawSchema.Type

/** A candidate store violated one adapter-independent retrieval law. */
export class SearchStoreConformanceViolation extends Schema.TaggedError<
  SearchStoreConformanceViolation
>()("SearchStoreConformanceViolation", {
  channel: Schema.Literals(["semantic", "text"]),
  law: SearchStoreConformanceLawSchema,
}) {}

/** Successful evidence that one candidate channel passed every retrieval law. */
export interface SearchStoreConformanceReport {
  readonly channel: "semantic" | "text"
  readonly verified: ReadonlyArray<SearchStoreConformanceLaw>
}

/** Deterministic fixture exposed for adapter-specific conformance support. */
export interface SearchStoreConformanceFixture {
  readonly replacements: readonly [
    ReplaceProjectedRevision,
    ...ReadonlyArray<ReplaceProjectedRevision>,
  ]
  readonly semanticProfile: EmbeddingProfile
  readonly semanticRequest: SemanticCandidateRequest
  readonly textRequest: TextCandidateRequest
  readonly scopedSemanticRequest: SemanticCandidateRequest
  readonly scopedTextRequest: TextCandidateRequest
  readonly expectedSemantic: readonly [
    SemanticSearchCandidate,
    SemanticSearchCandidate,
    SemanticSearchCandidate,
    SemanticSearchCandidate,
  ]
  readonly expectedText: readonly [
    TextSearchCandidate,
    TextSearchCandidate,
    TextSearchCandidate,
  ]
}

const GraphId = "@popcomputer/document-graph/conformance"
const ProjectionId = "evidence"
const ProjectionVersion = "v1"
const CandidateLimit = Schema.decodeSync(SearchResultCountSchema)(10)
const ScopedCandidateLimit = Schema.decodeSync(SearchResultCountSchema)(1)

const semanticProfile = defineEmbeddingProfile({
  id: "honertia:conformance",
  version: "v1",
  dimensions: 2,
})

const parseEnabledTextPolicy = (): Exclude<
  TextSearchPolicy,
  "disabled"
> => {
  const policy = parseTextSearchPolicy({
    language: "simple",
    weights: { context: 0, label: 0, content: 1 },
  })
  if (policy === "disabled") {
    throw new Error("The conformance text policy must remain enabled")
  }

  return policy
}

const textPolicy = parseEnabledTextPolicy()

const makeScope = (
  input: Parameters<typeof makeGraphSearchScope>[1],
): GraphSearchScope =>
  makeGraphSearchScope(GraphId, input, [
    { documentKind: "Included", projection: ProjectionId },
    { documentKind: "Excluded", projection: ProjectionId },
  ])

const broadScope = makeScope({})
const scopedScope = makeScope({
  include: ["Included"],
  exclude: ["Excluded"],
  includeProjections: [ProjectionId],
  excludeProjections: ["retired"],
  where: [metadataEquals("visibility", "public")],
})

const makeReplacement = (input: {
  readonly identity: string
  readonly chunkIdentity: string
  readonly revisionIdentity: string
  readonly contentIdentity: string
  readonly kind: "Included" | "Excluded"
  readonly id: string
  readonly content: string
  readonly visibility: "public" | "private"
  readonly vector: readonly [number, number]
  readonly context?: string
  readonly label?: string
}): ReplaceProjectedRevision => {
  const documentKey = Schema.decodeSync(DocumentKeySchema)(
    input.identity.repeat(64),
  )
  const chunkId = Schema.decodeSync(ChunkIdSchema)(
    input.chunkIdentity.repeat(64),
  )
  const contentHash = Schema.decodeSync(ContentHashSchema)(
    input.contentIdentity.repeat(64),
  )

  return {
    key: { documentKey, projection: ProjectionId },
    expectedToken: Option.none(),
    encodedTarget: {
      graph: GraphId,
      kind: input.kind,
      id: input.id,
    },
    projectionVersion: ProjectionVersion,
    revisionHash: Schema.decodeSync(ProjectionRevisionHashSchema)(
      input.revisionIdentity.repeat(64),
    ),
    embeddingProfile: semanticProfile,
    chunks: [
      {
        chunkId,
        contentHash,
        ordinal: 0,
        sectionKey: "body",
        sectionIndex: 0,
        sectionPart: 0,
        content: input.content,
        embeddingContent: input.content,
        text: {
          context: input.context,
          label: input.label,
          content: input.content,
        },
        metadata: { visibility: input.visibility },
      },
    ],
    embeddings: [{ contentHash, vector: input.vector }],
  }
}

const tieFirst = makeReplacement({
  identity: "1",
  chunkIdentity: "a",
  revisionIdentity: "d",
  contentIdentity: "4",
  kind: "Included",
  id: "tie-first",
  content: "needle alpha",
  visibility: "public",
  vector: [0.8, 0.2],
})
const tieSecond = makeReplacement({
  identity: "2",
  chunkIdentity: "b",
  revisionIdentity: "e",
  contentIdentity: "5",
  kind: "Included",
  id: "tie-second",
  content: "needle beta",
  visibility: "public",
  vector: [0.8, 0.2],
})
const excludedHighScore = makeReplacement({
  identity: "3",
  chunkIdentity: "c",
  revisionIdentity: "f",
  contentIdentity: "6",
  kind: "Excluded",
  id: "excluded-high-score",
  content: "needle needle needle excluded",
  visibility: "private",
  vector: [1, 0],
})
const zeroWeightedContextOnly = makeReplacement({
  identity: "7",
  chunkIdentity: "d",
  revisionIdentity: "8",
  contentIdentity: "9",
  kind: "Included",
  id: "zero-weighted-context-only",
  context: "needle guide",
  content: "completely unrelated words",
  visibility: "public",
  vector: [0.2, 0.8],
})

const candidateFrom = (
  replacement: ReplaceProjectedRevision,
  score: number,
): CandidateFields => {
  const chunk = replacement.chunks[0]
  return {
    score,
    chunkId: chunk.chunkId,
    documentKey: replacement.key.documentKey,
    reference: replacement.encodedTarget,
    projection: {
      id: replacement.key.projection,
      version: replacement.projectionVersion,
    },
    revisionHash: replacement.revisionHash,
    sectionKey: chunk.sectionKey,
    sectionPart: chunk.sectionPart,
    content: chunk.content,
    metadata: chunk.metadata,
  }
}

const tieSemanticScore = 0.8 / Math.sqrt(0.8 ** 2 + 0.2 ** 2)
const zeroWeightedSemanticScore =
  0.2 / Math.sqrt(0.2 ** 2 + 0.8 ** 2)

/** Create the deterministic records and expected candidates used by the harness. */
export const makeSearchStoreConformanceFixture =
  (): SearchStoreConformanceFixture => {
    const expectedSemantic = [
      candidateFrom(excludedHighScore, 1),
      candidateFrom(tieFirst, tieSemanticScore),
      candidateFrom(tieSecond, tieSemanticScore),
      candidateFrom(zeroWeightedContextOnly, zeroWeightedSemanticScore),
    ] as const
    const expectedText = [
      candidateFrom(excludedHighScore, 3),
      candidateFrom(tieFirst, 1),
      candidateFrom(tieSecond, 1),
    ] as const

    return {
      replacements: [
        tieFirst,
        tieSecond,
        excludedHighScore,
        zeroWeightedContextOnly,
      ],
      semanticProfile,
      semanticRequest: {
        vector: [1, 0],
        embeddingProfile: semanticProfile,
        scope: broadScope,
        candidates: CandidateLimit,
      },
      textRequest: {
        query: "needle",
        policy: textPolicy,
        scope: broadScope,
        candidates: CandidateLimit,
      },
      scopedSemanticRequest: {
        vector: [1, 0],
        embeddingProfile: semanticProfile,
        scope: scopedScope,
        candidates: ScopedCandidateLimit,
      },
      scopedTextRequest: {
        query: "needle",
        policy: textPolicy,
        scope: scopedScope,
        candidates: ScopedCandidateLimit,
      },
      expectedSemantic,
      expectedText,
    }
  }

const conformanceViolation = (
  channel: "semantic" | "text",
  law: SearchStoreConformanceLaw,
): SearchStoreConformanceViolation =>
  new SearchStoreConformanceViolation({ channel, law })

const sameCandidateIdentity = (
  left: CandidateFields,
  right: CandidateFields,
): boolean =>
  left.chunkId === right.chunkId &&
  left.documentKey === right.documentKey &&
  left.reference.graph === right.reference.graph &&
  left.reference.kind === right.reference.kind &&
  left.reference.id === right.reference.id &&
  left.projection.id === right.projection.id &&
  left.projection.version === right.projection.version &&
  left.sectionKey === right.sectionKey &&
  left.content === right.content &&
  JSON.stringify(left.metadata) === JSON.stringify(right.metadata)

const sameCandidateOrder = (
  left: ReadonlyArray<CandidateFields>,
  right: ReadonlyArray<CandidateFields>,
): boolean =>
  left.length === right.length &&
  left.every((candidate, index) => {
    const expected = right[index]
    return (
      expected !== undefined &&
      sameCandidateIdentity(candidate, expected)
    )
  })

const sameCandidateResults = (
  left: ReadonlyArray<CandidateFields>,
  right: ReadonlyArray<CandidateFields>,
): boolean =>
  sameCandidateOrder(left, right) &&
  left.every((candidate, index) => candidate.score === right[index]?.score)

const verifyCandidates = (
  channel: "semantic" | "text",
  candidates: ReadonlyArray<CandidateFields>,
  expected: ReadonlyArray<CandidateFields>,
  request: { readonly scope: GraphSearchScope; readonly candidates: number },
): Effect.Effect<void, SearchStoreConformanceViolation> => {
  if (candidates.length > request.candidates) {
    return Effect.fail(conformanceViolation(channel, "candidate_bound"))
  }

  const seen = new Set<string>()
  let previous: CandidateFields | undefined
  for (const candidate of candidates) {
    if (seen.has(candidate.chunkId)) {
      return Effect.fail(conformanceViolation(channel, "unique_chunks"))
    }
    seen.add(candidate.chunkId)

    if (
      !Number.isFinite(candidate.score) ||
      candidate.content.trim().length === 0 ||
      candidate.reference.graph !== request.scope.graph
    ) {
      return Effect.fail(
        conformanceViolation(channel, "boundary_validity"),
      )
    }

    if (channel === "text" && candidate.score <= 0) {
      return Effect.fail(
        conformanceViolation(channel, "positive_text_score"),
      )
    }

    if (previous !== undefined && candidate.score > previous.score) {
      return Effect.fail(conformanceViolation(channel, "score_order"))
    }

    if (
      previous !== undefined &&
      candidate.score === previous.score &&
      String(candidate.chunkId).localeCompare(String(previous.chunkId)) < 0
    ) {
      return Effect.fail(conformanceViolation(channel, "stable_ties"))
    }
    previous = candidate
  }

  if (!sameCandidateOrder(candidates, expected)) {
    return Effect.fail(
      conformanceViolation(channel, "boundary_validity"),
    )
  }

  return Effect.void
}

const seedFixture = (
  fixture: SearchStoreConformanceFixture,
) =>
  Effect.gen(function*() {
    const store = yield* ProjectionIndexStore
    for (const replacement of fixture.replacements) {
      yield* store.deleteRevision(replacement.key)
      yield* store.replaceRevision(replacement)
    }
  })

const commonVerifiedLaws: ReadonlyArray<SearchStoreConformanceLaw> = [
  "scope_before_limit",
  "candidate_bound",
  "score_order",
  "unique_chunks",
  "boundary_validity",
  "stable_ties",
  "repeatability",
]

const textVerifiedLaws: ReadonlyArray<SearchStoreConformanceLaw> = [
  ...commonVerifiedLaws.slice(0, 5),
  "positive_text_score",
  ...commonVerifiedLaws.slice(5),
]

/** Verify the semantic store through its production Effect service interface. */
export const verifySemanticSearchStoreConformance = () =>
  Effect.gen(function*() {
    const fixture = makeSearchStoreConformanceFixture()
    yield* seedFixture(fixture)
    const store = yield* ProjectionSearchStore

    const scoped = yield* store.searchCandidates(
      fixture.scopedSemanticRequest,
    )
    yield* verifyCandidates(
      "semantic",
      scoped,
      [fixture.expectedSemantic[1]],
      fixture.scopedSemanticRequest,
    ).pipe(
      Effect.mapError(() =>
        conformanceViolation("semantic", "scope_before_limit"),
      ),
    )

    const bounded = yield* store.searchCandidates({
      ...fixture.semanticRequest,
      candidates: Schema.decodeSync(SearchResultCountSchema)(2),
    })
    yield* verifyCandidates(
      "semantic",
      bounded,
      fixture.expectedSemantic.slice(0, 2),
      { ...fixture.semanticRequest, candidates: 2 },
    )

    const first = yield* store.searchCandidates(fixture.semanticRequest)
    yield* verifyCandidates(
      "semantic",
      first,
      fixture.expectedSemantic,
      fixture.semanticRequest,
    )
    const repeated = yield* store.searchCandidates(
      fixture.semanticRequest,
    )
    if (!sameCandidateResults(first, repeated)) {
      return yield* Effect.fail(
        conformanceViolation("semantic", "repeatability"),
      )
    }

    return {
      channel: "semantic" as const,
      verified: commonVerifiedLaws,
    }
  })

/** Verify the text store through its production Effect service interface. */
export const verifyTextSearchStoreConformance = () =>
  Effect.gen(function*() {
    const fixture = makeSearchStoreConformanceFixture()
    yield* seedFixture(fixture)
    const store = yield* ProjectionTextSearchStore

    const scoped = yield* store.searchTextCandidates(
      fixture.scopedTextRequest,
    )
    yield* verifyCandidates(
      "text",
      scoped,
      [fixture.expectedText[1]],
      fixture.scopedTextRequest,
    ).pipe(
      Effect.mapError(() =>
        conformanceViolation("text", "scope_before_limit"),
      ),
    )

    const boundedRequest = {
      ...fixture.textRequest,
      candidates: Schema.decodeSync(SearchResultCountSchema)(2),
    }
    const bounded = yield* store.searchTextCandidates(boundedRequest)
    yield* verifyCandidates(
      "text",
      bounded,
      fixture.expectedText.slice(0, 2),
      boundedRequest,
    )

    const first = yield* store.searchTextCandidates(fixture.textRequest)
    yield* verifyCandidates(
      "text",
      first,
      fixture.expectedText,
      fixture.textRequest,
    )
    const repeated = yield* store.searchTextCandidates(
      fixture.textRequest,
    )
    if (!sameCandidateResults(first, repeated)) {
      return yield* Effect.fail(
        conformanceViolation("text", "repeatability"),
      )
    }

    return {
      channel: "text" as const,
      verified: textVerifiedLaws,
    }
  })

/** Verify both standard retrieval capabilities with one composable Effect. */
export const verifySearchStoreConformance = () =>
  Effect.all(
    [
      verifySemanticSearchStoreConformance(),
      verifyTextSearchStoreConformance(),
    ],
    { concurrency: 1 },
  )

/** Evidence that one cohesive storage adapter passed every stable suite. */
export interface DocumentGraphStorageConformanceReport {
  readonly projectionIndex: ProjectionIndexStoreConformanceReport
  readonly graphRelations: GraphRelationStoreConformanceReport
  readonly retrieval: ReadonlyArray<SearchStoreConformanceReport>
}

/**
 * Verify indexing, graph relations, and retrieval through their Effect seams.
 *
 * Suites run sequentially because they deliberately mutate deterministic
 * namespaced fixtures in the same composed storage adapter.
 */
export const verifyDocumentGraphStorageConformance = () =>
  Effect.gen(function*() {
    const projectionIndex =
      yield* verifyProjectionIndexStoreConformance()
    const graphRelations =
      yield* verifyGraphRelationStoreConformance()
    const retrieval = yield* verifySearchStoreConformance()

    return { projectionIndex, graphRelations, retrieval }
  })
