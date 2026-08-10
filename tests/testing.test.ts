import { describe, expect, test } from "bun:test"
import { Effect, Either } from "effect"
import {
  evaluateMetadataFilter,
  GraphRelationStore,
  ProjectionIndexStore,
  ProjectionIndexStoreFailed,
  ProjectionTextSearchStore,
  type GraphRelationStoreService,
  type ProjectionIndexStoreService,
  type ProjectionTextSearchStoreService,
  type TextSearchCandidate,
} from "../src/adapter.js"
import { inMemoryDocumentGraph } from "../src/in-memory.js"
import {
  makeSearchStoreConformanceFixture,
  verifyDocumentGraphStorageConformance,
  verifyGraphRelationStoreConformance,
  verifyProjectionIndexStoreConformance,
  verifySearchStoreConformance,
  verifyTextSearchStoreConformance,
} from "../src/testing.js"

const candidateMatchesRequest = (
  candidate: TextSearchCandidate,
  request: Parameters<
    ProjectionTextSearchStoreService["searchTextCandidates"]
  >[0],
): boolean => {
  const scope = request.scope
  if (
    candidate.reference.graph !== scope.graph ||
    (scope.includeDocumentKinds.length > 0 &&
      !scope.includeDocumentKinds.includes(candidate.reference.kind)) ||
    scope.excludeDocumentKinds.includes(candidate.reference.kind) ||
    (scope.includeProjections.length > 0 &&
      !scope.includeProjections.includes(candidate.projection.id)) ||
    scope.excludeProjections.includes(candidate.projection.id)
  ) {
    return false
  }

  return scope.where.every((filter) =>
    evaluateMetadataFilter(candidate.metadata, filter),
  )
}

describe("adapter conformance", () => {
  test("certifies the complete in-memory document-graph adapter", async () => {
    const report = await Effect.runPromise(
      verifyDocumentGraphStorageConformance().pipe(
        Effect.provide(inMemoryDocumentGraph()),
      ),
    )

    expect(report.projectionIndex.capability).toBe("projection_index")
    expect(report.projectionIndex.verified).toHaveLength(8)
    expect(report.graphRelations).toEqual({
      capability: "graph_relations",
      verified: [
        "complete_replacement",
        "bidirectional_traversal",
        "bounded_ordering",
        "stale_edge_deletion",
        "invalid_replacement_atomicity",
        "idempotent_node_deletion",
        "schema_pruning",
      ],
    })
    expect(report.retrieval.map((item) => item.channel)).toEqual([
      "semantic",
      "text",
    ])
  })

  test("certifies the in-memory projection-index capability", async () => {
    const report = await Effect.runPromise(
      verifyProjectionIndexStoreConformance().pipe(
        Effect.provide(inMemoryDocumentGraph()),
      ),
    )

    expect(report).toEqual({
      capability: "projection_index",
      verified: [
        "complete_replacement",
        "snapshot_inventory",
        "content_hash_reuse",
        "optimistic_conflict",
        "invalid_replacement_atomicity",
        "stale_chunk_deletion",
        "idempotent_deletion",
        "schema_pruning",
      ],
    })
  })

  test("rejects an index adapter that misreports stale deletion", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const correct = yield* ProjectionIndexStore
        const broken: ProjectionIndexStoreService = {
          ...correct,
          replaceRevision: (replacement) =>
            correct.replaceRevision(replacement).pipe(
              Effect.map((commit) => ({ ...commit, deleted: 0 })),
            ),
        }

        return yield* verifyProjectionIndexStoreConformance().pipe(
          Effect.provideService(ProjectionIndexStore, broken),
          Effect.either,
        )
      }).pipe(Effect.provide(inMemoryDocumentGraph())),
    )

    expect(Either.isLeft(result)).toBe(true)
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe(
        "ProjectionIndexStoreConformanceViolation",
      )
      if (
        result.left._tag ===
        "ProjectionIndexStoreConformanceViolation"
      ) {
        expect(result.left.law).toBe("stale_chunk_deletion")
      }
    }
  })

  test("rejects an index adapter that mutates before rejecting invalid input", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const correct = yield* ProjectionIndexStore
        const broken: ProjectionIndexStoreService = {
          ...correct,
          replaceRevision: (replacement) => {
            const invalid = replacement.chunks.some(
              (chunk) => chunk.content.trim().length === 0,
            )
            if (!invalid) return correct.replaceRevision(replacement)

            const [first, ...rest] = replacement.chunks
            return correct
              .replaceRevision({
                ...replacement,
                chunks: [
                  { ...first, content: "Mutated before rejection." },
                  ...rest,
                ],
              })
              .pipe(
                Effect.flatMap(() =>
                  Effect.fail(
                    new ProjectionIndexStoreFailed({
                      operation: "replace_revision",
                      reason: "invalid_stored_state",
                      cause: "rejected after mutation",
                    }),
                  ),
                ),
              )
          },
        }

        return yield* verifyProjectionIndexStoreConformance().pipe(
          Effect.provideService(ProjectionIndexStore, broken),
          Effect.either,
        )
      }).pipe(Effect.provide(inMemoryDocumentGraph())),
    )

    expect(Either.isLeft(result)).toBe(true)
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe(
        "ProjectionIndexStoreConformanceViolation",
      )
      if (
        result.left._tag ===
        "ProjectionIndexStoreConformanceViolation"
      ) {
        expect(result.left.law).toBe(
          "invalid_replacement_atomicity",
        )
      }
    }
  })

  test("rejects a relation adapter with unstable bounded ordering", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const correct = yield* GraphRelationStore
        const broken: GraphRelationStoreService = {
          ...correct,
          findOutgoing: (request) =>
            correct.findOutgoing(request).pipe(
              Effect.map((neighbours) =>
                request.limit === 2
                  ? [...neighbours].reverse()
                  : neighbours,
              ),
            ),
        }

        return yield* verifyGraphRelationStoreConformance().pipe(
          Effect.provideService(GraphRelationStore, broken),
          Effect.either,
        )
      }).pipe(Effect.provide(inMemoryDocumentGraph())),
    )

    expect(Either.isLeft(result)).toBe(true)
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe(
        "GraphRelationStoreConformanceViolation",
      )
      if (
        result.left._tag ===
        "GraphRelationStoreConformanceViolation"
      ) {
        expect(result.left.law).toBe("bounded_ordering")
      }
    }
  })

  test("certifies the in-memory semantic and text capabilities", async () => {
    const reports = await Effect.runPromise(
      verifySearchStoreConformance().pipe(
        Effect.provide(inMemoryDocumentGraph()),
      ),
    )

    expect(reports.map((report) => report.channel)).toEqual([
      "semantic",
      "text",
    ])
    expect(reports[0]?.verified).toEqual([
      "scope_before_limit",
      "candidate_bound",
      "score_order",
      "unique_chunks",
      "boundary_validity",
      "stable_ties",
      "repeatability",
    ])
    expect(reports[1]?.verified).toEqual([
      "scope_before_limit",
      "candidate_bound",
      "score_order",
      "unique_chunks",
      "boundary_validity",
      "positive_text_score",
      "stable_ties",
      "repeatability",
    ])
  })

  test("rejects an adapter that filters only after candidate limiting", async () => {
    const fixture = makeSearchStoreConformanceFixture()
    const brokenStore: ProjectionTextSearchStoreService = {
      searchTextCandidates: (request) =>
        Effect.succeed(
          fixture.expectedText
            .slice(0, request.candidates)
            .filter((candidate) =>
              candidateMatchesRequest(candidate, request),
            ),
        ),
    }

    const result = await Effect.runPromise(
      verifyTextSearchStoreConformance().pipe(
        Effect.provideService(ProjectionTextSearchStore, brokenStore),
        Effect.provide(inMemoryDocumentGraph()),
        Effect.either,
      ),
    )

    expect(Either.isLeft(result)).toBe(true)
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("SearchStoreConformanceViolation")
      if (result.left._tag === "SearchStoreConformanceViolation") {
        expect(result.left.channel).toBe("text")
        expect(result.left.law).toBe("scope_before_limit")
      }
    }
  })

  test("rejects an adapter whose repeated result order changes", async () => {
    const fixture = makeSearchStoreConformanceFixture()
    let completeRequests = 0
    const unstableStore: ProjectionTextSearchStoreService = {
      searchTextCandidates: (request) => {
        if (request.scope.includeDocumentKinds.length > 0) {
          return Effect.succeed([fixture.expectedText[1]])
        }

        if (request.candidates === 2) {
          return Effect.succeed(fixture.expectedText.slice(0, 2))
        }

        completeRequests += 1
        if (completeRequests === 1) {
          return Effect.succeed(fixture.expectedText)
        }

        return Effect.succeed([
          fixture.expectedText[0],
          fixture.expectedText[2],
          fixture.expectedText[1],
        ])
      },
    }

    const result = await Effect.runPromise(
      verifyTextSearchStoreConformance().pipe(
        Effect.provideService(ProjectionTextSearchStore, unstableStore),
        Effect.provide(inMemoryDocumentGraph()),
        Effect.either,
      ),
    )

    expect(Either.isLeft(result)).toBe(true)
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("SearchStoreConformanceViolation")
      if (result.left._tag === "SearchStoreConformanceViolation") {
        expect(result.left.channel).toBe("text")
        expect(result.left.law).toBe("repeatability")
      }
    }
  })

  test("rejects text candidates that match only zero-weighted channels", async () => {
    const fixture = makeSearchStoreConformanceFixture()
    const phantom = {
      ...fixture.expectedSemantic[3],
      score: 0,
    }
    const brokenStore: ProjectionTextSearchStoreService = {
      searchTextCandidates: (request) => {
        if (request.scope.includeDocumentKinds.length > 0) {
          return Effect.succeed([fixture.expectedText[1]])
        }

        if (request.candidates === 2) {
          return Effect.succeed(fixture.expectedText.slice(0, 2))
        }

        return Effect.succeed([...fixture.expectedText, phantom])
      },
    }

    const result = await Effect.runPromise(
      verifyTextSearchStoreConformance().pipe(
        Effect.provideService(ProjectionTextSearchStore, brokenStore),
        Effect.provide(inMemoryDocumentGraph()),
        Effect.either,
      ),
    )

    expect(Either.isLeft(result)).toBe(true)
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("SearchStoreConformanceViolation")
      if (result.left._tag === "SearchStoreConformanceViolation") {
        expect(result.left.channel).toBe("text")
        expect(result.left.law).toBe("positive_text_score")
      }
    }
  })
})
