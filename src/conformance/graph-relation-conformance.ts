import { Effect, Either, Schema } from "effect"
import {
  DocumentKeySchema,
  type DocumentKey,
} from "../document/document-identity.js"
import type { EncodedDocumentReference } from "../document/document-instance.js"
import {
  GraphNeighbourLimitSchema,
  GraphRelationStore,
  type FindIncomingGraphNeighbours,
  type FindOutgoingGraphNeighbours,
  type GraphRelationCommit,
  type OutgoingGraphRelationTarget,
  type OutgoingGraphRelationSet,
  type ReplaceOutgoingGraphRelations,
  type StoredGraphNeighbour,
} from "../graph/graph-relation.js"

/** Persistence laws checked against every graph-relation adapter. */
export const GraphRelationStoreConformanceLawSchema = Schema.Literal(
  "complete_replacement",
  "bidirectional_traversal",
  "bounded_ordering",
  "stale_edge_deletion",
  "invalid_replacement_atomicity",
  "idempotent_node_deletion",
  "schema_pruning",
)

/** Persistence law checked against every graph-relation adapter. */
export type GraphRelationStoreConformanceLaw =
  typeof GraphRelationStoreConformanceLawSchema.Type

/** A graph-relation adapter violated one storage-independent law. */
export class GraphRelationStoreConformanceViolation extends Schema.TaggedError<
  GraphRelationStoreConformanceViolation
>()("GraphRelationStoreConformanceViolation", {
  law: GraphRelationStoreConformanceLawSchema,
}) {}

/** Evidence that one graph-relation adapter passed every stable law. */
export interface GraphRelationStoreConformanceReport {
  readonly capability: "graph_relations"
  readonly verified: ReadonlyArray<GraphRelationStoreConformanceLaw>
}

/** Deterministic relation commands available for adapter-specific support. */
export interface GraphRelationStoreConformanceFixture {
  readonly initial: ReplaceOutgoingGraphRelations
  readonly reduced: ReplaceOutgoingGraphRelations
  readonly withRetired: ReplaceOutgoingGraphRelations
  readonly outgoing: FindOutgoingGraphNeighbours
  readonly incoming: FindIncomingGraphNeighbours
}

const GraphId = "@popcomputer/document-graph/conformance/relations"
const SourceKind = "Work"
const TargetKind = "Agency"
const RelationId = "deliveredBy"
const RelationVersion = "v1"
const RetiredRelationId = "retiredBy"

const sourceDocumentKey = Schema.decodeSync(DocumentKeySchema)(
  "6".repeat(64),
)
const firstTargetKey = Schema.decodeSync(DocumentKeySchema)("7".repeat(64))
const secondTargetKey = Schema.decodeSync(DocumentKeySchema)("8".repeat(64))
const thirdTargetKey = Schema.decodeSync(DocumentKeySchema)("9".repeat(64))
const allLimit = Schema.decodeSync(GraphNeighbourLimitSchema)(10)
const boundedLimit = Schema.decodeSync(GraphNeighbourLimitSchema)(2)

const source: EncodedDocumentReference = {
  graph: GraphId,
  kind: SourceKind,
  id: "source-work",
}

const makeTarget = (
  documentKey: DocumentKey,
  id: string,
): OutgoingGraphRelationTarget => ({
  documentKey,
  reference: { graph: GraphId, kind: TargetKind, id },
})

const firstTarget = makeTarget(firstTargetKey, "first-agency")
const secondTarget = makeTarget(secondTargetKey, "second-agency")
const thirdTarget = makeTarget(thirdTargetKey, "third-agency")

const initialRelation: OutgoingGraphRelationSet = {
  id: RelationId,
  version: RelationVersion,
  targetDocumentKind: TargetKind,
  targets: [thirdTarget, firstTarget, secondTarget],
}

const reducedRelation: OutgoingGraphRelationSet = {
  ...initialRelation,
  targets: [thirdTarget, secondTarget],
}

const retiredRelation: OutgoingGraphRelationSet = {
  id: RetiredRelationId,
  version: RelationVersion,
  targetDocumentKind: TargetKind,
  targets: [firstTarget],
}

/** Create deterministic commands used by the graph-relation verifier. */
export const makeGraphRelationStoreConformanceFixture =
  (): GraphRelationStoreConformanceFixture => ({
    initial: {
      graph: GraphId,
      sourceDocumentKey,
      source,
      relations: [initialRelation],
    },
    reduced: {
      graph: GraphId,
      sourceDocumentKey,
      source,
      relations: [reducedRelation],
    },
    withRetired: {
      graph: GraphId,
      sourceDocumentKey,
      source,
      relations: [
        { ...initialRelation, targets: [firstTarget] },
        retiredRelation,
      ],
    },
    outgoing: {
      graph: GraphId,
      sourceDocumentKey,
      sourceDocumentKind: SourceKind,
      relation: RelationId,
      relationVersion: RelationVersion,
      targetDocumentKind: TargetKind,
      limit: allLimit,
    },
    incoming: {
      graph: GraphId,
      targetDocumentKey: firstTargetKey,
      targetDocumentKind: TargetKind,
      relation: RelationId,
      relationVersion: RelationVersion,
      sourceDocumentKind: SourceKind,
      limit: allLimit,
    },
  })

const violation = (
  law: GraphRelationStoreConformanceLaw,
): GraphRelationStoreConformanceViolation =>
  new GraphRelationStoreConformanceViolation({ law })

const commitMatches = (
  commit: GraphRelationCommit,
  expected: GraphRelationCommit,
): boolean =>
  commit.inserted === expected.inserted &&
  commit.retained === expected.retained &&
  commit.deleted === expected.deleted

const sameNeighbour = (
  left: StoredGraphNeighbour,
  right: StoredGraphNeighbour,
): boolean =>
  left.documentKey === right.documentKey &&
  left.reference.graph === right.reference.graph &&
  left.reference.kind === right.reference.kind &&
  left.reference.id === right.reference.id

const sameNeighbours = (
  left: ReadonlyArray<StoredGraphNeighbour>,
  right: ReadonlyArray<StoredGraphNeighbour>,
): boolean =>
  left.length === right.length &&
  left.every((neighbour, index) => {
    const expected = right[index]
    return expected !== undefined && sameNeighbour(neighbour, expected)
  })

const verifiedLaws: ReadonlyArray<GraphRelationStoreConformanceLaw> = [
  "complete_replacement",
  "bidirectional_traversal",
  "bounded_ordering",
  "stale_edge_deletion",
  "invalid_replacement_atomicity",
  "idempotent_node_deletion",
  "schema_pruning",
]

/** Verify a graph relation store through its production Effect service seam. */
export const verifyGraphRelationStoreConformance = () =>
  Effect.gen(function*() {
    const fixture = makeGraphRelationStoreConformanceFixture()
    const store = yield* GraphRelationStore

    yield* store.deleteNode({ graph: GraphId, documentKey: sourceDocumentKey })
    for (const target of [firstTarget, secondTarget, thirdTarget]) {
      yield* store.deleteNode({
        graph: GraphId,
        documentKey: target.documentKey,
      })
    }

    const initialCommit = yield* store.replaceOutgoing(fixture.initial)
    if (
      !commitMatches(initialCommit, {
        inserted: 3,
        retained: 0,
        deleted: 0,
      })
    ) {
      return yield* Effect.fail(violation("complete_replacement"))
    }

    const outgoing = yield* store.findOutgoing(fixture.outgoing)
    const expectedAll = [firstTarget, secondTarget, thirdTarget]
    const incoming = yield* store.findIncoming(fixture.incoming)
    const expectedIncoming: ReadonlyArray<StoredGraphNeighbour> = [
      { documentKey: sourceDocumentKey, reference: source },
    ]
    if (
      !sameNeighbours(outgoing, expectedAll) ||
      !sameNeighbours(incoming, expectedIncoming)
    ) {
      return yield* Effect.fail(violation("bidirectional_traversal"))
    }

    const bounded = yield* store.findOutgoing({
      ...fixture.outgoing,
      limit: boundedLimit,
    })
    if (!sameNeighbours(bounded, expectedAll.slice(0, 2))) {
      return yield* Effect.fail(violation("bounded_ordering"))
    }

    const reducedCommit = yield* store.replaceOutgoing(fixture.reduced)
    if (
      !commitMatches(reducedCommit, {
        inserted: 0,
        retained: 2,
        deleted: 1,
      })
    ) {
      return yield* Effect.fail(violation("stale_edge_deletion"))
    }
    const afterReduction = yield* store.findOutgoing(fixture.outgoing)
    if (!sameNeighbours(afterReduction, [secondTarget, thirdTarget])) {
      return yield* Effect.fail(violation("stale_edge_deletion"))
    }

    const invalid = yield* store
      .replaceOutgoing({
        ...fixture.reduced,
        relations: [reducedRelation, reducedRelation],
      })
      .pipe(Effect.either)
    if (
      !Either.isLeft(invalid) ||
      invalid.left.reason !== "invalid_stored_state"
    ) {
      return yield* Effect.fail(
        violation("invalid_replacement_atomicity"),
      )
    }
    const afterInvalid = yield* store.findOutgoing(fixture.outgoing)
    if (!sameNeighbours(afterInvalid, afterReduction)) {
      return yield* Effect.fail(
        violation("invalid_replacement_atomicity"),
      )
    }

    const removedTarget = yield* store.deleteNode({
      graph: GraphId,
      documentKey: secondTargetKey,
    })
    const removedAgain = yield* store.deleteNode({
      graph: GraphId,
      documentKey: secondTargetKey,
    })
    const afterNodeDelete = yield* store.findOutgoing(fixture.outgoing)
    if (
      removedTarget.deleted !== 1 ||
      removedAgain.deleted !== 0 ||
      !sameNeighbours(afterNodeDelete, [thirdTarget])
    ) {
      return yield* Effect.fail(violation("idempotent_node_deletion"))
    }

    yield* store.replaceOutgoing(fixture.withRetired)
    const pruned = yield* store.pruneRelations({
      graph: GraphId,
      registered: [
        {
          id: RelationId,
          version: RelationVersion,
          sourceDocumentKind: SourceKind,
          targetDocumentKind: TargetKind,
        },
      ],
    })
    const activeAfterPrune = yield* store.findOutgoing(fixture.outgoing)
    const retiredAfterPrune = yield* store.findOutgoing({
      ...fixture.outgoing,
      relation: RetiredRelationId,
    })
    if (
      pruned.deleted !== 1 ||
      !sameNeighbours(activeAfterPrune, [firstTarget]) ||
      retiredAfterPrune.length !== 0
    ) {
      return yield* Effect.fail(violation("schema_pruning"))
    }

    yield* store.deleteNode({ graph: GraphId, documentKey: sourceDocumentKey })

    return {
      capability: "graph_relations" as const,
      verified: verifiedLaws,
    }
  })
