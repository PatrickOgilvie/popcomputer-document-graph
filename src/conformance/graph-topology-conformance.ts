import { Effect, Option, Result, Schema } from "effect"
import {
  makeDocumentKey,
  type DocumentKey,
} from "../document/document-identity.js"
import type { EncodedDocumentReference } from "../document/document-instance.js"
import {
  GraphNeighbourLimitSchema,
  type GraphRelationCommit,
  type OutgoingGraphRelationSet,
  type ReplaceOutgoingGraphRelations,
} from "../graph/graph-relation.js"
import {
  GraphNodePageLimitSchema,
  GraphTopologyStore,
  type FindRelatedGraphNodes,
  type StoredGraphNode,
} from "../graph/graph-topology.js"

/** Persistence laws checked against every canonical graph-topology adapter. */
export const GraphTopologyStoreConformanceLawSchema = Schema.Literals([
  "empty_source_materialization",
  "referenced_target_creation",
  "materialized_promotion",
  "complete_replacement",
  "bidirectional_traversal",
  "bounded_ordering",
  "batch_bounds_and_identity",
  "orphan_reference_collection",
  "invalid_replacement_atomicity",
  "idempotent_node_deletion",
  "hard_deletion_orphan_collection",
  "schema_pruning",
])

/** Persistence law checked against every graph-topology adapter. */
export type GraphTopologyStoreConformanceLaw =
  typeof GraphTopologyStoreConformanceLawSchema.Type

/** A graph-topology adapter violated one storage-independent law. */
export class GraphTopologyStoreConformanceViolation extends Schema.TaggedError<
  GraphTopologyStoreConformanceViolation
>()("GraphTopologyStoreConformanceViolation", {
  law: GraphTopologyStoreConformanceLawSchema,
}) {}

/** Evidence that one graph-topology adapter passed every stable law. */
export interface GraphTopologyStoreConformanceReport {
  readonly capability: "graph_topology"
  readonly verified: ReadonlyArray<GraphTopologyStoreConformanceLaw>
}

/** Deterministic topology commands available for adapter-specific support. */
export interface GraphTopologyStoreConformanceFixture {
  readonly initial: ReplaceOutgoingGraphRelations
  readonly reduced: ReplaceOutgoingGraphRelations
  readonly withRetired: ReplaceOutgoingGraphRelations
  readonly deletion: ReplaceOutgoingGraphRelations
  readonly empty: ReplaceOutgoingGraphRelations
  readonly outgoing: FindRelatedGraphNodes
  readonly incoming: FindRelatedGraphNodes
}

const GraphId = "@popcomputer/document-graph/conformance/topology"

const SourceKind = "Work"

const TargetKind = "Agency"

const RelationId = "deliveredBy"

const RelationVersion = "v1"

const RetiredRelationId = "retiredBy"

const reference = (
  kind: string,
  id: string,
): EncodedDocumentReference => ({ graph: GraphId, kind, id })

const keyedReference = (kind: string, id: string) => {
  const encoded = reference(kind, id)

  return {
    documentKey: makeDocumentKey({
      graph: encoded.graph,
      documentKind: encoded.kind,
      encodedId: encoded.id,
    }),
    reference: encoded,
  }
}

const source = keyedReference(SourceKind, "source-work")

const emptySource = keyedReference(SourceKind, "empty-work")

const firstTarget = keyedReference(TargetKind, "first-agency")

const secondTarget = keyedReference(TargetKind, "second-agency")

const thirdTarget = keyedReference(TargetKind, "third-agency")

const deletionSource = keyedReference(SourceKind, "deletion-source")

const deletionTarget = keyedReference(TargetKind, "deletion-target")

const allLimit = Schema.decodeSync(GraphNeighbourLimitSchema)(10)

const boundedLimit = Schema.decodeSync(GraphNeighbourLimitSchema)(2)

const initialRelation: OutgoingGraphRelationSet = {
  id: RelationId,
  version: RelationVersion,
  targetDocumentKind: TargetKind,
  targets: [thirdTarget, firstTarget, secondTarget],
}

const reducedRelation: OutgoingGraphRelationSet = {
  ...initialRelation,
  targets: [firstTarget, secondTarget],
}

const retiredRelation: OutgoingGraphRelationSet = {
  id: RetiredRelationId,
  version: RelationVersion,
  targetDocumentKind: TargetKind,
  targets: [thirdTarget],
}

/** Create deterministic commands used by the topology verifier. */
export const makeGraphTopologyStoreConformanceFixture =
  (): GraphTopologyStoreConformanceFixture => ({
    initial: {
      graph: GraphId,
      sourceDocumentKey: source.documentKey,
      source: source.reference,
      relations: [initialRelation],
    },
    reduced: {
      graph: GraphId,
      sourceDocumentKey: source.documentKey,
      source: source.reference,
      relations: [reducedRelation],
    },
    withRetired: {
      graph: GraphId,
      sourceDocumentKey: source.documentKey,
      source: source.reference,
      relations: [
        { ...initialRelation, targets: [firstTarget] },
        retiredRelation,
      ],
    },
    deletion: {
      graph: GraphId,
      sourceDocumentKey: deletionSource.documentKey,
      source: deletionSource.reference,
      relations: [{
        ...initialRelation,
        targets: [deletionTarget],
      }],
    },
    empty: {
      graph: GraphId,
      sourceDocumentKey: emptySource.documentKey,
      source: emptySource.reference,
      relations: [],
    },
    outgoing: {
      graph: GraphId,
      documentKeys: [source.documentKey],
      documentKind: SourceKind,
      direction: "outgoing",
      relation: RelationId,
      relationVersion: RelationVersion,
      relatedDocumentKind: TargetKind,
      limit: allLimit,
    },
    incoming: {
      graph: GraphId,
      documentKeys: [firstTarget.documentKey],
      documentKind: TargetKind,
      direction: "incoming",
      relation: RelationId,
      relationVersion: RelationVersion,
      relatedDocumentKind: SourceKind,
      limit: allLimit,
    },
  })

const violation = (
  law: GraphTopologyStoreConformanceLaw,
): GraphTopologyStoreConformanceViolation =>
  new GraphTopologyStoreConformanceViolation({ law })

const commitMatches = (
  commit: GraphRelationCommit,
  expected: GraphRelationCommit,
): boolean =>
  commit.inserted === expected.inserted &&
  commit.retained === expected.retained &&
  commit.deleted === expected.deleted

const nodeMatches = (
  node: StoredGraphNode | undefined,
  expected: {
    readonly documentKey: DocumentKey
    readonly state: StoredGraphNode["state"]
  },
): boolean =>
  node?.documentKey === expected.documentKey && node.state === expected.state

const listAllNodes = Effect.fn("GraphTopologyConformance.listAllNodes")(
  function*(store: GraphTopologyStore["Service"]) {
    return yield* store.listNodes({
      graph: GraphId,
      documentKinds: [],
      states: [],
      after: Option.none(),
      limit: Schema.decodeSync(GraphNodePageLimitSchema)(100),
    })
  },
)

const verifiedLaws: ReadonlyArray<GraphTopologyStoreConformanceLaw> = [
  "empty_source_materialization",
  "referenced_target_creation",
  "materialized_promotion",
  "complete_replacement",
  "bidirectional_traversal",
  "bounded_ordering",
  "batch_bounds_and_identity",
  "orphan_reference_collection",
  "invalid_replacement_atomicity",
  "idempotent_node_deletion",
  "hard_deletion_orphan_collection",
  "schema_pruning",
]

/** Verify a topology store through its production Effect service seam. */
export const verifyGraphTopologyStoreConformance = () =>
  Effect.gen(function*() {
    const fixture = makeGraphTopologyStoreConformanceFixture()
    const store = yield* GraphTopologyStore

    for (const node of [
      source,
      emptySource,
      firstTarget,
      secondTarget,
      thirdTarget,
      deletionSource,
      deletionTarget,
    ]) {
      yield* store.deleteNode({ graph: GraphId, documentKey: node.documentKey })
    }

    yield* store.replaceDocumentTopology(fixture.empty)
    const emptyNodes = yield* listAllNodes(store)

    if (!nodeMatches(emptyNodes.nodes[0], {
      documentKey: emptySource.documentKey,
      state: "Materialized",
    })) {
      return yield* violation("empty_source_materialization")
    }

    const initialCommit = yield* store.replaceDocumentTopology(fixture.initial)

    if (!commitMatches(initialCommit, { inserted: 3, retained: 0, deleted: 0 })) {
      return yield* violation("complete_replacement")
    }

    const afterInitial = yield* listAllNodes(store)

    const first = afterInitial.nodes.find(
      (node) => node.documentKey === firstTarget.documentKey,
    )

    if (!nodeMatches(first, {
      documentKey: firstTarget.documentKey,
      state: "Referenced",
    })) {
      return yield* violation("referenced_target_creation")
    }

    yield* store.replaceDocumentTopology({
      graph: GraphId,
      sourceDocumentKey: firstTarget.documentKey,
      source: firstTarget.reference,
      relations: [],
    })
    yield* store.replaceDocumentTopology(fixture.initial)
    const afterPromotion = yield* listAllNodes(store)

    const promoted = afterPromotion.nodes.find(
      (node) => node.documentKey === firstTarget.documentKey,
    )

    if (!nodeMatches(promoted, {
      documentKey: firstTarget.documentKey,
      state: "Materialized",
    })) {
      return yield* violation("materialized_promotion")
    }

    const outgoing = yield* store.findRelatedNodes(fixture.outgoing).pipe(Effect.map((groups) => groups.flatMap((group) => group.nodes)))
    const incoming = yield* store.findRelatedNodes(fixture.incoming).pipe(Effect.map((groups) => groups.flatMap((group) => group.nodes)))

    if (
      outgoing.map((node) => node.documentKey).join() !==
        [firstTarget, secondTarget, thirdTarget]
          .map((target) => target.documentKey)
          .sort()
          .join() ||
      !nodeMatches(incoming[0], {
        documentKey: source.documentKey,
        state: "Materialized",
      })
    ) {
      return yield* violation("bidirectional_traversal")
    }

    const bounded = yield* store.findRelatedNodes({
      ...fixture.outgoing,
      limit: boundedLimit,
    }).pipe(Effect.map((groups) => groups.flatMap((group) => group.nodes)))

    if (bounded.length !== 2 || String(bounded[0]?.documentKey).localeCompare(
      String(bounded[1]?.documentKey),
    ) >= 0) {
      return yield* violation("bounded_ordering")
    }

    const batchKeys = [source.documentKey, emptySource.documentKey, deletionSource.documentKey, source.documentKey]

    const batched = yield* store.findRelatedNodes({
      ...fixture.outgoing,
      documentKeys: batchKeys,
      limit: boundedLimit,
    })

    const emptyBatch = yield* store.findRelatedNodes({ ...fixture.outgoing, documentKeys: [] })

    const staleBatch = yield* store.findRelatedNodes({
      ...fixture.outgoing, documentKeys: batchKeys, relationVersion: "retired-version",
    })

    if (batched.length !== batchKeys.length || emptyBatch.length !== 0 ||
      batched.some((group, index) => group.documentKey !== batchKeys[index]) ||
      batched[0]?.nodes.map((node) => node.documentKey).join() !== bounded.map((node) => node.documentKey).join() ||
      batched[3]?.nodes.map((node) => node.documentKey).join() !== bounded.map((node) => node.documentKey).join() ||
      batched[1]?.nodes.length !== 0 || batched[2]?.nodes.length !== 0 ||
      staleBatch.length !== batchKeys.length || staleBatch.some((group) => group.nodes.length !== 0)) {
      return yield* violation("batch_bounds_and_identity")
    }

    const reduced = yield* store.replaceDocumentTopology(fixture.reduced)

    if (!commitMatches(reduced, { inserted: 0, retained: 2, deleted: 1 })) {
      return yield* violation("complete_replacement")
    }

    const afterReduction = yield* listAllNodes(store)

    if (afterReduction.nodes.some(
      (node) => node.documentKey === thirdTarget.documentKey,
    )) {
      return yield* violation("orphan_reference_collection")
    }

    const invalid = yield* store.replaceDocumentTopology({
      ...fixture.reduced,
      sourceDocumentKey: thirdTarget.documentKey,
    }).pipe(Effect.result)

    if (!Result.isFailure(invalid) ||
      invalid.failure.reason !== "invalid_stored_state") {
      return yield* violation("invalid_replacement_atomicity")
    }

    const afterInvalid = yield* store.findRelatedNodes(fixture.outgoing).pipe(Effect.map((groups) => groups.flatMap((group) => group.nodes)))

    if (afterInvalid.length !== 2) {
      return yield* violation("invalid_replacement_atomicity")
    }

    const removed = yield* store.deleteNode({
      graph: GraphId,
      documentKey: secondTarget.documentKey,
    })

    const removedAgain = yield* store.deleteNode({
      graph: GraphId,
      documentKey: secondTarget.documentKey,
    })

    if (
      removed.deletedNodes !== 1 ||
      removed.deletedRelations !== 1 ||
      removed.deletedReferencedNodes !== 0 ||
      removedAgain.deletedNodes !== 0 ||
      removedAgain.deletedRelations !== 0 ||
      removedAgain.deletedReferencedNodes !== 0
    ) {
      return yield* violation("idempotent_node_deletion")
    }

    yield* store.replaceDocumentTopology(fixture.deletion)

    const deletion = yield* store.deleteNode({
      graph: GraphId,
      documentKey: deletionSource.documentKey,
    })

    const repeatedDeletion = yield* store.deleteNode({
      graph: GraphId,
      documentKey: deletionSource.documentKey,
    })

    const afterDeletion = yield* listAllNodes(store)

    if (
      deletion.deletedNodes !== 1 ||
      deletion.deletedRelations !== 1 ||
      deletion.deletedReferencedNodes !== 1 ||
      repeatedDeletion.deletedNodes !== 0 ||
      repeatedDeletion.deletedRelations !== 0 ||
      repeatedDeletion.deletedReferencedNodes !== 0 ||
      afterDeletion.nodes.some(
        (node) =>
          node.documentKey === deletionSource.documentKey ||
          node.documentKey === deletionTarget.documentKey,
      )
    ) {
      return yield* violation("hard_deletion_orphan_collection")
    }

    yield* store.replaceDocumentTopology(fixture.withRetired)

    const pruned = yield* store.pruneTopology({
      graph: GraphId,
      registered: [{
        id: RelationId,
        version: RelationVersion,
        sourceDocumentKind: SourceKind,
        targetDocumentKind: TargetKind,
      }],
    })

    const retired = yield* store.findRelatedNodes({
      ...fixture.outgoing,
      relation: RetiredRelationId,
    }).pipe(Effect.map((groups) => groups.flatMap((group) => group.nodes)))

    if (
      pruned.deletedRelations !== 1 ||
      pruned.deletedReferencedNodes !== 1 ||
      retired.length !== 0
    ) {
      return yield* violation("schema_pruning")
    }

    for (const node of [
      source,
      emptySource,
      firstTarget,
      secondTarget,
      thirdTarget,
      deletionSource,
      deletionTarget,
    ]) {
      yield* store.deleteNode({ graph: GraphId, documentKey: node.documentKey })
    }

    return { capability: "graph_topology" as const, verified: verifiedLaws }
  })
