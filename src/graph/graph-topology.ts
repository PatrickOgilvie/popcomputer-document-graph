import { Context, type Effect, type Option, Schema } from "effect"
import type { EncodedDocumentReference } from "../document/document-instance.js"
import type { DocumentKey } from "../document/document-identity.js"
import type {
  GraphRelationCommit,
  GraphNeighbourLimit,
  RegisteredGraphRelation,
  ReplaceOutgoingGraphRelations,
} from "./graph-relation.js"

/** Materialization state retained for one canonical graph node. */
export const GraphNodeStateSchema = Schema.Literals([
  "Referenced",
  "Materialized",
])

/** Materialization state retained for one canonical graph node. */
export type GraphNodeState = typeof GraphNodeStateSchema.Type

/** One graph node reconstructed from canonical topology storage. */
export interface StoredGraphNode {
  readonly documentKey: DocumentKey
  readonly reference: EncodedDocumentReference
  readonly state: GraphNodeState
}

/** Complete topology replacement projected from one materialized document. */
export type ReplaceDocumentTopology = ReplaceOutgoingGraphRelations

/** Idempotent result of hard-deleting one node, incident edges, and orphans. */
export interface GraphTopologyDeletion {
  /** Whether the explicitly requested node was deleted. */
  readonly deletedNodes: number
  readonly deletedRelations: number
  /** Referenced neighbours garbage-collected after their last edge vanished. */
  readonly deletedReferencedNodes: number
}

/** Command for pruning topology no longer present in a graph manifest. */
export interface PruneGraphTopology {
  readonly graph: string
  readonly registered: ReadonlyArray<RegisteredGraphRelation>
}

/** Result of pruning stale edges and newly orphaned referenced nodes. */
export interface GraphTopologyPrune {
  readonly deletedRelations: number
  readonly deletedReferencedNodes: number
}

/** Maximum number of nodes returned by one bounded catalog read. */
export const GraphNodePageLimitSchema = Schema.Number.pipe(
  Schema.check(Schema.isInt()),
  Schema.check(Schema.isBetween({ minimum: 1, maximum: 1_000 })),
  Schema.brand("GraphNodePageLimit"),
)

/** Maximum number of nodes returned by one bounded catalog read. */
export type GraphNodePageLimit = typeof GraphNodePageLimitSchema.Type

/** Deterministically page through canonical nodes in one graph. */
export interface ListGraphNodes {
  readonly graph: string
  readonly documentKinds: ReadonlyArray<string>
  readonly states: ReadonlyArray<GraphNodeState>
  readonly after: Option.Option<DocumentKey>
  readonly limit: GraphNodePageLimit
}

/** One bounded node page and its exclusive continuation cursor. */
export interface GraphNodePage {
  readonly nodes: ReadonlyArray<StoredGraphNode>
  readonly next: Option.Option<DocumentKey>
}

/** One result per requested document, including empty neighbour sets. */
export interface RelatedGraphNodeSet {
  readonly documentKey: DocumentKey
  readonly nodes: ReadonlyArray<StoredGraphNode>
}

/** Batched exact-relation request with an independent limit per document. */
export interface FindRelatedGraphNodes {
  readonly graph: string
  readonly documentKeys: ReadonlyArray<DocumentKey>
  readonly documentKind: string
  readonly direction: "outgoing" | "incoming"
  readonly relation: string
  readonly relationVersion: string
  readonly relatedDocumentKind: string
  readonly limit: GraphNeighbourLimit
}

/** Canonical graph topology storage could not complete an operation. */
export class GraphTopologyStoreFailed extends Schema.TaggedError<
  GraphTopologyStoreFailed
>()("GraphTopologyStoreFailed", {
  operation: Schema.Literals([
    "replace_document",
    "delete_node",
    "prune_graph",
    "list_nodes",
    "find_related",
  ]),
  reason: Schema.Literals([
    "unavailable",
    "invalid_stored_state",
    "capacity_exceeded",
  ]),
  cause: Schema.Unknown,
}) {}

/** A topology adapter returned a node set outside its exact-read contract. */
export class InvalidGraphTopologyOutput extends Schema.TaggedError<
  InvalidGraphTopologyOutput
>()("InvalidGraphTopologyOutput", {
  output: Schema.Literals(["nodes", "related_nodes"]),
  reason: Schema.Literals([
    "too_many",
    "duplicate",
    "not_ordered",
    "invalid_identity",
    "out_of_scope",
    "invalid_cursor",
    "invalid_batch",
  ]),
}) {}

/** Exact topology capability consumed by graph indexing and traversal. */
export interface GraphTopologyStoreService {
  /** Atomically materialize the source and replace its complete outgoing set. */
  readonly replaceDocumentTopology: (
    replacement: ReplaceDocumentTopology,
  ) => Effect.Effect<GraphRelationCommit, GraphTopologyStoreFailed>

  /**
   * Idempotently hard-delete one node and every incoming/outgoing edge, then
   * garbage-collect referenced nodes left without any edge in the same write.
   */
  readonly deleteNode: (input: {
    readonly graph: string
    readonly documentKey: DocumentKey
  }) => Effect.Effect<GraphTopologyDeletion, GraphTopologyStoreFailed>

  /** Delete stale relations and referenced nodes left without any edge. */
  readonly pruneTopology: (
    input: PruneGraphTopology,
  ) => Effect.Effect<GraphTopologyPrune, GraphTopologyStoreFailed>

  /** List bounded, deterministically ordered canonical graph nodes. */
  readonly listNodes: (
    input: ListGraphNodes,
  ) => Effect.Effect<GraphNodePage, GraphTopologyStoreFailed>

  /**
   * Return one group for each input key in input order, including duplicates
   * and empty sets. Each group contains unique, key-ordered nodes up to limit.
   */
  readonly findRelatedNodes: (
    input: FindRelatedGraphNodes,
  ) => Effect.Effect<ReadonlyArray<RelatedGraphNodeSet>, GraphTopologyStoreFailed>
}

/** Effect service tag for canonical graph topology persistence. */
export class GraphTopologyStore extends Context.Service<
  GraphTopologyStore,
  GraphTopologyStoreService
>()("@popcomputer/document-graph/GraphTopologyStore") {}
