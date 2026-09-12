import { Effect } from "effect"
import {
  ProjectionIndexStore,
  type RegisteredGraphProjection,
} from "../indexing/projection-index.js"
import type { RegisteredGraphRelation } from "./graph-relation.js"
import { GraphTopologyStore } from "./graph-topology.js"

export interface ReconcileDocumentGraphWorkflowInput {
  readonly graph: string
  readonly registeredProjections: ReadonlyArray<RegisteredGraphProjection>
  readonly registeredRelations: ReadonlyArray<RegisteredGraphRelation>
}

/** Prune stored projection and relation state outside one compiled manifest. */
export const reconcileDocumentGraphWorkflow = Effect.fn(
  "DocumentGraph.reconcile",
)(function*(input: ReconcileDocumentGraphWorkflowInput) {
  const projectionStore = yield* ProjectionIndexStore

  const projectionPrune = yield* projectionStore.pruneGraph({
    graph: input.graph,
    registered: input.registeredProjections,
  })

  const topologyStore = yield* GraphTopologyStore

  const topologyPrune = yield* topologyStore.pruneTopology({
    graph: input.graph,
    registered: input.registeredRelations,
  })

  return {
    deletedRevisions: projectionPrune.deletedRevisions,
    deletedChunks: projectionPrune.deletedChunks,
    deletedRelations: topologyPrune.deletedRelations,
  }
})
