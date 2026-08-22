import { Effect } from "effect"
import { ProjectionIndexStore } from "../indexing/projection-index.js"
import {
  GraphRelationStore,
  type RegisteredGraphRelation,
} from "./graph-relation.js"

export interface ReconcileDocumentGraphWorkflowInput {
  readonly graph: string
  readonly registeredProjections: ReadonlyArray<{
    readonly documentKind: string
    readonly projection: string
  }>
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
  const relationStore = yield* GraphRelationStore
  const relationPrune = yield* relationStore.pruneRelations({
    graph: input.graph,
    registered: input.registeredRelations,
  })
  return {
    deletedRevisions: projectionPrune.deletedRevisions,
    deletedChunks: projectionPrune.deletedChunks,
    deletedRelations: relationPrune.deleted,
  }
})
