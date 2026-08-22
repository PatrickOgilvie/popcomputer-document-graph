import { Effect } from "effect"
import type {
  DocumentDefinitions,
  RegisteredDocumentDefinition,
} from "../document/document-definition.js"
import { parseDocumentInstance } from "../document/document-instance.js"
import { projectParsedDocument } from "../document/document-projection.js"
import type { DocumentKey } from "../document/document-identity.js"
import {
  indexProjectedRevision,
  ProjectionIndexStore,
} from "../indexing/projection-index.js"
import {
  GraphRelationStore,
  projectOutgoingGraphRelationsForInstance,
  type GraphRelationDefinitions,
} from "./graph-relation.js"

export interface IndexGraphDocumentWorkflowInput {
  readonly graph: string
  readonly documentKind: string
  readonly documents: DocumentDefinitions
  readonly relations: GraphRelationDefinitions
  readonly definition: RegisteredDocumentDefinition
  readonly value: unknown
}

/** Parse once, project every registered view, then replace outgoing relations. */
export const indexGraphDocumentWorkflow = Effect.fn(
  "DocumentGraph.indexDocument",
)(function*(input: IndexGraphDocumentWorkflowInput) {
  const document = yield* parseDocumentInstance({
    graph: input.graph,
    documentKind: input.documentKind,
    definition: input.definition,
    value: input.value,
  })
  const projectedRevisions = yield* Effect.forEach(
    input.definition.projections,
    (projection) =>
      projectParsedDocument(
        input.graph,
        input.documents,
        input.documentKind,
        projection.id,
        document,
      ),
  )
  const outgoing = yield* projectOutgoingGraphRelationsForInstance({
    graph: input.graph,
    documentKind: input.documentKind,
    documents: input.documents,
    relations: input.relations,
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
  const relations = yield* relationStore.replaceOutgoing(outgoing)
  return { projections, relations }
})

export interface RemoveGraphDocumentWorkflowInput {
  readonly graph: string
  readonly documentKey: DocumentKey
  readonly projections: ReadonlyArray<string>
}

/** Delete all projection revisions and incident relation edges for one node. */
export const removeGraphDocumentWorkflow = Effect.fn(
  "DocumentGraph.removeDocument",
)(function*(input: RemoveGraphDocumentWorkflowInput) {
  const projectionStore = yield* ProjectionIndexStore
  const deletions = yield* Effect.forEach(input.projections, (projection) =>
    projectionStore.deleteRevision({
      documentKey: input.documentKey,
      projection,
    })
  )
  const projectionDeletion = deletions.reduce(
    (total, deletion) => ({
      deletedRevisions: total.deletedRevisions + deletion.deletedRevisions,
      deletedChunks: total.deletedChunks + deletion.deletedChunks,
    }),
    { deletedRevisions: 0, deletedChunks: 0 },
  )
  const relationStore = yield* GraphRelationStore
  const relationDeletion = yield* relationStore.deleteNode({
    graph: input.graph,
    documentKey: input.documentKey,
  })
  return {
    ...projectionDeletion,
    deletedRelations: relationDeletion.deleted,
  }
})
