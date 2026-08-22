import type {
  DocumentDefinitions,
  RegisteredDocumentDefinition,
} from "../document/document-definition.js"
import type { RegisteredVectorProjection } from "../document/vector-projection.js"
import type {
  DefineGraphRelation,
  GraphRelationDefinitions,
} from "./graph-relation.js"
import {
  assertUniqueProjectionIds,
  assertValidGraphRelations,
  makeDocumentGraphManifest,
  registeredGraphProjections,
  registeredGraphRelations,
  type DocumentGraphManifest,
} from "./document-graph-schema.js"
import type { DefineDocumentGraphInput } from "./document-graph.js"

/** Pure, validated graph data shared by every bound workflow. */
export interface CompiledDocumentGraph<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Relations extends GraphRelationDefinitions,
> {
  readonly id: GraphId
  readonly documents: Documents
  readonly relations: Relations
  readonly manifest: DocumentGraphManifest
  readonly registeredProjections: ReturnType<typeof registeredGraphProjections>
  readonly registeredRelations: ReturnType<typeof registeredGraphRelations>
  readonly documentsByKind: ReadonlyMap<string, RegisteredDocumentDefinition>
  readonly projectionsByDocumentKind: ReadonlyMap<
    string,
    ReadonlyMap<string, RegisteredVectorProjection>
  >
}

/** Validate and compile a graph definition without acquiring Effect services. */
export const compileDocumentGraph = <
  const GraphId extends string,
  const Documents extends DocumentDefinitions,
  const Relations extends GraphRelationDefinitions = {},
>(
  input: DefineDocumentGraphInput<GraphId, Documents, Relations>,
): CompiledDocumentGraph<GraphId, Documents, Relations> => {
  const defineRelation: DefineGraphRelation<Documents> = (relation) => relation
  // SAFETY: The empty default agrees with Relations' default. A supplied
  // callback is already constrained by DefineDocumentGraphInput.
  const relations = (input.relations === undefined
    ? {}
    : input.relations(defineRelation)) as Relations

  assertUniqueProjectionIds(input.documents)
  assertValidGraphRelations(input.documents, relations)

  const documentsByKind = new Map<string, RegisteredDocumentDefinition>()
  const projectionsByDocumentKind = new Map<
    string,
    ReadonlyMap<string, RegisteredVectorProjection>
  >()
  for (const [kind, document] of Object.entries(input.documents)) {
    documentsByKind.set(kind, document)
    projectionsByDocumentKind.set(
      kind,
      new Map(document.projections.map((projection) => [projection.id, projection])),
    )
  }

  return Object.freeze({
    id: input.id,
    documents: input.documents,
    relations,
    manifest: makeDocumentGraphManifest(input, relations),
    registeredProjections: registeredGraphProjections(input.documents),
    registeredRelations: registeredGraphRelations(relations),
    documentsByKind,
    projectionsByDocumentKind,
  })
}
