import { Schema } from "effect"
import type { ChunkingStrategyDescriptor } from "../document/chunking-strategy.js"
import type { DocumentDefinitions } from "../document/document-definition.js"
import type { RegisteredVectorProjection } from "../document/vector-projection.js"
import {
  GraphRelationIdSchema,
  GraphRelationVersionSchema,
  type GraphRelationDefinitions,
  type RegisteredGraphRelation,
} from "./graph-relation.js"
import {
  InvalidDocumentGraphDefinition,
  InvalidGraphRelationDefinition,
} from "./document-graph-errors.js"

/** Serializable manifest containing active document projection policy. */
export interface DocumentGraphManifest {
  readonly id: string
  readonly documents: ReadonlyArray<{
    readonly kind: string
    readonly projections: ReadonlyArray<{
      readonly id: string
      readonly version: string
      readonly chunking: ChunkingStrategyDescriptor
      readonly text:
        | "disabled"
        | {
            readonly language: "english" | "simple"
            readonly weights: {
              readonly context: number
              readonly label: number
              readonly content: number
            }
          }
    }>
  }>
  readonly relations: ReadonlyArray<{
    readonly id: string
    readonly version: string
    readonly from: string
    readonly to: string
  }>
}

type ManifestProjection =
  DocumentGraphManifest["documents"][number]["projections"][number]

const projectTextPolicy = (
  policy: RegisteredVectorProjection["text"],
): ManifestProjection["text"] =>
  policy === "disabled"
    ? "disabled"
    : {
        language: policy.language,
        weights: {
          context: policy.weights.context,
          label: policy.weights.label,
          content: policy.weights.content,
        },
      }

/** Build the stable serializable view of one compiled graph schema. */
export const makeDocumentGraphManifest = (
  input: {
    readonly id: string
    readonly documents: DocumentDefinitions
  },
  relations: GraphRelationDefinitions,
): DocumentGraphManifest => ({
  id: input.id,
  documents: Object.entries(input.documents)
    .map(([kind, definition]) => ({
      kind,
      projections: definition.projections.map((projection) => ({
        id: projection.id,
        version: projection.version,
        chunking: {
          id: projection.chunking.id,
          version: projection.chunking.version,
          config: projection.chunking.encodedConfig,
        },
        text: projectTextPolicy(projection.text),
      })),
    }))
    .sort((left, right) => left.kind.localeCompare(right.kind)),
  relations: Object.entries(relations)
    .map(([id, relation]) => ({
      id,
      version: relation.version,
      from: relation.from,
      to: relation.to,
    }))
    .sort((left, right) => left.id.localeCompare(right.id)),
})

/** Validate relation declarations against their owning document schema. */
export const assertValidGraphRelations = (
  documents: DocumentDefinitions,
  relations: GraphRelationDefinitions,
): void => {
  for (const [id, relation] of Object.entries(relations)) {
    if (!Schema.is(GraphRelationIdSchema)(id)) {
      throw new InvalidGraphRelationDefinition({
        relation: id,
        reason: "invalid_id",
      })
    }
    if (!Schema.is(GraphRelationVersionSchema)(relation.version)) {
      throw new InvalidGraphRelationDefinition({
        relation: id,
        reason: "invalid_version",
      })
    }
    if (documents[relation.from] === undefined) {
      throw new InvalidGraphRelationDefinition({
        relation: id,
        reason: "unknown_source",
      })
    }
    if (documents[relation.to] === undefined) {
      throw new InvalidGraphRelationDefinition({
        relation: id,
        reason: "unknown_target",
      })
    }
  }
}

/** Convert relation declarations to the persistence registry contract. */
export const registeredGraphRelations = (
  relations: GraphRelationDefinitions,
): ReadonlyArray<RegisteredGraphRelation> =>
  Object.entries(relations).map(([id, relation]) => ({
    id,
    version: relation.version,
    sourceDocumentKind: relation.from,
    targetDocumentKind: relation.to,
  }))

/** Convert document projections to the persistence registry contract. */
export const registeredGraphProjections = (
  documents: DocumentDefinitions,
): ReadonlyArray<{
  readonly documentKind: string
  readonly projection: string
}> =>
  Object.entries(documents).flatMap(([documentKind, definition]) =>
    definition.projections.map((projection) => ({
      documentKind,
      projection: projection.id,
    })),
  )

/** Reject ambiguous projection handles at graph composition time. */
export const assertUniqueProjectionIds = (
  documents: DocumentDefinitions,
): void => {
  for (const [documentKind, definition] of Object.entries(documents)) {
    const seen = new Set<string>()
    for (const projection of definition.projections) {
      if (seen.has(projection.id)) {
        throw new InvalidDocumentGraphDefinition({
          reason: "duplicate_projection",
          documentKind,
          projection: projection.id,
        })
      }
      seen.add(projection.id)
    }
  }
}
