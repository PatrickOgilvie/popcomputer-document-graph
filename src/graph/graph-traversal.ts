import { Effect } from "effect"
import type { DocumentKey, InvalidDocumentIdentity } from "../document/document-identity.js"
import type { EncodedDocumentReference } from "../document/document-instance.js"
import type { InvalidDocumentReference } from "./document-graph-errors.js"
import type {
  GraphNeighbourLimit,
  GraphRelationDefinitions,
} from "./graph-relation.js"
import { invalidDocumentReference } from "./graph-reference.js"
import {
  GraphTopologyStore,
  InvalidGraphTopologyOutput,
} from "./graph-topology.js"

export interface FindGraphNeighboursWorkflowInput<Reference> {
  readonly graph: string
  readonly relations: GraphRelationDefinitions
  readonly currentDocumentKeys: ReadonlyArray<DocumentKey>
  readonly currentDocumentKind: string
  readonly relationId: string
  readonly direction: "outgoing" | "incoming"
  readonly limit: GraphNeighbourLimit
  readonly parseReference: (
    candidate: EncodedDocumentReference,
  ) => Effect.Effect<Reference, InvalidDocumentReference>
  readonly referenceKind: (reference: Reference) => string
  readonly referenceKey: (
    reference: Reference,
  ) => Effect.Effect<DocumentKey, InvalidDocumentIdentity>
}

export interface RuntimeGraphNeighbour<Reference> {
  readonly documentKey: DocumentKey
  readonly reference: Reference
}

/** Read ordered neighbour groups and validate every source and result identity. */
export const findGraphNeighboursWorkflow = Effect.fn(
  "GraphTraversal.findNeighbours",
)(function*<Reference>(input: FindGraphNeighboursWorkflowInput<Reference>) {
  const relation = input.relations[input.relationId]

  const currentKind = input.direction === "outgoing"
    ? relation?.from
    : relation?.to

  if (relation === undefined || currentKind !== input.currentDocumentKind) {
    return yield* Effect.die(
      new Error(
        `Unknown ${input.direction} relation ${input.relationId} for ${input.currentDocumentKind}`,
      ),
    )
  }

  const store = yield* GraphTopologyStore

  const neighbourKind = input.direction === "outgoing"
    ? relation.to
    : relation.from

  const stored = yield* store.findRelatedNodes({
    graph: input.graph,
    documentKeys: input.currentDocumentKeys,
    documentKind: input.currentDocumentKind,
    direction: input.direction,
    relation: input.relationId,
    relationVersion: relation.version,
    relatedDocumentKind: neighbourKind,
    limit: input.limit,
  })

  if (stored.length !== input.currentDocumentKeys.length || stored.some(
    (group, index) => group.documentKey !== input.currentDocumentKeys[index],
  )) {
    return yield* new InvalidGraphTopologyOutput({
      output: "related_nodes",
      reason: "invalid_batch",
    })
  }

  return yield* Effect.forEach(stored, (group) => Effect.gen(function*() {
    if (group.nodes.length > input.limit) {
      return yield* new InvalidGraphTopologyOutput({
        output: "related_nodes",
        reason: "too_many",
      })
    }

    const seen = new Set<DocumentKey>()
    let previousKey: DocumentKey | undefined

    for (const candidate of group.nodes) {
      if (seen.has(candidate.documentKey)) {
        return yield* new InvalidGraphTopologyOutput({
          output: "related_nodes",
          reason: "duplicate",
        })
      }

      if (
        previousKey !== undefined &&
        String(previousKey).localeCompare(String(candidate.documentKey)) > 0
      ) {
        return yield* new InvalidGraphTopologyOutput({
          output: "related_nodes",
          reason: "not_ordered",
        })
      }

      seen.add(candidate.documentKey)
      previousKey = candidate.documentKey
    }

    const nodes = yield* Effect.forEach(group.nodes, (candidate) =>
      input.parseReference(candidate.reference).pipe(
        Effect.flatMap((reference) => {
          if (input.referenceKind(reference) !== neighbourKind) {
            return Effect.fail(
              invalidDocumentReference("unknown_document_kind"),
            )
          }

          return input.referenceKey(reference).pipe(
            Effect.flatMap((parsedKey) =>
              parsedKey === candidate.documentKey
                ? Effect.succeed({
                    documentKey: candidate.documentKey,
                    reference,
                  })
                : Effect.fail(
                    invalidDocumentReference("invalid_document_id"),
                  ),
            ),
          )
        }),
      )
    )

    return { documentKey: group.documentKey, nodes }
  }))
})
