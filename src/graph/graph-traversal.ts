import { Effect } from "effect"
import type { DocumentKey, InvalidDocumentIdentity } from "../document/document-identity.js"
import type { EncodedDocumentReference } from "../document/document-instance.js"
import type { InvalidDocumentReference } from "./document-graph-errors.js"
import {
  GraphRelationStore,
  InvalidGraphNeighbourOutput,
  type GraphNeighbourLimit,
  type GraphRelationDefinitions,
} from "./graph-relation.js"
import { invalidDocumentReference } from "./graph-reference.js"

export interface FindGraphNeighboursWorkflowInput<Reference> {
  readonly graph: string
  readonly relations: GraphRelationDefinitions
  readonly currentDocumentKey: DocumentKey
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

/** Read and validate one bounded, deterministic neighbour set. */
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

  const store = yield* GraphRelationStore
  const stored = yield* (input.direction === "outgoing"
    ? store.findOutgoing({
        graph: input.graph,
        sourceDocumentKey: input.currentDocumentKey,
        sourceDocumentKind: input.currentDocumentKind,
        relation: input.relationId,
        relationVersion: relation.version,
        targetDocumentKind: relation.to,
        limit: input.limit,
      })
    : store.findIncoming({
        graph: input.graph,
        targetDocumentKey: input.currentDocumentKey,
        targetDocumentKind: input.currentDocumentKind,
        relation: input.relationId,
        relationVersion: relation.version,
        sourceDocumentKind: relation.from,
        limit: input.limit,
      }))
  const neighbourKind = input.direction === "outgoing"
    ? relation.to
    : relation.from
  if (stored.length > input.limit) {
    return yield* Effect.fail(
      new InvalidGraphNeighbourOutput({ reason: "too_many" }),
    )
  }

  const seen = new Set<DocumentKey>()
  let previousKey: DocumentKey | undefined
  for (const candidate of stored) {
    if (seen.has(candidate.documentKey)) {
      return yield* Effect.fail(
        new InvalidGraphNeighbourOutput({ reason: "duplicate" }),
      )
    }
    if (
      previousKey !== undefined &&
      String(previousKey).localeCompare(String(candidate.documentKey)) > 0
    ) {
      return yield* Effect.fail(
        new InvalidGraphNeighbourOutput({ reason: "not_ordered" }),
      )
    }
    seen.add(candidate.documentKey)
    previousKey = candidate.documentKey
  }

  return yield* Effect.forEach(stored, (candidate) =>
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
})
