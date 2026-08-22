import { Effect, Schema } from "effect"
import type {
  DocumentDefinitions,
  DocumentId,
  DocumentKind,
  RegisteredDocumentDefinition,
} from "../document/document-definition.js"
import {
  encodeDocumentId,
  makeDocumentKey,
  type DocumentKey,
  type InvalidDocumentIdentity,
} from "../document/document-identity.js"
import type { DocumentReference } from "../document/document-reference.js"
import { InvalidDocumentReference } from "./document-graph-errors.js"

const UnknownReferenceSchema = Schema.Struct({
  graph: Schema.String,
  kind: Schema.String,
  id: Schema.Unknown,
})

export const invalidDocumentReference = (
  reason: InvalidDocumentReference["reason"],
): InvalidDocumentReference => new InvalidDocumentReference({ reason })

/** Reference construction, parsing, and stable-key derivation for one graph. */
export const makeGraphReferenceCodec = <
  const GraphId extends string,
  const Documents extends DocumentDefinitions,
>(graph: GraphId, documents: Documents) => {
  const ref = <Kind extends DocumentKind<Documents>>(
    kind: Kind,
    id: DocumentId<Documents, Kind>,
  ): DocumentReference<GraphId, Documents, Kind> => {
    const definition: Documents[Kind] | undefined = documents[kind]
    if (definition === undefined) {
      throw new Error(`Unknown document kind: ${kind}`)
    }
    const parsedId = Schema.decodeUnknownSync(definition.id)(id)
    // SAFETY: Kind selected this definition and its ID schema parsed parsedId.
    return {
      graph,
      kind,
      id: parsedId,
    } as DocumentReference<GraphId, Documents, Kind>
  }

  const parse: (
    // oxlint-disable-next-line anti-slop/no-unknown-parameters -- This public boundary immediately decodes the value with UnknownReferenceSchema.
    candidate: unknown,
  ) => Effect.Effect<
    DocumentReference<GraphId, Documents>,
    InvalidDocumentReference
  > = Effect.fn("DocumentGraph.parseReference")(function*(candidate) {
    const parsed = yield* Schema.decodeUnknownEffect(UnknownReferenceSchema)(
      candidate,
      { onExcessProperty: "error" },
    ).pipe(
      Effect.mapError(() => invalidDocumentReference("invalid_shape")),
    )
    if (parsed.graph !== graph) {
      return yield* Effect.fail(invalidDocumentReference("wrong_graph"))
    }

    const definition: RegisteredDocumentDefinition | undefined =
      documents[parsed.kind]
    if (definition === undefined) {
      return yield* Effect.fail(
        invalidDocumentReference("unknown_document_kind"),
      )
    }
    const id = yield* Schema.decodeUnknownEffect(definition.id)(parsed.id).pipe(
      Effect.mapError(() => invalidDocumentReference("invalid_document_id")),
    )
    // SAFETY: Graph, kind, and the selected definition's ID schema were checked.
    return {
      graph,
      kind: parsed.kind,
      id,
    } as DocumentReference<GraphId, Documents>
  })

  const key: <Kind extends DocumentKind<Documents>>(
    target: DocumentReference<GraphId, Documents, Kind>,
  ) => Effect.Effect<DocumentKey, InvalidDocumentIdentity> = Effect.fn(
    "DocumentGraph.referenceKey",
  )(function*(target) {
    const definition = documents[target.kind]
    if (definition === undefined) {
      return yield* Effect.die(
        new Error(`Unknown document kind ${target.kind} for graph ${graph}`),
      )
    }
    const encodedId = yield* encodeDocumentId(
      graph,
      target.kind,
      definition.id,
      target.id,
    )
    return makeDocumentKey({
      graph,
      documentKind: target.kind,
      encodedId,
    })
  })

  return Object.freeze({ ref, parse, key })
}
