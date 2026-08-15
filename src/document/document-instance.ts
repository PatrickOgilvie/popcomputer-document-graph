import { Effect, Schema } from "effect"
import type { JsonValue } from "./json-value.js"
import type { RegisteredDocumentDefinition } from "./document-definition.js"
import {
  encodeDocumentId,
  makeDocumentKey,
  type DocumentKey,
  type InvalidDocumentIdentity,
} from "./document-identity.js"

/** A document value or its derived identity failed its owning schema. */
export class InvalidDocumentValue extends Schema.TaggedError<
  InvalidDocumentValue
>()("InvalidDocumentValue", {
  graph: Schema.String,
  documentKind: Schema.String,
  reason: Schema.Literals(["schema_rejected", "identity_rejected"]),
}) {}

/** JSON-safe graph reference persisted by storage adapters. */
export interface EncodedDocumentReference {
  readonly graph: string
  readonly kind: string
  readonly id: JsonValue
}

/** One document parsed, identified, encoded and keyed through its definition. */
export interface ParsedDocumentInstance<
  GraphId extends string = string,
  Kind extends string = string,
  Definition extends RegisteredDocumentDefinition = RegisteredDocumentDefinition,
> {
  readonly value: Definition["value"]["Type"]
  readonly id: Definition["id"]["Type"]
  readonly encodedId: JsonValue
  readonly documentKey: DocumentKey
  readonly reference: {
    readonly graph: GraphId
    readonly kind: Kind
    readonly id: Definition["id"]["Type"]
  }
  readonly encodedReference: EncodedDocumentReference
}

/** Parse and derive every stable identity representation for one document. */
export const parseDocumentInstance = <
  const GraphId extends string,
  const Kind extends string,
  Definition extends RegisteredDocumentDefinition,
>(input: {
  readonly graph: GraphId
  readonly documentKind: Kind
  readonly definition: Definition
  readonly value: unknown
}): Effect.Effect<
  ParsedDocumentInstance<GraphId, Kind, Definition>,
  InvalidDocumentValue | InvalidDocumentIdentity
> =>
  Effect.gen(function*() {
    const value = yield* Schema.decodeEffect(
      Schema.toType(input.definition.value),
    )(input.value, { onExcessProperty: "error" }).pipe(
      Effect.mapError(
        () =>
          new InvalidDocumentValue({
            graph: input.graph,
            documentKind: input.documentKind,
            reason: "schema_rejected",
          }),
      ),
    )
    const identified = yield* Effect.sync(() =>
      input.definition.identify(value),
    )
    const id = yield* Schema.decodeEffect(
      Schema.toType(input.definition.id),
    )(identified, { onExcessProperty: "error" }).pipe(
      Effect.mapError(
        () =>
          new InvalidDocumentValue({
            graph: input.graph,
            documentKind: input.documentKind,
            reason: "identity_rejected",
          }),
      ),
    )
    const encodedId = yield* encodeDocumentId(
      input.graph,
      input.documentKind,
      input.definition.id,
      id,
    )
    const documentKey = makeDocumentKey({
      graph: input.graph,
      documentKind: input.documentKind,
      encodedId,
    })

    return {
      value,
      id,
      encodedId,
      documentKey,
      reference: {
        graph: input.graph,
        kind: input.documentKind,
        id,
      },
      encodedReference: {
        graph: input.graph,
        kind: input.documentKind,
        id: encodedId,
      },
    }
  })
