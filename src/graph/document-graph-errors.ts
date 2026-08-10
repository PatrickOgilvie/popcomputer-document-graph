import { Schema } from "effect"

/** Safe failure while reconstructing a graph document reference. */
export class InvalidDocumentReference extends Schema.TaggedError<
  InvalidDocumentReference
>()("InvalidDocumentReference", {
  reason: Schema.Literal(
    "invalid_shape",
    "wrong_graph",
    "unknown_document_kind",
    "invalid_document_id",
  ),
}) {}

/** A graph declaration contains contradictory schema metadata. */
export class InvalidDocumentGraphDefinition extends Schema.TaggedError<
  InvalidDocumentGraphDefinition
>()("InvalidDocumentGraphDefinition", {
  reason: Schema.Literal("duplicate_projection"),
  documentKind: Schema.String,
  projection: Schema.String,
}) {}

/** A relation declaration does not match the graph's document schemas. */
export class InvalidGraphRelationDefinition extends Schema.TaggedError<
  InvalidGraphRelationDefinition
>()("InvalidGraphRelationDefinition", {
  relation: Schema.String,
  reason: Schema.Literal(
    "invalid_id",
    "invalid_version",
    "unknown_source",
    "unknown_target",
  ),
}) {}

/** A graph traversal request does not satisfy its bounded public contract. */
export class InvalidGraphTraversal extends Schema.TaggedError<
  InvalidGraphTraversal
>()("InvalidGraphTraversal", {
  reason: Schema.Literal("invalid_limit"),
}) {}
