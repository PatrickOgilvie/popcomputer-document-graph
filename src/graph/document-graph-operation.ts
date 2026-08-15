import { Effect, Schema, type Tracer } from "effect"

/** Public operations that may depend on document-graph infrastructure. */
export const DocumentGraphOperationSchema = Schema.Literals([
  "index",
  "remove",
  "reconcile_index",
  "search",
  "hydrate",
  "neighbours",
])

/** Public operations that may depend on document-graph infrastructure. */
export type DocumentGraphOperation = typeof DocumentGraphOperationSchema.Type

/** Stable, non-sensitive categories for unavailable graph operations. */
export const DocumentGraphUnavailableReasonSchema = Schema.Literals([
  "embedding_failed",
  "storage_failed",
  "hydration_failed",
  "invalid_stored_data",
  "invalid_adapter_output",
])

/** Stable, non-sensitive categories for unavailable graph operations. */
export type DocumentGraphUnavailableReason =
  typeof DocumentGraphUnavailableReasonSchema.Type

/** An operational dependency prevented a public graph operation completing. */
export class DocumentGraphUnavailable extends Schema.TaggedError<
  DocumentGraphUnavailable
>()("DocumentGraphUnavailable", {
  operation: DocumentGraphOperationSchema,
  reason: DocumentGraphUnavailableReasonSchema,
  cause: Schema.Unknown,
}) {}

/** Safe fields suitable for logs, metrics, traces, and protocol responses. */
export interface DocumentGraphErrorTelemetry {
  readonly errorTag: "DocumentGraphUnavailable"
  readonly errorOperation: DocumentGraphOperation
  readonly errorReason: DocumentGraphUnavailableReason
}

/** Remove provider causes and other sensitive details from public telemetry. */
export const toDocumentGraphErrorTelemetry = (
  error: DocumentGraphUnavailable,
): DocumentGraphErrorTelemetry => ({
  errorTag: error._tag,
  errorOperation: error.operation,
  errorReason: error.reason,
})

/** Create one public operational failure while preserving its internal cause. */
export const documentGraphUnavailable = (
  operation: DocumentGraphOperation,
  reason: DocumentGraphUnavailableReason,
  cause: unknown,
): DocumentGraphUnavailable =>
  new DocumentGraphUnavailable({ operation, reason, cause })

/** Non-sensitive attributes attached to a public document-graph operation. */
export type DocumentGraphSpanAttributes = Readonly<
  Record<string, string | number | boolean>
>

/** Trace one public operation without recording content, queries, or metadata. */
export const traceDocumentGraphOperation = (
  operation: DocumentGraphOperation,
  attributes: DocumentGraphSpanAttributes,
) =>
  <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E, Exclude<R, Tracer.ParentSpan>> =>
    effect.pipe(
      Effect.withSpan(`honertia.document_graph.${operation}`, {
        attributes,
      }),
    )
