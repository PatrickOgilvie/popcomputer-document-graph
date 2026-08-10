import { Context, Effect, Schema } from "effect"
import type { EncodedDocumentReference } from "../document/document-instance.js"
import type { JsonValue } from "../document/json-value.js"
import {
  documentGraphUnavailable,
  traceDocumentGraphOperation,
  type DocumentGraphUnavailable,
} from "../graph/document-graph-operation.js"
import type { SearchHit } from "./graph-retrieval.js"

/** Requested amount of source material surrounding a retrieval hit. */
export type GroundingLevel = "section" | "document"

/** Request passed to an application-owned grounding hydrator. */
export interface GroundingHydrationRequest {
  readonly hit: SearchHit
  readonly level: GroundingLevel
}

/** Application-owned source payload loaded for one attributed search hit. */
export interface GroundingPayload {
  readonly content: string
  readonly metadata: JsonValue | undefined
}

/** Larger grounding material carrying package-owned attribution. */
export interface GroundingMaterial extends GroundingPayload {
  readonly reference: EncodedDocumentReference
  readonly sectionKey: string | undefined
}

/** An application source adapter could not hydrate grounding material. */
export class GroundingHydrationFailed extends Schema.TaggedError<
  GroundingHydrationFailed
>()("GroundingHydrationFailed", {
  reason: Schema.Literal("not_found", "unavailable", "invalid_output"),
  cause: Schema.optional(Schema.Unknown),
}) {}

/** Application-owned capability for loading section or document grounding. */
export interface GroundingHydratorService {
  readonly hydrate: (
    request: GroundingHydrationRequest,
  ) => Effect.Effect<GroundingPayload, GroundingHydrationFailed>
}

/** Effect service tag for application-owned grounding hydration. */
export class GroundingHydrator extends Context.Tag(
  "@popcomputer/document-graph/GroundingHydrator",
)<GroundingHydrator, GroundingHydratorService>() {}

/** Hydrate one compact hit into larger section or document material. */
export const hydrateGrounding = (
  hit: SearchHit,
  input: { readonly level: GroundingLevel } = { level: "section" },
): Effect.Effect<
  GroundingMaterial,
  DocumentGraphUnavailable,
  GroundingHydrator
> =>
  Effect.gen(function*() {
    const hydrator = yield* GroundingHydrator
    const payload = yield* hydrator.hydrate({
      hit,
      level: input.level,
    })

    if (payload.content.trim().length === 0) {
      return yield* Effect.fail(
        new GroundingHydrationFailed({
          reason: "invalid_output",
        }),
      )
    }

    return {
      reference: hit.reference,
      sectionKey: input.level === "section" ? hit.sectionKey : undefined,
      content: payload.content,
      metadata: payload.metadata,
    }
  }).pipe(
    Effect.catchTag("GroundingHydrationFailed", (error) =>
      Effect.fail(
        documentGraphUnavailable(
          "hydrate",
          error.reason === "invalid_output"
            ? "invalid_adapter_output"
            : "hydration_failed",
          error,
        ),
      ),
    ),
    traceDocumentGraphOperation("hydrate", {
      "document_graph.graph": hit.reference.graph,
      "document_graph.document_kind": hit.reference.kind,
      "document_graph.projection": hit.projection.id,
      "document_graph.hydration.level": input.level,
    }),
  )
