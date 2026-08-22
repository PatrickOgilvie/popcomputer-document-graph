import { Effect, Option } from "effect"
import type {
  DocumentKey,
  ProjectionRevisionHash,
} from "../document/document-identity.js"
import {
  documentGraphUnavailable,
  traceDocumentGraphOperation,
  type DocumentGraphUnavailable,
} from "../graph/document-graph-operation.js"
import {
  ProjectionIndexStore,
  type ProjectionIndexKey,
} from "../indexing/projection-index.js"
import type { SearchHit } from "./graph-retrieval.js"

/** Identity of one piece of retrieved evidence, carried from a search hit. */
export interface EvidenceReference {
  readonly documentKey: DocumentKey
  readonly projectionId: string
  readonly projectionVersion: string
  readonly revisionHash: ProjectionRevisionHash
}

/** Whether one evidence reference still matches the indexed revision. */
export type EvidenceCurrency = "Current" | "Stale" | "Missing"

/**
 * Build an evidence reference from one compact search hit.
 *
 * Hits already carry the document key and revision hash of the indexed
 * revision they were produced from, so references survive as plain values
 * between retrieval and later verification.
 */
export const evidenceReferenceFromHit = (hit: SearchHit): EvidenceReference => ({
  documentKey: hit.documentKey,
  projectionId: hit.projection.id,
  projectionVersion: hit.projection.version,
  revisionHash: hit.revisionHash,
})

/**
 * Report currency for each reference, aligned by input index.
 *
 * A reference is "Current" when the stored revision for its document and
 * projection carries the same revision hash, "Stale" when a different
 * revision is indexed (the hash covers content, metadata, chunking policy,
 * text-search policy, and projection version), and "Missing" when nothing is
 * indexed. Absence is data, never a failure; only storage failures fail this
 * effect.
 *
 * Currency describes the projected evidence, not the vector space used to
 * retrieve it. Re-embedding an otherwise identical revision therefore leaves
 * evidence current, while changing the projection version makes it stale.
 */
const revisionIdentity = (key: ProjectionIndexKey): string =>
  JSON.stringify([key.documentKey, key.projection])

export const verifyEvidenceCurrency: (
  references: ReadonlyArray<EvidenceReference>,
) => Effect.Effect<
  ReadonlyArray<EvidenceCurrency>,
  DocumentGraphUnavailable,
  ProjectionIndexStore
> = Effect.fn("DocumentGraph.verifyEvidenceCurrency")(function*(references) {
    if (references.length === 0) {
      return []
    }

    const store = yield* ProjectionIndexStore
    const uniqueKeys = new Map<string, ProjectionIndexKey>()
    for (const reference of references) {
      const key = {
        documentKey: reference.documentKey,
        projection: reference.projectionId,
      }
      uniqueKeys.set(revisionIdentity(key), key)
    }

    const keys = Array.from(uniqueKeys.values())
    const [firstKey, ...remainingKeys] = keys
    if (firstKey === undefined) return []

    const lookups = yield* store.loadRevisions([firstKey, ...remainingKeys])
    const revisionByIdentity = new Map(
      lookups.map((lookup) => [revisionIdentity(lookup.key), lookup.revision]),
    )

    return references.map((reference): EvidenceCurrency => {
      const revision = revisionByIdentity.get(revisionIdentity({
        documentKey: reference.documentKey,
        projection: reference.projectionId,
      })) ?? Option.none()
      return Option.isNone(revision)
        ? "Missing"
        : revision.value.revisionHash === reference.revisionHash
        ? "Current"
        : "Stale"
    })
  }, (effect, references) =>
    effect.pipe(
      Effect.catchTag("ProjectionIndexStoreFailed", (error) =>
        Effect.fail(
          documentGraphUnavailable(
            "search",
            error.reason === "invalid_stored_state"
              ? "invalid_stored_data"
              : "storage_failed",
            error,
          ),
        )),
      traceDocumentGraphOperation("search", {
        "document_graph.currency_references": references.length,
      }),
    ))
