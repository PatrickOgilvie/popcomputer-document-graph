import { Effect, Schema } from "effect"
import type {
  IndexGraphDocumentError,
  SearchDocumentGraphError,
} from "@popcomputer/document-graph"
import {
  action,
  httpError,
  json,
  validateRequest,
  type HttpError,
} from "honertia/effect"
import {
  FindAgencies,
  WorkEvidence,
  WorkNode,
  WorkSchema,
} from "./site-graph.js"

const SearchRequest = Schema.Struct({
  query: Schema.NonEmptyTrimmedString.pipe(Schema.maxLength(4_000)),
})

const strictBody = {
  request: { order: ["body"] },
  parseOptions: { onExcessProperty: "error" },
} as const

const searchForHttp = <Success, Requirements>(
  operation: Effect.Effect<
    Success,
    SearchDocumentGraphError,
    Requirements
  >,
): Effect.Effect<Success, HttpError, Requirements> =>
  operation.pipe(
    Effect.catchTags({
      InvalidSearchQuery: () =>
        httpError(400, "Enter a valid search query"),
      DocumentGraphUnavailable: () =>
        httpError(503, "Search is temporarily unavailable"),
    }),
  )

const indexForHttp = <Success, Requirements>(
  operation: Effect.Effect<
    Success,
    IndexGraphDocumentError,
    Requirements
  >,
): Effect.Effect<Success, HttpError, Requirements> =>
  operation.pipe(
    Effect.catchTag("ProjectionIndexConflict", () =>
      httpError(409, "This document changed while it was being indexed"),
    ),
    Effect.catchTag("DocumentGraphUnavailable", () =>
      httpError(503, "Indexing is temporarily unavailable"),
    ),
    Effect.catchAll(() =>
      httpError(422, "The document could not be projected"),
    ),
  )

/** Index a Work after the application's source document changes. */
export const indexWorkAction = action(
  Effect.gen(function* () {
    const work = yield* validateRequest(WorkSchema, strictBody)
    const indexed = yield* indexForHttp(WorkNode.index(work))

    return yield* json({
      projections: indexed.projections.map(({ projection, result }) => ({
        projection,
        change: result._tag,
      })),
      relations: {
        inserted: indexed.relations.inserted,
        retained: indexed.relations.retained,
        deleted: indexed.relations.deleted,
      },
    })
  }),
)

/** Search one projection using its schema-defined hybrid strategy. */
export const searchWorkAction = action(
  Effect.gen(function* () {
    const input = yield* validateRequest(SearchRequest, strictBody)
    const hits = yield* WorkEvidence.search(input.query, {
      limit: 10,
    }).pipe(
      Effect.catchTag("InvalidSearchQuery", () =>
        httpError(400, "Enter a valid search query"),
      ),
      Effect.catchTag("DocumentGraphUnavailable", () =>
        httpError(503, "Search is temporarily unavailable"),
      ),
    )

    return yield* json({
      hits: hits.map((hit) => ({
        workId: hit.reference.id,
        kind: hit.metadata.kind,
        content: hit.content,
      })),
    })
  }),
)

/** Rank Agency nodes using profiles and evidence from related Work. */
export const findAgenciesAction = action(
  Effect.gen(function* () {
    const input = yield* validateRequest(SearchRequest, strictBody)
    const agencies = yield* searchForHttp(
      FindAgencies.search(input.query, { limit: 6 }),
    )

    return yield* json({
      agencies: agencies.map((agency) => ({
        agencyId: agency.target.id,
        evidence: agency.evidence.map(({ source }) => ({
          sourceKind: source.reference.kind,
          content: source.content,
        })),
      })),
    })
  }),
)
