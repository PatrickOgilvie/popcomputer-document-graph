import { describe, expect, test } from "bun:test"
import { Effect, Layer, Option, Result, Schema } from "effect"
import {
  defineDocument,
  defineDocumentGraph,
  defineEmbeddingProfile,
  EmbeddingProvider,
  type EmbeddingProviderService,
} from "../src/index.js"
import { inMemoryDocumentGraph } from "../src/in-memory.js"

const AgencyId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("PublicGraphQueryAgencyId"),
)

const WorkId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("PublicGraphQueryWorkId"),
)

const Agency = Schema.Struct({
  id: AgencyId,
  name: Schema.Trimmed.check(Schema.isNonEmpty()),
})

const Work = Schema.Struct({
  id: WorkId,
  agencyIds: Schema.Array(AgencyId),
})

const AgencyDocument = defineDocument(Agency, { id: "id" }).vectorise({
  id: "agency-name",
  version: "v1",
  select: (agency) => ({
    sections: [{ key: "name", content: agency.name }],
  }),
})

const WorkDocument = defineDocument(Work, { id: "id" })

const graph = defineDocumentGraph({
  id: "public-graph-query-test",
  documents: { Agency: AgencyDocument, Work: WorkDocument },
  relations: (relation) => ({
    deliveredBy: relation({
      from: "Work",
      to: "Agency",
      version: "v1",
      select: (work) => work.agencyIds,
    }),
  }),
})

const WorkNode = graph.document("Work")

const AgencyNode = graph.document("Agency")

const workId = Schema.decodeSync(WorkId)(
  "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
)

const eligibleAgencyId = Schema.decodeSync(AgencyId)(
  "11111111-1111-4111-8111-111111111111",
)

const unrelatedAgencyId = Schema.decodeSync(AgencyId)(
  "99999999-9999-4999-8999-999999999999",
)

const profile = defineEmbeddingProfile({
  id: "test:public-graph-query",
  version: "v1",
  dimensions: 2,
})

const makeLayer = (recordQuery: () => void = () => undefined) => {
  const embeddings: EmbeddingProviderService = {
    profile,
    embedDocuments: (requests) =>
      Effect.succeed(
        requests.map((request) => ({
          contentHash: request.contentHash,
          vector: request.content.includes("Unrelated")
            ? [1, 0]
            : [0.8, 0.2],
        })),
      ),
    embedQuery: () => {
      recordQuery()

      return Effect.succeed([1, 0])
    },
  }

  return Layer.mergeAll(
    Layer.succeed(EmbeddingProvider, embeddings),
    inMemoryDocumentGraph(),
  )
}

describe("public graph topology queries", () => {
  test("default node pages exclude retired document kinds after reconciliation", async () => {
    const document = defineDocument(Schema.Struct({ id: Schema.String }), { id: "id" })

    const previous = defineDocumentGraph({
      id: "evolving-node-catalog",
      documents: { Agency: document, RetiredDocument: document },
    })

    const current = defineDocumentGraph({
      id: previous.id,
      documents: { Agency: document },
    })

    const retired = defineDocumentGraph({ id: previous.id, documents: {} })

    const pages = await Effect.runPromise(Effect.gen(function*() {
      yield* previous.document("RetiredDocument").index({ id: "retired" })
      yield* previous.document("Agency").index({ id: "first" })
      yield* previous.document("Agency").index({ id: "second" })
      yield* current.reconcileIndex()
      const first = yield* current.nodes({ limit: 1 })

      const second = yield* current.nodes({
        limit: 1,
        after: Option.getOrThrow(first.next),
      })

      const explicitEmpty = yield* current.nodes({ include: [] })
      const allRetired = yield* retired.nodes()

      return { first, second, explicitEmpty, allRetired }
    }).pipe(Effect.provide(makeLayer())))

    expect([...pages.first.nodes, ...pages.second.nodes].map((node) => node.reference.kind))
      .toEqual(["Agency", "Agency"])
    expect(new Set([...pages.first.nodes, ...pages.second.nodes].map((node) => node.documentKey)).size).toBe(2)
    expect(Option.isNone(pages.second.next)).toBe(true)
    expect(pages.explicitEmpty.nodes).toEqual([...pages.first.nodes, ...pages.second.nodes])
    expect(pages.allRetired).toEqual({ nodes: [], next: Option.none() })
  })

  test("pages nodes, resolves related nodes, and constrains search before limiting", async () => {
    let queryCount = 0

    const result = await Effect.runPromise(
      Effect.gen(function*() {
        yield* WorkNode.index({
          id: workId,
          agencyIds: [eligibleAgencyId],
        })
        const referenced = yield* graph.nodes({ states: ["Referenced"] })

        yield* AgencyNode.index({
          id: eligibleAgencyId,
          name: "Eligible studio",
        })
        yield* AgencyNode.index({
          id: unrelatedAgencyId,
          name: "Unrelated studio",
        })

        const firstPage = yield* graph.nodes({ limit: 2 })
        const next = Option.getOrUndefined(firstPage.next)

        if (next === undefined) {
          return yield* Effect.die(
            new Error("Expected a continuation for the first node page"),
          )
        }

        const secondPage = yield* graph.nodes({ after: next, limit: 2 })

        const related = yield* WorkNode.relatedNodes(workId, {
          via: "deliveredBy",
        })

        const hits = yield* WorkNode.searchWithin(workId, "studio", {
          via: "deliveredBy",
          maximumDocuments: 1,
          search: { candidates: 1, limit: 1 },
        })

        return { referenced, firstPage, secondPage, related, hits }
      }).pipe(Effect.provide(makeLayer(() => { queryCount += 1 }))),
    )

    expect(result.referenced.nodes).toEqual([
      {
        documentKey: expect.any(String),
        reference: {
          graph: graph.id,
          kind: "Agency",
          id: eligibleAgencyId,
        },
        state: "Referenced",
      },
    ])
    expect([
      ...result.firstPage.nodes,
      ...result.secondPage.nodes,
    ]).toHaveLength(3)
    expect(result.related).toEqual([
      { graph: graph.id, kind: "Agency", id: eligibleAgencyId },
    ])
    expect(result.hits.map((hit) => hit.reference)).toEqual([
      { graph: graph.id, kind: "Agency", id: eligibleAgencyId },
    ])
    expect(queryCount).toBe(1)
  })

  test("preserves an empty topology result as NoDocuments and skips embedding", async () => {
    let queryCount = 0

    const hits = await Effect.runPromise(
      Effect.gen(function*() {
        yield* WorkNode.index({ id: workId, agencyIds: [] })

        return yield* WorkNode.searchWithin(workId, "studio", {
          via: "deliveredBy",
          maximumDocuments: 10,
        })
      }).pipe(Effect.provide(makeLayer(() => { queryCount += 1 }))),
    )

    expect(hits).toEqual([])
    expect(queryCount).toBe(0)
  })

  test("rejects an omitted search-within population bound at runtime", async () => {
    let queryCount = 0

    const result = await Effect.runPromise(
      WorkNode.searchWithin(workId, "studio", {
        via: "deliveredBy",
        // @ts-expect-error JavaScript callers can omit this compile-time-required bound.
        maximumDocuments: undefined,
      }).pipe(
        Effect.result,
        Effect.provide(makeLayer(() => { queryCount += 1 })),
      ),
    )

    expect(Result.isFailure(result)).toBe(true)

    if (Result.isFailure(result)) {
      expect(result.failure).toMatchObject({
        _tag: "InvalidGraphTraversal",
        reason: "invalid_limit",
      })
    }

    expect(queryCount).toBe(0)
  })
})
