import { describe, expect, test } from "bun:test"
import { Effect, Either, Layer, Schema } from "effect"
import {
  defineDocument,
  defineDocumentGraph,
  defineEmbeddingProfile,
  EmbeddingProvider,
  type EmbeddingProviderService,
} from "../src/index.js"
import { inMemoryDocumentGraph } from "../src/in-memory.js"
import {
  GraphRelationStore,
  type GraphRelationStoreService,
} from "../src/adapter.js"

const AgencyId = Schema.UUID.pipe(Schema.brand("RelationAgencyId"))
const WorkId = Schema.UUID.pipe(Schema.brand("RelationWorkId"))
const Agency = Schema.Struct({
  id: AgencyId,
  name: Schema.NonEmptyTrimmedString,
})
const Work = Schema.Struct({
  id: WorkId,
  agencyIds: Schema.Array(AgencyId),
})
const AgencyDocument = defineDocument(Agency, { id: "id" })
const WorkDocument = defineDocument(Work, { id: "id" })

const graph = defineDocumentGraph({
  id: "relations-test",
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
const firstAgencyId = Schema.decodeSync(AgencyId)(
  "11111111-1111-4111-8111-111111111111",
)
const secondAgencyId = Schema.decodeSync(AgencyId)(
  "22222222-2222-4222-8222-222222222222",
)
const embeddingProfile = defineEmbeddingProfile({
  id: "test:relations",
  version: "v1",
  dimensions: 1,
})
const embeddings: EmbeddingProviderService = {
  profile: embeddingProfile,
  embedDocuments: (requests) =>
    Effect.succeed(
      requests.map((request) => ({
        contentHash: request.contentHash,
        vector: [1],
      })),
    ),
  embedQuery: () => Effect.succeed([1]),
}

const makeLive = () =>
  Layer.mergeAll(
    Layer.succeed(EmbeddingProvider, embeddings),
    inMemoryDocumentGraph(),
  )

describe("graph relations", () => {
  test("replaces directed edges and traverses them both ways", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const first = yield* WorkNode.index({
          id: workId,
          agencyIds: [firstAgencyId, secondAgencyId],
        })
        const initial = yield* WorkNode.neighbours(workId, {
          via: "deliveredBy",
        })
        const initialIncoming = yield* AgencyNode.neighbours(
          firstAgencyId,
          {
            via: "deliveredBy",
            direction: "incoming",
          },
        )
        const second = yield* WorkNode.index({
          id: workId,
          agencyIds: [secondAgencyId],
        })
        const updated = yield* WorkNode.neighbours(workId, {
          via: "deliveredBy",
        })
        const removedIncoming = yield* AgencyNode.neighbours(
          firstAgencyId,
          {
            via: "deliveredBy",
            direction: "incoming",
          },
        )
        const retainedIncoming = yield* AgencyNode.neighbours(
          secondAgencyId,
          {
            via: "deliveredBy",
            direction: "incoming",
          },
        )
        return {
          first,
          initial,
          initialIncoming,
          second,
          updated,
          removedIncoming,
          retainedIncoming,
        }
      }).pipe(Effect.provide(makeLive())),
    )

    expect(result.first.projections).toEqual([])
    expect(result.first.relations).toEqual({
      inserted: 2,
      retained: 0,
      deleted: 0,
    })
    expect(result.initial.map((reference) => reference.id).sort()).toEqual([
      firstAgencyId,
      secondAgencyId,
    ])
    expect(result.initialIncoming).toEqual([
      { graph: graph.id, kind: "Work", id: workId },
    ])
    expect(result.second.relations).toEqual({
      inserted: 0,
      retained: 1,
      deleted: 1,
    })
    expect(result.updated).toEqual([
      { graph: graph.id, kind: "Agency", id: secondAgencyId },
    ])
    expect(result.removedIncoming).toEqual([])
    expect(result.retainedIncoming).toEqual([
      { graph: graph.id, kind: "Work", id: workId },
    ])
  })

  test("rejects duplicate selected targets without replacing stored edges", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        yield* WorkNode.index({
          id: workId,
          agencyIds: [firstAgencyId],
        })
        const rejected = yield* WorkNode.index({
          id: workId,
          agencyIds: [secondAgencyId, secondAgencyId],
        }).pipe(Effect.either)
        const retained = yield* WorkNode.neighbours(workId, {
          via: "deliveredBy",
        })
        return { rejected, retained }
      }).pipe(Effect.provide(makeLive())),
    )

    expect(Either.isLeft(result.rejected)).toBe(true)
    if (Either.isLeft(result.rejected)) {
      expect(result.rejected.left._tag).toBe("InvalidGraphRelationOutput")
      if (result.rejected.left._tag === "InvalidGraphRelationOutput") {
        expect(result.rejected.left).toMatchObject({
          graph: graph.id,
          documentKind: "Work",
          relation: "deliveredBy",
          reason: "duplicate_target",
        })
      }
    }
    expect(result.retained).toEqual([
      { graph: graph.id, kind: "Agency", id: firstAgencyId },
    ])
  })

  test("bounds neighbour traversal with a precise caller failure", async () => {
    const result = await Effect.runPromise(
      WorkNode.neighbours(workId, {
        via: "deliveredBy",
        limit: 0,
      }).pipe(Effect.provide(makeLive()), Effect.either),
    )

    expect(Either.isLeft(result)).toBe(true)
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("InvalidGraphTraversal")
      expect(result.left.reason).toBe("invalid_limit")
    }
  })

  test("rejects duplicate neighbours returned by an adapter", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const documentKey = yield* AgencyNode.key(firstAgencyId)
        const candidate = {
          documentKey,
          reference: {
            graph: graph.id,
            kind: "Agency",
            id: firstAgencyId,
          },
        }
        const malformed: GraphRelationStoreService = {
          replaceOutgoing: () =>
            Effect.succeed({ inserted: 0, retained: 0, deleted: 0 }),
          deleteNode: () => Effect.succeed({ deleted: 0 }),
          pruneRelations: () => Effect.succeed({ deleted: 0 }),
          findOutgoing: () => Effect.succeed([candidate, candidate]),
          findIncoming: () => Effect.succeed([]),
        }
        return yield* WorkNode.neighbours(workId, {
          via: "deliveredBy",
        }).pipe(
          Effect.provide(Layer.succeed(GraphRelationStore, malformed)),
          Effect.either,
        )
      }),
    )

    expect(Either.isLeft(result)).toBe(true)
    if (Either.isLeft(result)) {
      expect(result.left._tag).toBe("DocumentGraphUnavailable")
      if (result.left._tag === "DocumentGraphUnavailable") {
        expect(result.left.operation).toBe("neighbours")
        expect(result.left.reason).toBe("invalid_stored_data")
      }
    }
  })

  test("deleting a target removes incoming edges", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        yield* WorkNode.index({
          id: workId,
          agencyIds: [firstAgencyId, secondAgencyId],
        })
        const removed = yield* AgencyNode.remove(firstAgencyId)
        const neighbours = yield* WorkNode.neighbours(workId, {
          via: "deliveredBy",
        })
        return { removed, neighbours }
      }).pipe(Effect.provide(makeLive())),
    )

    expect(result.removed.deletedRelations).toBe(1)
    expect(result.neighbours).toEqual([
      { graph: graph.id, kind: "Agency", id: secondAgencyId },
    ])
  })

  test("reconciliation removes relations absent from the current schema", async () => {
    const graphWithoutRelations = defineDocumentGraph({
      id: graph.id,
      documents: { Agency: AgencyDocument, Work: WorkDocument },
    })
    const live = makeLive()
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        yield* WorkNode.index({
          id: workId,
          agencyIds: [firstAgencyId],
        })
        const reconciled = yield* graphWithoutRelations.reconcileIndex()
        const neighbours = yield* WorkNode.neighbours(workId, {
          via: "deliveredBy",
        })
        return { reconciled, neighbours }
      }).pipe(Effect.provide(live)),
    )

    expect(result.reconciled.deletedRelations).toBe(1)
    expect(result.neighbours).toEqual([])
  })

  test("includes versioned relation policy in the graph manifest", () => {
    expect(graph.manifest.relations).toEqual([
      {
        id: "deliveredBy",
        version: "v1",
        from: "Work",
        to: "Agency",
      },
    ])
  })
})

if (false) {
  AgencyNode.neighbours(firstAgencyId, {
    via: "deliveredBy",
    direction: "incoming",
  })

  // @ts-expect-error A Work has no outgoing relation with this name.
  WorkNode.neighbours(workId, { via: "unknown" })

  // @ts-expect-error Agency has no outgoing deliveredBy relation.
  AgencyNode.neighbours(firstAgencyId, { via: "deliveredBy" })

  // @ts-expect-error Work is not the target of deliveredBy.
  WorkNode.neighbours(workId, {
    via: "deliveredBy",
    direction: "incoming",
  })

  defineDocumentGraph({
    id: "invalid-relation-types",
    documents: { Agency: AgencyDocument, Work: WorkDocument },
    relations: (relation) => ({
      deliveredBy: relation({
        from: "Work",
        to: "Agency",
        version: "v1",
        // @ts-expect-error Relation targets must use the target document ID.
        select: (work) => [work.id],
      }),
    }),
  })
}
