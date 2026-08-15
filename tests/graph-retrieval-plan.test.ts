import { describe, expect, test } from "bun:test"
import { Effect, Layer, Result, Schema } from "effect"
import {
  defineDocument,
  defineDocumentGraph,
  defineEmbeddingProfile,
  EmbeddingProvider,
  type EmbeddingProviderService,
  type SearchDocumentGraphError,
} from "../src/index.js"
import {
  GraphRelationStore,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
  type GraphRelationStoreService,
  type ProjectionTextSearchStoreService,
} from "../src/adapter.js"
import { inMemoryDocumentGraph } from "../src/in-memory.js"
import { makeChunkId } from "../src/document/document-identity.js"

const AgencyId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("RetrievalPlanAgencyId"),
)
const WorkId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("RetrievalPlanWorkId"),
)
const Agency = Schema.Struct({
  id: AgencyId,
  name: Schema.Trimmed.check(Schema.isNonEmpty()),
  profile: Schema.Trimmed.check(Schema.isNonEmpty()),
})
const Work = Schema.Struct({
  id: WorkId,
  title: Schema.Trimmed.check(Schema.isNonEmpty()),
  evidence: Schema.Trimmed.check(Schema.isNonEmpty()),
  agencyIds: Schema.Array(AgencyId),
})

const AgencyDocument = defineDocument(Agency, { id: "id" }).vectorise({
  id: "profile",
  version: "v1",
  select: (agency) => ({
    context: agency.name,
    sections: [{ key: "profile", content: agency.profile }],
  }),
})
const WorkDocument = defineDocument(Work, { id: "id" }).vectorise({
  id: "evidence",
  version: "v1",
  select: (work) => ({
    context: work.title,
    sections: [{ key: "evidence", content: work.evidence }],
  }),
})
const graph = defineDocumentGraph({
  id: "retrieval-plan-test",
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

const AgencyNode = graph.document("Agency")
const WorkNode = graph.document("Work")
const AgencyProfile = AgencyNode.projection("profile")
const WorkEvidence = WorkNode.projection("evidence")
const FindAgencies = graph.retrieval({
  target: "Agency",
  routes: [AgencyProfile, WorkEvidence.through("deliveredBy")],
  strategy: "text",
  candidates: { text: 20 },
  maximumEvidencePerTarget: 2,
})
const FindAgenciesReordered = graph.retrieval({
  target: "Agency",
  routes: [WorkEvidence.through("deliveredBy"), AgencyProfile],
  strategy: "text",
  candidates: { text: 20 },
  maximumEvidencePerTarget: 2,
})
const FindAgenciesDirectly = graph.retrieval({
  target: "Agency",
  routes: [AgencyProfile],
  strategy: "text",
})
const FindAgenciesSemantically = graph.retrieval({
  target: "Agency",
  routes: [AgencyProfile],
  strategy: "semantic",
})
const FindAgenciesFromWork = graph.retrieval({
  target: "Agency",
  routes: [WorkEvidence.through("deliveredBy")],
  strategy: "text",
})
const FindAgenciesWithHybridRoutes = graph.retrieval({
  target: "Agency",
  routes: [AgencyProfile, WorkEvidence.through("deliveredBy")],
})

const agencyId = Schema.decodeSync(AgencyId)(
  "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
)
const unrelatedAgencyId = Schema.decodeSync(AgencyId)(
  "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
)
const firstWorkId = Schema.decodeSync(WorkId)(
  "11111111-1111-4111-8111-111111111111",
)
const secondWorkId = Schema.decodeSync(WorkId)(
  "22222222-2222-4222-8222-222222222222",
)
const orphanWorkId = Schema.decodeSync(WorkId)(
  "33333333-3333-4333-8333-333333333333",
)

const profile = defineEmbeddingProfile({
  id: "test:retrieval-plan",
  version: "v1",
  dimensions: 2,
})
const embeddings: EmbeddingProviderService = {
  profile,
  embedDocuments: (requests) =>
    Effect.succeed(
      requests.map((request) => ({
        contentHash: request.contentHash,
        vector: [1, 1],
      })),
    ),
  embedQuery: () => Effect.succeed([1, 1]),
}

const live = Layer.mergeAll(
  inMemoryDocumentGraph(),
  Layer.succeed(EmbeddingProvider, embeddings),
)

const seed = Effect.all([
  AgencyNode.index({
    id: agencyId,
    name: "North Studio",
    profile: "National creative partner.",
  }),
  AgencyNode.index({
    id: unrelatedAgencyId,
    name: "Local Studio",
    profile: "Neighbourhood identity specialist.",
  }),
  WorkNode.index({
    id: firstWorkId,
    title: "Retail launch",
    evidence: "National retail campaign delivery.",
    agencyIds: [agencyId],
  }),
  WorkNode.index({
    id: secondWorkId,
    title: "Distribution programme",
    evidence: "National distribution evidence.",
    agencyIds: [agencyId],
  }),
  WorkNode.index({
    id: orphanWorkId,
    title: "Orphan initiative",
    evidence: "Orphanphrase research evidence.",
    agencyIds: [],
  }),
])

describe("graph retrieval plans", () => {
  test("returns invalid runtime options through the typed channel", async () => {
    const operations = [
      FindAgencies.search("national", { limit: 0 }),
      FindAgencies.search("national", {
        candidates: { text: 0 },
      }),
    ]

    const results = await Effect.runPromise(
      Effect.forEach(operations, (operation) =>
        operation.pipe(Effect.result),
      ).pipe(Effect.provide(live)),
    )

    for (const result of results) {
      expect(Result.isFailure(result)).toBe(true)
      if (Result.isFailure(result)) {
        expect(result.failure._tag).toBe("InvalidSearchQuery")
        if (result.failure._tag === "InvalidSearchQuery") {
          expect(result.failure.reason).toBe("invalid_options")
        }
      }
    }
  })

  test("bounds data-sized neighbour expansion", async () => {
    const work = Array.from({ length: 32 }, (_, index) => ({
      id: Schema.decodeSync(WorkId)(
        `00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
      ),
      title: `National work ${index + 1}`,
      evidence: `National evidence ${index + 1}`,
      agencyIds: [agencyId],
    }))
    const revisions = await Effect.runPromise(
      Effect.forEach(work, (value) => WorkEvidence.project(value)),
    )
    const candidates = revisions.map((revision, index) => {
      const chunk = revision.chunks[0]
      return {
        score: revisions.length - index,
        chunkId: chunk.chunkId,
        documentKey: revision.documentKey,
        reference: revision.encodedTarget,
        projection: revision.projection,
        revisionHash: revision.revisionHash,
        sectionKey: chunk.sectionKey,
        sectionPart: chunk.sectionPart,
        content: chunk.content,
        metadata: chunk.metadata,
      }
    })
    const agencyKey = await Effect.runPromise(AgencyNode.key(agencyId))
    let active = 0
    let maximumActive = 0
    const textStore: ProjectionTextSearchStoreService = {
      searchTextCandidates: (request) =>
        Effect.succeed(candidates.slice(0, request.candidates)),
    }
    const relationStore: GraphRelationStoreService = {
      replaceOutgoing: () =>
        Effect.succeed({ inserted: 0, retained: 0, deleted: 0 }),
      deleteNode: () => Effect.succeed({ deleted: 0 }),
      pruneRelations: () => Effect.succeed({ deleted: 0 }),
      findOutgoing: () =>
        Effect.acquireUseRelease(
          Effect.sync(() => {
            active += 1
            maximumActive = Math.max(maximumActive, active)
          }),
          () =>
            Effect.sleep("5 millis").pipe(
              Effect.as([
                {
                  documentKey: agencyKey,
                  reference: AgencyNode.ref(agencyId),
                },
              ]),
            ),
          () =>
            Effect.sync(() => {
              active -= 1
            }),
        ),
      findIncoming: () => Effect.succeed([]),
    }

    const result = await Effect.runPromise(
      FindAgenciesFromWork.search("national", {
        candidates: { text: 32 },
        limit: 1,
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeed(ProjectionTextSearchStore, textStore),
            Layer.succeed(GraphRelationStore, relationStore),
          ),
        ),
      ),
    )

    expect(result).toHaveLength(1)
    expect(maximumActive).toBeGreaterThan(1)
    expect(maximumActive).toBeLessThanOrEqual(4)
  })

  test("traverses once for multiple candidate chunks from one source document", async () => {
    const revision = await Effect.runPromise(
      WorkEvidence.project({
        id: firstWorkId,
        title: "Retail launch",
        evidence: "National retail campaign delivery.",
        agencyIds: [agencyId],
      }),
    )
    const chunk = revision.chunks[0]
    const candidates = Array.from({ length: 12 }, (_, index) => ({
      score: 12 - index,
      chunkId: makeChunkId({
        documentKey: revision.documentKey,
        projection: revision.projection.id,
        sectionKey: chunk.sectionKey,
        sectionPart: index,
      }),
      documentKey: revision.documentKey,
      reference: revision.encodedTarget,
      projection: revision.projection,
      revisionHash: revision.revisionHash,
      sectionKey: chunk.sectionKey,
      sectionPart: index,
      content: `${chunk.content} ${index}`,
      metadata: chunk.metadata,
    }))
    const agencyKey = await Effect.runPromise(AgencyNode.key(agencyId))
    let traversals = 0
    const textStore: ProjectionTextSearchStoreService = {
      searchTextCandidates: (request) =>
        Effect.succeed(candidates.slice(0, request.candidates)),
    }
    const relationStore: GraphRelationStoreService = {
      replaceOutgoing: () =>
        Effect.succeed({ inserted: 0, retained: 0, deleted: 0 }),
      deleteNode: () => Effect.succeed({ deleted: 0 }),
      pruneRelations: () => Effect.succeed({ deleted: 0 }),
      findOutgoing: () => {
        traversals += 1
        return Effect.succeed([
          {
            documentKey: agencyKey,
            reference: AgencyNode.ref(agencyId),
          },
        ])
      },
      findIncoming: () => Effect.succeed([]),
    }

    const result = await Effect.runPromise(
      FindAgenciesFromWork.search("national", {
        candidates: { text: 12 },
        limit: 1,
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeed(ProjectionTextSearchStore, textStore),
            Layer.succeed(GraphRelationStore, relationStore),
          ),
        ),
      ),
    )

    expect(result).toHaveLength(1)
    expect(traversals).toBe(1)
  })

  test("fuses direct and related evidence at the target node", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        yield* seed
        const combined = yield* FindAgencies.search("national", { limit: 6 })
        const direct = yield* FindAgenciesDirectly.search("national", {
          limit: 6,
        })
        return { combined, direct }
      }).pipe(Effect.provide(live)),
    )

    expect(result.combined).toHaveLength(1)
    expect(result.combined[0]?.target).toEqual(AgencyNode.ref(agencyId))
    expect(
      result.combined[0]?.signals.map((signal) => signal.stream.route._tag),
    ).toEqual(["Projection", "Relation"])
    expect(result.combined[0]?.score).toBeGreaterThan(
      result.direct[0]?.score ?? Number.POSITIVE_INFINITY,
    )
    expect(result.combined[0]?.evidence).toHaveLength(2)
    expect(
      new Set(
        result.combined[0]?.evidence.map((item) => item.source.chunkId),
      ).size,
    ).toBe(2)
  })

  test("does not multiply one relation stream and drops dangling sources", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        yield* seed
        const national = yield* FindAgencies.search("national")
        const orphan = yield* FindAgencies.search("orphanphrase")
        return { national, orphan }
      }).pipe(Effect.provide(live)),
    )

    expect(
      result.national[0]?.signals.filter(
        (signal) => signal.stream.route._tag === "Relation",
      ),
    ).toHaveLength(1)
    expect(result.orphan).toEqual([])
  })

  test("keeps target results stable when route declaration order changes", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function*() {
        yield* seed
        const declared = yield* FindAgencies.search("national")
        const reordered = yield* FindAgenciesReordered.search("national")
        return { declared, reordered }
      }).pipe(Effect.provide(live)),
    )

    expect(result.declared).toEqual(result.reordered)
  })

  test("embeds one query once across every semantic retrieval route", async () => {
    const queries: Array<string> = []
    const recordingEmbeddings: EmbeddingProviderService = {
      ...embeddings,
      embedQuery: (query) => {
        queries.push(query)
        return Effect.succeed([1, 1])
      },
    }
    const recordingLive = Layer.mergeAll(
      inMemoryDocumentGraph(),
      Layer.succeed(EmbeddingProvider, recordingEmbeddings),
    )

    await Effect.runPromise(
      Effect.gen(function*() {
        yield* seed
        yield* FindAgenciesWithHybridRoutes.search("national")
      }).pipe(Effect.provide(recordingLive)),
    )

    expect(queries).toEqual(["national"])
  })
})

if (import.meta.url === "") {
  const relatedTextAction: Effect.Effect<
    unknown,
    SearchDocumentGraphError,
    ProjectionTextSearchStore | GraphRelationStore
  > = FindAgencies.search("query")
  void relatedTextAction

  const directTextAction: Effect.Effect<
    unknown,
    SearchDocumentGraphError,
    ProjectionTextSearchStore
  > = FindAgenciesDirectly.search("query")
  void directTextAction

  const directSemanticAction: Effect.Effect<
    unknown,
    SearchDocumentGraphError,
    EmbeddingProvider | ProjectionSearchStore
  > = FindAgenciesSemantically.search("query")
  void directSemanticAction

  // @ts-expect-error A direct Work route cannot target Agency.
  graph.retrieval({ target: "Agency", routes: [WorkEvidence.route()] })

  graph.retrieval({
    target: "Work",
    // @ts-expect-error The relation route terminates at Agency, not Work.
    routes: [WorkEvidence.through("deliveredBy")],
  })

  // @ts-expect-error Only schema-declared outgoing relations are available.
  WorkEvidence.through("unknown")
}
