/// <reference types="./env.d.ts" />

import { env } from "cloudflare:workers"
import { Effect, Option, Schema } from "effect"
import { describe, expect, test } from "vitest"
import {
  ChunkIdSchema,
  ContentHashSchema,
  makeDocumentKey,
  ProjectionRevisionHashSchema,
} from "../../src/document/document-identity.js"
import type { EncodedDocumentReference } from "../../src/document/document-instance.js"
import { GraphNeighbourLimitSchema } from "../../src/graph/graph-relation.js"
import {
  GraphNodePageLimitSchema,
  GraphTopologyStore,
} from "../../src/graph/graph-topology.js"
import { defineEmbeddingProfile } from "../../src/indexing/embedding-provider.js"
import {
  ProjectionMutationIdSchema,
  ProjectionPayloadDigestSchema,
  ProjectionPublicationCoordinator,
  type ProjectionReplacementIntent,
} from "../../src/indexing/projection-publication.js"
import {
  IndexRevisionTokenSchema,
  type ProjectionIndexKey,
} from "../../src/indexing/projection-index.js"
import { d1ProjectionPublicationCoordinator } from "../../src/storage/d1/projection-publication.js"
import { d1GraphTopology } from "../../src/storage/d1/runtime.js"

const Graph = "workerd-contracts"

const reference = (
  kind: string,
  id: string,
): EncodedDocumentReference => ({
  graph: Graph,
  kind,
  id,
})

const source = reference("Contract", "contract-1")
const target = reference("Invoice", "invoice-1")
const sourceDocumentKey = makeDocumentKey({
  graph: Graph,
  documentKind: source.kind,
  encodedId: source.id,
})
const targetDocumentKey = makeDocumentKey({
  graph: Graph,
  documentKind: target.kind,
  encodedId: target.id,
})
const projectionKey: ProjectionIndexKey = {
  documentKey: sourceDocumentKey,
  projection: "search",
}
const revisionToken = Schema.decodeSync(IndexRevisionTokenSchema)(
  "workerd-revision-1",
)
const replacementIntent: ProjectionReplacementIntent = {
  _tag: "Replace",
  key: projectionKey,
  expectedToken: Option.none(),
  mutationId: Schema.decodeSync(ProjectionMutationIdSchema)(
    "workerd-publication-1",
  ),
  payloadDigest: Schema.decodeSync(ProjectionPayloadDigestSchema)(
    "a".repeat(64),
  ),
  snapshot: {
    token: revisionToken,
    revisionHash: Schema.decodeSync(ProjectionRevisionHashSchema)(
      "b".repeat(64),
    ),
    embeddingProfile: defineEmbeddingProfile({
      id: "test/workerd",
      version: "v1",
      dimensions: 3,
    }),
    chunks: [{
      chunkId: Schema.decodeSync(ChunkIdSchema)("c".repeat(64)),
      contentHash: Schema.decodeSync(ContentHashSchema)("d".repeat(64)),
    }],
  },
  catalog: {
    graph: Graph,
    documentKind: source.kind,
    projectionVersion: "v1",
  },
  liveSlotCount: 1,
  requiredSlotHighWater: 1,
  slotHighWater: 1,
  maximumSlotHighWater: 10,
  commit: {
    token: revisionToken,
    inserted: 1,
    updated: 0,
    deleted: 0,
  },
}

describe("D1 adapters in workerd", () => {
  test("commits topology and publication state through the real D1 binding", async () => {
    const topology = await Effect.runPromise(
      Effect.gen(function*() {
        const store = yield* GraphTopologyStore
        const commit = yield* store.replaceDocumentTopology({
          graph: Graph,
          sourceDocumentKey,
          source,
          relations: [{
            id: "references_invoice",
            version: "v1",
            targetDocumentKind: target.kind,
            targets: [{
              documentKey: targetDocumentKey,
              reference: target,
            }],
          }],
        })
        const nodes = yield* store.listNodes({
          graph: Graph,
          documentKinds: [],
          states: [],
          after: Option.none(),
          limit: Schema.decodeSync(GraphNodePageLimitSchema)(10),
        })
        const related = yield* store.findRelatedNodes({
          graph: Graph,
          documentKeys: [sourceDocumentKey, targetDocumentKey, sourceDocumentKey],
          documentKind: source.kind,
          relation: "references_invoice",
          relationVersion: "v1",
          relatedDocumentKind: target.kind,
          direction: "outgoing",
          limit: Schema.decodeSync(GraphNeighbourLimitSchema)(1),
        })
        return { commit, nodes, related }
      }).pipe(
        Effect.provide(d1GraphTopology({ database: env.WORKSPACE_DB })),
      ),
    )

    expect(topology.commit).toEqual({ inserted: 1, retained: 0, deleted: 0 })
    expect(topology.nodes.nodes).toEqual([
      {
        documentKey: sourceDocumentKey,
        reference: source,
        state: "Materialized",
      },
      {
        documentKey: targetDocumentKey,
        reference: target,
        state: "Referenced",
      },
    ].sort((left, right) =>
      left.documentKey.localeCompare(right.documentKey)
    ))
    const expectedNeighbour = { documentKey: targetDocumentKey, reference: target, state: "Referenced" }
    expect(topology.related).toEqual([
      { documentKey: sourceDocumentKey, nodes: [expectedNeighbour] },
      { documentKey: targetDocumentKey, nodes: [] },
      { documentKey: sourceDocumentKey, nodes: [expectedNeighbour] },
    ])

    const publication = await Effect.runPromise(
      Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const begun = yield* coordinator.beginPublication(replacementIntent)
        if (begun._tag !== "Publish") {
          return yield* Effect.die("Expected a new publication lease")
        }
        const outcome = yield* coordinator.finalizePublication(begun.lease)
        const replay = yield* coordinator.beginPublication({
          ...replacementIntent,
          commit: { ...replacementIntent.commit, inserted: 0, updated: 1 },
        })
        const [head] = yield* coordinator.loadHeads([projectionKey])
        const [revision] = yield* coordinator.loadRevisions([projectionKey])
        return { lease: begun.lease, outcome, replay, head, revision }
      }).pipe(
        Effect.provide(d1ProjectionPublicationCoordinator({
          database: env.WORKSPACE_DB,
          indexGeneration: "workerd-schema-v1",
        })),
      ),
    )

    expect(publication.outcome).toEqual({
      _tag: "Replaced",
      commit: replacementIntent.commit,
    })
    expect(publication.replay).toEqual({
      _tag: "AlreadyCommitted",
      outcome: publication.outcome,
    })
    expect(Number(publication.lease.generation)).toBe(1)
    expect(Option.isSome(publication.head?.head ?? Option.none())).toBe(true)
    const head = Option.getOrThrow(publication.head?.head ?? Option.none())
    expect(head.active).toEqual({
      _tag: "Revision",
      token: revisionToken,
    })
    expect(Option.isNone(head.pending)).toBe(true)
    expect(Option.isSome(
      publication.revision?.revision ?? Option.none(),
    )).toBe(true)

    const persisted = await env.WORKSPACE_DB.prepare(
      `SELECT status, generation
       FROM document_graph_projection_publications
       WHERE publication_id = ?1`,
    ).bind(publication.lease.publicationId).first<{
      readonly status: string
      readonly generation: number
    }>()
    expect(persisted).toEqual({ status: "committed", generation: 1 })
  })
})
