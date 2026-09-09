import { describe, expect, test } from "bun:test"
import type {
  NamespaceMultiQueryParams,
  NamespaceQueryParams,
} from "@turbopuffer/turbopuffer"
import { Effect, Option, Schema } from "effect"
import {
  ContentHashSchema,
  makeChunkId,
  makeDocumentKey,
  ProjectionRevisionHashSchema,
} from "../src/document/document-identity.js"
import { parseTextSearchPolicy } from "../src/document/text-search-policy.js"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import {
  ProjectionPublicationCoordinatorFailed,
  type ProjectionPublicationCoordinatorService,
} from "../src/indexing/projection-publication.js"
import {
  IndexRevisionTokenSchema,
  type IndexedRevisionSnapshot,
} from "../src/indexing/projection-index.js"
import {
  documentKeys,
  makeGraphSearchScope,
  noDocuments,
  semantic,
  text,
} from "../src/retrieval/graph-retrieval.js"
import type { TurbopufferClientService } from "../src/storage/turbopuffer/client.js"
import { InvalidTurbopufferConfiguration } from "../src/storage/turbopuffer/errors.js"
import {
  makeTurbopufferWorkspacePartition,
  type TurbopufferWorkspacePartition,
} from "../src/storage/turbopuffer/partition.js"
import {
  makeTurbopufferProjectionSearchStores,
} from "../src/storage/turbopuffer/projection-search.js"

const profile = defineEmbeddingProfile({
  id: "test:tp-search",
  version: "v1",
  dimensions: 2,
})
const deployment = {
  deploymentId: "test:turbopuffer-projection-search",
  endpoint: { _tag: "Region" as const, region: "gcp-us-central1" },
}
const partition = makeTurbopufferWorkspacePartition({
  ...deployment,
  workspace: "tp-search-tests",
  embeddingProfile: profile,
  schemaGeneration: 3,
})
const textPolicy = parseTextSearchPolicy({ language: "english" })
if (textPolicy === "disabled") {
  throw new Error("Turbopuffer search tests require full-text search")
}

const reference = {
  graph: "contracts",
  kind: "Article",
  id: "article-1",
} as const
const documentKey = makeDocumentKey({
  graph: reference.graph,
  documentKind: reference.kind,
  encodedId: reference.id,
})
const chunkId = makeChunkId({
  documentKey,
  projection: "sections",
  sectionKey: "body",
  sectionPart: 0,
})
const revisionHash = Schema.decodeSync(ProjectionRevisionHashSchema)(
  "a".repeat(64),
)
const contentHash = Schema.decodeSync(ContentHashSchema)("c".repeat(64))

const resultRow = (
  providerScore: number,
  rowPartition = partition,
  overrides: {
    readonly chunkId?: typeof chunkId | undefined
    readonly contentHash?: typeof contentHash | undefined
    readonly revisionHash?: typeof revisionHash | undefined
    readonly sectionKey?: string | undefined
  } = {},
) => ({
  id: "b".repeat(64),
  $dist: providerScore,
  row_kind: "slot",
  is_live: true,
  partition_id: rowPartition.identity,
  graph_id: reference.graph,
  document_kind: reference.kind,
  projection_id: "sections",
  projection_version: "v1",
  document_key: documentKey,
  revision_hash: overrides.revisionHash ?? revisionHash,
  chunk_id: overrides.chunkId ?? chunkId,
  content_hash: overrides.contentHash ?? contentHash,
  section_key: overrides.sectionKey ?? "body",
  section_part: 0,
  content: "A graph-constrained Turbopuffer result.",
  encoded_id_json: JSON.stringify(reference.id),
  metadata_json: JSON.stringify({ visibility: "public" }),
})

const unused = () => Effect.die(new Error("Unexpected provider operation"))

const activeRevision = (input?: {
  readonly revisionHash?: typeof revisionHash | undefined
  readonly chunks?: IndexedRevisionSnapshot["chunks"] | undefined
}): IndexedRevisionSnapshot => ({
  token: Schema.decodeSync(IndexRevisionTokenSchema)("active-token"),
  revisionHash: input?.revisionHash ?? revisionHash,
  embeddingProfile: profile,
  chunks: input?.chunks ?? [{ chunkId, contentHash }],
})

const makeCoordinator = (input?: {
  readonly indexGeneration?: string | undefined
  readonly revision?: IndexedRevisionSnapshot | undefined
  readonly loadRevisions?: ProjectionPublicationCoordinatorService["loadRevisions"] | undefined
}): ProjectionPublicationCoordinatorService => {
  const revision = input?.revision ?? activeRevision()
  return {
    indexGeneration:
      input?.indexGeneration ?? partition.d1IndexGeneration,
    loadRevisions: input?.loadRevisions ?? ((keys) =>
      Effect.succeed(keys.map((key) => ({
        key,
        revision: Option.some(revision),
      })))),
    loadHeads: unused,
    beginPublication: unused,
    finalizePublication: unused,
    supersedePublication: unused,
    listStaleRevisions: unused,
  }
}

const makeClient = (input: {
  readonly query?: TurbopufferClientService["query"] | undefined
  readonly multiQuery?: TurbopufferClientService["multiQuery"] | undefined
  readonly partition?: TurbopufferWorkspacePartition | undefined
}): TurbopufferClientService => ({
  partition: input.partition ?? partition,
  write: unused,
  query: input.query ?? unused,
  multiQuery: input.multiQuery ?? unused,
  inspectSchema: unused,
  updateSchema: unused,
  destroyNamespace: unused,
})

const config = { partition }
const registered = [
  {
    documentKind: "Article",
    projection: "sections",
    projectionVersion: "v1",
  },
] as const

describe("Turbopuffer projection search", () => {
  test("rejects invalid consistency before a provider request", () => {
    let providerCalls = 0

    expect(() => makeTurbopufferProjectionSearchStores({
      config: {
        partition,
        // @ts-expect-error Deliberately exercise the JavaScript/runtime boundary.
        consistency: "bogus",
      },
      coordinator: makeCoordinator(),
      client: makeClient({
        query: () => {
          providerCalls += 1
          return Effect.succeed({ rows: [] })
        },
      }),
    })).toThrow(expect.objectContaining({
      _tag: "InvalidTurbopufferConfiguration",
      field: "consistency",
      reason: "invalid_value",
    }))
    expect(providerCalls).toBe(0)
  })

  test("pushes resolved document keys into ANN filters before top-k", async () => {
    const requests: Array<NamespaceQueryParams> = []
    const stores = makeTurbopufferProjectionSearchStores({
      config,
      coordinator: makeCoordinator(),
      client: makeClient({
        query: (request) => {
          requests.push(request)
          return Effect.succeed({ rows: [resultRow(0.2)] })
        },
      }),
    })
    const strategy = semantic({ candidates: 17, results: 4 })
    const scope = makeGraphSearchScope(
      reference.graph,
      { target: documentKeys([documentKey]) },
      registered,
    )

    const candidates = await Effect.runPromise(
      stores.searchCandidates({
        vector: [0.25, 0.75],
        embeddingProfile: profile,
        scope,
        candidates: strategy.candidates,
      }),
    )

    expect(requests).toHaveLength(1)
    expect(requests[0]?.top_k).toBe(17)
    expect(requests[0]?.consistency).toEqual({ level: "strong" })
    expect(requests[0]?.include_attributes).toContain("content_hash")
    expect(requests[0]?.filters).toEqual([
      "And",
      expect.arrayContaining([
        ["document_key", "In", [documentKey]],
        ["is_live", "Eq", true],
      ]),
    ])
    expect(candidates).toEqual([
      expect.objectContaining({
        score: 0.8,
        documentKey,
        chunkId,
        content: "A graph-constrained Turbopuffer result.",
      }),
    ])
  })

  test("runs lexical retrieval with the namespace embedding partition", async () => {
    const requests: Array<NamespaceQueryParams> = []
    const stores = makeTurbopufferProjectionSearchStores({
      config,
      coordinator: makeCoordinator(),
      client: makeClient({
        query: (request) => {
          requests.push(request)
          return Effect.succeed({ rows: [resultRow(6.25)] })
        },
      }),
    })
    const strategy = text({
      policy: textPolicy,
      candidates: 9,
      results: 3,
    })
    const scope = makeGraphSearchScope(reference.graph, {}, registered)

    const candidates = await Effect.runPromise(
      stores.searchTextCandidates({
        query: "contract radar",
        policy: textPolicy,
        scope,
        candidates: strategy.candidates,
      }),
    )

    expect(requests).toHaveLength(1)
    expect(requests[0]?.top_k).toBe(9)
    expect(requests[0]?.filters).toEqual([
      "And",
      expect.arrayContaining([
        ["embedding_profile_id", "Eq", profile.id],
        ["embedding_profile_version", "Eq", profile.version],
        ["schema_generation", "Eq", partition.schemaGeneration],
        ["partition_id", "Eq", partition.identity],
      ]),
    ])
    expect(candidates[0]?.score).toBe(6.25)
  })

  test("keeps hybrid channel rankings separate and requests no provider rerank", async () => {
    const requests: Array<NamespaceMultiQueryParams> = []
    const stores = makeTurbopufferProjectionSearchStores({
      config,
      coordinator: makeCoordinator(),
      client: makeClient({
        multiQuery: (request) => {
          requests.push(request)
          return Effect.succeed({
            results: [
              { rows: [resultRow(0.1)] },
              { rows: [resultRow(8.5)] },
            ],
          })
        },
      }),
    })
    const semanticStrategy = semantic({ candidates: 12, results: 4 })
    const textStrategy = text({
      policy: textPolicy,
      candidates: 8,
      results: 4,
    })
    const scope = makeGraphSearchScope(
      reference.graph,
      { target: documentKeys([documentKey]) },
      registered,
    )

    const channels = await Effect.runPromise(
      stores.searchHybridCandidates({
        query: "contract radar",
        vector: [0.25, 0.75],
        embeddingProfile: profile,
        textPolicy,
        scope,
        semanticCandidates: semanticStrategy.candidates,
        textCandidates: textStrategy.candidates,
      }),
    )

    expect(requests).toHaveLength(1)
    expect(requests[0]?.queries).toHaveLength(2)
    expect(requests[0]?.queries[0]?.top_k).toBe(12)
    expect(requests[0]?.queries[1]?.top_k).toBe(8)
    expect(requests[0]).not.toHaveProperty("rerank_by")
    expect(requests[0]?.queries[0]?.filters).toEqual(
      requests[0]?.queries[1]?.filters,
    )
    expect(JSON.stringify(requests[0]?.queries[0]?.filters)).toContain(
      documentKey,
    )
    expect(channels.semantic[0]?.score).toBe(0.9)
    expect(channels.text[0]?.score).toBe(8.5)
  })

  test("short-circuits every channel when D1 resolves no graph documents", async () => {
    const calls = { query: 0, multiQuery: 0 }
    const stores = makeTurbopufferProjectionSearchStores({
      config,
      coordinator: makeCoordinator(),
      client: makeClient({
        query: () => {
          calls.query += 1
          return Effect.succeed({ rows: [] })
        },
        multiQuery: () => {
          calls.multiQuery += 1
          return Effect.succeed({ results: [] })
        },
      }),
    })
    const scope = makeGraphSearchScope(
      reference.graph,
      { target: noDocuments() },
      registered,
    )
    const semanticStrategy = semantic({ candidates: 5, results: 2 })
    const textStrategy = text({
      policy: textPolicy,
      candidates: 5,
      results: 2,
    })

    const [semanticRows, textRows, hybridRows] = await Effect.runPromise(
      Effect.all([
        stores.searchCandidates({
          vector: [],
          embeddingProfile: defineEmbeddingProfile({
            id: "irrelevant:no-documents",
            version: "v9",
            dimensions: 1,
          }),
          scope,
          candidates: semanticStrategy.candidates,
        }),
        stores.searchTextCandidates({
          query: "nothing",
          policy: textPolicy,
          scope,
          candidates: textStrategy.candidates,
        }),
        stores.searchHybridCandidates({
          query: "nothing",
          vector: [],
          embeddingProfile: defineEmbeddingProfile({
            id: "irrelevant:no-documents",
            version: "v9",
            dimensions: 1,
          }),
          textPolicy,
          scope,
          semanticCandidates: semanticStrategy.candidates,
          textCandidates: textStrategy.candidates,
        }),
      ]),
    )

    expect(semanticRows).toEqual([])
    expect(textRows).toEqual([])
    expect(hybridRows).toEqual({ semantic: [], text: [] })
    expect(calls).toEqual({ query: 0, multiQuery: 0 })
  })

  test("requires rows on ranked query and multi-query envelopes", async () => {
    const queryStores = makeTurbopufferProjectionSearchStores({
      config,
      coordinator: makeCoordinator(),
      client: makeClient({
        // SAFETY: This intentionally violates the SDK response type to exercise
        // the runtime decoder for a ranked response with missing rows.
        query: () => Effect.succeed({} as never),
      }),
    })
    const hybridStores = makeTurbopufferProjectionSearchStores({
      config,
      coordinator: makeCoordinator(),
      client: makeClient({
        // SAFETY: This intentionally violates the SDK response type to exercise
        // the runtime decoder for a multi-query result with missing rows.
        multiQuery: () => Effect.succeed({
          results: [{}, { rows: [] }],
        } as never),
      }),
    })
    const semanticStrategy = semantic({ candidates: 2, results: 1 })
    const textStrategy = text({
      policy: textPolicy,
      candidates: 2,
      results: 1,
    })
    const scope = makeGraphSearchScope(reference.graph, {}, registered)

    const [queryError, multiQueryError] = await Effect.runPromise(
      Effect.all([
        queryStores.searchCandidates({
          vector: [0.25, 0.75],
          embeddingProfile: profile,
          scope,
          candidates: semanticStrategy.candidates,
        }).pipe(Effect.flip),
        hybridStores.searchHybridCandidates({
          query: "contract radar",
          vector: [0.25, 0.75],
          embeddingProfile: profile,
          textPolicy,
          scope,
          semanticCandidates: semanticStrategy.candidates,
          textCandidates: textStrategy.candidates,
        }).pipe(Effect.flip),
      ]),
    )

    expect(queryError).toMatchObject({
      _tag: "ProjectionSearchStoreFailed",
      reason: "invalid_stored_state",
      cause: { _tag: "InvalidTurbopufferResponse" },
    })
    expect(multiQueryError).toMatchObject({
      _tag: "ProjectionSearchStoreFailed",
      reason: "invalid_stored_state",
      cause: { _tag: "InvalidTurbopufferResponse" },
    })
  })

  test("sorts normalized scores descending and breaks ties by chunk ID", async () => {
    const firstChunkId = makeChunkId({
      documentKey,
      projection: "sections",
      sectionKey: "first",
      sectionPart: 0,
    })
    const secondChunkId = makeChunkId({
      documentKey,
      projection: "sections",
      sectionKey: "second",
      sectionPart: 0,
    })
    const lowerChunkId = makeChunkId({
      documentKey,
      projection: "sections",
      sectionKey: "lower",
      sectionPart: 0,
    })
    const firstContentHash = Schema.decodeSync(ContentHashSchema)(
      "1".repeat(64),
    )
    const secondContentHash = Schema.decodeSync(ContentHashSchema)(
      "2".repeat(64),
    )
    const lowerContentHash = Schema.decodeSync(ContentHashSchema)(
      "3".repeat(64),
    )
    const coordinator = makeCoordinator({
      revision: activeRevision({
        chunks: [
          { chunkId: firstChunkId, contentHash: firstContentHash },
          { chunkId: secondChunkId, contentHash: secondContentHash },
          { chunkId: lowerChunkId, contentHash: lowerContentHash },
        ],
      }),
    })
    const stores = makeTurbopufferProjectionSearchStores({
      config,
      coordinator,
      client: makeClient({
        query: () => Effect.succeed({ rows: [
          resultRow(3, partition, {
            chunkId: lowerChunkId,
            contentHash: lowerContentHash,
            sectionKey: "lower",
          }),
          resultRow(7, partition, {
            chunkId: secondChunkId,
            contentHash: secondContentHash,
            sectionKey: "second",
          }),
          resultRow(7, partition, {
            chunkId: firstChunkId,
            contentHash: firstContentHash,
            sectionKey: "first",
          }),
        ] }),
      }),
    })
    const strategy = text({
      policy: textPolicy,
      candidates: 5,
      results: 3,
    })

    const candidates = await Effect.runPromise(
      stores.searchTextCandidates({
        query: "contract radar",
        policy: textPolicy,
        scope: makeGraphSearchScope(reference.graph, {}, registered),
        candidates: strategy.candidates,
      }),
    )

    expect(candidates.map((candidate) => candidate.chunkId)).toEqual([
      ...[firstChunkId, secondChunkId].sort(),
      lowerChunkId,
    ])
    expect(candidates.map((candidate) => candidate.score)).toEqual([7, 7, 3])
  })

  test("skips the D1 authority read when a ranked response has no rows", async () => {
    let loads = 0
    const stores = makeTurbopufferProjectionSearchStores({
      config,
      coordinator: makeCoordinator({
        loadRevisions: () => {
          loads += 1
          return Effect.die("The empty result must not read D1")
        },
      }),
      client: makeClient({
        query: () => Effect.succeed({ rows: [] }),
      }),
    })
    const strategy = semantic({ candidates: 2, results: 1 })

    const candidates = await Effect.runPromise(
      stores.searchCandidates({
        vector: [0.25, 0.75],
        embeddingProfile: profile,
        scope: makeGraphSearchScope(reference.graph, {}, registered),
        candidates: strategy.candidates,
      }),
    )

    expect(candidates).toEqual([])
    expect(loads).toBe(0)
  })

  test("fails the whole result when a pending candidate is mixed with the active revision", async () => {
    const pendingRevisionHash = Schema.decodeSync(
      ProjectionRevisionHashSchema,
    )("d".repeat(64))
    const pendingChunkId = makeChunkId({
      documentKey,
      projection: "sections",
      sectionKey: "pending",
      sectionPart: 0,
    })
    const pendingContentHash = Schema.decodeSync(ContentHashSchema)(
      "e".repeat(64),
    )
    const loadedKeys: Array<ReadonlyArray<unknown>> = []
    const stores = makeTurbopufferProjectionSearchStores({
      config,
      coordinator: makeCoordinator({
        loadRevisions: (keys) => {
          loadedKeys.push(keys)
          return Effect.succeed(keys.map((key) => ({
            key,
            revision: Option.some(activeRevision()),
          })))
        },
      }),
      client: makeClient({
        query: () => Effect.succeed({ rows: [
          resultRow(0.1),
          resultRow(0.2, partition, {
            chunkId: pendingChunkId,
            contentHash: pendingContentHash,
            revisionHash: pendingRevisionHash,
            sectionKey: "pending",
          }),
        ] }),
      }),
    })
    const strategy = semantic({ candidates: 3, results: 2 })

    const error = await Effect.runPromise(
      stores.searchCandidates({
        vector: [0.25, 0.75],
        embeddingProfile: profile,
        scope: makeGraphSearchScope(reference.graph, {}, registered),
        candidates: strategy.candidates,
      }).pipe(Effect.flip),
    )

    expect(loadedKeys).toHaveLength(1)
    expect(loadedKeys[0]).toHaveLength(1)
    expect(error).toMatchObject({
      _tag: "ProjectionSearchStoreFailed",
      reason: "invalid_stored_state",
      cause: {
        _tag: "InvalidTurbopufferResponse",
        reason: "invalid_row",
      },
    })
  })

  test("maps coordinator outages and invalid state into public search failures", async () => {
    const scope = makeGraphSearchScope(reference.graph, {}, registered)
    const strategy = semantic({ candidates: 2, results: 1 })

    for (const reason of ["unavailable", "invalid_stored_state"] as const) {
      const stores = makeTurbopufferProjectionSearchStores({
        config,
        coordinator: makeCoordinator({
          loadRevisions: () => Effect.fail(
            new ProjectionPublicationCoordinatorFailed({
              operation: "load_revisions",
              reason,
              cause: "simulated coordinator failure",
            }),
          ),
        }),
        client: makeClient({
          query: () => Effect.succeed({ rows: [resultRow(0.2)] }),
        }),
      })

      const error = await Effect.runPromise(
        stores.searchCandidates({
          vector: [0.25, 0.75],
          embeddingProfile: profile,
          scope,
          candidates: strategy.candidates,
        }).pipe(Effect.flip),
      )

      expect(error).toMatchObject({
        _tag: "ProjectionSearchStoreFailed",
        reason,
        cause: { _tag: "ProjectionPublicationCoordinatorFailed" },
      })
    }
  })

  test("maps malformed provider rows to invalid stored state", async () => {
    const stores = makeTurbopufferProjectionSearchStores({
      config,
      coordinator: makeCoordinator(),
      client: makeClient({
        query: () => Effect.succeed({ rows: [{ id: "not-a-row" }] }),
      }),
    })
    const strategy = semantic({ candidates: 2, results: 1 })
    const scope = makeGraphSearchScope(reference.graph, {}, registered)

    const error = await Effect.runPromise(
      stores.searchCandidates({
        vector: [0.25, 0.75],
        embeddingProfile: profile,
        scope,
        candidates: strategy.candidates,
      }).pipe(Effect.flip),
    )

    expect(error).toMatchObject({
      _tag: "ProjectionSearchStoreFailed",
      reason: "invalid_stored_state",
      cause: { _tag: "InvalidTurbopufferResponse" },
    })
  })

  test("rejects a client wired to a different workspace partition", () => {
    const otherPartition = makeTurbopufferWorkspacePartition({
      ...deployment,
      workspace: "other-workspace",
      embeddingProfile: profile,
      schemaGeneration: 3,
    })

    expect(() => makeTurbopufferProjectionSearchStores({
      config,
      coordinator: makeCoordinator(),
      client: makeClient({ partition: otherPartition }),
    })).toThrow(InvalidTurbopufferConfiguration)
  })

  test("rejects a coordinator wired to a different D1 index generation", () => {
    expect(() => makeTurbopufferProjectionSearchStores({
      config,
      coordinator: makeCoordinator({
        indexGeneration: "projection-index:another-generation",
      }),
      client: makeClient({}),
    })).toThrow(InvalidTurbopufferConfiguration)
  })

  test("fails closed when a provider row carries another partition identity", async () => {
    const otherPartition = makeTurbopufferWorkspacePartition({
      ...deployment,
      workspace: "other-workspace",
      embeddingProfile: profile,
      schemaGeneration: 3,
    })
    const stores = makeTurbopufferProjectionSearchStores({
      config,
      coordinator: makeCoordinator(),
      client: makeClient({
        query: () => Effect.succeed({
          rows: [resultRow(0.2, otherPartition)],
        }),
      }),
    })
    const strategy = semantic({ candidates: 2, results: 1 })
    const scope = makeGraphSearchScope(reference.graph, {}, registered)

    const error = await Effect.runPromise(
      stores.searchCandidates({
        vector: [0.25, 0.75],
        embeddingProfile: profile,
        scope,
        candidates: strategy.candidates,
      }).pipe(Effect.flip),
    )

    expect(error).toMatchObject({
      _tag: "ProjectionSearchStoreFailed",
      reason: "invalid_stored_state",
      cause: {
        _tag: "InvalidTurbopufferResponse",
        reason: "invalid_row",
      },
    })
  })
})
