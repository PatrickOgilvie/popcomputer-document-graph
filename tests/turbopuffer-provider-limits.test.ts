import { describe, expect, test } from "bun:test"
import { Effect, Layer, Option, Schema } from "effect"
import {
  ChunkIdSchema,
  ContentHashSchema,
  makeDocumentKey,
  ProjectionRevisionHashSchema,
} from "../src/document/document-identity.js"
import { parseTextSearchPolicy } from "../src/document/text-search-policy.js"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import {
  ProjectionPublicationCoordinator,
  type ProjectionPublicationCoordinatorService,
} from "../src/indexing/projection-publication.js"
import type {
  ProjectedChunkRecord,
  ReplaceProjectedRevision,
} from "../src/indexing/projection-index.js"
import {
  TurbopufferClient,
  type TurbopufferClientService,
} from "../src/storage/turbopuffer/client.js"
import {
  TurbopufferMaximumAttributeBytes,
  TurbopufferMaximumFilterableValueBytes,
  TurbopufferMaximumWriteBytes,
} from "../src/storage/turbopuffer/config.js"
import { makeTurbopufferProjectionIndexStore } from "../src/storage/turbopuffer/projection-index.js"
import { makeTurbopufferWorkspacePartition } from "../src/storage/turbopuffer/partition.js"

const profile = defineEmbeddingProfile({
  id: "test:provider-limits",
  version: "v1",
  dimensions: 2,
})

const partition = makeTurbopufferWorkspacePartition({
  workspace: "provider-limits",
  deploymentId: "test-account/main",
  endpoint: { _tag: "Region", region: "gcp-us-central1" },
  embeddingProfile: profile,
  schemaGeneration: 1,
})

let publicationBegins = 0

const coordinator: ProjectionPublicationCoordinatorService = {
  indexGeneration: partition.d1IndexGeneration,
  loadRevisions: (keys) => Effect.succeed(keys.map((key) => ({
    key,
    revision: Option.none(),
  }))),
  loadHeads: (keys) => Effect.succeed(keys.map((key) => ({
    key,
    head: Option.none(),
  }))),
  beginPublication: () => {
    publicationBegins += 1

    return Effect.die("Publication must not begin for an oversized mutation")
  },
  finalizePublication: () => Effect.die("Unexpected finalization"),
  supersedePublication: () => Effect.die("Unexpected supersession"),
  listStaleRevisions: () => Effect.succeed([]),
}

const client: TurbopufferClientService = {
  partition,
  write: () => Effect.die("Unexpected write"),
  query: () => Effect.die("Unexpected query"),
  multiQuery: () => Effect.die("Unexpected multi-query"),
  inspectSchema: () => Effect.die("Unexpected schema inspection"),
  updateSchema: () => Effect.die("Unexpected schema update"),
  destroyNamespace: () => Effect.die("Unexpected namespace deletion"),
}

const dependencies = Layer.mergeAll(
  Layer.succeed(ProjectionPublicationCoordinator, coordinator),
  Layer.succeed(TurbopufferClient, client),
)

const makeStore = (input?: {
  readonly maximumSlotsPerRevision?: number
  readonly maximumPublicationBytes?: number
}) => Effect.runPromise(
  makeTurbopufferProjectionIndexStore({
    partition,
    ...input,
  }).pipe(Effect.provide(dependencies)),
)

const replacementWithSectionKey = (
  sectionKey: string,
  graph = "contracts",
): ReplaceProjectedRevision => {
  const contentHash = Schema.decodeSync(ContentHashSchema)("b".repeat(64))

  const chunk: ProjectedChunkRecord = {
    chunkId: Schema.decodeSync(ChunkIdSchema)("c".repeat(64)),
    contentHash,
    ordinal: 0,
    sectionKey,
    sectionIndex: 0,
    sectionPart: 0,
    content: "Plain text",
    embeddingContent: "Plain text",
    text: {
      context: undefined,
      label: undefined,
      content: "Plain text",
    },
    metadata: undefined,
  }

  return {
    key: {
      documentKey: makeDocumentKey({
        graph,
        documentKind: "contract",
        encodedId: { id: "contract-1" },
      }),
      projection: "search",
    },
    expectedToken: Option.none(),
    encodedTarget: {
      graph,
      kind: "contract",
      id: { id: "contract-1" },
    },
    projectionVersion: "v1",
    textPolicy: parseTextSearchPolicy("disabled"),
    revisionHash: Schema.decodeSync(ProjectionRevisionHashSchema)(
      "a".repeat(64),
    ),
    embeddingProfile: profile,
    chunks: [chunk],
    embeddings: [{ contentHash, vector: [0.25, 0.75] }],
  }
}

describe("Turbopuffer provider limits", () => {
  test("accepts the provider maxima at adapter construction", async () => {
    await expect(makeStore({
      maximumSlotsPerRevision: 10_000,
      maximumPublicationBytes: TurbopufferMaximumWriteBytes,
    })).resolves.toBeDefined()
  })

  test("rejects publication and slot limits above provider maxima", async () => {
    await expect(makeStore({
      maximumPublicationBytes: TurbopufferMaximumWriteBytes + 1,
    })).rejects.toMatchObject({
      _tag: "InvalidTurbopufferConfiguration",
      field: "maximum_publication_bytes",
      reason: "invalid_value",
    })
    await expect(makeStore({
      maximumSlotsPerRevision: 10_001,
    })).rejects.toMatchObject({
      _tag: "InvalidTurbopufferConfiguration",
      field: "maximum_slots_per_revision",
      reason: "invalid_value",
    })
  })

  test("rejects an oversized attribute before beginning D1 publication", async () => {
    publicationBegins = 0

    const store = await makeStore({
      maximumPublicationBytes: 32 * 1_024 * 1_024,
    })

    const oversized = "s".repeat(TurbopufferMaximumAttributeBytes)

    await expect(Effect.runPromise(
      store.replaceRevision(replacementWithSectionKey(oversized)),
    )).rejects.toMatchObject({
      _tag: "ProjectionIndexStoreFailed",
      reason: "capacity_exceeded",
      cause: {
        _tag: "TurbopufferMutationTooLarge",
        maximumBytes: TurbopufferMaximumAttributeBytes,
      },
    })
    expect(publicationBegins).toBe(0)
  })

  test("rejects an oversized filterable value before beginning D1 publication", async () => {
    publicationBegins = 0
    const store = await makeStore()

    const oversizedGraph = "g".repeat(
      TurbopufferMaximumFilterableValueBytes,
    )

    await expect(Effect.runPromise(
      store.replaceRevision(
        replacementWithSectionKey("section", oversizedGraph),
      ),
    )).rejects.toMatchObject({
      _tag: "ProjectionIndexStoreFailed",
      reason: "capacity_exceeded",
      cause: {
        _tag: "TurbopufferMutationTooLarge",
        maximumBytes: TurbopufferMaximumFilterableValueBytes,
      },
    })
    expect(publicationBegins).toBe(0)
  })
})
