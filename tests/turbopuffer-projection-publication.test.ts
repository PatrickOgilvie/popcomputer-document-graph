import type {
  NamespaceQueryParams,
  NamespaceWriteParams,
} from "@turbopuffer/turbopuffer"
import { describe, expect, test } from "bun:test"
import { Effect, Layer, Option, Schema } from "effect"
import {
  ChunkIdSchema,
  ContentHashSchema,
  makeDocumentKey,
  ProjectionRevisionHashSchema,
  type ContentHash,
} from "../src/document/document-identity.js"
import { parseTextSearchPolicy } from "../src/document/text-search-policy.js"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import {
  ProjectionPublicationCoordinator,
  ProjectionPublicationCoordinatorFailed,
  ProjectionPublicationGenerationSchema,
  ProjectionPublicationIdSchema,
  ProjectionPublicationPlanStale,
  type ProjectionPublicationCoordinatorService,
  type ProjectionPublicationHead,
  type ProjectionPublicationIntent,
  type ProjectionPublicationLease,
  type ProjectionPublicationOutcome,
} from "../src/indexing/projection-publication.js"
import type {
  IndexedRevisionSnapshot,
  ProjectedChunkRecord,
  ReplaceProjectedRevision,
} from "../src/indexing/projection-index.js"
import { IndexRevisionTokenSchema } from "../src/indexing/projection-index.js"
import {
  TurbopufferClient,
  type TurbopufferClientService,
} from "../src/storage/turbopuffer/client.js"
import { TurbopufferTransportFailed } from "../src/storage/turbopuffer/errors.js"
import {
  makeTurbopufferProjectionIndexStore,
  type TurbopufferProjectionIndexConfig,
} from "../src/storage/turbopuffer/projection-index.js"
import { makeTurbopufferWorkspacePartition } from "../src/storage/turbopuffer/partition.js"

const profile = defineEmbeddingProfile({
  id: "test/publication",
  version: "v1",
  dimensions: 2,
})
const deployment = {
  deploymentId: "test:turbopuffer-publication",
  endpoint: { _tag: "Region" as const, region: "gcp-us-central1" },
}
const partition = makeTurbopufferWorkspacePartition({
  ...deployment,
  workspace: "publication-tests",
  embeddingProfile: profile,
  schemaGeneration: 1,
})
const documentKey = makeDocumentKey({
  graph: "contracts",
  documentKind: "contract",
  encodedId: { id: "contract-1" },
})
const revisionHash = Schema.decodeSync(ProjectionRevisionHashSchema)(
  "a".repeat(64),
)

const contentHash = (character: string): ContentHash =>
  Schema.decodeSync(ContentHashSchema)(character.repeat(64))

const chunk = (
  ordinal: number,
  hash: ContentHash,
): ProjectedChunkRecord => ({
  chunkId: Schema.decodeSync(ChunkIdSchema)(
    (ordinal + 1).toString(16).repeat(64),
  ),
  contentHash: hash,
  ordinal,
  sectionKey: `section-${ordinal}`,
  sectionIndex: ordinal,
  sectionPart: 0,
  content: `Plain text ${ordinal}`,
  embeddingContent: `Context: Plain text ${ordinal}`,
  text: {
    context: "Contract",
    label: `Section ${ordinal}`,
    content: `Plain text ${ordinal}`,
  },
  metadata: { visibility: "public" },
})

const replacement = (input?: {
  readonly chunks?: ReplaceProjectedRevision["chunks"]
  readonly embeddings?: ReplaceProjectedRevision["embeddings"]
}): ReplaceProjectedRevision => {
  const first = chunk(0, contentHash("b"))
  return {
    key: { documentKey, projection: "search" },
    expectedToken: Option.none(),
    encodedTarget: {
      graph: "contracts",
      kind: "contract",
      id: { id: "contract-1" },
    },
    projectionVersion: "v1",
    textPolicy: parseTextSearchPolicy({ language: "english" }),
    revisionHash,
    embeddingProfile: profile,
    chunks: input?.chunks ?? [first],
    embeddings: input?.embeddings ?? [{
      contentHash: first.contentHash,
      vector: [0.25, 0.75],
    }],
  }
}

type WriteBehavior =
  | "success"
  | "zero"
  | "partial"
  | "rejected"
  | "ambiguous_rejection"
  | "authentication_failed"
  | "timed_out"

interface PublicationHarness {
  readonly coordinator: ProjectionPublicationCoordinatorService
  readonly client: TurbopufferClientService
  readonly queries: Array<NamespaceQueryParams>
  readonly writes: Array<NamespaceWriteParams>
  readonly finalized: Array<ProjectionPublicationLease>
  readonly superseded: Array<ProjectionPublicationLease>
  readonly beginIntents: Array<ProjectionPublicationIntent>
}

const outcomeFor = (
  lease: ProjectionPublicationLease,
): ProjectionPublicationOutcome =>
  lease.intent._tag === "Replace"
    ? { _tag: "Replaced", commit: lease.intent.commit }
    : { _tag: "Deleted", deletion: lease.intent.deletion }

const makeHarness = (input?: {
  readonly slotHighWater?: number
  /** Simulates a larger publication being begun and superseded after planning. */
  readonly supersededSlotHighWaterBeforeFirstBegin?: number
  readonly reusableRows?: ReadonlyArray<unknown>
  readonly currentRevision?: IndexedRevisionSnapshot | undefined
  readonly writeBehavior?: WriteBehavior
  readonly writeBehaviors?: ReadonlyArray<WriteBehavior>
  readonly finalizeFailures?: number
  readonly omitReusableRowsField?: boolean
  readonly omitMarkerRowsField?: boolean
  readonly markerGenerationOffset?: number
  readonly markerPublicationId?: string
}): PublicationHarness => {
  const writes: Array<NamespaceWriteParams> = []
  const queries: Array<NamespaceQueryParams> = []
  const finalized: Array<ProjectionPublicationLease> = []
  const superseded: Array<ProjectionPublicationLease> = []
  const beginIntents: Array<ProjectionPublicationIntent> = []
  let slotHighWater = input?.slotHighWater ?? 0
  let lastAllocatedGeneration = 0
  let injectedSupersededPublication = false
  let pending: ProjectionPublicationLease | undefined
  let remainingFinalizeFailures = input?.finalizeFailures ?? 0

  const coordinator: ProjectionPublicationCoordinatorService = {
    indexGeneration: partition.d1IndexGeneration,
    loadRevisions: (keys) => Effect.succeed(keys.map((key) => ({
      key,
      revision: input?.currentRevision === undefined
        ? Option.none()
        : Option.some(input.currentRevision),
    }))),
    loadHeads: (keys) => Effect.succeed(keys.map((key) => {
      const head: ProjectionPublicationHead = {
        key,
        lastAllocatedGeneration,
        slotHighWater,
        active: input?.currentRevision === undefined
          ? { _tag: "NeverPublished" }
          : {
              _tag: "Revision",
              token: input.currentRevision.token,
            },
        activeMutationId: Option.none(),
        activePayloadDigest: Option.none(),
        pending: pending === undefined
          ? Option.none()
          : Option.some({
              mutationId: pending.intent.mutationId,
              payloadDigest: pending.intent.payloadDigest,
              publicationId: pending.publicationId,
              generation: pending.generation,
              slotHighWater: pending.slotHighWater,
              operation: pending.intent._tag === "Replace"
                ? "replace"
                : "delete",
            }),
      }
      return { key, head: Option.some(head) }
    })),
    beginPublication: (intent) => {
      beginIntents.push(intent)
      if (pending !== undefined) {
        if (
          pending.intent.mutationId === intent.mutationId &&
          pending.intent.payloadDigest === intent.payloadDigest
        ) {
          return Effect.succeed({ _tag: "Publish", lease: {
            ...pending,
            intent,
          } })
        }
        return Effect.fail(new ProjectionPublicationCoordinatorFailed({
          operation: "begin_publication",
          reason: "publication_in_progress",
          cause: "A different publication is already pending",
        }))
      }
      if (!injectedSupersededPublication &&
        input?.supersededSlotHighWaterBeforeFirstBegin !== undefined) {
        injectedSupersededPublication = true
        slotHighWater = Math.max(
          slotHighWater,
          input.supersededSlotHighWaterBeforeFirstBegin,
        )
        lastAllocatedGeneration += 1
      }
      if (slotHighWater > intent.slotHighWater) {
        return Effect.fail(new ProjectionPublicationPlanStale({
          documentKey: intent.key.documentKey,
          projection: intent.key.projection,
          plannedSlotHighWater: intent.slotHighWater,
          currentSlotHighWater: slotHighWater,
        }))
      }
      slotHighWater = intent.slotHighWater
      lastAllocatedGeneration += 1
      pending = {
        intent,
        publicationId: Schema.decodeSync(ProjectionPublicationIdSchema)(
          `publication-${lastAllocatedGeneration}`,
        ),
        generation: Schema.decodeSync(
          ProjectionPublicationGenerationSchema,
        )(lastAllocatedGeneration),
        slotHighWater: intent.slotHighWater,
      }
      return Effect.succeed({ _tag: "Publish", lease: pending })
    },
    finalizePublication: (lease) => {
      finalized.push(lease)
      if (remainingFinalizeFailures > 0) {
        remainingFinalizeFailures -= 1
        return Effect.fail(new ProjectionPublicationCoordinatorFailed({
          operation: "finalize_publication",
          reason: "unavailable",
          cause: "simulated D1 outage",
        }))
      }
      pending = undefined
      return Effect.succeed(outcomeFor(lease))
    },
    supersedePublication: (lease) => {
      superseded.push(lease)
      pending = undefined
      return Effect.void
    },
    listStaleRevisions: () => Effect.succeed([]),
  }

  const client: TurbopufferClientService = {
    partition,
    query: (request) => {
      queries.push(request)
      const attributes = request.include_attributes
      if (Array.isArray(attributes) &&
        attributes.includes("publication_id")) {
        if (input?.omitMarkerRowsField === true) return Effect.succeed({})
        if (pending === undefined) return Effect.succeed({ rows: [] })
        return Effect.succeed({
          rows: [{
            row_kind: "marker",
            partition_id: partition.identity,
            publication_id: input?.markerPublicationId ??
              pending.publicationId,
            publication_generation: pending.generation +
              (input?.markerGenerationOffset ?? 0),
            slot_high_water: pending.slotHighWater,
          }],
        })
      }
      if (input?.omitReusableRowsField === true) return Effect.succeed({})
      return Effect.succeed({ rows: input?.reusableRows ?? [] })
    },
    write: (request) => {
      writes.push(request)
      const behavior = input?.writeBehaviors?.[writes.length - 1] ?? input?.writeBehavior
      if (behavior === "rejected") {
        return Effect.fail(new TurbopufferTransportFailed({
          operation: "write",
          reason: "rejected",
          requestOutcome: "definitely_not_applied",
          cause: "simulated definite rejection",
        }))
      }
      if (behavior === "ambiguous_rejection") {
        return Effect.fail(new TurbopufferTransportFailed({
          operation: "write",
          reason: "rejected",
          requestOutcome: "unknown",
          cause: "an earlier write attempt had an ambiguous outcome",
        }))
      }
      if (behavior === "authentication_failed") {
        return Effect.fail(new TurbopufferTransportFailed({
          operation: "write",
          reason: "authentication_failed",
          requestOutcome: "definitely_not_applied",
          cause: "simulated authentication rejection",
        }))
      }
      if (behavior === "timed_out") {
        return Effect.fail(new TurbopufferTransportFailed({
          operation: "write",
          reason: "timed_out",
          cause: "simulated ambiguous timeout",
        }))
      }
      const rows = request.upsert_rows?.length ?? 0
      return Effect.succeed({
        status: "OK",
        rows_affected: behavior === "partial"
          ? 1
          : behavior === "zero"
            ? 0
            : rows,
      })
    },
    multiQuery: () => Effect.die("Unexpected multi-query"),
    inspectSchema: () => Effect.die("Unexpected schema inspection"),
    updateSchema: () => Effect.die("Unexpected schema update"),
    destroyNamespace: () => Effect.die("Unexpected namespace deletion"),
  }

  return {
    coordinator,
    client,
    queries,
    writes,
    finalized,
    superseded,
    beginIntents,
  }
}

const activeRevision = (
  chunks: ReplaceProjectedRevision["chunks"],
): IndexedRevisionSnapshot => {
  const [first, ...rest] = chunks
  return {
    token: Schema.decodeSync(IndexRevisionTokenSchema)("active-token"),
    revisionHash,
    embeddingProfile: profile,
    chunks: [
      { chunkId: first.chunkId, contentHash: first.contentHash },
      ...rest.map((item) => ({
        chunkId: item.chunkId,
        contentHash: item.contentHash,
      })),
    ],
  }
}

const makeStore = (
  harness: PublicationHarness,
  limits?: Pick<
    TurbopufferProjectionIndexConfig,
    "maximumSlotsPerRevision" | "maximumPublicationBytes"
  >,
) =>
  Effect.runPromise(
    makeTurbopufferProjectionIndexStore({
      partition,
      ...limits,
    }).pipe(
      Effect.provide(Layer.mergeAll(
        Layer.succeed(
          ProjectionPublicationCoordinator,
          harness.coordinator,
        ),
        Layer.succeed(TurbopufferClient, harness.client),
      )),
    ),
  )

describe("Turbopuffer projection publication", () => {
  test("uses supplied vectors without fetching the previous revision's vectors", async () => {
    const input = replacement()
    const harness = makeHarness({ currentRevision: activeRevision(input.chunks) })
    const store = await makeStore(harness)
    const commit = await Effect.runPromise(store.replaceRevision({
      ...input,
      expectedToken: Option.some(activeRevision(input.chunks).token),
    }))
    expect(commit).toMatchObject({ inserted: 0, updated: 1, deleted: 0 })
    expect(harness.queries).toHaveLength(0)
    const live = harness.writes[0]?.upsert_rows?.find((row) => row["is_live"] === true)
    expect(live?.vector).toEqual([0.25, 0.75])
  })

  test("distinguishes absent metadata from explicit null in publication identities", async () => {
    const input = replacement()
    const absent = makeHarness()
    const present = makeHarness()
    const absentStore = await makeStore(absent)
    const presentStore = await makeStore(present)
    await Effect.runPromise(absentStore.replaceRevision({
      ...input,
      chunks: [{ ...input.chunks[0], metadata: undefined }],
    }))
    await Effect.runPromise(presentStore.replaceRevision({
      ...input,
      chunks: [{ ...input.chunks[0], metadata: null }],
    }))
    expect(absent.beginIntents[0]?.mutationId).not.toBe(present.beginIntents[0]?.mutationId)
    expect(absent.beginIntents[0]?.payloadDigest).not.toBe(present.beginIntents[0]?.payloadDigest)
  })

  test("rejects a D1 coordinator wired to another physical partition", async () => {
    const harness = makeHarness()
    const mismatched: PublicationHarness = {
      ...harness,
      coordinator: {
        ...harness.coordinator,
        indexGeneration: "another-index-generation",
      },
    }

    await expect(makeStore(mismatched)).rejects.toMatchObject({
      _tag: "InvalidTurbopufferConfiguration",
      field: "partition",
      reason: "mismatch",
    })
    expect(harness.queries).toHaveLength(0)
    expect(harness.writes).toHaveLength(0)
  })

  test("reports invalid publication bounds as typed configuration failures", async () => {
    const harness = makeHarness()

    await expect(makeStore(harness, {
      maximumSlotsPerRevision: 0,
    })).rejects.toMatchObject({
      _tag: "InvalidTurbopufferConfiguration",
      field: "maximum_slots_per_revision",
      reason: "invalid_value",
    })
  })

  test("publishes the marker, live slots, and every high-water tombstone", async () => {
    const harness = makeHarness({ slotHighWater: 3 })
    const store = await makeStore(harness)

    const commit = await Effect.runPromise(
      store.replaceRevision(replacement()),
    )

    expect(commit).toMatchObject({ inserted: 1, updated: 0, deleted: 0 })
    expect(harness.finalized).toHaveLength(1)
    expect(harness.queries).toHaveLength(0)
    expect(harness.writes).toHaveLength(1)
    const request = harness.writes[0]
    expect(request?.schema?.["vector"]).toEqual({
      type: "[2]f32",
      ann: { distance_metric: "cosine_distance" },
    })
    expect(request?.schema?.["document_key"]).toEqual({
      type: "string",
      filterable: true,
    })
    expect(request?.schema?.["metadata_terms"]).toEqual({
      type: "[]string",
      filterable: true,
    })
    expect(request?.schema?.["fts_en_content"]).toEqual({
      type: "string",
      filterable: false,
      full_text_search: {
        language: "english",
        tokenizer: "word_v4",
      },
    })
    expect(request?.upsert_condition).toEqual(["Or", [
      [
        "publication_generation",
        "Lt",
        { $ref_new: "publication_generation" },
      ],
      ["And", [
        [
          "publication_generation",
          "Eq",
          { $ref_new: "publication_generation" },
        ],
        ["publication_id", "Eq", { $ref_new: "publication_id" }],
      ]],
    ]])
    const rows = request?.upsert_rows ?? []
    expect(rows).toHaveLength(4)
    expect(rows.map((row) => [row["row_kind"], row["is_live"]])).toEqual([
      ["marker", false],
      ["slot", true],
      ["slot", false],
      ["slot", false],
    ])
    expect(rows.every((row) => row["publication_generation"] === 1)).toBe(
      true,
    )
    expect(rows.every((row) => row["publication_id"] === "publication-1"))
      .toBe(true)
  })

  test("replans when a superseded publication raises the durable closure", async () => {
    const harness = makeHarness({
      supersededSlotHighWaterBeforeFirstBegin: 4,
    })
    const store = await makeStore(harness, {
      maximumSlotsPerRevision: 3,
    })

    await expect(
      Effect.runPromise(store.replaceRevision(replacement())),
    ).rejects.toMatchObject({
      _tag: "ProjectionIndexStoreFailed",
      reason: "capacity_exceeded",
    })
    expect(harness.writes).toHaveLength(0)
    expect(harness.finalized).toHaveLength(0)
    expect(harness.superseded).toHaveLength(0)
    expect(harness.beginIntents).toHaveLength(1)
  })

  test("reconciles an ambiguous timeout through the strongly read marker", async () => {
    const harness = makeHarness({ writeBehavior: "timed_out" })
    const store = await makeStore(harness)

    const commit = await Effect.runPromise(
      store.replaceRevision(replacement()),
    )

    expect(commit.inserted).toBe(1)
    expect(harness.finalized).toHaveLength(1)
    expect(harness.superseded).toHaveLength(0)
    expect(harness.queries).toEqual([{
      filters: ["And", [
        ["id", "Eq", harness.writes[0]?.upsert_rows?.[0]?.id],
        ["partition_id", "Eq", partition.identity],
      ]],
      include_attributes: [
        "row_kind",
        "partition_id",
        "publication_id",
        "publication_generation",
        "slot_high_water",
      ],
      limit: 1,
      rank_by: ["id", "asc"],
      consistency: { level: "strong" },
    }])
  })

  test("supersedes leases proven fenced by newer or competing markers", async () => {
    const cases = [
      { markerGenerationOffset: 1 },
      { markerPublicationId: "competing-publication" },
    ] as const

    for (const input of cases) {
      const harness = makeHarness({
        writeBehavior: "timed_out",
        ...input,
      })
      const store = await makeStore(harness)

      await expect(
        Effect.runPromise(store.replaceRevision(replacement())),
      ).rejects.toMatchObject({
        _tag: "ProjectionIndexStoreFailed",
        reason: "invalid_stored_state",
      })
      expect(harness.superseded).toHaveLength(1)
      expect(harness.finalized).toHaveLength(0)
    }
  })

  test("requires rows on ranked marker reconciliation responses", async () => {
    const harness = makeHarness({
      writeBehavior: "timed_out",
      omitMarkerRowsField: true,
    })
    const store = await makeStore(harness)

    await expect(
      Effect.runPromise(store.replaceRevision(replacement())),
    ).rejects.toMatchObject({
      _tag: "ProjectionIndexStoreFailed",
      reason: "publication_in_doubt",
      cause: {
        _tag: "InvalidTurbopufferResponse",
        reason: "invalid_shape",
      },
    })
    expect(harness.finalized).toHaveLength(0)
  })

  test("accepts a zero-row replay only after its marker is visible", async () => {
    const harness = makeHarness({ writeBehavior: "zero" })
    const store = await makeStore(harness)

    const commit = await Effect.runPromise(
      store.replaceRevision(replacement()),
    )

    expect(commit.inserted).toBe(1)
    expect(harness.finalized).toHaveLength(1)
  })

  test("leaves a visible publication replayable when D1 finalization lags", async () => {
    const harness = makeHarness({ finalizeFailures: 1 })
    const store = await makeStore(harness)
    const command = replacement()

    await expect(
      Effect.runPromise(store.replaceRevision(command)),
    ).rejects.toMatchObject({
      _tag: "ProjectionIndexStoreFailed",
      reason: "publication_in_doubt",
    })

    const commit = await Effect.runPromise(store.replaceRevision(command))

    expect(commit.inserted).toBe(1)
    expect(harness.writes).toHaveLength(2)
    expect(harness.writes[0]?.upsert_rows).toEqual(
      harness.writes[1]?.upsert_rows,
    )
    expect(harness.finalized).toHaveLength(2)
  })

  test("replans a stale closure and keeps an ambiguous retry exact", async () => {
    const harness = makeHarness({
      supersededSlotHighWaterBeforeFirstBegin: 3,
      finalizeFailures: 1,
    })
    const store = await makeStore(harness)
    const command = replacement()

    await expect(
      Effect.runPromise(store.replaceRevision(command)),
    ).rejects.toMatchObject({
      _tag: "ProjectionIndexStoreFailed",
      reason: "publication_in_doubt",
    })
    const commit = await Effect.runPromise(store.replaceRevision(command))

    expect(commit.inserted).toBe(1)
    expect(harness.beginIntents).toHaveLength(3)
    const [stale, replanned, replayed] = harness.beginIntents
    expect(stale?.slotHighWater).toBe(1)
    expect(replanned?.slotHighWater).toBe(3)
    expect(replayed?.slotHighWater).toBe(3)
    expect(stale?.mutationId).not.toBe(replanned?.mutationId)
    expect(replayed?.mutationId).toBe(replanned?.mutationId)
    expect(replayed?.payloadDigest).toBe(replanned?.payloadDigest)
    expect(harness.writes).toHaveLength(2)
    expect(harness.writes[0]?.upsert_rows).toEqual(
      harness.writes[1]?.upsert_rows,
    )
    expect(harness.finalized).toHaveLength(2)
    expect(harness.finalized.map((lease) => String(lease.publicationId)))
      .toEqual(["publication-2", "publication-2"])
  })

  test("rejects a partial conditional write instead of finalizing it", async () => {
    const harness = makeHarness({
      slotHighWater: 3,
      writeBehavior: "partial",
    })
    const store = await makeStore(harness)

    await expect(
      Effect.runPromise(store.replaceRevision(replacement())),
    ).rejects.toMatchObject({
      _tag: "ProjectionIndexStoreFailed",
      reason: "invalid_stored_state",
      cause: {
        _tag: "InvalidTurbopufferResponse",
        reason: "partial_write",
      },
    })
    expect(harness.finalized).toHaveLength(0)
  })

  test("retains a definitely rejected publication for an exact retry", async () => {
    const harness = makeHarness({ writeBehaviors: ["rejected", "success"] })
    const store = await makeStore(harness)
    const command = replacement()

    await expect(
      Effect.runPromise(store.replaceRevision(command)),
    ).rejects.toMatchObject({
      _tag: "ProjectionIndexStoreFailed",
      reason: "invalid_replacement",
    })
    expect(harness.superseded).toHaveLength(0)
    expect(harness.finalized).toHaveLength(0)
    const commit = await Effect.runPromise(store.replaceRevision(command))
    expect(commit.inserted).toBe(1)
    expect(harness.finalized).toHaveLength(1)
    expect(harness.writes).toHaveLength(2)
    expect(harness.writes[1]?.upsert_rows).toEqual(harness.writes[0]?.upsert_rows)
  })

  test("marker-reconciles a rejection after an ambiguous write attempt", async () => {
    const harness = makeHarness({ writeBehavior: "ambiguous_rejection" })
    const store = await makeStore(harness)

    const commit = await Effect.runPromise(
      store.replaceRevision(replacement()),
    )

    expect(commit.inserted).toBe(1)
    expect(harness.queries).toHaveLength(1)
    expect(harness.superseded).toHaveLength(0)
    expect(harness.finalized).toHaveLength(1)
  })

  test("does not marker-reconcile a definitive authentication rejection", async () => {
    const harness = makeHarness({ writeBehavior: "authentication_failed" })
    const store = await makeStore(harness)

    await expect(
      Effect.runPromise(store.replaceRevision(replacement())),
    ).rejects.toMatchObject({
      _tag: "ProjectionIndexStoreFailed",
      reason: "unavailable",
      cause: {
        _tag: "TurbopufferTransportFailed",
        reason: "authentication_failed",
        requestOutcome: "definitely_not_applied",
      },
    })
    expect(harness.superseded).toHaveLength(0)
    expect(harness.queries).toHaveLength(0)
    expect(harness.finalized).toHaveLength(0)
  })

  test("deletion republishes the complete inherited slot closure", async () => {
    const harness = makeHarness({ slotHighWater: 3 })
    const store = await makeStore(harness)

    const deletion = await Effect.runPromise(store.deleteRevision({
      documentKey,
      projection: "search",
    }))

    expect(deletion).toEqual({ deletedRevisions: 0, deletedChunks: 0 })
    const rows = harness.writes[0]?.upsert_rows ?? []
    expect(rows).toHaveLength(4)
    expect(rows.every((row) => row["is_live"] === false)).toBe(true)
    expect(rows.slice(1).map((row) => row["slot_ordinal"])).toEqual([
      0,
      1,
      2,
    ])
  })

  test("replans deletion when the durable closure advances", async () => {
    const harness = makeHarness({
      supersededSlotHighWaterBeforeFirstBegin: 3,
    })
    const store = await makeStore(harness)

    const deletion = await Effect.runPromise(store.deleteRevision({
      documentKey,
      projection: "search",
    }))

    expect(deletion).toEqual({ deletedRevisions: 0, deletedChunks: 0 })
    expect(harness.beginIntents.map((intent) => intent.slotHighWater))
      .toEqual([0, 3])
    expect(harness.beginIntents[0]?.mutationId).not.toBe(
      harness.beginIntents[1]?.mutationId,
    )
    expect(harness.writes[0]?.upsert_rows).toHaveLength(4)
  })

  test("reuses identical duplicate vectors for repeated content hashes", async () => {
    const sharedHash = contentHash("c")
    const chunks = [chunk(0, sharedHash), chunk(1, sharedHash)] as const
    const currentRevision = activeRevision(chunks)
    const harness = makeHarness({
      currentRevision,
      reusableRows: [
        { content_hash: sharedHash, vector: [0.5, 0.5] },
        { content_hash: sharedHash, vector: [0.5, 0.5] },
      ],
    })
    const store = await makeStore(harness)

    const command = replacement({ chunks, embeddings: [] })
    const commit = await Effect.runPromise(store.replaceRevision({
      ...command,
      expectedToken: Option.some(currentRevision.token),
    }))

    expect(commit).toMatchObject({ inserted: 0, updated: 2, deleted: 0 })
    expect(harness.queries[0]).toMatchObject({
      filters: ["And", [
        ["row_kind", "Eq", "slot"],
        ["is_live", "Eq", true],
        ["partition_id", "Eq", partition.identity],
        ["document_key", "Eq", documentKey],
        ["projection_id", "Eq", "search"],
        ["embedding_profile_id", "Eq", profile.id],
        ["embedding_profile_version", "Eq", profile.version],
        ["schema_generation", "Eq", partition.schemaGeneration],
      ]],
      include_attributes: ["content_hash", "vector"],
      limit: 10_000,
      rank_by: ["id", "asc"],
      consistency: { level: "strong" },
    })
    const rows = harness.writes[0]?.upsert_rows ?? []
    expect(rows.filter((row) => row["is_live"] === true)).toHaveLength(2)
  })

  test("requires rows on ranked reusable-vector responses", async () => {
    const currentRevision = activeRevision([
      chunk(0, contentHash("e")),
    ])
    const harness = makeHarness({
      currentRevision,
      omitReusableRowsField: true,
    })
    const store = await makeStore(harness)

    await expect(
      Effect.runPromise(store.replaceRevision({
        ...replacement({ embeddings: [] }),
        expectedToken: Option.some(currentRevision.token),
      })),
    ).rejects.toMatchObject({
      _tag: "ProjectionIndexStoreFailed",
      reason: "invalid_stored_state",
      cause: {
        _tag: "InvalidTurbopufferResponse",
        reason: "invalid_shape",
      },
    })
    expect(harness.writes).toHaveLength(0)
  })

  test("rejects inconsistent reusable vectors for one content hash", async () => {
    const sharedHash = contentHash("d")
    const chunks = [chunk(0, sharedHash), chunk(1, sharedHash)] as const
    const currentRevision = activeRevision(chunks)
    const harness = makeHarness({
      currentRevision,
      reusableRows: [
        { content_hash: sharedHash, vector: [0.5, 0.5] },
        { content_hash: sharedHash, vector: [0.75, 0.25] },
      ],
    })
    const store = await makeStore(harness)

    const command = replacement({ chunks, embeddings: [] })
    await expect(Effect.runPromise(store.replaceRevision({
      ...command,
      expectedToken: Option.some(currentRevision.token),
    }))).rejects.toMatchObject({
      _tag: "ProjectionIndexStoreFailed",
      reason: "invalid_stored_state",
    })
    expect(harness.writes).toHaveLength(0)
  })
})
