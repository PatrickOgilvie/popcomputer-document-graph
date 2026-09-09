import { describe, expect, test } from "bun:test"
import { Effect, Schema } from "effect"
import {
  ChunkIdSchema,
  ContentHashSchema,
  makeDocumentKey,
  ProjectionRevisionHashSchema,
} from "../src/document/document-identity.js"
import { parseTextSearchPolicy } from "../src/document/text-search-policy.js"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import {
  ProjectionPublicationGenerationSchema,
  ProjectionPublicationIdSchema,
} from "../src/indexing/projection-publication.js"
import { makeTurbopufferWorkspacePartition } from "../src/storage/turbopuffer/partition.js"
import {
  decodeTurbopufferSearchResultRow,
  makeTurbopufferLiveSlotRow,
  makeTurbopufferMarkerRow,
  makeTurbopufferTombstoneRow,
  scoreTurbopufferBm25,
  scoreTurbopufferCosineDistance,
  type TurbopufferPublicationRowContext,
} from "../src/storage/turbopuffer/row-codec.js"

const documentKey = makeDocumentKey({
  graph: "contracts",
  documentKind: "contract",
  encodedId: { source: "alpha", number: 42 },
})
const profile = defineEmbeddingProfile({
  id: "test/embedding",
  version: "v1",
  dimensions: 3,
})
const deployment = {
  deploymentId: "test:turbopuffer-row-codec",
  endpoint: { _tag: "Region" as const, region: "gcp-us-central1" },
}
const partition = makeTurbopufferWorkspacePartition({
  ...deployment,
  workspace: "workspace-1",
  embeddingProfile: profile,
  schemaGeneration: 2,
})

const context = (
  generation: number,
  selectedPartition = partition,
): TurbopufferPublicationRowContext => ({
  graph: "contracts",
  documentKind: "contract",
  key: { documentKey, projection: "search" },
  projectionVersion: "v3",
  partition: selectedPartition,
  publicationId: Schema.decodeSync(ProjectionPublicationIdSchema)(
    `publication-${generation}`,
  ),
  publicationGeneration: Schema.decodeSync(
    ProjectionPublicationGenerationSchema,
  )(generation),
})

const revisionHash = Schema.decodeSync(ProjectionRevisionHashSchema)(
  "a".repeat(64),
)
const chunkId = Schema.decodeSync(ChunkIdSchema)("b".repeat(64))
const contentHash = Schema.decodeSync(ContentHashSchema)("c".repeat(64))
const chunk = {
  chunkId,
  contentHash,
  ordinal: 0,
  sectionKey: "summary",
  sectionIndex: 0,
  sectionPart: 0,
  content: "A public contract notice",
  embeddingContent: "Contract: A public contract notice",
  text: {
    context: "Contract",
    label: "Summary",
    content: "A public contract notice",
  },
  metadata: {
    status: "active",
    amount: 42,
    nested: { ignored: true },
  },
} as const

describe("Turbopuffer row codec", () => {
  test("partitions namespace identity by workspace, vector space, and schema generation", () => {
    const first = makeTurbopufferWorkspacePartition({
      ...deployment,
      workspace: "workspace-1",
      embeddingProfile: profile,
      schemaGeneration: 2,
    })
    expect(String(first.identity)).toHaveLength(64)
    expect(first.namespace).toContain(first.identity)
    expect(first.d1IndexGeneration).toContain(first.identity)
    expect(first.identity).not.toBe(makeTurbopufferWorkspacePartition({
      ...deployment,
      workspace: "workspace-2",
      embeddingProfile: profile,
      schemaGeneration: 2,
    }).identity)
    expect(first.identity).not.toBe(makeTurbopufferWorkspacePartition({
      ...deployment,
      workspace: "workspace-1",
      embeddingProfile: profile,
      schemaGeneration: 3,
    }).identity)
  })

  test("uses stable <=64-byte marker and slot IDs across generations", () => {
    const firstMarker = makeTurbopufferMarkerRow({
      context: context(1),
      liveSlotCount: 1,
      slotHighWater: 2,
    })
    const laterMarker = makeTurbopufferMarkerRow({
      context: context(2),
      liveSlotCount: 1,
      slotHighWater: 2,
    })
    const firstSlot = makeTurbopufferTombstoneRow({
      context: context(1),
      slotOrdinal: 0,
    })
    const laterSlot = makeTurbopufferTombstoneRow({
      context: context(2),
      slotOrdinal: 0,
    })
    const otherPartition = makeTurbopufferWorkspacePartition({
      ...deployment,
      workspace: "workspace-2",
      embeddingProfile: profile,
      schemaGeneration: 2,
    })
    const isolatedSlot = makeTurbopufferTombstoneRow({
      context: context(1, otherPartition),
      slotOrdinal: 0,
    })

    expect(firstMarker.id).toHaveLength(64)
    expect(firstSlot.id).toHaveLength(64)
    expect(firstMarker.id).toBe(laterMarker.id)
    expect(firstSlot.id).toBe(laterSlot.id)
    expect(firstMarker.id).not.toBe(firstSlot.id)
    expect(firstSlot.id).not.toBe(isolatedSlot.id)
    expect(firstMarker.vector).toEqual([1, 0, 0])
    expect(firstSlot.vector).toEqual([1, 0, 0])
  })

  test("populates only the selected full-text language on live rows", () => {
    const policy = parseTextSearchPolicy({ language: "english" })
    const row = makeTurbopufferLiveSlotRow({
      context: context(1),
      encodedTarget: {
        graph: "contracts",
        kind: "contract",
        id: { number: 42, source: "alpha" },
      },
      revisionHash,
      textPolicy: policy,
      slotOrdinal: 0,
      chunk,
      vector: [0.1, 0.2, 0.3],
    })

    expect(row.row_kind).toBe("slot")
    expect(row.is_live).toBe(true)
    expect(row.partition_id).toBe(partition.identity)
    expect(row.fts_en_context).toBe("Contract")
    expect(row.fts_en_label).toBe("Summary")
    expect(row.fts_en_content).toBe("A public contract notice")
    expect(row).not.toHaveProperty("fts_simple_context")
    expect(row).not.toHaveProperty("fts_simple_label")
    expect(row).not.toHaveProperty("fts_simple_content")
    expect(row.metadata_terms).toHaveLength(2)
    expect(row.encoded_id_json).toBe('{"number":42,"source":"alpha"}')
  })

  test("runtime-decodes attributed result rows and rejects malformed JSON", async () => {
    const wire = {
      id: "d".repeat(64),
      $dist: 0.25,
      row_kind: "slot",
      is_live: true,
      partition_id: partition.identity,
      graph_id: "contracts",
      document_kind: "contract",
      projection_id: "search",
      projection_version: "v3",
      document_key: documentKey,
      revision_hash: revisionHash,
      chunk_id: chunkId,
      content_hash: contentHash,
      section_key: "summary",
      section_part: 0,
      content: "A public contract notice",
      encoded_id_json: '{"number":42,"source":"alpha"}',
      metadata_json: '{"status":"active"}',
    }

    const decoded = await Effect.runPromise(
      decodeTurbopufferSearchResultRow(wire),
    )
    expect(decoded.providerScore).toBe(0.25)
    expect(decoded.partitionIdentity).toBe(partition.identity)
    expect(decoded.contentHash).toBe(contentHash)
    expect(decoded.reference).toEqual({
      graph: "contracts",
      kind: "contract",
      id: { number: 42, source: "alpha" },
    })
    expect(decoded.metadata).toEqual({ status: "active" })
    expect(scoreTurbopufferCosineDistance(decoded.providerScore)).toBe(0.75)
    expect(scoreTurbopufferBm25(7.5)).toBe(7.5)

    await expect(
      Effect.runPromise(
        decodeTurbopufferSearchResultRow({
          ...wire,
          encoded_id_json: "not-json",
        }),
      ),
    ).rejects.toMatchObject({
      _tag: "InvalidTurbopufferResponse",
      reason: "invalid_row",
    })
  })
})
