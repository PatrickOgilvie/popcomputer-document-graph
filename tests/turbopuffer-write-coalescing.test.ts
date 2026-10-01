import { describe, expect, test } from "bun:test"
import type { NamespaceWriteParams } from "@turbopuffer/turbopuffer"
import { Effect, Option, Result, Schema } from "effect"
import {
  ChunkIdSchema,
  ContentHashSchema,
  makeDocumentKey,
  ProjectionRevisionHashSchema,
} from "../src/document/document-identity.js"
import { parseTextSearchPolicy } from "../src/document/text-search-policy.js"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import type { ReplaceProjectedRevision } from "../src/indexing/projection-index.js"
import { ProjectionPublicationCoordinator } from "../src/indexing/projection-publication.js"
import { d1ProjectionPublicationCoordinator } from "../src/storage/d1/projection-publication.js"
import { TurbopufferClient, type TurbopufferClientService } from "../src/storage/turbopuffer/client.js"
import { makeTurbopufferWorkspacePartition } from "../src/storage/turbopuffer/partition.js"
import { makeTurbopufferProjectionIndexStore } from "../src/storage/turbopuffer/projection-index.js"
import { makeDatabase } from "./support/sqlite-d1.js"

const profile = defineEmbeddingProfile({ id: "test/coalescing", version: "v1", dimensions: 2 })

const partition = makeTurbopufferWorkspacePartition({
  deploymentId: "test:write-coalescing",
  endpoint: { _tag: "Region", region: "gcp-us-central1" },
  workspace: "write-coalescing",
  embeddingProfile: profile,
  schemaGeneration: 1,
})

const replacement = (id: string, digit: string): ReplaceProjectedRevision => {
  const contentHash = Schema.decodeSync(ContentHashSchema)(digit.repeat(64))

  return {
    key: {
      documentKey: makeDocumentKey({ graph: "contracts", documentKind: "contract", encodedId: { id } }),
      projection: "search",
    },
    expectedToken: Option.none(),
    encodedTarget: { graph: "contracts", kind: "contract", id: { id } },
    projectionVersion: "v1",
    textPolicy: parseTextSearchPolicy({ language: "english" }),
    revisionHash: Schema.decodeSync(ProjectionRevisionHashSchema)(digit.repeat(64)),
    embeddingProfile: profile,
    chunks: [{
      chunkId: Schema.decodeSync(ChunkIdSchema)(digit.repeat(64)),
      contentHash,
      ordinal: 0,
      sectionKey: "body",
      sectionIndex: 0,
      sectionPart: 0,
      content: `Contract ${id}`,
      embeddingContent: `Contract ${id}`,
      text: { context: undefined, label: undefined, content: `Contract ${id}` },
      metadata: undefined,
    }],
    embeddings: [{ contentHash, vector: [0.6, 0.8] }],
  }
}

const replacements = [replacement("a", "1"), replacement("b", "2"), replacement("c", "3")]

type ProviderRow = NonNullable<NamespaceWriteParams["upsert_rows"]>[number]

/** Provider double that applies only the rows `applies` accepts. */
const makeClient = (applies: (row: ProviderRow) => boolean) => {
  const writes: Array<NamespaceWriteParams["upsert_rows"]> = []

  const client: TurbopufferClientService = {
    partition,
    query: () => Effect.succeed({ rows: [] }),
    multiQuery: () => Effect.die("Unexpected multi-query"),
    inspectSchema: () => Effect.die("Unexpected schema inspection"),
    updateSchema: () => Effect.die("Unexpected schema update"),
    destroyNamespace: () => Effect.die("Unexpected namespace deletion"),
    write: (request) => Effect.sync(() => {
      writes.push(request.upsert_rows)
      const applied = (request.upsert_rows ?? []).filter(applies)

      return {
        status: "OK",
        rows_affected: applied.length,
        upserted_ids: applied.map((row) => String(row["id"])),
      }
    }),
  }

  return { client, writes }
}

const publishAll = async (client: TurbopufferClientService) => {
  const database = await makeDatabase()

  try {
    return await Effect.runPromise(Effect.gen(function*() {
      const store = yield* makeTurbopufferProjectionIndexStore({
        partition,
        coalesceWrites: { windowMilliseconds: 5, maximumPublications: 16 },
      })

      const results = yield* Effect.forEach(
        replacements,
        (revision) => store.replaceRevision(revision).pipe(Effect.result),
        { concurrency: "unbounded" },
      )

      const coordinator = yield* ProjectionPublicationCoordinator
      const [first, ...rest] = replacements.map((revision) => revision.key)
      if (first === undefined) return yield* Effect.die("Expected keys")
      const heads = yield* coordinator.loadHeads([first, ...rest])

      return { results, heads }
    }).pipe(
      Effect.provideService(TurbopufferClient, client),
      Effect.provide(d1ProjectionPublicationCoordinator({
        database,
        indexGeneration: partition.d1IndexGeneration,
      })),
    ))
  } finally {
    database.close()
  }
}

describe("Turbopuffer write coalescing", () => {
  test("publishes concurrent revisions in one fenced provider write", async () => {
    const { client, writes } = makeClient(() => true)
    const { results, heads } = await publishAll(client)

    expect(writes).toHaveLength(1)
    // A marker plus one live slot per document.
    expect(writes[0]).toHaveLength(replacements.length * 2)
    expect(results.every(Result.isSuccess)).toBe(true)
    expect(heads.map((lookup) => lookup.head.pipe(
      Option.map((head) => head.active._tag),
      Option.getOrNull,
    ))).toEqual(["Revision", "Revision", "Revision"])
  })

  test("finalizes only the publications whose rows the merged write applied", async () => {
    const applied = replacements[0]?.key.documentKey
    const { client } = makeClient((row) => row["document_key"] === applied)
    const { results, heads } = await publishAll(client)

    expect(Result.isSuccess(results[0] ?? Result.fail(undefined))).toBe(true)
    expect(results.slice(1).map((result) => Result.isFailure(result) ? result.failure : result))
      .toEqual([1, 2].map(() => expect.objectContaining({ reason: "publication_in_doubt" })))
    expect(heads.map((lookup) => lookup.head.pipe(
      Option.map((head) => [head.active._tag, Option.isSome(head.pending)]),
      Option.getOrNull,
    ))).toEqual([["Revision", false], ["NeverPublished", true], ["NeverPublished", true]])
  })
})
