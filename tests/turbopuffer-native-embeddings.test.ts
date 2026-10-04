import { describe, expect, test } from "bun:test"
import { Effect, Exit, Schema } from "effect"
import { makeContentHash } from "../src/document/document-identity.js"
import {
  defineEmbeddingProfile,
  type EmbeddingRequest,
} from "../src/indexing/embedding-provider.js"
import type { TurbopufferClientService } from "../src/storage/turbopuffer/client.js"
import {
  InvalidTurbopufferConfiguration,
  TurbopufferTransportFailed,
} from "../src/storage/turbopuffer/errors.js"
import { makeTurbopufferNativeEmbeddingProvider } from "../src/storage/turbopuffer/native-embeddings.js"
import { makeTurbopufferWorkspacePartition } from "../src/storage/turbopuffer/partition.js"

const profile = defineEmbeddingProfile({
  id: "test:native-embeddings",
  version: "v1",
  dimensions: 3,
})

const model = "qwen/qwen3-embedding-0p6b"

const partition = makeTurbopufferWorkspacePartition({
  workspace: "native-embeddings-test",
  deploymentId: "test-deployment",
  endpoint: { _tag: "Region", region: "aws-eu-west-2" },
  embeddingProfile: profile,
  schemaGeneration: 1,
})

const request = (content: string): EmbeddingRequest => ({
  contentHash: makeContentHash(content),
  content,
})

/** Stands in for the provider's model: a vector derived from the text. */
const vectorFor = (content: string) => [content.length, content.charCodeAt(0), 1]

interface StoredRow {
  readonly id: string
  readonly content: string
  readonly model: string
  readonly vector: ReadonlyArray<number>
}

const WrittenRowsSchema = Schema.Array(Schema.Struct({
  id: Schema.String,
  content: Schema.String,
  model: Schema.String,
}))

/** A namespace that embeds `content` on write, like a native-embedding schema. */
const fakeNamespace = (options: {
  readonly exists?: boolean
  readonly failQueries?: number
  readonly dropVectors?: boolean
  readonly embed?: (content: string) => ReadonlyArray<number>
} = {}) => {
  const rows = new Map<string, StoredRow>()
  const writes: Array<{ readonly rows: ReadonlyArray<StoredRow>; readonly schema: unknown }> = []
  const queries: Array<ReadonlyArray<string>> = []
  let exists = options.exists ?? true
  let failQueries = options.failQueries ?? 0

  const transport = (operation: TurbopufferTransportFailed["operation"], status: number) =>
    new TurbopufferTransportFailed({
      operation,
      reason: status === 404 ? "rejected" : "unavailable",
      cause: { status },
    })

  const client: TurbopufferClientService = {
    partition,
    write: (input) => Effect.sync(() => {
      const upserts = Schema.decodeUnknownSync(WrittenRowsSchema)(input.upsert_rows ?? [])
      const stored = upserts.map((row) => ({
        ...row,
        vector: (options.embed ?? vectorFor)(row.content),
      }))
      writes.push({ rows: stored, schema: input.schema })
      exists = true
      for (const row of stored) rows.set(row.id, row)

      return { rows_upserted: stored.length }
    }),
    query: (input) => {
      if (failQueries > 0) {
        failQueries -= 1
        return Effect.fail(transport("query", 503))
      }
      if (!exists) return Effect.fail(transport("query", 404))
      // SAFETY: the provider under test filters only by `["id", "In", ids]`.
      const [, , ids] = input.filters as [string, string, ReadonlyArray<string>]
      queries.push(ids)

      return Effect.succeed({
        rows: ids.flatMap((id) => {
          const row = rows.get(id)
          if (row === undefined) return []

          return [{ id: row.id, model: row.model, vector: options.dropVectors ? null : row.vector }]
        }),
      })
    },
    multiQuery: () => Effect.die("unused"),
    inspectSchema: () => Effect.die("unused"),
    updateSchema: () => Effect.die("unused"),
    destroyNamespace: () => Effect.die("unused"),
  }

  return { client, rows, writes, queries }
}

const provider = (
  client: TurbopufferClientService,
  batchSize?: number,
) =>
  makeTurbopufferNativeEmbeddingProvider({
    profile,
    model,
    client,
    batchSize,
    embedQuery: (query) => Effect.succeed([query.length, 0, 0]),
  })

describe("Turbopuffer native embeddings", () => {
  test("embeds documents on write and returns the stored vectors", async () => {
    const namespace = fakeNamespace({ exists: false })
    const requests = [request("Brand identity"), request("Food photography")] as const

    const embedded = await Effect.runPromise(
      provider(namespace.client).embedDocuments(requests),
    )

    expect(embedded).toEqual(requests.map(({ contentHash, content }) => ({
      contentHash,
      vector: vectorFor(content),
    })))
    expect(namespace.writes).toHaveLength(1)
    expect(namespace.writes[0]!.rows.map(({ id, content, model }) => ({ id, content, model })))
      .toEqual(requests.map(({ contentHash, content }) => ({ id: contentHash, content, model })))
    expect(namespace.writes[0]!.schema).toEqual({
      content: {
        type: "string",
        filterable: false,
        embed: { model, attribute: "vector", dims: 3 },
      },
      vector: { type: "[3]f32", ann: true },
      model: { type: "string", filterable: false },
    })
  })

  test("reads content already embedded instead of embedding it again", async () => {
    const namespace = fakeNamespace()
    const known = request("Packaging design")
    await Effect.runPromise(provider(namespace.client).embedDocuments([known]))

    const fresh = request("Sports sponsorship")
    const embedded = await Effect.runPromise(
      provider(namespace.client).embedDocuments([known, fresh]),
    )

    expect(embedded.map(({ contentHash }) => contentHash)).toEqual([known.contentHash, fresh.contentHash])
    expect(namespace.writes).toHaveLength(2)
    expect(namespace.writes[1]!.rows.map(({ id }) => id)).toEqual([fresh.contentHash])
  })

  test("never reuses a vector stored for another model", async () => {
    const namespace = fakeNamespace()
    const document = request("Employer branding")
    namespace.rows.set(document.contentHash, {
      id: document.contentHash,
      content: document.content,
      model: "another/model",
      vector: [9, 9, 9],
    })

    const [embedded] = await Effect.runPromise(
      provider(namespace.client).embedDocuments([document]),
    )

    expect(embedded!.vector).toEqual(vectorFor(document.content))
    expect(namespace.writes).toHaveLength(1)
  })

  test("returns one vector per content hash in request order", async () => {
    const namespace = fakeNamespace()
    const first = request("Healthcare campaign")
    const second = request("Gaming launch")

    const embedded = await Effect.runPromise(
      provider(namespace.client).embedDocuments([first, second, first]),
    )

    expect(embedded.map(({ contentHash }) => contentHash)).toEqual([first.contentHash, second.contentHash])
    expect(namespace.writes[0]!.rows).toHaveLength(2)
  })

  test("writes and reads in batches", async () => {
    const namespace = fakeNamespace()
    const requests = ["a", "bb", "ccc", "dddd", "eeeee"].map(request)

    const embedded = await Effect.runPromise(
      provider(namespace.client, 2).embedDocuments([requests[0]!, ...requests.slice(1)]),
    )

    expect(embedded).toHaveLength(5)
    expect(namespace.writes.map((write) => write.rows.length)).toEqual([2, 2, 1])
    expect(Math.max(...namespace.queries.map((ids) => ids.length))).toBe(2)
  })

  test("reports an unreachable provider as unavailable", async () => {
    const namespace = fakeNamespace({ failQueries: 1 })

    const exit = await Effect.runPromiseExit(
      provider(namespace.client).embedDocuments([request("Experiential event")]),
    )

    expect(Exit.isFailure(exit)).toBe(true)
    expect(JSON.stringify(exit)).toContain("\"reason\":\"unavailable\"")
    expect(namespace.writes).toHaveLength(0)
  })

  test("fails when the provider returns no vector for written content", async () => {
    const namespace = fakeNamespace({ dropVectors: true })

    const exit = await Effect.runPromiseExit(
      provider(namespace.client).embedDocuments([request("Website redesign")]),
    )

    expect(JSON.stringify(exit)).toContain("\"reason\":\"invalid_response\"")
  })

  test("treats a vector of the wrong size as missing", async () => {
    const namespace = fakeNamespace({ embed: () => [1, 2] })

    const exit = await Effect.runPromiseExit(
      provider(namespace.client).embedDocuments([request("Fashion campaign")]),
    )

    expect(JSON.stringify(exit)).toContain("\"reason\":\"invalid_response\"")
  })

  test("delegates query embeddings", async () => {
    const vector = await Effect.runPromise(provider(fakeNamespace().client).embedQuery("brief"))

    expect(vector).toEqual([5, 0, 0])
  })

  test("rejects an invalid batch size or model at composition", () => {
    const client = fakeNamespace().client

    expect(() => provider(client, 0)).toThrow(InvalidTurbopufferConfiguration)
    expect(() => provider(client, 10_001)).toThrow(InvalidTurbopufferConfiguration)
    expect(() => makeTurbopufferNativeEmbeddingProvider({
      profile,
      model: " ",
      client,
      embedQuery: () => Effect.succeed([0, 0, 0]),
    })).toThrow(InvalidTurbopufferConfiguration)
  })
})
