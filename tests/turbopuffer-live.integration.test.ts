/**
 * Run with `bun run test:live:turbopuffer` plus an API key, deployment ID,
 * and exactly one of `TURBOPUFFER_REGION` or `TURBOPUFFER_BASE_URL`.
 */

import { describe, expect, test } from "bun:test"
import { Effect, Redacted, Schema } from "effect"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import { makeOfficialTurbopufferClient } from "../src/storage/turbopuffer/client.js"
import {
  makeTurbopufferWorkspacePartition,
  type TurbopufferEndpointInput,
} from "../src/storage/turbopuffer/partition.js"

interface LiveTurbopufferConfig {
  readonly apiKey: Redacted.Redacted<string>
  readonly deploymentId: string
  readonly endpoint: TurbopufferEndpointInput
}

const nonBlank = (value: string | undefined): string | undefined => {
  if (value === undefined || value.trim() === "") return undefined
  return value
}

const liveTurbopufferConfig = (): LiveTurbopufferConfig | undefined => {
  if (Bun.env.RUN_DOCUMENT_GRAPH_TURBOPUFFER_TESTS !== "true") {
    return undefined
  }
  const apiKeyValue = nonBlank(Bun.env.TURBOPUFFER_API_KEY)
  const deploymentId = nonBlank(Bun.env.TURBOPUFFER_DEPLOYMENT_ID)
  const region = nonBlank(Bun.env.TURBOPUFFER_REGION)
  const baseURL = nonBlank(Bun.env.TURBOPUFFER_BASE_URL)
  if (apiKeyValue === undefined) {
    throw new Error(
      "Live Turbopuffer conformance requires TURBOPUFFER_API_KEY",
    )
  }
  if (deploymentId === undefined) {
    throw new Error(
      "Live Turbopuffer conformance requires TURBOPUFFER_DEPLOYMENT_ID",
    )
  }
  const apiKey = Redacted.make(apiKeyValue)
  if (region !== undefined && baseURL === undefined) {
    return {
      apiKey,
      deploymentId,
      endpoint: { _tag: "Region", region },
    }
  }
  if (baseURL !== undefined && region === undefined) {
    return {
      apiKey,
      deploymentId,
      endpoint: { _tag: "Custom", baseURL },
    }
  }
  throw new Error(
    "Live Turbopuffer conformance requires exactly one of TURBOPUFFER_REGION or TURBOPUFFER_BASE_URL",
  )
}

const WriteResponseSchema = Schema.Struct({
  status: Schema.Literal("OK"),
  rows_affected: Schema.Number.pipe(
    Schema.check(Schema.isInt()),
    Schema.check(Schema.isGreaterThanOrEqualTo(0)),
  ),
})

const LiveRowSchema = Schema.Struct({
  id: Schema.Union([Schema.String, Schema.Number]),
  content: Schema.String,
  version: Schema.Number.pipe(Schema.check(Schema.isInt())),
  $dist: Schema.optional(Schema.Finite),
})

const QueryResponseSchema = Schema.Struct({
  rows: Schema.Array(LiveRowSchema),
})

const MultiQueryResponseSchema = Schema.Struct({
  results: Schema.Array(Schema.Struct({
    rows: Schema.Array(LiveRowSchema),
  })),
})

const NamespaceSchemaResponseSchema = Schema.Struct({
  vector: Schema.Struct({ type: Schema.Literal("[3]f32") }),
  content: Schema.Struct({ type: Schema.Literal("string") }),
  version: Schema.Struct({ type: Schema.Literal("uint") }),
})

const decodeWriteResponse = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- The live provider response is decoded immediately at this test boundary.
  response: unknown,
) => Schema.decodeUnknownSync(WriteResponseSchema)(response)

const decodeQueryResponse = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- The live provider response is decoded immediately at this test boundary.
  response: unknown,
) => Schema.decodeUnknownSync(QueryResponseSchema)(response)

const decodeMultiQueryResponse = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- The live provider response is decoded immediately at this test boundary.
  response: unknown,
) => Schema.decodeUnknownSync(MultiQueryResponseSchema)(response)

const errorFromCause = (message: string, cause: unknown): Error =>
  cause instanceof Error ? cause : new Error(message, { cause })

const config = liveTurbopufferConfig()

if (config === undefined) {
  describe.skip("live Turbopuffer conformance", () => {
    test("requires an explicit opt-in and complete deployment credentials", () => {})
  })
} else {
  describe("live Turbopuffer conformance", () => {
    test("creates, searches, overwrites, deletes, and destroys one disposable namespace", async () => {
      const profile = defineEmbeddingProfile({
        id: "test:turbopuffer-live",
        version: "v1",
        dimensions: 3,
      })
      const partition = makeTurbopufferWorkspacePartition({
        workspace:
          `live-conformance-${Date.now()}-${crypto.randomUUID()}`,
        deploymentId: config.deploymentId,
        endpoint: config.endpoint,
        embeddingProfile: profile,
        schemaGeneration: 1,
      })
      const client = makeOfficialTurbopufferClient({
        apiKey: config.apiKey,
        partition,
      })
      let conformanceFailure: Error | undefined
      let cleanupFailure: Error | undefined

      try {
        const initialWrite = decodeWriteResponse(await Effect.runPromise(
          client.write({
            distance_metric: "cosine_distance",
            schema: {
              vector: {
                type: "[3]f32",
                ann: { distance_metric: "cosine_distance" },
              },
              content: {
                type: "string",
                full_text_search: {
                  language: "english",
                  tokenizer: "word_v4",
                },
              },
              version: { type: "uint", filterable: true },
            },
            upsert_rows: [
              {
                id: "alpha",
                vector: [1, 0, 0],
                content: "enterprise document graph retrieval",
                version: 1,
              },
              {
                id: "beta",
                vector: [0, 1, 0],
                content: "lexical search and vector ranking",
                version: 1,
              },
            ],
          }),
        ))
        expect(initialWrite.rows_affected).toBe(2)

        const inspected = Schema.decodeUnknownSync(
          NamespaceSchemaResponseSchema,
        )(await Effect.runPromise(client.inspectSchema()))
        expect(inspected.vector.type).toBe("[3]f32")
        expect(inspected.content.type).toBe("string")
        expect(inspected.version.type).toBe("uint")

        const semantic = decodeQueryResponse(await Effect.runPromise(
          client.query({
            rank_by: ["vector", "ANN", [1, 0, 0]],
            top_k: 2,
            include_attributes: ["content", "version"],
            consistency: { level: "strong" },
          }),
        ))
        expect(semantic.rows[0]?.id).toBe("alpha")

        const hybridChannels = decodeMultiQueryResponse(
          await Effect.runPromise(client.multiQuery({
            queries: [
              {
                rank_by: ["vector", "ANN", [1, 0, 0]],
                top_k: 2,
                include_attributes: ["content", "version"],
              },
              {
                rank_by: ["content", "BM25", "document graph"],
                top_k: 2,
                include_attributes: ["content", "version"],
              },
            ],
            consistency: { level: "strong" },
          })),
        )
        expect(hybridChannels.results).toHaveLength(2)
        expect(hybridChannels.results[0]?.rows[0]?.id).toBe("alpha")
        expect(hybridChannels.results[1]?.rows).not.toHaveLength(0)

        const overwrite = decodeWriteResponse(await Effect.runPromise(
          client.write({
            upsert_rows: [{
              id: "alpha",
              vector: [0, 1, 0],
              content: "overwritten authoritative document",
              version: 2,
            }],
          }),
        ))
        expect(overwrite.rows_affected).toBe(1)

        const overwritten = decodeQueryResponse(await Effect.runPromise(
          client.query({
            filters: ["id", "Eq", "alpha"],
            rank_by: ["id", "asc"],
            include_attributes: ["content", "version"],
            limit: 1,
            consistency: { level: "strong" },
          }),
        ))
        expect(overwritten.rows).toEqual([expect.objectContaining({
          id: "alpha",
          content: "overwritten authoritative document",
          version: 2,
        })])

        const deletion = decodeWriteResponse(await Effect.runPromise(
          client.write({ deletes: ["beta"] }),
        ))
        expect(deletion.rows_affected).toBe(1)

        const remaining = decodeQueryResponse(await Effect.runPromise(
          client.query({
            rank_by: ["id", "asc"],
            include_attributes: ["content", "version"],
            limit: 10,
            consistency: { level: "strong" },
          }),
        ))
        expect(remaining.rows.map((row) => row.id)).toEqual(["alpha"])
      } catch (cause: unknown) {
        conformanceFailure = errorFromCause(
          "Live Turbopuffer conformance failed",
          cause,
        )
      } finally {
        try {
          await Effect.runPromise(client.destroyNamespace())
        } catch (cleanupCause: unknown) {
          cleanupFailure = errorFromCause(
            "Live Turbopuffer namespace cleanup failed",
            cleanupCause,
          )
        }
      }
      if (conformanceFailure !== undefined && cleanupFailure !== undefined) {
        throw new AggregateError(
          [conformanceFailure, cleanupFailure],
          "Live Turbopuffer conformance and namespace cleanup failed",
        )
      }
      if (conformanceFailure !== undefined) throw conformanceFailure
      if (cleanupFailure !== undefined) throw cleanupFailure
    }, 120_000)
  })
}
