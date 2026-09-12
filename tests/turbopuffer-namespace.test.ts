import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import {
  TurbopufferClient,
  type TurbopufferClientService,
} from "../src/storage/turbopuffer/client.js"
import {
  destroyTurbopufferNamespace,
  inspectTurbopufferNamespaceSchema,
  updateTurbopufferNamespaceSchema,
} from "../src/storage/turbopuffer/namespace.js"
import { makeTurbopufferWorkspacePartition } from "../src/storage/turbopuffer/partition.js"

const unused = () => Effect.die(new Error("Unexpected provider operation"))

const deployment = {
  deploymentId: "test:turbopuffer-namespace",
  endpoint: { _tag: "Region" as const, region: "gcp-us-central1" },
}

describe("Turbopuffer namespace administration", () => {
  test("keeps schema updates, inspection, and destruction explicit", async () => {
    const updates: Array<unknown> = []
    let destroyed = 0

    const profile = defineEmbeddingProfile({
      id: "test:namespace-administration",
      version: "v1",
      dimensions: 3,
    })

    const partition = makeTurbopufferWorkspacePartition({
      ...deployment,
      workspace: "namespace-administration-test",
      embeddingProfile: profile,
      schemaGeneration: 1,
    })

    const client: TurbopufferClientService = {
      partition,
      write: unused,
      query: unused,
      multiQuery: unused,
      inspectSchema: () => Effect.succeed({
        vector: { type: "[3]f32", ann: true },
      }),
      updateSchema: (request) => {
        updates.push(request)

        return Effect.succeed({})
      },
      destroyNamespace: () => {
        destroyed += 1

        return Effect.succeed({})
      },
    }

    const inspection = await Effect.runPromise(
      Effect.gen(function*() {
        yield* updateTurbopufferNamespaceSchema()
        const inspected = yield* inspectTurbopufferNamespaceSchema
        yield* destroyTurbopufferNamespace

        return inspected
      }).pipe(
        Effect.provideService(TurbopufferClient, client),
      ),
    )

    expect(updates).toHaveLength(1)
    expect(updates[0]).toEqual({
      schema: expect.objectContaining({
        vector: {
          type: "[3]f32",
          ann: { distance_metric: "cosine_distance" },
        },
        metadata_terms: { type: "[]string", filterable: true },
      }),
    })
    expect(inspection).toEqual({
      format: "honertia.document-graph/turbopuffer-schema-inspection-v1",
      namespace: partition.namespace,
      attributes: { vector: { type: "[3]f32", ann: true } },
    })
    expect(destroyed).toBe(1)
  })
})
