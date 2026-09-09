import { describe, expect, test } from "bun:test"
import { Effect, Redacted } from "effect"
import {
  EmbeddingProvider,
  defineEmbeddingProfile,
  type EmbeddingProviderService,
} from "../src/indexing/embedding-provider.js"
import { ProjectionIndexStore } from "../src/indexing/projection-index.js"
import {
  ProjectionHybridSearchStore,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
} from "../src/retrieval/graph-retrieval.js"
import { GraphTopologyStore } from "../src/graph/graph-topology.js"
import type {
  DocumentGraphD1Database,
  DocumentGraphD1PreparedStatement,
  DocumentGraphD1Result,
  DocumentGraphD1Session,
} from "../src/storage/d1/contract.js"
import { makeTurbopufferD1Workspace } from "../src/storage/turbopuffer/workspace.js"

class UnusedD1Database implements DocumentGraphD1Database {
  prepare(_query: string): DocumentGraphD1PreparedStatement {
    throw new Error("The composition test must not access D1")
  }

  async batch<Row = unknown>(
    _statements: Array<DocumentGraphD1PreparedStatement>,
  ): Promise<Array<DocumentGraphD1Result<Row>>> {
    throw new Error("The composition test must not access D1")
  }

  withSession(_constraint: "first-primary"): DocumentGraphD1Session {
    return {
      prepare: (_query) => {
        throw new Error("The composition test must not access D1")
      },
    }
  }
}

const profile = defineEmbeddingProfile({
  id: "test:workspace-facade",
  version: "v1",
  dimensions: 3,
})

const embeddings: EmbeddingProviderService = {
  profile,
  embedDocuments: () =>
    Effect.die(new Error("The composition test must not embed documents")),
  embedQuery: () =>
    Effect.die(new Error("The composition test must not embed a query")),
}

describe("Turbopuffer D1 workspace composition", () => {
  test("derives one partition and exposes only cohesive graph capabilities", async () => {
    const workspace = makeTurbopufferD1Workspace({
      workspace: "workspace-facade-test",
      database: new UnusedD1Database(),
      embeddings,
      turbopuffer: {
        apiKey: Redacted.make("workspace-facade-test-key"),
        deploymentId: "test-deployment",
        endpoint: {
          _tag: "Region",
          region: "gcp-us-central1",
        },
        schemaGeneration: 1,
        retries: 0,
      },
    })

    const services = await Effect.runPromise(
      Effect.gen(function*() {
        return {
          embeddings: yield* EmbeddingProvider,
          topology: yield* GraphTopologyStore,
          index: yield* ProjectionIndexStore,
          semantic: yield* ProjectionSearchStore,
          text: yield* ProjectionTextSearchStore,
          hybrid: yield* ProjectionHybridSearchStore,
        }
      }).pipe(Effect.provide(workspace.layer)),
    )

    expect(String(workspace.partition.workspace)).toBe(
      "workspace-facade-test",
    )
    expect(String(workspace.partition.deploymentId)).toBe("test-deployment")
    expect(workspace.partition.endpoint._tag).toBe("Region")
    if (workspace.partition.endpoint._tag === "Region") {
      expect(String(workspace.partition.endpoint.region)).toBe(
        "gcp-us-central1",
      )
    }
    expect(workspace.partition.embeddingProfile).toEqual(profile)
    expect(services.embeddings).toBe(embeddings)
    expect(services.topology).toBeDefined()
    expect(services.index).toBeDefined()
    expect(services.semantic).toBeDefined()
    expect(services.text).toBeDefined()
    expect(services.hybrid).toBeDefined()
  })

  test("rejects facade runtime settings through safe typed configuration errors", () => {
    const baseConfig = {
      workspace: "workspace-facade-test",
      database: new UnusedD1Database(),
      embeddings,
      turbopuffer: {
        apiKey: Redacted.make("workspace-facade-test-key"),
        deploymentId: "test-deployment",
        endpoint: {
          _tag: "Region" as const,
          region: "gcp-us-central1",
        },
        schemaGeneration: 1,
      },
    }
    const cases = [
      {
        input: { retries: -1 },
        field: "retries",
      },
      {
        input: { timeoutMilliseconds: 0 },
        field: "timeout_milliseconds",
      },
      {
        input: { publicationLeaseMilliseconds: 999 },
        field: "publication_lease_milliseconds",
      },
      {
        input: { retainedPublicationHistory: 1_001 },
        field: "retained_publication_history",
      },
    ] as const

    for (const item of cases) {
      expect(() => makeTurbopufferD1Workspace({
        ...baseConfig,
        turbopuffer: {
          ...baseConfig.turbopuffer,
          ...item.input,
        },
      })).toThrow(expect.objectContaining({
        _tag: "InvalidTurbopufferConfiguration",
        field: item.field,
        reason: "invalid_value",
      }))
    }
  })
})
