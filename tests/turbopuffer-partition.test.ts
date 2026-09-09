import { describe, expect, test } from "bun:test"
import { Redacted } from "effect"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import { makeOfficialTurbopufferClient } from "../src/storage/turbopuffer/client.js"
import { compileTurbopufferSchemaManifest } from "../src/storage/turbopuffer/config.js"
import { InvalidTurbopufferConfiguration } from "../src/storage/turbopuffer/errors.js"
import {
  makeTurbopufferWorkspacePartition,
  validateTurbopufferWorkspacePartition,
} from "../src/storage/turbopuffer/partition.js"

const profile = defineEmbeddingProfile({
  id: "test:partition",
  version: "v1",
  dimensions: 3,
})
const deploymentId = "test-account/main"
const endpoint = {
  _tag: "Region",
  region: "gcp-us-central1",
} as const

describe("Turbopuffer workspace partition", () => {
  test("derives one namespace and D1 generation from the complete scope", () => {
    const first = makeTurbopufferWorkspacePartition({
      workspace: "workspace-1",
      deploymentId,
      endpoint,
      embeddingProfile: profile,
      schemaGeneration: 1,
    })
    const repeated = makeTurbopufferWorkspacePartition({
      workspace: "workspace-1",
      deploymentId,
      endpoint,
      embeddingProfile: profile,
      schemaGeneration: 1,
    })
    const otherWorkspace = makeTurbopufferWorkspacePartition({
      workspace: "workspace-2",
      deploymentId,
      endpoint,
      embeddingProfile: profile,
      schemaGeneration: 1,
    })

    expect(repeated).toEqual(first)
    expect(String(first.namespace)).toBe(`document-graph-${first.identity}`)
    expect(String(first.d1IndexGeneration)).toBe(
      `turbopuffer-v2-${first.identity}`,
    )
    expect(otherWorkspace.identity).not.toBe(first.identity)
    expect(otherWorkspace.namespace).not.toBe(first.namespace)
    expect(otherWorkspace.d1IndexGeneration).not.toBe(
      first.d1IndexGeneration,
    )
  })

  test("rejects a structurally valid partition with mismatched derivations", () => {
    const first = makeTurbopufferWorkspacePartition({
      workspace: "workspace-1",
      deploymentId,
      endpoint,
      embeddingProfile: profile,
      schemaGeneration: 1,
    })
    const other = makeTurbopufferWorkspacePartition({
      workspace: "workspace-2",
      deploymentId,
      endpoint,
      embeddingProfile: profile,
      schemaGeneration: 1,
    })

    expect(() => validateTurbopufferWorkspacePartition({
      workspace: first.workspace,
      deploymentId: first.deploymentId,
      endpoint: first.endpoint,
      embeddingProfile: {
        id: first.embeddingProfile.id,
        version: first.embeddingProfile.version,
        dimensions: first.embeddingProfile.dimensions,
      },
      schemaGeneration: first.schemaGeneration,
      identity: first.identity,
      namespace: other.namespace,
      d1IndexGeneration: first.d1IndexGeneration,
    })).toThrow(InvalidTurbopufferConfiguration)
  })

  test("partitions the same workspace across deployments and endpoints", () => {
    const regional = makeTurbopufferWorkspacePartition({
      workspace: "workspace-1",
      deploymentId,
      endpoint,
      embeddingProfile: profile,
      schemaGeneration: 1,
    })
    const otherDeployment = makeTurbopufferWorkspacePartition({
      workspace: "workspace-1",
      deploymentId: "test-account/other",
      endpoint,
      embeddingProfile: profile,
      schemaGeneration: 1,
    })
    const otherRegion = makeTurbopufferWorkspacePartition({
      workspace: "workspace-1",
      deploymentId,
      endpoint: { _tag: "Region", region: "aws-us-east-1" },
      embeddingProfile: profile,
      schemaGeneration: 1,
    })
    const custom = makeTurbopufferWorkspacePartition({
      workspace: "workspace-1",
      deploymentId,
      endpoint: {
        _tag: "Custom",
        baseURL: "https://TP.example.com/api/",
      },
      embeddingProfile: profile,
      schemaGeneration: 1,
    })

    expect(otherDeployment.identity).not.toBe(regional.identity)
    expect(otherRegion.identity).not.toBe(regional.identity)
    expect(custom.identity).not.toBe(regional.identity)
    expect(custom.endpoint._tag).toBe("Custom")
    if (custom.endpoint._tag !== "Custom") {
      throw new Error("Expected a custom endpoint")
    }
    expect(String(custom.endpoint.baseURL)).toBe(
      "https://tp.example.com/api",
    )
  })

  test("rejects invalid deployment and endpoint coordinates precisely", () => {
    expect(() => makeTurbopufferWorkspacePartition({
      workspace: "workspace-1",
      deploymentId: " invalid-deployment ",
      endpoint,
      embeddingProfile: profile,
      schemaGeneration: 1,
    })).toThrow(expect.objectContaining({
      _tag: "InvalidTurbopufferConfiguration",
      field: "deployment_id",
    }))
    expect(() => makeTurbopufferWorkspacePartition({
      workspace: "workspace-1",
      deploymentId,
      endpoint: {
        _tag: "Custom",
        baseURL: "https://secret@example.com",
      },
      embeddingProfile: profile,
      schemaGeneration: 1,
    })).toThrow(expect.objectContaining({
      _tag: "InvalidTurbopufferConfiguration",
      field: "endpoint",
    }))
    expect(() => makeTurbopufferWorkspacePartition({
      workspace: "workspace-1",
      deploymentId,
      endpoint: {
        _tag: "Custom",
        baseURL: "http://gateway.example.test",
      },
      embeddingProfile: profile,
      schemaGeneration: 1,
    })).toThrow(expect.objectContaining({
      _tag: "InvalidTurbopufferConfiguration",
      field: "endpoint",
    }))
  })

  test("accepts the vector maximum and rejects the first unsupported size", () => {
    const maximumProfile = defineEmbeddingProfile({
      id: "test:partition",
      version: "maximum",
      dimensions: 10_752,
    })
    const maximumPartition = makeTurbopufferWorkspacePartition({
      workspace: "workspace-1",
      deploymentId,
      endpoint,
      embeddingProfile: maximumProfile,
      schemaGeneration: 1,
    })
    expect(Number(maximumPartition.embeddingProfile.dimensions)).toBe(10_752)
    expect(
      compileTurbopufferSchemaManifest(maximumProfile.dimensions)
        .attributes["vector"],
    ).toEqual({
      type: "[10752]f32",
      ann: { distance_metric: "cosine_distance" },
    })

    const unsupportedProfile = defineEmbeddingProfile({
      id: "test:partition",
      version: "too-wide",
      dimensions: 10_753,
    })
    expect(() => makeTurbopufferWorkspacePartition({
      workspace: "workspace-1",
      deploymentId,
      endpoint,
      embeddingProfile: unsupportedProfile,
      schemaGeneration: 1,
    })).toThrow(expect.objectContaining({
      _tag: "InvalidTurbopufferConfiguration",
      field: "vector_dimensions",
    }))
    expect(() => compileTurbopufferSchemaManifest(
      unsupportedProfile.dimensions,
    )).toThrow(expect.objectContaining({
      _tag: "InvalidTurbopufferConfiguration",
      field: "vector_dimensions",
    }))
  })

  test("binds the official client to the partition-derived namespace", () => {
    const partition = makeTurbopufferWorkspacePartition({
      workspace: "workspace-1",
      deploymentId,
      endpoint,
      embeddingProfile: profile,
      schemaGeneration: 1,
    })
    const client = makeOfficialTurbopufferClient({
      apiKey: Redacted.make("test-api-key"),
      partition,
      retries: 0,
    })
    const rotatedClient = makeOfficialTurbopufferClient({
      apiKey: Redacted.make("rotated-test-api-key"),
      partition,
      retries: 0,
    })

    expect(client.partition).toEqual(partition)
    expect(rotatedClient.partition.identity).toBe(client.partition.identity)
  })
})
