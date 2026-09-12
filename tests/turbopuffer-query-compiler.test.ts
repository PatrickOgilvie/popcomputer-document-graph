import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import {
  defineEmbeddingProfile,
} from "../src/indexing/embedding-provider.js"
import {
  makeDocumentKey,
} from "../src/document/document-identity.js"
import { parseTextSearchPolicy } from "../src/document/text-search-policy.js"
import {
  documentKeys,
  makeGraphSearchScope,
  noDocuments,
  SearchResultCountSchema,
} from "../src/retrieval/graph-retrieval.js"
import {
  metadataAll,
  metadataAny,
  metadataEquals,
  metadataNot,
  metadataOneOf,
} from "../src/retrieval/metadata-filter.js"
import {
  compileTurbopufferSchemaManifest,
  parseTurbopufferNamespace,
  TurbopufferFilterableAttributes,
} from "../src/storage/turbopuffer/config.js"
import {
  makeTurbopufferMetadataTerm,
} from "../src/storage/turbopuffer/metadata-terms.js"
import { makeTurbopufferWorkspacePartition } from "../src/storage/turbopuffer/partition.js"
import {
  compileTurbopufferHybridQuery,
  compileTurbopufferSemanticQuery,
} from "../src/storage/turbopuffer/query-compiler.js"

const profile = defineEmbeddingProfile({
  id: "test/embedding",
  version: "v1",
  dimensions: 3,
})

const deployment = {
  deploymentId: "test:turbopuffer-query-compiler",
  endpoint: { _tag: "Region" as const, region: "gcp-us-central1" },
}

const partition = makeTurbopufferWorkspacePartition({
  ...deployment,
  workspace: "query-compiler-tests",
  embeddingProfile: profile,
  schemaGeneration: 2,
})

const candidates = Schema.decodeSync(SearchResultCountSchema)(25)

const firstDocument = makeDocumentKey({
  graph: "contracts",
  documentKind: "contract",
  encodedId: "first",
})

const secondDocument = makeDocumentKey({
  graph: "contracts",
  documentKind: "contract",
  encodedId: "second",
})

describe("Turbopuffer query compiler", () => {
  test("validates namespaces and pins the explicit six-field FTS manifest", () => {
    expect(String(parseTurbopufferNamespace("workspace.graph_v2-1"))).toBe(
      "workspace.graph_v2-1",
    )
    expect(() => parseTurbopufferNamespace("workspace/graph")).toThrow()
    expect(() => parseTurbopufferNamespace("x".repeat(129))).toThrow()

    const manifest = compileTurbopufferSchemaManifest(profile.dimensions)
    expect(manifest.distanceMetric).toBe("cosine_distance")
    expect(manifest.attributes.vector).toEqual({
      type: "[3]f32",
      ann: { distance_metric: "cosine_distance" },
    })
    expect(manifest.attributes.partition_id).toEqual({
      type: "string",
      filterable: true,
    })

    for (const attribute of TurbopufferFilterableAttributes) {
      expect(manifest.attributes[attribute]).toMatchObject({
        filterable: true,
      })
    }

    for (const attribute of [
      "fts_en_context",
      "fts_en_label",
      "fts_en_content",
      "fts_simple_context",
      "fts_simple_label",
      "fts_simple_content",
    ]) {
      expect(manifest.attributes[attribute]).toMatchObject({
        type: "string",
        full_text_search: { tokenizer: "word_v4" },
      })
    }
  })

  test("hashes metadata terms canonically and with scalar type separation", () => {
    expect(makeTurbopufferMetadataTerm("status", 1)).toHaveLength(64)
    expect(makeTurbopufferMetadataTerm("status", 1)).toBe(
      makeTurbopufferMetadataTerm("status", 1),
    )
    expect(makeTurbopufferMetadataTerm("status", 1)).not.toBe(
      makeTurbopufferMetadataTerm("status", "1"),
    )
    expect(makeTurbopufferMetadataTerm("status", false)).not.toBe(
      makeTurbopufferMetadataTerm("status", "false"),
    )
  })

  test("pushes graph, projection, target, profile, generation, and metadata into both channels", () => {
    const metadata = metadataAll(
      metadataEquals("status", "active"),
      metadataAny(
        metadataOneOf("tier", [1, 2]),
        metadataNot(metadataEquals("private", true)),
      ),
    )

    const scope = makeGraphSearchScope(
      "contracts",
      {
        include: ["contract", "notice"],
        exclude: ["notice"],
        includeProjections: ["search"],
        target: documentKeys([firstDocument, secondDocument]),
        where: [metadata],
      },
      [
        {
          documentKind: "contract",
          projection: "search",
          projectionVersion: "v3",
        },
      ],
    )

    const policy = parseTextSearchPolicy({
      language: "english",
      weights: { context: 2, label: 3, content: 1 },
    })

    if (policy === "disabled") throw new Error("Expected enabled text fixture")

    const compiled = compileTurbopufferHybridQuery({
      scope,
      partition,
      query: "public procurement",
      queryVector: [0.2, 0.3, 0.4],
      policy,
      semanticCandidates: candidates,
      textCandidates: candidates,
    })

    expect(compiled._tag).toBe("MultiQuery")

    if (compiled._tag !== "MultiQuery") return

    expect(compiled.request).not.toHaveProperty("rerank_by")
    expect(compiled.request.consistency).toEqual({ level: "strong" })
    expect(compiled.request.queries).toHaveLength(2)
    const [semantic, text] = compiled.request.queries
    expect(semantic.distance_metric).toBe("cosine_distance")
    expect(semantic.rank_by).toEqual([
      "vector",
      "ANN",
      [0.2, 0.3, 0.4],
    ])
    expect(text.rank_by).toEqual([
      "Sum",
      [
        ["Product", 2, ["fts_en_context", "BM25", "public procurement"]],
        ["Product", 3, ["fts_en_label", "BM25", "public procurement"]],
        ["Product", 1, ["fts_en_content", "BM25", "public procurement"]],
      ],
    ])
    expect(semantic.filters).toEqual(text.filters)
    expect(JSON.stringify(semantic.filters)).toContain(
      JSON.stringify(["row_kind", "Eq", "slot"]),
    )
    expect(JSON.stringify(semantic.filters)).toContain(
      JSON.stringify(["is_live", "Eq", true]),
    )
    expect(JSON.stringify(semantic.filters)).toContain(
      JSON.stringify(["partition_id", "Eq", partition.identity]),
    )
    expect(JSON.stringify(semantic.filters)).toContain(
      JSON.stringify(["projection_version", "Eq", "v3"]),
    )
    expect(JSON.stringify(semantic.filters)).toContain(firstDocument)
    expect(JSON.stringify(semantic.filters)).toContain(
      makeTurbopufferMetadataTerm("status", "active"),
    )
    expect(JSON.stringify(semantic.filters)).toContain("ContainsAny")
    expect(JSON.stringify(semantic.filters)).toContain('"Not"')
  })

  test("short-circuits an empty graph target before constructing ANN work", () => {
    const scope = makeGraphSearchScope("contracts", {
      target: noDocuments(),
    })

    expect(
      compileTurbopufferSemanticQuery({
        scope,
        partition,
        queryVector: [0.2, 0.3, 0.4],
        candidates,
      }),
    ).toEqual({ _tag: "NoDocuments" })
  })
})
