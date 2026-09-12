import type { D1Database } from "@cloudflare/workers-types"
import { Redacted } from "effect"
import type {
  EmbeddingProviderService,
} from "@popcomputer/document-graph"
import {
  makeTurbopufferD1Workspace,
} from "@popcomputer/document-graph/turbopuffer"

/** Compile-checked data-plane composition for one isolated workspace. */
export const cloudflareWorkspaceDocumentGraph = (input: {
  readonly workspace: string
  readonly database: D1Database
  readonly embeddings: EmbeddingProviderService
  readonly turbopuffer: {
    readonly apiKey: string
    readonly deploymentId: string
    readonly region: string
    readonly schemaGeneration: number
  }
}) =>
  makeTurbopufferD1Workspace({
    workspace: input.workspace,
    database: input.database,
    embeddings: input.embeddings,
    turbopuffer: {
      apiKey: Redacted.make(input.turbopuffer.apiKey),
      deploymentId: input.turbopuffer.deploymentId,
      endpoint: {
        _tag: "Region",
        region: input.turbopuffer.region,
      },
      schemaGeneration: input.turbopuffer.schemaGeneration,
    },
  }).layer
