import { Pool } from "pg"
import { postgresDocumentGraph } from "@popcomputer/document-graph/postgres"
import {
  EmbeddingProvider,
  type EmbeddingProviderService,
} from "@popcomputer/document-graph"
import { Layer } from "effect"
import { SiteGraph } from "./site-graph.js"

export const documentGraphLive = (
  pool: Pool,
  embeddings: EmbeddingProviderService,
) =>
  Layer.mergeAll(
    Layer.succeed(EmbeddingProvider, embeddings),
    postgresDocumentGraph({ pool }),
  )

export const searchSite = (query: string) => SiteGraph.search(query)
