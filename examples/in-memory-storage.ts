import { Layer } from "effect"
import {
  EmbeddingProvider,
  type EmbeddingProviderService,
} from "@popcomputer/document-graph"
import { inMemoryDocumentGraph } from "@popcomputer/document-graph/in-memory"

/** Compose a caller-provided embedding service with isolated local storage. */
export const localDocumentGraph = (
  embeddings: EmbeddingProviderService,
) =>
  Layer.mergeAll(
    Layer.succeed(EmbeddingProvider, embeddings),
    inMemoryDocumentGraph(),
  )
