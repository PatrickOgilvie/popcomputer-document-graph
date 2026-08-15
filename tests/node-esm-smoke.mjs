import { Schema } from "effect"

const [core, adapter, inMemory, postgres, testing] = await Promise.all([
  import("@popcomputer/document-graph"),
  import("@popcomputer/document-graph/adapter"),
  import("@popcomputer/document-graph/in-memory"),
  import("@popcomputer/document-graph/postgres"),
  import("@popcomputer/document-graph/testing"),
])

if (
  !(core.defineDocumentGraph instanceof Function) ||
  !(core.DocumentGraphUnavailable instanceof Function) ||
  !(core.toDocumentGraphErrorTelemetry instanceof Function) ||
  !Schema.isSchema(core.GraphRelationIdSchema) ||
  !(adapter.makeDocumentGraphStorage instanceof Function) ||
  !(adapter.EmbeddingProviderFailed instanceof Function) ||
  adapter.GraphRelationStore.key !==
    "@popcomputer/document-graph/GraphRelationStore" ||
  !(inMemory.inMemoryDocumentGraph instanceof Function) ||
  !(postgres.postgresDocumentGraph instanceof Function) ||
  !(testing.verifyTextSearchStoreConformance instanceof Function) ||
  !(testing.verifyDocumentGraphStorageConformance instanceof Function)
) {
  throw new Error("The published Node.js entry points are incomplete")
}

if ("defineAdapterDocumentGraph" in adapter) {
  throw new Error("The removed adapter graph compiler is still published")
}
