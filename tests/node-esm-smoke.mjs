const [core, adapter, inMemory, postgres, testing] = await Promise.all([
  import("@popcomputer/document-graph"),
  import("@popcomputer/document-graph/adapter"),
  import("@popcomputer/document-graph/in-memory"),
  import("@popcomputer/document-graph/postgres"),
  import("@popcomputer/document-graph/testing"),
])

if (
  typeof core.defineDocumentGraph !== "function" ||
  typeof core.DocumentGraphUnavailable !== "function" ||
  typeof core.toDocumentGraphErrorTelemetry !== "function" ||
  typeof core.GraphRelationIdSchema !== "function" ||
  typeof adapter.makeDocumentGraphStorage !== "function" ||
  typeof adapter.EmbeddingProviderFailed !== "function" ||
  typeof adapter.GraphRelationStore !== "function" ||
  typeof inMemory.inMemoryDocumentGraph !== "function" ||
  typeof postgres.postgresDocumentGraph !== "function" ||
  typeof testing.verifyTextSearchStoreConformance !== "function" ||
  typeof testing.verifyDocumentGraphStorageConformance !== "function"
) {
  throw new Error("The published Node.js entry points are incomplete")
}

if ("defineAdapterDocumentGraph" in adapter) {
  throw new Error("The removed adapter graph compiler is still published")
}
