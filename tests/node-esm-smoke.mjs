import { Schema } from "effect"

const [
  core,
  adapter,
  d1,
  d1Schema,
  inMemory,
  postgres,
  testing,
  turbopuffer,
] = await Promise.all([
  import("@popcomputer/document-graph"),
  import("@popcomputer/document-graph/adapter"),
  import("@popcomputer/document-graph/d1"),
  import("@popcomputer/document-graph/d1/schema"),
  import("@popcomputer/document-graph/in-memory"),
  import("@popcomputer/document-graph/postgres"),
  import("@popcomputer/document-graph/testing"),
  import("@popcomputer/document-graph/turbopuffer"),
])

if (
  !(core.defineDocumentGraph instanceof Function) ||
  !(core.DocumentGraphUnavailable instanceof Function) ||
  !(core.toDocumentGraphErrorTelemetry instanceof Function) ||
  !Schema.isSchema(core.GraphRelationIdSchema) ||
  !(adapter.makeDocumentGraphStorage instanceof Function) ||
  !(adapter.EmbeddingProviderFailed instanceof Function) ||
  adapter.GraphTopologyStore.key !==
    "@popcomputer/document-graph/GraphTopologyStore" ||
  !(d1.d1GraphTopology instanceof Function) ||
  !(d1.d1ProjectionPublicationCoordinator instanceof Function) ||
  "d1DocumentGraphSchema" in d1 ||
  !("documentGraphProjectionHeads" in d1Schema.d1DocumentGraphSchema) ||
  !(inMemory.inMemoryDocumentGraph instanceof Function) ||
  !(postgres.postgresDocumentGraph instanceof Function) ||
  !(testing.verifyTextSearchStoreConformance instanceof Function) ||
  !(testing.verifyDocumentGraphStorageConformance instanceof Function) ||
  !(turbopuffer.officialTurbopufferClient instanceof Function) ||
  !(turbopuffer.makeTurbopufferWorkspacePartition instanceof Function) ||
  !(turbopuffer.turbopufferProjectionIndex instanceof Function) ||
  !(turbopuffer.turbopufferProjectionSearch instanceof Function)
) {
  throw new Error("The published Node.js entry points are incomplete")
}

if ("defineAdapterDocumentGraph" in adapter) {
  throw new Error("The removed adapter graph compiler is still published")
}
