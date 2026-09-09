/** Workspace-local Cloudflare D1 storage for canonical graph topology. */
export type {
  DocumentGraphD1Database,
  DocumentGraphD1PreparedStatement,
  DocumentGraphD1Result,
  DocumentGraphD1Session,
} from "./storage/d1/contract.js"

export {
  D1_GRAPH_TOPOLOGY_MAX_DOCUMENT_KIND_FILTERS,
  D1_GRAPH_TOPOLOGY_MAX_MUTATION_BYTES,
  D1_GRAPH_TOPOLOGY_MAX_OUTGOING_EDGES,
  D1_GRAPH_TOPOLOGY_MAX_REGISTERED_RELATIONS,
  d1GraphTopology,
  type D1GraphTopologyConfig,
} from "./storage/d1/runtime.js"

/** Durable D1 CAS, inventory, and journal for remote projection publication. */
export {
  d1ProjectionPublicationCoordinator,
  type D1ProjectionPublicationConfig,
} from "./storage/d1/projection-publication.js"

export {
  ProjectionMutationIdSchema,
  ProjectionPayloadDigestSchema,
  ProjectionPublicationCoordinator,
  ProjectionPublicationCoordinatorFailed,
  ProjectionPublicationGenerationSchema,
  ProjectionPublicationIdSchema,
  ProjectionPublicationSuperseded,
  type BeginProjectionPublication,
  type PendingProjectionPublication,
  type ProjectionDeletionIntent,
  type ProjectionMutationId,
  type ProjectionPayloadDigest,
  type ProjectionPublicationCatalog,
  type ProjectionPublicationCoordinatorService,
  type ProjectionPublicationGeneration,
  type ProjectionPublicationHead,
  type ProjectionPublicationHeadLookup,
  type ProjectionPublicationId,
  type ProjectionPublicationIntent,
  type ProjectionPublicationLease,
  type ProjectionPublicationOutcome,
  type ProjectionReplacementIntent,
} from "./indexing/projection-publication.js"
