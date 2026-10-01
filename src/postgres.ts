/** PostgreSQL document-graph storage using a pool or caller-owned transaction. */
export {
  postgresDocumentGraph,
} from "./storage/postgres/runtime.js"

export {
  postgresTransactionClient,
} from "./storage/postgres/connection.js"

export type {
  PostgresApproximateVectorSearch,
  PostgresDocumentGraphConfig,
  PostgresQueryClient,
  PostgresSearchCoalescing,
} from "./storage/postgres/connection.js"

export {
  postgresVectorIndexSql,
} from "./storage/postgres/vector-index.js"

export type {
  PostgresVectorIndexSqlOptions,
  VectorIndexRepresentation,
} from "./storage/postgres/vector-index.js"

export {
  postgresProjectionPublicationCoordinator,
  type PostgresProjectionPublicationConfig,
} from "./storage/postgres/projection-publication.js"

export {
  copyPostgresProjectionIndex,
  mirrorPostgresProjectionChanges,
  PostgresProjectionIndexReadFailed,
  readPostgresProjectionIndexPage,
  type PostgresProjectionIndexCursor,
  type PostgresProjectionIndexPage,
  type ProjectionIndexCopyProgress,
  type ProjectionIndexMirrorFailure,
  type ProjectionIndexMirrorProgress,
  type StoredProjectedRevision,
} from "./storage/postgres/projection-index-copy.js"
