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
} from "./storage/postgres/connection.js"

export {
  postgresVectorIndexSql,
} from "./storage/postgres/vector-index.js"

export type {
  PostgresVectorIndexSqlOptions,
  VectorIndexRepresentation,
} from "./storage/postgres/vector-index.js"
