/** PostgreSQL document-graph storage using a pool or caller-owned transaction. */
export {
  postgresDocumentGraph,
} from "./storage/postgres/runtime.js"

export {
  postgresTransactionClient,
} from "./storage/postgres/connection.js"

export type {
  PostgresDocumentGraphConfig,
  PostgresQueryClient,
} from "./storage/postgres/connection.js"
