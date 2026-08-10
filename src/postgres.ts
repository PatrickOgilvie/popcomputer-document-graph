/** PostgreSQL document-graph storage using a pool or caller-owned transaction. */
export {
  postgresDocumentGraph,
} from "./storage/postgres/runtime.js"

export type { PostgresDocumentGraphConfig } from "./storage/postgres/connection.js"
