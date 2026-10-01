import { Schema } from "effect"

/** Schema created by the package's PostgreSQL migrations. */
export const DefaultPostgresSchema = "honertia_document_graph"

/** Unquoted PostgreSQL schema identifier accepted by the adapters. */
export const PostgresSchemaNameSchema = Schema.Trimmed.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(63),
  Schema.isPattern(/^[a-z_][a-z0-9_]*$/i),
)
