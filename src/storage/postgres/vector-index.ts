import { Schema } from "effect"
import type { PostgresDocumentGraphConfig } from "./connection.js"

const quoteIdentifier = (identifier: string): string => `"${identifier.replaceAll('"', '""')}"`

const IdentifierSchema = Schema.Trimmed.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(63),
  Schema.isPattern(/^[a-z_][a-z0-9_]*$/i),
)

const integer = (minimum: number, maximum: number) =>
  Schema.Number.check(Schema.isInt(), Schema.isBetween({ minimum, maximum }))

const RepresentationSchema = Schema.Literals(["halfvec", "vector"])

/** Index element type: `halfvec` stores two bytes per dimension, `vector` four. */
export type VectorIndexRepresentation = typeof RepresentationSchema.Type

// pgvector's HNSW dimension limits per element type.
const maximumIndexedDimensions = { vector: 2_000, halfvec: 4_000 } as const satisfies Record<VectorIndexRepresentation, number>

const ApproximateVectorSearchSchema = Schema.Struct({
  mode: Schema.Literal("approximate"),
  index: IdentifierSchema,
  dimensions: integer(1, 4_000),
  representation: Schema.optional(RepresentationSchema),
  efSearch: Schema.optional(integer(1, 1_000)),
  overfetch: Schema.optional(integer(1, 20)),
  maxScanTuples: Schema.optional(integer(1, 1_000_000)),
})

/** Which embeddings the index covers and the element type it stores them as; queries and DDL must agree. */
export interface IndexedEmbeddings {
  readonly dimensions: number
  readonly representation: VectorIndexRepresentation
}

/** Validated approximate-search settings with defaults applied. */
export interface ApproximateVectorIndex extends IndexedEmbeddings {
  readonly index: string
  readonly efSearch: number
  readonly overfetch: number
  readonly maxScanTuples: number
}

const checkIndexedEmbeddings = (dimensions: number, representation: VectorIndexRepresentation): IndexedEmbeddings => {
  if (dimensions > maximumIndexedDimensions[representation]) {
    throw new RangeError(`HNSW indexes ${representation} up to ${maximumIndexedDimensions[representation]} dimensions`)
  }

  return { dimensions, representation }
}

const VectorSearchSchema = Schema.Union([Schema.Literals(["auto", "float64"]), ApproximateVectorSearchSchema])

/** How semantic search scores candidates, decoded once when the storage layer is built. */
export type ResolvedVectorSearch =
  | { readonly mode: "auto" | "float64" }
  | { readonly mode: "approximate"; readonly index: ApproximateVectorIndex }

export const resolveVectorSearch = (input: PostgresDocumentGraphConfig["vectorSearch"]): ResolvedVectorSearch => {
  const decoded = Schema.decodeUnknownSync(VectorSearchSchema)(input ?? "auto", { onExcessProperty: "error" })
  if (decoded === "auto" || decoded === "float64") return { mode: decoded }

  return {
    mode: "approximate",
    index: {
      ...checkIndexedEmbeddings(decoded.dimensions, decoded.representation ?? "halfvec"),
      index: decoded.index,
      efSearch: decoded.efSearch ?? 100,
      overfetch: decoded.overfetch ?? 4,
      maxScanTuples: decoded.maxScanTuples ?? 20_000,
    },
  }
}

const column = (alias: string | undefined, name: string): string =>
  alias === undefined ? quoteIdentifier(name) : `${alias}.${quoteIdentifier(name)}`

/**
 * The indexed expression. Queries must repeat it exactly, including the
 * dimension typmod, for PostgreSQL to order by the index.
 */
export const indexedEmbeddingSql = (
  alias: string | undefined,
  vectorNamespace: string,
  indexed: IndexedEmbeddings,
): string => `(${column(alias, "embedding")}::${vectorNamespace}.${indexed.representation}(${indexed.dimensions}))`

/**
 * The partial-index predicate. Queries repeat it with literal dimensions so
 * PostgreSQL can prove the index covers them; it is then never re-evaluated.
 */
export const indexedRowsSql = (
  alias: string | undefined,
  graphNamespace: string,
  indexed: IndexedEmbeddings,
): string => {
  const dimensions = `${column(alias, "embedding_dimensions")} = ${indexed.dimensions}`

  return indexed.representation === "halfvec"
    ? `${dimensions} AND ${graphNamespace}."native_halfvec_eligible"(${column(alias, "embedding")})`
    : `${dimensions} AND ${column(alias, "embedding_native_eligible")}`
}

/** Whether a query vector converts to the index type without overflow or collapsing to zero. */
export const queryVectorFitsIndex = (
  vector: ReadonlyArray<number>,
  indexed: IndexedEmbeddings,
): boolean => {
  if (vector.length !== indexed.dimensions) return false
  let maximum = 0

  for (const component of vector) {
    if (!Number.isFinite(component)) return false
    maximum = Math.max(maximum, Math.abs(component))
  }

  return indexed.representation === "halfvec"
    ? maximum >= 2 ** -14 && maximum <= 65_504
    : maximum >= 1e-18 && maximum <= 1e15
}

/** One row, `ready`, true when the named HNSW index on projected_chunks is valid and maintained. */
export const approximateIndexReadySql = `SELECT EXISTS (
   SELECT 1
   FROM pg_catalog.pg_index AS index_state
   INNER JOIN pg_catalog.pg_class AS index_relation ON index_relation.oid = index_state.indexrelid
   INNER JOIN pg_catalog.pg_am AS access_method ON access_method.oid = index_relation.relam
   INNER JOIN pg_catalog.pg_class AS table_relation ON table_relation.oid = index_state.indrelid
   INNER JOIN pg_catalog.pg_namespace AS table_namespace ON table_namespace.oid = table_relation.relnamespace
   WHERE table_namespace.nspname = $1
     AND table_relation.relname = 'projected_chunks'
     AND index_relation.relname = $2
     AND access_method.amname = 'hnsw'
     AND index_state.indisvalid
     AND index_state.indisready
 ) AS ready`

/** Options for {@link postgresVectorIndexSql}. */
export interface PostgresVectorIndexSqlOptions {
  /** Unqualified index name; pass the same name as `vectorSearch.index`. */
  readonly index: string
  /** Embedding dimensions to cover; pass the same value as `vectorSearch.dimensions`. */
  readonly dimensions: number
  /** Defaults to `halfvec`; pass the same value as `vectorSearch.representation`. */
  readonly representation?: VectorIndexRepresentation
  /** Document-graph schema. Defaults to `honertia_document_graph`. */
  readonly schema?: string
  /** Schema where pgvector is installed. Defaults to `public`. */
  readonly vectorSchema?: string
  /** Build without blocking writes. Defaults to true, which must run outside a transaction. */
  readonly concurrently?: boolean
  /** HNSW links per node. Defaults to 16. */
  readonly m?: number
  /** HNSW build candidate list. Defaults to 64 and must be at least twice `m`. */
  readonly efConstruction?: number
}

/**
 * The DDL for the HNSW index used by `vectorSearch: { mode: "approximate" }`.
 *
 * It indexes an expression over the canonical float64 arrays, so it adds no
 * column and needs no backfill. halfvec indexes require migration 0005. A
 * failed concurrent build leaves an invalid index that writes still maintain:
 * drop it with `DROP INDEX CONCURRENTLY` before retrying.
 */
export const postgresVectorIndexSql = (options: PostgresVectorIndexSqlOptions): string => {
  const index = Schema.decodeUnknownSync(IdentifierSchema)(options.index)
  const schema = Schema.decodeUnknownSync(IdentifierSchema)(options.schema ?? "honertia_document_graph")
  const vectorSchema = Schema.decodeUnknownSync(IdentifierSchema)(options.vectorSchema ?? "public")
  const indexed = checkIndexedEmbeddings(Schema.decodeUnknownSync(integer(1, 4_000))(options.dimensions), options.representation ?? "halfvec")
  const m = Schema.decodeUnknownSync(integer(2, 100))(options.m ?? 16)
  const efConstruction = Schema.decodeUnknownSync(integer(4, 1_000))(options.efConstruction ?? 64)

  if (efConstruction < 2 * m) throw new RangeError("efConstruction must be at least twice m")
  const vectorNamespace = quoteIdentifier(vectorSchema)

  return `CREATE INDEX ${options.concurrently === false ? "" : "CONCURRENTLY "}${quoteIdentifier(index)}
  ON ${quoteIdentifier(schema)}."projected_chunks"
  USING hnsw (${indexedEmbeddingSql(undefined, vectorNamespace, indexed)} ${vectorNamespace}.${indexed.representation}_cosine_ops)
  WITH (m = ${m}, ef_construction = ${efConstruction})
  WHERE ${indexedRowsSql(undefined, quoteIdentifier(schema), indexed)}`
}
