import {
  JsonValueSchema,
  type JsonValue,
} from "../../document/json-value.js"
import {
  makeDocumentGraphStorage,
  type DocumentGraphStorageService,
} from "../document-graph-storage.js"
import {
  ChunkIdSchema,
  ContentHashSchema,
  DocumentKeySchema,
  ProjectionRevisionHashSchema,
  type ChunkId,
  type ContentHash,
} from "../../document/document-identity.js"
import {
  ProjectionSearchStoreFailed,
  ProjectionTextSearchStoreFailed,
  type GraphSearchScope,
  type SemanticCandidateRequest,
  type SemanticSearchCandidate,
  type TextCandidateRequest,
  type TextSearchCandidate,
} from "../../retrieval/graph-retrieval.js"
import type { MetadataFilter } from "../../retrieval/metadata-filter.js"
import {
  countProjectedRevisionReplacement,
  embeddingProfilesEqual,
  IndexRevisionTokenSchema,
  isValidEmbeddingVector,
  planProjectedRevisionReplacement,
  ProjectionIndexConflict,
  ProjectionIndexStoreFailed,
  type IndexedRevisionSnapshot,
  type IndexRevisionToken,
  type ProjectionIndexCommit,
  type ReplaceProjectedRevision,
} from "../../indexing/projection-index.js"
import {
  EmbeddingDimensionsSchema,
  EmbeddingProfileIdSchema,
  EmbeddingProfileVersionSchema,
  type EmbeddingProfile,
} from "../../indexing/embedding-provider.js"
import {
  countGraphRelationReplacement,
  GraphRelationStoreFailed,
  makeGraphRelationEdgeIdentity,
  planOutgoingGraphRelationReplacement,
  type GraphRelationCommit,
  type GraphRelationDeletion,
  type GraphRelationPrune,
  type PruneGraphRelations,
  type ReplaceOutgoingGraphRelations,
  type StoredGraphNeighbour,
} from "../../graph/graph-relation.js"
import { Effect, Either, Option, ParseResult, Schema } from "effect"
import {
  connectionFor,
  queryRows,
  transactionEffect,
  type PostgresDocumentGraphConfig,
  type PostgresQueryable as Queryable,
  type PostgresTransactionClient as TransactionClient,
} from "./connection.js"

const DefaultSchema = "honertia_document_graph"
const InsertBatchSize = 250

const PostgresSchemaNameSchema = Schema.NonEmptyTrimmedString.pipe(
  Schema.maxLength(63),
  Schema.pattern(/^[a-z_][a-z0-9_]*$/i),
)

interface StoredRowParseIssue {
  readonly kind: ParseResult.ArrayFormatterIssue["_tag"]
  readonly path: string
}

class InvalidStoredState extends Error {
  override readonly name = "InvalidStoredState"

  constructor(
    message: string,
    readonly rowKind: string | undefined = undefined,
    readonly issues: ReadonlyArray<StoredRowParseIssue> = [],
  ) {
    super(message)
  }
}

const RevisionWithChunkRowSchema = Schema.Struct({
  revision_token: Schema.String.pipe(Schema.pattern(/^[1-9][0-9]*$/)),
  revision_hash: ProjectionRevisionHashSchema,
  embedding_profile_id: EmbeddingProfileIdSchema,
  embedding_profile_version: EmbeddingProfileVersionSchema,
  embedding_dimensions: EmbeddingDimensionsSchema,
  chunk_id: Schema.NullOr(ChunkIdSchema),
  content_hash: Schema.NullOr(ContentHashSchema),
  ordinal: Schema.NullOr(Schema.Number.pipe(Schema.int(), Schema.nonNegative())),
})

const ReusableChunkRowSchema = Schema.Struct({
  chunk_id: ChunkIdSchema,
  content_hash: ContentHashSchema,
  embedding: Schema.Array(Schema.Number),
})

const RevisionTokenRowSchema = Schema.Struct({
  revision_token: Schema.String.pipe(Schema.pattern(/^[1-9][0-9]*$/)),
})

const CurrentRevisionRowSchema = Schema.Struct({
  revision_token: Schema.String.pipe(Schema.pattern(/^[1-9][0-9]*$/)),
  revision_hash: ProjectionRevisionHashSchema,
  embedding_profile_id: EmbeddingProfileIdSchema,
  embedding_profile_version: EmbeddingProfileVersionSchema,
  embedding_dimensions: EmbeddingDimensionsSchema,
})

const DeletionCountRowSchema = Schema.Struct({
  revision_count: Schema.String.pipe(Schema.pattern(/^[0-9]+$/)),
  chunk_count: Schema.String.pipe(Schema.pattern(/^[0-9]+$/)),
})

const SearchCandidateRowSchema = Schema.Struct({
  score: Schema.Number,
  chunk_id: ChunkIdSchema,
  document_key: DocumentKeySchema,
  graph_id: Schema.NonEmptyTrimmedString,
  document_kind: Schema.NonEmptyTrimmedString,
  encoded_document_id: JsonValueSchema,
  projection_id: Schema.NonEmptyTrimmedString,
  projection_version: Schema.NonEmptyTrimmedString,
  revision_hash: ProjectionRevisionHashSchema,
  section_key: Schema.NonEmptyTrimmedString,
  section_part: Schema.Number.pipe(Schema.int(), Schema.nonNegative()),
  content: Schema.NonEmptyTrimmedString,
  has_metadata: Schema.Boolean,
  metadata: Schema.NullOr(JsonValueSchema),
})

const GraphEdgeIdentityRowSchema = Schema.Struct({
  relation_id: Schema.String,
  target_document_key: DocumentKeySchema,
})

const GraphNeighbourRowSchema = Schema.Struct({
  document_key: DocumentKeySchema,
  graph_id: Schema.String,
  document_kind: Schema.String,
  encoded_document_id: JsonValueSchema,
})

const DeletedGraphEdgeCountRowSchema = Schema.Struct({
  deleted_count: Schema.String.pipe(Schema.pattern(/^[0-9]+$/)),
})

const quoteIdentifier = (identifier: string): string => `"${identifier}"`

const parseRow = <A, I>(
  schema: Schema.Schema<A, I>,
  row: unknown,
  rowKind: string,
): A => {
  try {
    return Schema.decodeUnknownSync(schema)(row, {
      onExcessProperty: "error",
    })
  } catch (cause: unknown) {
    const issues = ParseResult.isParseError(cause)
      ? ParseResult.ArrayFormatter.formatErrorSync(cause)
          .slice(0, 8)
          .map((issue): StoredRowParseIssue => ({
            kind: issue._tag,
            path: issue.path
              .map((segment) =>
                typeof segment === "symbol"
                  ? segment.description ?? "symbol"
                  : String(segment),
              )
              .join("."),
          }))
      : []
    throw new InvalidStoredState(
      `PostgreSQL returned an invalid ${rowKind} row`,
      rowKind,
      issues,
    )
  }
}

const revisionToken = (value: string): IndexRevisionToken =>
  Schema.decodeSync(IndexRevisionTokenSchema)(`postgres:${value}`)

const indexFailure = (
  operation:
    | "load_revision"
    | "replace_revision"
    | "delete_revision"
    | "prune_graph",
  cause: unknown,
): ProjectionIndexStoreFailed =>
  new ProjectionIndexStoreFailed({
    operation,
    reason: cause instanceof InvalidStoredState
      ? "invalid_stored_state"
      : "unavailable",
    cause,
  })

const searchFailure = (cause: unknown): ProjectionSearchStoreFailed =>
  new ProjectionSearchStoreFailed({
    reason: cause instanceof InvalidStoredState
      ? "invalid_stored_state"
      : "unavailable",
    cause,
  })

const textSearchFailure = (
  cause: unknown,
): ProjectionTextSearchStoreFailed =>
  new ProjectionTextSearchStoreFailed({
    reason:
      cause instanceof InvalidStoredState
        ? "invalid_stored_state"
        : "unavailable",
    cause,
  })

const relationFailure = (
  operation: GraphRelationStoreFailed["operation"],
  cause: unknown,
): GraphRelationStoreFailed =>
  new GraphRelationStoreFailed({
    operation,
    reason: cause instanceof InvalidStoredState
      ? "invalid_stored_state"
      : "unavailable",
    cause,
  })

const invalidReplacement = (message: string): InvalidStoredState =>
  new InvalidStoredState(message)

const vectorsEqual = (
  left: ReadonlyArray<number>,
  right: ReadonlyArray<number>,
): boolean =>
  left.length === right.length &&
  left.every((value, index) => value === right[index])

const encodeJson = (value: JsonValue): string => {
  const encoded = JSON.stringify(value)
  if (encoded === undefined) {
    throw invalidReplacement("A JSON value could not be encoded")
  }
  return encoded
}

interface StoredChunkState {
  readonly chunkIds: ReadonlySet<ChunkId>
  readonly reusableVectors: ReadonlyMap<
    ContentHash,
    ReadonlyArray<number>
  >
}

const loadStoredChunkState = async (
  client: TransactionClient,
  tables: { readonly chunks: string },
  replacement: ReplaceProjectedRevision,
  currentProfile: EmbeddingProfile | undefined,
): Promise<StoredChunkState> => {
  const chunkIds = new Set<ChunkId>()
  const vectors = new Map<ContentHash, ReadonlyArray<number>>()
  const canReuse =
    currentProfile !== undefined &&
    embeddingProfilesEqual(currentProfile, replacement.embeddingProfile)

  const rows = await queryRows<Record<string, unknown>>(
    client,
    `SELECT chunk_id, content_hash, embedding
     FROM ${tables.chunks}
     WHERE document_key = $1 AND projection_id = $2
     ORDER BY ordinal`,
    [replacement.key.documentKey, replacement.key.projection],
  )

  for (const unknownRow of rows) {
    const row = parseRow(
      ReusableChunkRowSchema,
      unknownRow,
      "reusable chunk",
    )
    chunkIds.add(row.chunk_id)
    if (!canReuse) continue

    if (
      !isValidEmbeddingVector(
        row.embedding,
        replacement.embeddingProfile.dimensions,
      )
    ) {
      throw invalidReplacement("A reusable embedding has invalid dimensions")
    }

    const previous = vectors.get(row.content_hash)
    if (previous !== undefined && !vectorsEqual(previous, row.embedding)) {
      throw invalidReplacement("One content hash has conflicting stored embeddings")
    }
    vectors.set(row.content_hash, row.embedding)
  }

  return { chunkIds, reusableVectors: vectors }
}

const insertChunks = async (
  client: TransactionClient,
  tables: { readonly chunks: string },
  replacement: ReplaceProjectedRevision,
  vectors: ReadonlyMap<ContentHash, ReadonlyArray<number>>,
): Promise<void> => {
  for (let offset = 0; offset < replacement.chunks.length; offset += InsertBatchSize) {
    const batch = replacement.chunks.slice(offset, offset + InsertBatchSize)
    const values: Array<unknown> = []
    const rows = batch.map((chunk) => {
      const vector = vectors.get(chunk.contentHash)
      if (vector === undefined) {
        throw invalidReplacement("A validated embedding unexpectedly disappeared")
      }

      const start = values.length
      values.push(
        chunk.chunkId,
        replacement.key.documentKey,
        replacement.key.projection,
        chunk.ordinal,
        chunk.sectionKey,
        chunk.sectionIndex,
        chunk.sectionPart,
        chunk.contentHash,
        chunk.content,
        chunk.embeddingContent,
        chunk.text.context ?? null,
        chunk.text.label ?? null,
        chunk.text.content,
        chunk.metadata !== undefined,
        chunk.metadata === undefined ? null : encodeJson(chunk.metadata),
        replacement.embeddingProfile.dimensions,
        [...vector],
      )

      const parameter = (index: number): string => `$${start + index}`
      return `(${parameter(1)}, ${parameter(2)}, ${parameter(3)}, ${parameter(4)},
        ${parameter(5)}, ${parameter(6)}, ${parameter(7)}, ${parameter(8)},
        ${parameter(9)}, ${parameter(10)}, ${parameter(11)},
        ${parameter(12)}, ${parameter(13)}, ${parameter(14)},
        ${parameter(15)}::jsonb, ${parameter(16)},
        ${parameter(17)}::double precision[])`
    })

    await client.query(
      `INSERT INTO ${tables.chunks}
        (chunk_id, document_key, projection_id, ordinal, section_key,
         section_index, section_part, content_hash, content,
         embedding_content, text_context, text_label, text_content,
         has_metadata, metadata, embedding_dimensions, embedding)
       VALUES ${rows.join(",\n")}`,
      values,
    )
  }
}

const replaceInTransaction = async (
  client: TransactionClient,
  tables: { readonly revisions: string; readonly chunks: string },
  replacement: ReplaceProjectedRevision,
): Promise<ProjectionIndexCommit> => {
  await client.query(
    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
    [`${replacement.key.documentKey}:${replacement.key.projection}`],
  )

  const currentRows = await queryRows<Record<string, unknown>>(
    client,
    `SELECT revision_token::text AS revision_token, revision_hash,
            embedding_profile_id, embedding_profile_version,
            embedding_dimensions
     FROM ${tables.revisions}
     WHERE document_key = $1 AND projection_id = $2
     FOR UPDATE`,
    [replacement.key.documentKey, replacement.key.projection],
  )
  const current = currentRows[0] === undefined
    ? undefined
    : parseRow(
        CurrentRevisionRowSchema,
        currentRows[0],
        "current revision",
      )

  const currentToken = current === undefined
    ? undefined
    : revisionToken(current.revision_token)
  const expectedMatches = Option.match(replacement.expectedToken, {
    onNone: () => currentToken === undefined,
    onSome: (expected) => expected === currentToken,
  })
  if (!expectedMatches) {
    throw new ProjectionIndexConflict({
      documentKey: replacement.key.documentKey,
      projection: replacement.key.projection,
    })
  }

  const currentProfile: EmbeddingProfile | undefined = current === undefined
    ? undefined
    : {
        id: current.embedding_profile_id,
        version: current.embedding_profile_version,
        dimensions: current.embedding_dimensions,
      }
  const storedChunks = await loadStoredChunkState(
    client,
    tables,
    replacement,
    currentProfile,
  )
  const plan = planProjectedRevisionReplacement(
    replacement,
    storedChunks.reusableVectors,
  )
  if (Either.isLeft(plan)) {
    throw invalidReplacement(plan.left)
  }
  const vectors = plan.right.vectors

  const previousIds = storedChunks.chunkIds
  const counts = countProjectedRevisionReplacement(
    previousIds,
    plan.right.chunkIds,
  )

  await client.query(
    `DELETE FROM ${tables.chunks}
     WHERE document_key = $1 AND projection_id = $2`,
    [replacement.key.documentKey, replacement.key.projection],
  )

  const tokenRows = await queryRows<Record<string, unknown>>(
    client,
    `INSERT INTO ${tables.revisions} AS current_revision
      (document_key, projection_id, graph_id, document_kind,
       encoded_document_id, projection_version, revision_hash,
       embedding_profile_id, embedding_profile_version,
       embedding_dimensions, revision_token, updated_at)
     VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, 1, now())
     ON CONFLICT (document_key, projection_id) DO UPDATE SET
       graph_id = EXCLUDED.graph_id,
       document_kind = EXCLUDED.document_kind,
       encoded_document_id = EXCLUDED.encoded_document_id,
       projection_version = EXCLUDED.projection_version,
       revision_hash = EXCLUDED.revision_hash,
       embedding_profile_id = EXCLUDED.embedding_profile_id,
       embedding_profile_version = EXCLUDED.embedding_profile_version,
       embedding_dimensions = EXCLUDED.embedding_dimensions,
       revision_token = current_revision.revision_token + 1,
       updated_at = now()
     RETURNING revision_token::text AS revision_token`,
    [
      replacement.key.documentKey,
      replacement.key.projection,
      replacement.encodedTarget.graph,
      replacement.encodedTarget.kind,
      encodeJson(replacement.encodedTarget.id),
      replacement.projectionVersion,
      replacement.revisionHash,
      replacement.embeddingProfile.id,
      replacement.embeddingProfile.version,
      replacement.embeddingProfile.dimensions,
    ],
  )
  const tokenRow = tokenRows[0]
  if (tokenRow === undefined) {
    throw invalidReplacement("PostgreSQL did not return the replacement token")
  }
  const parsedToken = parseRow(
    RevisionTokenRowSchema,
    tokenRow,
    "replacement token",
  )

  await insertChunks(client, tables, replacement, vectors)

  return {
    token: revisionToken(parsedToken.revision_token),
    ...counts,
  }
}

const parseDeletionCounts = (
  unknownRow: unknown,
): { readonly deletedRevisions: number; readonly deletedChunks: number } => {
  const row = parseRow(
    DeletionCountRowSchema,
    unknownRow,
    "projection deletion count",
  )
  return {
    deletedRevisions: Number(row.revision_count),
    deletedChunks: Number(row.chunk_count),
  }
}

const deleteRevisionInTransaction = async (
  client: TransactionClient,
  tables: { readonly revisions: string; readonly chunks: string },
  key: { readonly documentKey: string; readonly projection: string },
): Promise<{ readonly deletedRevisions: number; readonly deletedChunks: number }> => {
  await client.query(
    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
    [`${key.documentKey}:${key.projection}`],
  )
  const countRows = await queryRows<Record<string, unknown>>(
    client,
    `SELECT count(DISTINCT r.document_key)::text AS revision_count,
            count(c.chunk_id)::text AS chunk_count
     FROM ${tables.revisions} AS r
     LEFT JOIN ${tables.chunks} AS c
       ON c.document_key = r.document_key
      AND c.projection_id = r.projection_id
     WHERE r.document_key = $1 AND r.projection_id = $2`,
    [key.documentKey, key.projection],
  )
  const countRow = countRows[0]
  if (countRow === undefined) {
    throw new InvalidStoredState("PostgreSQL did not return deletion counts")
  }
  const counts = parseDeletionCounts(countRow)

  await client.query(
    `DELETE FROM ${tables.revisions}
     WHERE document_key = $1 AND projection_id = $2`,
    [key.documentKey, key.projection],
  )
  return counts
}

const staleGraphSql = (
  input: {
    readonly graph: string
    readonly registered: ReadonlyArray<{
      readonly documentKind: string
      readonly projection: string
    }>
  },
  values: Array<unknown>,
): string => {
  values.push(input.graph)
  const graph = `$${values.length}`
  if (input.registered.length === 0) {
    return `r.graph_id = ${graph}`
  }

  values.push(input.registered.map((target) => target.documentKind))
  const documentKinds = `$${values.length}`
  values.push(input.registered.map((target) => target.projection))
  const projections = `$${values.length}`
  return `r.graph_id = ${graph}
    AND NOT EXISTS (
      SELECT 1
      FROM unnest(${documentKinds}::text[], ${projections}::text[])
        AS registered(document_kind, projection_id)
      WHERE registered.document_kind = r.document_kind
        AND registered.projection_id = r.projection_id
    )`
}

const pruneGraphInTransaction = async (
  client: TransactionClient,
  tables: { readonly revisions: string; readonly chunks: string },
  input: {
    readonly graph: string
    readonly registered: ReadonlyArray<{
      readonly documentKind: string
      readonly projection: string
    }>
  },
): Promise<{ readonly deletedRevisions: number; readonly deletedChunks: number }> => {
  const values: Array<unknown> = []
  const stale = staleGraphSql(input, values)
  const countRows = await queryRows<Record<string, unknown>>(
    client,
    `SELECT count(DISTINCT (r.document_key, r.projection_id))::text
              AS revision_count,
            count(c.chunk_id)::text AS chunk_count
     FROM ${tables.revisions} AS r
     LEFT JOIN ${tables.chunks} AS c
       ON c.document_key = r.document_key
      AND c.projection_id = r.projection_id
     WHERE ${stale}`,
    values,
  )
  const countRow = countRows[0]
  if (countRow === undefined) {
    throw new InvalidStoredState("PostgreSQL did not return prune counts")
  }
  const counts = parseDeletionCounts(countRow)

  await client.query(
    `DELETE FROM ${tables.revisions} AS r WHERE ${stale}`,
    values,
  )
  return counts
}

const metadataSql = (
  filter: MetadataFilter,
  values: Array<unknown>,
): string => {
  if (filter._tag === "Not") {
    return `NOT (${metadataSql(filter.filter, values)})`
  }
  if (filter._tag === "All" || filter._tag === "Any") {
    const operator = filter._tag === "All" ? " AND " : " OR "
    return `(${filter.filters
      .map((child) => metadataSql(child, values))
      .join(operator)})`
  }

  values.push(filter.key)
  const key = `$${values.length}`
  const comparisons =
    filter._tag === "Equals" ? [filter.value] : filter.values

  const clauses = comparisons.map((value) => {
    values.push(encodeJson(value))
    return `c.metadata @> jsonb_build_object(${key}::text, $${values.length}::jsonb)`
  })
  return `c.has_metadata AND (${clauses.join(" OR ")})`
}

const appendScopeSql = (
  scope: GraphSearchScope,
  values: Array<unknown>,
  filters: Array<string>,
): void => {
  values.push(scope.graph)
  filters.push(`r.graph_id = $${values.length}`)

  if (scope.registered !== undefined) {
    if (scope.registered.length === 0) {
      filters.push("FALSE")
    } else {
      values.push(scope.registered.map((target) => target.documentKind))
      const documentKinds = `$${values.length}`
      values.push(scope.registered.map((target) => target.projection))
      const projections = `$${values.length}`
      filters.push(
        `EXISTS (
          SELECT 1
          FROM unnest(${documentKinds}::text[], ${projections}::text[])
            AS registered(document_kind, projection_id)
          WHERE registered.document_kind = r.document_kind
            AND registered.projection_id = r.projection_id
        )`,
      )
    }
  }

  const addTextArrayFilter = (
    column: string,
    items: ReadonlyArray<string>,
    include: boolean,
  ): void => {
    if (items.length === 0) return
    values.push([...items])
    const comparison = `${column} = ANY($${values.length}::text[])`
    filters.push(include ? comparison : `NOT (${comparison})`)
  }

  addTextArrayFilter("r.document_kind", scope.includeDocumentKinds, true)
  addTextArrayFilter("r.document_kind", scope.excludeDocumentKinds, false)
  addTextArrayFilter("r.projection_id", scope.includeProjections, true)
  addTextArrayFilter("r.projection_id", scope.excludeProjections, false)
  for (const filter of scope.where) {
    filters.push(metadataSql(filter, values))
  }
}

const projectSearchCandidate = (
  unknownRow: unknown,
  rowKind: "semantic candidate" | "text candidate",
): SemanticSearchCandidate => {
  const row = parseRow(SearchCandidateRowSchema, unknownRow, rowKind)
  return {
    score: row.score,
    chunkId: row.chunk_id,
    documentKey: row.document_key,
    reference: {
      graph: row.graph_id,
      kind: row.document_kind,
      id: row.encoded_document_id,
    },
    projection: {
      id: row.projection_id,
      version: row.projection_version,
    },
    revisionHash: row.revision_hash,
    sectionKey: row.section_key,
    sectionPart: row.section_part,
    content: row.content,
    metadata: row.has_metadata ? row.metadata : undefined,
  }
}

const searchCandidates = async (
  connection: Queryable,
  tables: { readonly revisions: string; readonly chunks: string },
  request: SemanticCandidateRequest,
): Promise<ReadonlyArray<SemanticSearchCandidate>> => {
  const values: Array<unknown> = [
    [...request.vector],
    request.embeddingProfile.id,
    request.embeddingProfile.version,
    request.embeddingProfile.dimensions,
  ]
  const filters = [
    "r.embedding_profile_id = $2",
    "r.embedding_profile_version = $3",
    "r.embedding_dimensions = $4",
  ]
  appendScopeSql(request.scope, values, filters)

  values.push(request.candidates)
  const rows = await queryRows<Record<string, unknown>>(
    connection,
    `WITH scoped AS MATERIALIZED (
       SELECT c.chunk_id, c.document_key, c.section_key, c.section_part,
              c.content,
              c.has_metadata, c.metadata, c.embedding,
              r.graph_id, r.document_kind, r.encoded_document_id,
              r.projection_id, r.projection_version, r.revision_hash
       FROM ${tables.chunks} AS c
       INNER JOIN ${tables.revisions} AS r
         ON r.document_key = c.document_key
        AND r.projection_id = c.projection_id
       WHERE ${filters.join("\n         AND ")}
     )
     SELECT similarity.score, scoped.chunk_id, scoped.document_key,
            scoped.graph_id, scoped.document_kind,
            scoped.encoded_document_id, scoped.projection_id,
            scoped.projection_version, scoped.revision_hash,
            scoped.section_key, scoped.section_part, scoped.content,
            scoped.has_metadata, scoped.metadata
     FROM scoped
     CROSS JOIN LATERAL (
       SELECT sum(stored.value * query.value) /
              NULLIF(
                sqrt(sum(stored.value * stored.value)) *
                sqrt(sum(query.value * query.value)),
                0
              ) AS score
       FROM unnest(scoped.embedding) WITH ORDINALITY AS stored(value, ordinal)
       INNER JOIN unnest($1::double precision[]) WITH ORDINALITY AS query(value, ordinal)
         USING (ordinal)
     ) AS similarity
     WHERE similarity.score IS NOT NULL
     ORDER BY similarity.score DESC, scoped.chunk_id ASC
     LIMIT $${values.length}`,
    values,
  )

  return rows.map((row) =>
    projectSearchCandidate(row, "semantic candidate"),
  )
}

const searchTextCandidates = async (
  connection: Queryable,
  tables: { readonly revisions: string; readonly chunks: string },
  request: TextCandidateRequest,
): Promise<ReadonlyArray<TextSearchCandidate>> => {
  const config = request.policy.language
  const searchColumn =
    config === "english" ? "text_search_english" : "text_search_simple"
  const values: Array<unknown> = [request.query]
  const filters: Array<string> = []
  appendScopeSql(request.scope, values, filters)

  values.push(request.policy.weights.context)
  const contextWeight = `$${values.length}`
  values.push(request.policy.weights.label)
  const labelWeight = `$${values.length}`
  values.push(request.policy.weights.content)
  const contentWeight = `$${values.length}`
  values.push(request.candidates)
  const candidateLimit = `$${values.length}`

  const rows = await queryRows<Record<string, unknown>>(
    connection,
    `WITH parsed AS (
       SELECT websearch_to_tsquery('${config}'::regconfig, $1) AS query
     ),
     scoped AS MATERIALIZED (
       SELECT c.chunk_id, c.document_key, c.section_key, c.section_part,
              c.content,
              c.has_metadata, c.metadata,
              r.graph_id, r.document_kind, r.encoded_document_id,
              r.projection_id, r.projection_version, r.revision_hash,
              (
                ${contextWeight} * ts_rank_cd(
                  to_tsvector('${config}'::regconfig, coalesce(c.text_context, '')),
                  parsed.query
                ) +
                ${labelWeight} * ts_rank_cd(
                  to_tsvector('${config}'::regconfig, coalesce(c.text_label, '')),
                  parsed.query
                ) +
                ${contentWeight} * ts_rank_cd(
                  to_tsvector('${config}'::regconfig, c.text_content),
                  parsed.query
                )
              ) AS score
       FROM ${tables.chunks} AS c
       INNER JOIN ${tables.revisions} AS r
         ON r.document_key = c.document_key
        AND r.projection_id = c.projection_id
       CROSS JOIN parsed
       WHERE parsed.query <> ''::tsquery
         AND c.${searchColumn} @@ parsed.query
         AND ${filters.join("\n         AND ")}
     )
     SELECT score, chunk_id, document_key, graph_id, document_kind,
            encoded_document_id, projection_id, projection_version,
            revision_hash, section_key, section_part, content,
            has_metadata, metadata
     FROM scoped
     WHERE score > 0
     ORDER BY score DESC, chunk_id ASC
     LIMIT ${candidateLimit}`,
    values,
  )

  return rows.map((row) =>
    projectSearchCandidate(row, "text candidate"),
  )
}

const replaceOutgoingRelationsInTransaction = async (
  client: TransactionClient,
  table: string,
  replacement: ReplaceOutgoingGraphRelations,
): Promise<GraphRelationCommit> => {
  const plan = planOutgoingGraphRelationReplacement(replacement)
  if (Either.isLeft(plan)) {
    throw new InvalidStoredState(plan.left)
  }

  await client.query(
    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
    [`relations:${replacement.graph}:${replacement.sourceDocumentKey}`],
  )
  const previousRows = await queryRows<Record<string, unknown>>(
    client,
    `SELECT relation_id, target_document_key
     FROM ${table}
     WHERE graph_id = $1 AND source_document_key = $2`,
    [replacement.graph, replacement.sourceDocumentKey],
  )
  const previous = new Set(
    previousRows.map((unknownRow) => {
      const row = parseRow(
        GraphEdgeIdentityRowSchema,
        unknownRow,
        "graph edge identity",
      )
      return makeGraphRelationEdgeIdentity({
        relation: row.relation_id,
        targetDocumentKey: row.target_document_key,
      })
    }),
  )

  const next = plan.right.identities
  const rows = plan.right.edges

  await client.query(
    `DELETE FROM ${table}
     WHERE graph_id = $1 AND source_document_key = $2`,
    [replacement.graph, replacement.sourceDocumentKey],
  )

  for (let offset = 0; offset < rows.length; offset += InsertBatchSize) {
    const batch = rows.slice(offset, offset + InsertBatchSize)
    const values: Array<unknown> = []
    const valueRows = batch.map((row) => {
      const start = values.length
      values.push(
        replacement.graph,
        row.relation,
        row.version,
        replacement.sourceDocumentKey,
        replacement.source.kind,
        encodeJson(replacement.source.id),
        row.target.documentKey,
        row.targetDocumentKind,
        encodeJson(row.target.reference.id),
      )
      const parameter = (index: number): string => `$${start + index}`
      return `(${parameter(1)}, ${parameter(2)}, ${parameter(3)},
        ${parameter(4)}, ${parameter(5)}, ${parameter(6)}::jsonb,
        ${parameter(7)}, ${parameter(8)}, ${parameter(9)}::jsonb)`
    })
    await client.query(
      `INSERT INTO ${table}
        (graph_id, relation_id, relation_version, source_document_key,
         source_document_kind, encoded_source_document_id,
         target_document_key, target_document_kind,
         encoded_target_document_id)
       VALUES ${valueRows.join(",\n")}`,
      values,
    )
  }

  return countGraphRelationReplacement(previous, next)
}

const parseDeletedGraphEdges = (unknownRow: unknown): number => {
  const row = parseRow(
    DeletedGraphEdgeCountRowSchema,
    unknownRow,
    "graph edge deletion count",
  )
  return Number(row.deleted_count)
}

const deleteNodeRelationsInTransaction = async (
  client: TransactionClient,
  table: string,
  input: { readonly graph: string; readonly documentKey: string },
): Promise<GraphRelationDeletion> => {
  const rows = await queryRows<Record<string, unknown>>(
    client,
    `WITH deleted AS (
       DELETE FROM ${table}
       WHERE graph_id = $1
         AND (source_document_key = $2 OR target_document_key = $2)
       RETURNING 1
     )
     SELECT count(*)::text AS deleted_count FROM deleted`,
    [input.graph, input.documentKey],
  )
  const row = rows[0]
  if (row === undefined) {
    throw new InvalidStoredState("PostgreSQL did not return an edge count")
  }
  return { deleted: parseDeletedGraphEdges(row) }
}

const pruneRelationsInTransaction = async (
  client: TransactionClient,
  table: string,
  input: PruneGraphRelations,
): Promise<GraphRelationPrune> => {
  const values: Array<unknown> = [input.graph]
  let stale = "edge.graph_id = $1"
  if (input.registered.length > 0) {
    values.push(input.registered.map((relation) => relation.id))
    const ids = `$${values.length}`
    values.push(input.registered.map((relation) => relation.version))
    const versions = `$${values.length}`
    values.push(
      input.registered.map((relation) => relation.sourceDocumentKind),
    )
    const sources = `$${values.length}`
    values.push(
      input.registered.map((relation) => relation.targetDocumentKind),
    )
    const targets = `$${values.length}`
    stale += ` AND NOT EXISTS (
      SELECT 1
      FROM unnest(
        ${ids}::text[], ${versions}::text[],
        ${sources}::text[], ${targets}::text[]
      ) AS active(relation_id, relation_version, source_kind, target_kind)
      WHERE active.relation_id = edge.relation_id
        AND active.relation_version = edge.relation_version
        AND active.source_kind = edge.source_document_kind
        AND active.target_kind = edge.target_document_kind
    )`
  }

  const rows = await queryRows<Record<string, unknown>>(
    client,
    `WITH deleted AS (
       DELETE FROM ${table} AS edge WHERE ${stale}
       RETURNING 1
     )
     SELECT count(*)::text AS deleted_count FROM deleted`,
    values,
  )
  const row = rows[0]
  if (row === undefined) {
    throw new InvalidStoredState("PostgreSQL did not return a prune count")
  }
  return { deleted: parseDeletedGraphEdges(row) }
}

const findGraphNeighbours = async (
  connection: Queryable,
  table: string,
  input: {
    readonly direction: "outgoing" | "incoming"
    readonly graph: string
    readonly currentDocumentKey: string
    readonly currentDocumentKind: string
    readonly relation: string
    readonly relationVersion: string
    readonly neighbourDocumentKind: string
    readonly limit: number
  },
): Promise<ReadonlyArray<StoredGraphNeighbour>> => {
  const current = input.direction === "outgoing" ? "source" : "target"
  const neighbour = input.direction === "outgoing" ? "target" : "source"
  const rows = await queryRows<Record<string, unknown>>(
    connection,
    `SELECT ${neighbour}_document_key AS document_key, graph_id,
            ${neighbour}_document_kind AS document_kind,
            encoded_${neighbour}_document_id AS encoded_document_id
     FROM ${table}
     WHERE graph_id = $1
       AND ${current}_document_key = $2
       AND ${current}_document_kind = $3
       AND relation_id = $4
       AND relation_version = $5
       AND ${neighbour}_document_kind = $6
     ORDER BY ${neighbour}_document_key
     LIMIT $7`,
    [
      input.graph,
      input.currentDocumentKey,
      input.currentDocumentKind,
      input.relation,
      input.relationVersion,
      input.neighbourDocumentKind,
      input.limit,
    ],
  )

  return rows.map((unknownRow) => {
    const row = parseRow(
      GraphNeighbourRowSchema,
      unknownRow,
      `${input.direction} graph neighbour`,
    )
    return {
      documentKey: row.document_key,
      reference: {
        graph: row.graph_id,
        kind: row.document_kind,
        id: row.encoded_document_id,
      },
    }
  })
}

const makePostgresStorage = (
  config: PostgresDocumentGraphConfig,
): DocumentGraphStorageService => {
  const schema = Schema.decodeSync(PostgresSchemaNameSchema)(
    config.schema ?? DefaultSchema,
  )
  const namespace = quoteIdentifier(schema)
  const tables = {
    revisions: `${namespace}."projected_revisions"`,
    chunks: `${namespace}."projected_chunks"`,
    relations: `${namespace}."graph_relations"`,
  }

  return {
    loadRevision: (key) =>
      Effect.tryPromise({
        try: async () => {
          const rows = await queryRows<Record<string, unknown>>(
            connectionFor(config),
            `SELECT r.revision_token::text AS revision_token,
                    r.revision_hash, r.embedding_profile_id,
                    r.embedding_profile_version, r.embedding_dimensions,
                    c.chunk_id, c.content_hash, c.ordinal
             FROM ${tables.revisions} AS r
             LEFT JOIN ${tables.chunks} AS c
               ON c.document_key = r.document_key
              AND c.projection_id = r.projection_id
             WHERE r.document_key = $1 AND r.projection_id = $2
             ORDER BY c.ordinal`,
            [key.documentKey, key.projection],
          )
          if (rows.length === 0) return Option.none()

          const parsed = rows.map((row) =>
            parseRow(
              RevisionWithChunkRowSchema,
              row,
              "projected revision",
            ),
          )
          const first = parsed[0]
          if (
            first === undefined ||
            first.chunk_id === null ||
            first.content_hash === null ||
            parsed.some(
              (row) =>
                row.revision_token !== first.revision_token ||
                row.revision_hash !== first.revision_hash ||
                row.embedding_profile_id !== first.embedding_profile_id ||
                row.embedding_profile_version !== first.embedding_profile_version ||
                row.embedding_dimensions !== first.embedding_dimensions ||
                row.chunk_id === null ||
                row.content_hash === null,
            )
          ) {
            throw new InvalidStoredState("A projected revision is incomplete")
          }

          const toChunkSummary = (
            row: typeof first,
          ): IndexedRevisionSnapshot["chunks"][number] => {
            if (row.chunk_id === null || row.content_hash === null) {
              throw new InvalidStoredState("A projected revision has an incomplete chunk")
            }
            return {
              chunkId: row.chunk_id,
              contentHash: row.content_hash,
            }
          }
          const chunks: IndexedRevisionSnapshot["chunks"] = [
            toChunkSummary(first),
            ...parsed.slice(1).map(toChunkSummary),
          ]

          return Option.some({
            token: revisionToken(first.revision_token),
            revisionHash: first.revision_hash,
            embeddingProfile: {
              id: first.embedding_profile_id,
              version: first.embedding_profile_version,
              dimensions: first.embedding_dimensions,
            },
            chunks,
          })
        },
        catch: (cause) => indexFailure("load_revision", cause),
      }),

    replaceRevision: (replacement) =>
      transactionEffect(
        config,
        (client) => replaceInTransaction(client, tables, replacement),
        (cause) =>
          cause instanceof ProjectionIndexConflict
            ? cause
            : indexFailure("replace_revision", cause),
      ),

    deleteRevision: (key) =>
      transactionEffect(
        config,
        (client) => deleteRevisionInTransaction(client, tables, key),
        (cause) => indexFailure("delete_revision", cause),
      ),

    pruneGraph: (input) =>
      transactionEffect(
        config,
        (client) => pruneGraphInTransaction(client, tables, input),
        (cause) => indexFailure("prune_graph", cause),
      ),

    searchCandidates: (request) =>
      Effect.tryPromise({
        try: () =>
          searchCandidates(connectionFor(config), tables, request),
        catch: searchFailure,
      }),

    searchTextCandidates: (request) =>
      Effect.tryPromise({
        try: () =>
          searchTextCandidates(connectionFor(config), tables, request),
        catch: textSearchFailure,
      }),

    replaceOutgoing: (replacement) =>
      transactionEffect(
        config,
        (client) =>
          replaceOutgoingRelationsInTransaction(
            client,
            tables.relations,
            replacement,
          ),
        (cause) => relationFailure("replace_outgoing", cause),
      ),

    deleteNode: (input) =>
      transactionEffect(
        config,
        (client) =>
          deleteNodeRelationsInTransaction(
            client,
            tables.relations,
            input,
          ),
        (cause) => relationFailure("delete_node", cause),
      ),

    pruneRelations: (input) =>
      transactionEffect(
        config,
        (client) =>
          pruneRelationsInTransaction(client, tables.relations, input),
        (cause) => relationFailure("prune_graph", cause),
      ),

    findOutgoing: (input) =>
      Effect.tryPromise({
        try: () =>
          findGraphNeighbours(
            connectionFor(config),
            tables.relations,
            {
              direction: "outgoing",
              graph: input.graph,
              currentDocumentKey: input.sourceDocumentKey,
              currentDocumentKind: input.sourceDocumentKind,
              relation: input.relation,
              relationVersion: input.relationVersion,
              neighbourDocumentKind: input.targetDocumentKind,
              limit: input.limit,
            },
          ),
        catch: (cause) => relationFailure("find_outgoing", cause),
      }),

    findIncoming: (input) =>
      Effect.tryPromise({
        try: () =>
          findGraphNeighbours(
            connectionFor(config),
            tables.relations,
            {
              direction: "incoming",
              graph: input.graph,
              currentDocumentKey: input.targetDocumentKey,
              currentDocumentKind: input.targetDocumentKind,
              relation: input.relation,
              relationVersion: input.relationVersion,
              neighbourDocumentKind: input.sourceDocumentKind,
              limit: input.limit,
            },
          ),
        catch: (cause) => relationFailure("find_incoming", cause),
      }),
  }
}

/**
 * Provide durable indexing plus semantic and full-text retrieval through PostgreSQL.
 *
 * The caller owns the Pool or active transaction and remains responsible for
 * its lifecycle. Pool mode owns each operation's transaction; transaction
 * mode uses a savepoint and never commits the caller's transaction. The
 * adapter supports arbitrary embedding dimensions without pgvector.
 */
export const postgresDocumentGraph = (
  config: PostgresDocumentGraphConfig,
): ReturnType<typeof makeDocumentGraphStorage> =>
  makeDocumentGraphStorage(makePostgresStorage(config))
