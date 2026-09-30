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
  type DocumentKey,
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
  type ProjectionRevisionLookup,
  type ProjectionIndexCommit,
  type PruneGraphIndex,
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
  makeGraphRelationEdgeIdentity,
  planOutgoingGraphRelationReplacement,
  type GraphRelationCommit,
  type ReplaceOutgoingGraphRelations,
} from "../../graph/graph-relation.js"
import {
  GraphNodeStateSchema,
  GraphTopologyStoreFailed,
  type FindRelatedGraphNodes,
  type RelatedGraphNodeSet,
  type GraphNodePage,
  type GraphTopologyDeletion,
  type GraphTopologyPrune,
  type ListGraphNodes,
  type PruneGraphTopology,
  type StoredGraphNode,
} from "../../graph/graph-topology.js"
import { Cache, Effect, Exit, Layer, Option, Predicate, Result, Schema, SchemaIssue } from "effect"
import {
  connectionFor,
  queryRows,
  transactionEffect,
  withReadSettings,
  type PostgresDocumentGraphConfig,
  type PostgresQueryable as Queryable,
  type PostgresReadSetting,
  type PostgresTransactionClient as TransactionClient,
} from "./connection.js"
import {
  approximateIndexReadySql,
  indexedEmbeddingSql,
  indexedRowsSql,
  queryVectorFitsIndex,
  resolveVectorSearch,
  type ApproximateVectorIndex,
} from "./vector-index.js"

const DefaultSchema = "honertia_document_graph"

const InsertBatchSize = 250

const PostgresSchemaNameSchema = Schema.Trimmed.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(63),
  Schema.isPattern(/^[a-z_][a-z0-9_]*$/i),
)

interface StoredRowParseIssue {
  readonly message: string
  readonly path: string
}

class InvalidStoredState extends Error {
  override readonly name = "InvalidStoredState"

  constructor(
    message: string,
    readonly rowKind?: string,
    readonly issues: ReadonlyArray<StoredRowParseIssue> = [],
  ) {
    super(message)
  }
}

const RevisionWithChunkRowSchema = Schema.Struct({
  revision_token: Schema.String.check(Schema.isPattern(/^[1-9][0-9]*$/)),
  revision_hash: ProjectionRevisionHashSchema,
  embedding_profile_id: EmbeddingProfileIdSchema,
  embedding_profile_version: EmbeddingProfileVersionSchema,
  embedding_dimensions: EmbeddingDimensionsSchema,
  chunk_id: Schema.NullOr(ChunkIdSchema),
  content_hash: Schema.NullOr(ContentHashSchema),
  ordinal: Schema.NullOr(
    Schema.Number.check(
      Schema.isInt(),
      Schema.isGreaterThanOrEqualTo(0),
    ),
  ),
})

const RequestedRevisionWithChunkRowSchema = Schema.Struct({
  request_ordinal: Schema.Number.check(
    Schema.isInt(),
    Schema.isGreaterThanOrEqualTo(1),
  ),
  revision_token: Schema.String.check(Schema.isPattern(/^[1-9][0-9]*$/)),
  revision_hash: ProjectionRevisionHashSchema,
  embedding_profile_id: EmbeddingProfileIdSchema,
  embedding_profile_version: EmbeddingProfileVersionSchema,
  embedding_dimensions: EmbeddingDimensionsSchema,
  chunk_id: Schema.NullOr(ChunkIdSchema),
  content_hash: Schema.NullOr(ContentHashSchema),
  ordinal: Schema.NullOr(
    Schema.Number.check(
      Schema.isInt(),
      Schema.isGreaterThanOrEqualTo(0),
    ),
  ),
})

const ReusableChunkRowSchema = Schema.Struct({
  chunk_id: ChunkIdSchema,
  content_hash: ContentHashSchema,
  embedding: Schema.Array(Schema.Number),
})

const RevisionTokenRowSchema = Schema.Struct({
  revision_token: Schema.String.check(Schema.isPattern(/^[1-9][0-9]*$/)),
})

const CurrentRevisionRowSchema = Schema.Struct({
  revision_token: Schema.String.check(Schema.isPattern(/^[1-9][0-9]*$/)),
  revision_hash: ProjectionRevisionHashSchema,
  embedding_profile_id: EmbeddingProfileIdSchema,
  embedding_profile_version: EmbeddingProfileVersionSchema,
  embedding_dimensions: EmbeddingDimensionsSchema,
})

const DeletionCountRowSchema = Schema.Struct({
  revision_count: Schema.String.check(Schema.isPattern(/^[0-9]+$/)),
  chunk_count: Schema.String.check(Schema.isPattern(/^[0-9]+$/)),
})

const SearchCandidateRowSchema = Schema.Struct({
  score: Schema.Finite,
  chunk_id: ChunkIdSchema,
  document_key: DocumentKeySchema,
  graph_id: Schema.Trimmed.check(Schema.isNonEmpty()),
  document_kind: Schema.Trimmed.check(Schema.isNonEmpty()),
  encoded_document_id: JsonValueSchema,
  projection_id: Schema.Trimmed.check(Schema.isNonEmpty()),
  projection_version: Schema.Trimmed.check(Schema.isNonEmpty()),
  revision_hash: ProjectionRevisionHashSchema,
  section_key: Schema.Trimmed.check(Schema.isNonEmpty()),
  section_part: Schema.Number.check(
    Schema.isInt(),
    Schema.isGreaterThanOrEqualTo(0),
  ),
  content: Schema.Trimmed.check(Schema.isNonEmpty()),
  has_metadata: Schema.Boolean,
  metadata: Schema.NullOr(JsonValueSchema),
})

const GraphEdgeIdentityRowSchema = Schema.Struct({
  relation_id: Schema.String,
  target_document_key: DocumentKeySchema,
})

const GraphNodeRowSchema = Schema.Struct({
  document_key: DocumentKeySchema,
  graph_id: Schema.String,
  document_kind: Schema.String,
  encoded_document_id: JsonValueSchema,
  node_state: GraphNodeStateSchema,
})

const GraphNodeKeyRowSchema = Schema.Struct({
  document_key: DocumentKeySchema,
})

const DeletedGraphEdgeCountRowSchema = Schema.Struct({
  deleted_count: Schema.String.check(Schema.isPattern(/^[0-9]+$/)),
})

type RequestedRevisionWithChunkRow = Schema.Codec.Encoded<
  typeof RequestedRevisionWithChunkRowSchema
>

type ReusableChunkRow = Schema.Codec.Encoded<typeof ReusableChunkRowSchema>

type RevisionTokenRow = Schema.Codec.Encoded<typeof RevisionTokenRowSchema>

type CurrentRevisionRow = Schema.Codec.Encoded<typeof CurrentRevisionRowSchema>

type DeletionCountRow = Schema.Codec.Encoded<typeof DeletionCountRowSchema>

type SearchCandidateRow = Schema.Codec.Encoded<typeof SearchCandidateRowSchema>

type GraphEdgeIdentityRow = Schema.Codec.Encoded<
  typeof GraphEdgeIdentityRowSchema
>

type GraphNodeRow = Schema.Codec.Encoded<typeof GraphNodeRowSchema>

type DeletedGraphEdgeCountRow = Schema.Codec.Encoded<
  typeof DeletedGraphEdgeCountRowSchema
>

interface DeletionCounts {
  readonly deletedRevisions: number
  readonly deletedChunks: number
}

const quoteIdentifier = (identifier: string): string => `"${identifier.replaceAll('"', '""')}"`

interface PostgresTables {
  readonly revisions: string
  readonly chunks: string
  readonly relations: string
  readonly nodes: string
  readonly locks: string
}

const parseRow = <S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  row: Schema.Codec.Encoded<S>,
  rowKind: string,
): S["Type"] => {
  try {
    return Schema.decodeSync(schema)(row, {
      onExcessProperty: "error",
    })
  } catch (cause: unknown) {
    const issues = Schema.isSchemaError(cause)
      ? SchemaIssue.makeFormatterStandardSchemaV1()(cause.issue).issues
          .slice(0, 8)
          .map((issue): StoredRowParseIssue => ({
            message: issue.message,
            path: (issue.path ?? [])
              .map((segment) => {
                const key = Schema.is(Schema.PropertyKey)(segment)
                  ? segment
                  : segment.key

                return Schema.is(Schema.Symbol)(key)
                  ? key.description ?? "symbol"
                  : String(key)
              })
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
    | "load_revisions"
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

const indexedRevisionSnapshot = (
  rows: ReadonlyArray<typeof RevisionWithChunkRowSchema.Type>,
): IndexedRevisionSnapshot => {
  const first = rows[0]

  if (
    first === undefined ||
    first.chunk_id === null ||
    first.content_hash === null ||
    rows.some(
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
    row: typeof RevisionWithChunkRowSchema.Type,
  ): IndexedRevisionSnapshot["chunks"][number] => {
    if (row.chunk_id === null || row.content_hash === null) {
      throw new InvalidStoredState("A projected revision has an incomplete chunk")
    }

    return {
      chunkId: row.chunk_id,
      contentHash: row.content_hash,
    }
  }

  return {
    token: revisionToken(first.revision_token),
    revisionHash: first.revision_hash,
    embeddingProfile: {
      id: first.embedding_profile_id,
      version: first.embedding_profile_version,
      dimensions: first.embedding_dimensions,
    },
    chunks: [
      toChunkSummary(first),
      ...rows.slice(1).map(toChunkSummary),
    ],
  }
}

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

const topologyFailure = (
  operation: GraphTopologyStoreFailed["operation"],
  cause: unknown,
): GraphTopologyStoreFailed =>
  new GraphTopologyStoreFailed({
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

  const rows = await queryRows<ReusableChunkRow>(
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

const lockMutationInTransaction = async (
  client: TransactionClient,
  locksTable: string,
  kind: "projection" | "relations",
  scopeKey: string,
  memberKey: string,
): Promise<void> => {
  await client.query(
    `INSERT INTO ${locksTable} (mutation_kind, scope_key, member_key)
     VALUES ($1, $2, $3)
     ON CONFLICT (mutation_kind, scope_key, member_key) DO NOTHING`,
    [kind, scopeKey, memberKey],
  )
  await client.query(
    `SELECT member_key FROM ${locksTable}
     WHERE mutation_kind = $1 AND scope_key = $2 AND member_key = $3
     FOR UPDATE`,
    [kind, scopeKey, memberKey],
  )
}

const replaceInTransaction = async (
  client: TransactionClient,
  tables: {
    readonly revisions: string
    readonly chunks: string
    readonly locks: string
  },
  replacement: ReplaceProjectedRevision,
): Promise<ProjectionIndexCommit> => {
  await lockMutationInTransaction(
    client,
    tables.locks,
    "projection",
    replacement.key.documentKey,
    replacement.key.projection,
  )

  const currentRows = await queryRows<CurrentRevisionRow>(
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

  if (Result.isFailure(plan)) {
    throw invalidReplacement(plan.failure)
  }

  const vectors = plan.success.vectors

  const previousIds = storedChunks.chunkIds

  const counts = countProjectedRevisionReplacement(
    previousIds,
    plan.success.chunkIds,
  )

  await client.query(
    `DELETE FROM ${tables.chunks}
     WHERE document_key = $1 AND projection_id = $2`,
    [replacement.key.documentKey, replacement.key.projection],
  )

  const tokenRows = await queryRows<RevisionTokenRow>(
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
  rowInput: DeletionCountRow,
): DeletionCounts => {
  const row = parseRow(
    DeletionCountRowSchema,
    rowInput,
    "projection deletion count",
  )

  return {
    deletedRevisions: Number(row.revision_count),
    deletedChunks: Number(row.chunk_count),
  }
}

const deleteRevisionInTransaction = async (
  client: TransactionClient,
  tables: {
    readonly revisions: string
    readonly chunks: string
    readonly locks: string
  },
  key: { readonly documentKey: string; readonly projection: string },
): Promise<{ readonly deletedRevisions: number; readonly deletedChunks: number }> => {
  await lockMutationInTransaction(
    client,
    tables.locks,
    "projection",
    key.documentKey,
    key.projection,
  )

  const countRows = await queryRows<DeletionCountRow>(
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
  input: PruneGraphIndex,
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
  values.push(input.registered.map((target) => target.projectionVersion ?? null))
  const versions = `$${values.length}`

  return `r.graph_id = ${graph}
    AND NOT EXISTS (
      SELECT 1
      FROM unnest(${documentKinds}::text[], ${projections}::text[], ${versions}::text[])
        AS registered(document_kind, projection_id, projection_version)
      WHERE registered.document_kind = r.document_kind
        AND registered.projection_id = r.projection_id
        AND (registered.projection_version IS NULL
          OR registered.projection_version = r.projection_version)
    )`
}

const pruneGraphInTransaction = async (
  client: TransactionClient,
  tables: { readonly revisions: string; readonly chunks: string },
  input: PruneGraphIndex,
): Promise<{ readonly deletedRevisions: number; readonly deletedChunks: number }> => {
  const values: Array<unknown> = []
  const stale = staleGraphSql(input, values)

  const countRows = await queryRows<DeletionCountRow>(
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

  switch (scope.target._tag) {
    case "AllDocuments":
      break
    case "NoDocuments":
      filters.push("FALSE")
      break
    case "DocumentKeys":
      values.push([...scope.target.documentKeys])
      filters.push(`r.document_key = ANY($${values.length}::char(64)[])`)
      break
  }

  if (scope.registered !== undefined) {
    if (scope.registered.length === 0) {
      filters.push("FALSE")
    } else {
      values.push(scope.registered.map((target) => target.documentKind))
      const documentKinds = `$${values.length}`
      values.push(scope.registered.map((target) => target.projection))
      const projections = `$${values.length}`
      values.push(scope.registered.map((target) => target.projectionVersion ?? null))
      const versions = `$${values.length}`
      filters.push(
        `EXISTS (
          SELECT 1
          FROM unnest(
            ${documentKinds}::text[], ${projections}::text[],
            ${versions}::text[]
          ) AS registered(document_kind, projection_id, projection_version)
          WHERE registered.document_kind = r.document_kind
            AND registered.projection_id = r.projection_id
            AND (registered.projection_version IS NULL
              OR registered.projection_version = r.projection_version)
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
  rowInput: SearchCandidateRow,
  rowKind: "semantic candidate" | "text candidate",
): SemanticSearchCandidate => {
  const row = parseRow(SearchCandidateRowSchema, rowInput, rowKind)

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

// pgvector accumulates products in float32. These conservative bounds keep
// squared norms finite and nonzero, including at its 16,000-dimension limit.
// The same bounds are persisted for stored vectors by migration 0004.
const nativeQueryEligible = (vector: ReadonlyArray<number>): boolean => {
  if (vector.length === 0 || vector.length > 16_000) return false
  let maximum = 0

  for (const component of vector) {
    if (!Number.isFinite(component)) return false
    maximum = Math.max(maximum, Math.abs(component))
  }

  return maximum >= 1e-18 && maximum <= 1e15
}

const PgvectorNamespaceRowSchema = Schema.Struct({ namespace: Schema.String })

const discoverPgvectorNamespace = (connection: Queryable) => Effect.tryPromise({
  try: async () => {
    const rows = await queryRows<Schema.Codec.Encoded<typeof PgvectorNamespaceRowSchema>>(
      connection,
      `SELECT namespace.nspname AS namespace
       FROM pg_catalog.pg_extension AS extension
       INNER JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = extension.extnamespace
       INNER JOIN pg_catalog.pg_type AS vector_type
         ON vector_type.typnamespace = namespace.oid AND vector_type.typname = 'vector'
       INNER JOIN pg_catalog.pg_proc AS cosine
         ON cosine.pronamespace = namespace.oid AND cosine.proname = 'cosine_distance'
         AND cosine.pronargs = 2
         AND cosine.proargtypes[0] = vector_type.oid AND cosine.proargtypes[1] = vector_type.oid
       INNER JOIN pg_catalog.pg_cast AS vector_cast
         ON vector_cast.casttarget = vector_type.oid
         AND vector_cast.castsource = 'pg_catalog.float8[]'::pg_catalog.regtype
       WHERE extension.extname = 'vector'
         AND pg_catalog.has_schema_privilege(namespace.oid, 'USAGE')
         AND pg_catalog.has_type_privilege(vector_type.oid, 'USAGE')
         AND pg_catalog.has_function_privilege(cosine.oid, 'EXECUTE')
         AND pg_catalog.has_function_privilege(vector_cast.castfunc, 'EXECUTE')`,
    )

    if (rows.length > 1) throw new InvalidStoredState("Multiple pgvector extensions returned")
    const row = rows[0]

    return row === undefined ? Option.none<string>() : Option.some(
      quoteIdentifier(parseRow(PgvectorNamespaceRowSchema, row, "pgvector namespace").namespace),
    )
  },
  catch: searchFailure,
})

interface VectorCapabilities {
  readonly nativeNamespace: Option.Option<string>
  /** The configured approximate index exists, is HNSW, and is valid and maintained. */
  readonly approximateIndexReady: boolean
}

const noVectorCapabilities: VectorCapabilities = { nativeNamespace: Option.none(), approximateIndexReady: false }

const IndexReadyRowSchema = Schema.Struct({ ready: Schema.Boolean })

// A failed or in-progress CREATE INDEX CONCURRENTLY leaves an invalid index,
// so readiness is checked rather than assumed from configuration.
const discoverVectorCapabilities = (
  connection: Queryable,
  schema: string,
  approximate: ApproximateVectorIndex | undefined,
) => discoverPgvectorNamespace(connection).pipe(Effect.flatMap((nativeNamespace) =>
  approximate === undefined || Option.isNone(nativeNamespace)
    ? Effect.succeed({ nativeNamespace, approximateIndexReady: false })
    : Effect.tryPromise({
        try: async () => {
          const rows = await queryRows<Schema.Codec.Encoded<typeof IndexReadyRowSchema>>(
            connection, approximateIndexReadySql, [schema, approximate.index],
          )

          const row = rows[0]

          return {
            nativeNamespace,
            approximateIndexReady: row !== undefined && parseRow(IndexReadyRowSchema, row, "vector index readiness").ready,
          }
        },
        catch: searchFailure,
      })))

/**
 * Exhaustive plans score every chunk in scope. Approximate plans take the
 * nearest chunks from the HNSW index, applying scope during the index scan,
 * then rescore those exactly in float64.
 */
type SemanticSearchPlan =
  | { readonly _tag: "Exhaustive"; readonly nativeNamespace: Option.Option<string> }
  | {
      readonly _tag: "Approximate"
      readonly nativeNamespace: string
      readonly graphNamespace: string
      readonly index: ApproximateVectorIndex
    }

const scopedColumns = (nativeNamespace: Option.Option<string>): string =>
  `c.chunk_id, c.document_key, c.section_key, c.section_part,
              c.content,
              c.has_metadata, c.metadata, c.embedding,
              ${Option.isSome(nativeNamespace) ? "c.embedding_native_eligible," : ""}
              r.graph_id, r.document_kind, r.encoded_document_id,
              r.projection_id, r.projection_version, r.revision_hash`

const searchCandidates = async (
  connection: Queryable,
  tables: { readonly revisions: string; readonly chunks: string },
  request: SemanticCandidateRequest,
  plan: SemanticSearchPlan,
): Promise<ReadonlyArray<SemanticSearchCandidate>> => {
  if (request.scope.target._tag === "NoDocuments") return []

  // Index candidates are few, so they are rescored in float64: scores and ties
  // match the exhaustive float64 path whenever recall is complete.
  const nativeNamespace = plan._tag === "Approximate" ? Option.none<string>() : plan.nativeNamespace

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

  const float64Score = `(
    SELECT sum(component.stored * component.query) /
           NULLIF(sqrt(sum(component.stored * component.stored)) *
                  (SELECT norm FROM query_norm), 0)
    FROM unnest(scoped.embedding, $1::double precision[]) AS component(stored, query)
  )`

  const score = Option.isSome(nativeNamespace)
    ? `CASE WHEN scoped.embedding_native_eligible THEN
         1 - ${nativeNamespace.value}.cosine_distance(
           scoped.embedding::${nativeNamespace.value}.vector,
           $1::double precision[]::${nativeNamespace.value}.vector
         )
       ELSE ${float64Score} END`
    : float64Score

  let scoped: string

  if (plan._tag === "Approximate") {
    const vector = plan.nativeNamespace
    values.push(Math.min(10_000, request.candidates * plan.index.overfetch))

    // The scalar subquery keeps scope as a per-row filter on the index scan;
    // as a join, PostgreSQL could scan and sort every chunk instead.
    scoped = `nearest AS MATERIALIZED (
       SELECT c.chunk_id
       FROM ${tables.chunks} AS c
       WHERE ${indexedRowsSql("c", plan.graphNamespace, plan.index)}
         AND coalesce((
           SELECT TRUE
           FROM ${tables.revisions} AS r
           WHERE r.document_key = c.document_key
             AND r.projection_id = c.projection_id
             AND ${filters.join("\n             AND ")}
           LIMIT 1
         ), FALSE)
       ORDER BY ${indexedEmbeddingSql("c", vector, plan.index)}
         OPERATOR(${vector}.<=>) ($1::double precision[]::${vector}.${plan.index.representation}(${plan.index.dimensions}))
       LIMIT $${values.length}
     ),
     scoped AS MATERIALIZED (
       SELECT ${scopedColumns(nativeNamespace)}
       FROM nearest
       INNER JOIN ${tables.chunks} AS c ON c.chunk_id = nearest.chunk_id
       INNER JOIN ${tables.revisions} AS r
         ON r.document_key = c.document_key
        AND r.projection_id = c.projection_id
     )`
  } else {
    scoped = `scoped AS MATERIALIZED (
       SELECT ${scopedColumns(nativeNamespace)}
       FROM ${tables.chunks} AS c
       INNER JOIN ${tables.revisions} AS r
         ON r.document_key = c.document_key
        AND r.projection_id = c.projection_id
       WHERE ${filters.join("\n         AND ")}
     )`
  }

  values.push(request.candidates)

  const rows = await queryRows<SearchCandidateRow>(
    connection,
    `WITH query_norm AS MATERIALIZED (
       SELECT sqrt(sum(component * component)) AS norm
       FROM unnest($1::double precision[]) AS component
     ),
     ${scoped},
     scored AS MATERIALIZED (
       SELECT scoped.chunk_id, scoped.document_key, scoped.section_key, scoped.section_part,
              scoped.content, scoped.has_metadata, scoped.metadata,
              scoped.graph_id, scoped.document_kind, scoped.encoded_document_id,
              scoped.projection_id, scoped.projection_version, scoped.revision_hash,
              ${score} AS score FROM scoped
     )
     SELECT scored.score, scored.chunk_id, scored.document_key,
            scored.graph_id, scored.document_kind,
            scored.encoded_document_id, scored.projection_id,
            scored.projection_version, scored.revision_hash,
            scored.section_key, scored.section_part, scored.content,
            scored.has_metadata, scored.metadata
     FROM scored
     WHERE scored.score IS NOT NULL
       AND scored.score <> 'NaN'::double precision
     ORDER BY scored.score DESC, scored.chunk_id ASC
     LIMIT $${values.length}`,
    values,
  )

  return rows.map((row) =>
    projectSearchCandidate(row, "semantic candidate"),
  )
}

/** Lexical matches weighed exactly per requested candidate. */
const TextRerankDepth = 4

const searchTextCandidates = async (
  connection: Queryable,
  tables: { readonly revisions: string; readonly chunks: string },
  request: TextCandidateRequest,
): Promise<ReadonlyArray<TextSearchCandidate>> => {
  if (request.scope.target._tag === "NoDocuments") return []

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
  values.push(Math.min(10_000, request.candidates * TextRerankDepth))
  const prefetchLimit = `$${values.length}`

  // Weighted per-field scores re-tokenise attributed text, which is costly
  // when a common phrase matches thousands of chunks. Rank every match on
  // the stored combined vector first, then weigh only the best few exactly.
  const rows = await queryRows<SearchCandidateRow>(
    connection,
    `WITH parsed AS (
       SELECT websearch_to_tsquery('${config}'::regconfig, $1) AS query
     ),
     matched AS MATERIALIZED (
       SELECT c.chunk_id
       FROM ${tables.chunks} AS c
       INNER JOIN ${tables.revisions} AS r
         ON r.document_key = c.document_key
        AND r.projection_id = c.projection_id
       CROSS JOIN parsed
       WHERE parsed.query <> ''::tsquery
         AND c.${searchColumn} @@ parsed.query
         AND ${filters.join("\n         AND ")}
       ORDER BY ts_rank_cd(c.${searchColumn}, parsed.query) DESC, c.chunk_id ASC
       LIMIT ${prefetchLimit}
     ),
     scoped AS MATERIALIZED (
       SELECT c.chunk_id, c.document_key, c.section_key, c.section_part,
              c.content,
              c.has_metadata, c.metadata,
              r.graph_id, r.document_kind, r.encoded_document_id,
              r.projection_id, r.projection_version, r.revision_hash,
              CASE WHEN c.text_context IS NULL AND c.text_label IS NULL THEN
                ${contentWeight} * ts_rank_cd(c.${searchColumn}, parsed.query)
              ELSE (
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
              ) END AS score
       FROM matched
       INNER JOIN ${tables.chunks} AS c ON c.chunk_id = matched.chunk_id
       INNER JOIN ${tables.revisions} AS r
         ON r.document_key = c.document_key
        AND r.projection_id = c.projection_id
       CROSS JOIN parsed
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
  tables: PostgresTables,
  replacement: ReplaceOutgoingGraphRelations,
): Promise<GraphRelationCommit> => {
  const plan = planOutgoingGraphRelationReplacement(replacement)

  if (Result.isFailure(plan)) {
    throw new InvalidStoredState(plan.failure)
  }

  await lockMutationInTransaction(
    client,
    tables.locks,
    "relations",
    replacement.graph,
    replacement.sourceDocumentKey,
  )

  const previousRows = await queryRows<GraphEdgeIdentityRow>(
    client,
    `SELECT relation_id, target_document_key
     FROM ${tables.relations}
     WHERE graph_id = $1 AND source_document_key = $2`,
    [replacement.graph, replacement.sourceDocumentKey],
  )

  const previousEdges = previousRows.map((unknownRow) =>
    parseRow(
      GraphEdgeIdentityRowSchema,
      unknownRow,
      "graph edge identity",
    ),
  )

  const previous = new Set(
    previousEdges.map((row) =>
      makeGraphRelationEdgeIdentity({
        relation: row.relation_id,
        targetDocumentKey: row.target_document_key,
      }),
    ),
  )

  const next = plan.success.identities
  const rows = plan.success.edges

  // Include the source in the same key order as targets. Source-first writes
  // deadlock when two publishers simultaneously reference each other.
  const nodesByKey = new Map<DocumentKey, StoredGraphNode>(
    rows.map((row) => [
      row.target.documentKey,
      { ...row.target, state: "Referenced" },
    ]),
  )

  nodesByKey.set(replacement.sourceDocumentKey, {
    documentKey: replacement.sourceDocumentKey,
    reference: replacement.source,
    state: "Materialized",
  })

  const nodes = Array.from(nodesByKey.values()).sort((left, right) =>
    left.documentKey.localeCompare(right.documentKey),
  )

  for (let offset = 0; offset < nodes.length; offset += InsertBatchSize) {
    const batch = nodes.slice(offset, offset + InsertBatchSize)
    const values: Array<unknown> = []

    const valueRows = batch.map((node) => {
      const start = values.length
      values.push(
        replacement.graph,
        node.documentKey,
        node.reference.kind,
        encodeJson(node.reference.id),
        node.state,
      )

      return `($${start + 1}, $${start + 2}, $${start + 3},
        $${start + 4}::jsonb, $${start + 5})`
    })

    await client.query(
      `INSERT INTO ${tables.nodes}
         (graph_id, document_key, document_kind, encoded_document_id, node_state)
       VALUES ${valueRows.join(",\n")}
       ON CONFLICT (graph_id, document_key) DO UPDATE
         SET document_kind = EXCLUDED.document_kind,
             encoded_document_id = EXCLUDED.encoded_document_id,
             node_state = CASE
               WHEN ${tables.nodes}.node_state = 'Materialized'
                 OR EXCLUDED.node_state = 'Materialized'
                 THEN 'Materialized'
               ELSE 'Referenced'
             END,
             updated_at = now()`,
      values,
    )
  }

  await client.query(
    `DELETE FROM ${tables.relations}
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
      `INSERT INTO ${tables.relations}
        (graph_id, relation_id, relation_version, source_document_key,
         source_document_kind, encoded_source_document_id,
         target_document_key, target_document_kind,
         encoded_target_document_id)
       VALUES ${valueRows.join(",\n")}`,
      values,
    )
  }

  // Only former targets can become orphaned in this replacement. Global
  // collection belongs to reconciliation, not every document write.
  await deleteOrphanedReferencedNodesInTransaction(
    client,
    tables,
    replacement.graph,
    previousEdges.map((edge) => edge.target_document_key),
  )

  return countGraphRelationReplacement(previous, next)
}

const parseDeletedGraphEdges = (rowInput: DeletedGraphEdgeCountRow): number => {
  const row = parseRow(
    DeletedGraphEdgeCountRowSchema,
    rowInput,
    "graph edge deletion count",
  )

  return Number(row.deleted_count)
}

const deleteOrphanedReferencedNodesInTransaction = async (
  client: TransactionClient,
  tables: PostgresTables,
  graph: string,
  candidates?: ReadonlyArray<DocumentKey>,
): Promise<number> => {
  if (candidates?.length === 0) return 0

  const orphan = `node.graph_id = $1
    AND node.node_state = 'Referenced'
    AND NOT EXISTS (
      SELECT 1 FROM ${tables.relations} AS edge
      WHERE edge.graph_id = node.graph_id
        AND (edge.source_document_key = node.document_key
          OR edge.target_document_key = node.document_key)
    )`

  const values: Array<unknown> = [graph]

  const target = candidates === undefined
    ? ""
    : "AND node.document_key = ANY($2::char(64)[])"

  if (candidates !== undefined) values.push([...new Set(candidates)])

  // Never wait for another publisher while holding this publication's nodes.
  // Recheck under a fresh statement snapshot after acquiring these locks:
  // a publisher may have committed an edge since the first snapshot began.
  // Rows held by concurrent writers are left for a later reconciliation.
  const locked = await queryRows<typeof GraphNodeKeyRowSchema.Encoded>(
    client,
    `SELECT node.document_key FROM ${tables.nodes} AS node
     WHERE ${orphan} ${target}
     ORDER BY node.document_key
     FOR UPDATE SKIP LOCKED`,
    values,
  )

  const keys = locked.map((row) =>
    parseRow(GraphNodeKeyRowSchema, row, "orphan node key").document_key,
  )

  if (keys.length === 0) return 0

  const rows = await queryRows<DeletedGraphEdgeCountRow>(
    client,
    `WITH deleted AS (
       DELETE FROM ${tables.nodes} AS node
       WHERE ${orphan}
         AND node.document_key = ANY($2::char(64)[])
       RETURNING 1
     )
     SELECT count(*)::text AS deleted_count FROM deleted`,
    [graph, keys],
  )

  const row = rows[0]

  if (row === undefined) {
    throw new InvalidStoredState(
      "PostgreSQL did not return an orphaned referenced-node count",
    )
  }

  return parseDeletedGraphEdges(row)
}

const deleteNodeRelationsInTransaction = async (
  client: TransactionClient,
  tables: PostgresTables,
  input: { readonly graph: string; readonly documentKey: string },
): Promise<GraphTopologyDeletion> => {
  // Lock the node before counting/deleting incident edges. Every replacement
  // locks its source and targets before changing edges, so the counts and
  // hard deletion now describe the same topology state.
  const locked = await queryRows<typeof GraphNodeKeyRowSchema.Encoded>(
    client,
    `SELECT document_key FROM ${tables.nodes}
     WHERE graph_id = $1 AND document_key = $2 FOR UPDATE`,
    [input.graph, input.documentKey],
  )

  if (locked.length === 0) {
    return { deletedNodes: 0, deletedRelations: 0, deletedReferencedNodes: 0 }
  }

  const rows = await queryRows<DeletedGraphEdgeCountRow>(
    client,
    `WITH deleted AS (
       DELETE FROM ${tables.relations}
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

  const deletedRelations = parseDeletedGraphEdges(row)

  const deletedNodes = await queryRows<DeletedGraphEdgeCountRow>(
    client,
    `WITH deleted AS (
       DELETE FROM ${tables.nodes}
       WHERE graph_id = $1 AND document_key = $2
       RETURNING 1
     )
     SELECT count(*)::text AS deleted_count FROM deleted`,
    [input.graph, input.documentKey],
  )

  const nodeRow = deletedNodes[0]

  if (nodeRow === undefined) {
    throw new InvalidStoredState("PostgreSQL did not return a node count")
  }

  const deletedReferencedNodes =
    await deleteOrphanedReferencedNodesInTransaction(
      client,
      tables,
      input.graph,
    )

  return {
    deletedNodes: parseDeletedGraphEdges(nodeRow),
    deletedRelations,
    deletedReferencedNodes,
  }
}

const pruneRelationsInTransaction = async (
  client: TransactionClient,
  tables: PostgresTables,
  input: PruneGraphTopology,
): Promise<GraphTopologyPrune> => {
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

  const rows = await queryRows<DeletedGraphEdgeCountRow>(
    client,
    `WITH deleted AS (
       DELETE FROM ${tables.relations} AS edge WHERE ${stale}
       RETURNING 1
     )
     SELECT count(*)::text AS deleted_count FROM deleted`,
    values,
  )

  const row = rows[0]

  if (row === undefined) {
    throw new InvalidStoredState("PostgreSQL did not return a prune count")
  }

  const deletedRelations = parseDeletedGraphEdges(row)

  const deletedReferencedNodes =
    await deleteOrphanedReferencedNodesInTransaction(
      client,
      tables,
      input.graph,
    )

  return {
    deletedRelations,
    deletedReferencedNodes,
  }
}

const listGraphNodes = async (
  connection: Queryable,
  table: string,
  input: ListGraphNodes,
): Promise<GraphNodePage> => {
  const values: Array<unknown> = [input.graph]
  const filters = ["graph_id = $1"]

  if (input.documentKinds.length > 0) {
    values.push(input.documentKinds)
    filters.push(`document_kind = ANY($${values.length}::text[])`)
  }

  if (input.states.length > 0) {
    values.push(input.states)
    filters.push(`node_state = ANY($${values.length}::text[])`)
  }

  if (Option.isSome(input.after)) {
    values.push(input.after.value)
    filters.push(`document_key > $${values.length}`)
  }

  values.push(input.limit + 1)

  const rows = await queryRows<GraphNodeRow>(
    connection,
    `SELECT document_key, graph_id, document_kind, encoded_document_id,
            node_state
     FROM ${table}
     WHERE ${filters.join(" AND ")}
     ORDER BY document_key
     LIMIT $${values.length}`,
    values,
  )

  const parsed = rows.map((unknownRow) =>
    parseRow(GraphNodeRowSchema, unknownRow, "graph node"),
  )

  const hasMore = parsed.length > input.limit
  const visible = hasMore ? parsed.slice(0, input.limit) : parsed

  const nodes: ReadonlyArray<StoredGraphNode> = visible.map((row) => ({
    documentKey: row.document_key,
    reference: {
      graph: row.graph_id,
      kind: row.document_kind,
      id: row.encoded_document_id,
    },
    state: row.node_state,
  }))

  const last = nodes.at(-1)

  return {
    nodes,
    next: hasMore && last !== undefined
      ? Option.some(last.documentKey)
      : Option.none(),
  }
}

const RelatedGraphNodeRowSchema = Schema.Struct({
  ...GraphNodeRowSchema.fields,
  request_ordinal: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
})

const findRelatedGraphNodes = async (
  connection: Queryable,
  tables: PostgresTables,
  input: FindRelatedGraphNodes,
): Promise<ReadonlyArray<RelatedGraphNodeSet>> => {
  if (input.documentKeys.length === 0) return []
  const current = input.direction === "outgoing" ? "source" : "target"
  const related = input.direction === "outgoing" ? "target" : "source"

  const rows = await queryRows<Schema.Codec.Encoded<typeof RelatedGraphNodeRowSchema>>(
    connection,
    `SELECT (requested.ordinal - 1)::integer AS request_ordinal, neighbours.*
     FROM unnest($2::char(64)[]) WITH ORDINALITY AS requested(document_key, ordinal)
     CROSS JOIN LATERAL (
       SELECT node.document_key, node.graph_id, node.document_kind,
              node.encoded_document_id, node.node_state
       FROM ${tables.relations} AS edge
       INNER JOIN ${tables.nodes} AS node
         ON node.graph_id = edge.graph_id
        AND node.document_key = edge.${related}_document_key
       WHERE edge.graph_id = $1
         AND edge.${current}_document_key = requested.document_key
         AND edge.${current}_document_kind = $3
         AND edge.relation_id = $4
         AND edge.relation_version = $5
         AND edge.${related}_document_kind = $6
       ORDER BY node.document_key
       LIMIT $7
     ) AS neighbours
     ORDER BY requested.ordinal, neighbours.document_key`,
    [input.graph, [...input.documentKeys], input.documentKind, input.relation,
      input.relationVersion, input.relatedDocumentKind, input.limit],
  )

  const groups = input.documentKeys.map((documentKey) => ({
    documentKey,
    nodes: new Array<StoredGraphNode>(),
  }))

  for (const unknownRow of rows) {
    const row = parseRow(RelatedGraphNodeRowSchema, unknownRow, "related graph node")
    const group = groups[row.request_ordinal]

    if (group === undefined || group.nodes.length >= input.limit) {
      throw new InvalidStoredState("Invalid related graph node batch")
    }

    group.nodes.push({
      documentKey: row.document_key,
      reference: { graph: row.graph_id, kind: row.document_kind, id: row.encoded_document_id },
      state: row.node_state,
    })
  }

  return groups
}

const namedPostgresOperation = <
  Args extends ReadonlyArray<unknown>,
  A,
  E,
  R,
>(
  name: string,
  operation: (...args: Args) => Effect.Effect<A, E, R>,
): ((...args: Args) => Effect.Effect<A, E, R>) =>
  Effect.fn(name)(function*(...args: Args) {
    return yield* operation(...args)
  })

interface ResolvedPostgresOptions {
  readonly schema: string
  readonly vectorSearch: "auto" | "float64" | "approximate"
  readonly approximate: ApproximateVectorIndex | undefined
  readonly searchSettings: ReadonlyArray<PostgresReadSetting>
  /** Text searches run under this budget instead; a text timeout then yields no text candidates. */
  readonly bestEffortTextSettings: ReadonlyArray<PostgresReadSetting> | undefined
}

const SearchTimeoutSchema = Schema.Number.check(Schema.isInt(), Schema.isBetween({ minimum: 1, maximum: 2_147_483_647 }))

const resolvePostgresOptions = (config: PostgresDocumentGraphConfig): ResolvedPostgresOptions => {
  const vectorSearch = resolveVectorSearch(config.vectorSearch)
  const timeout = config.searchTimeoutMilliseconds
  const textTimeout = config.textSearchTimeoutMilliseconds

  return {
    schema: Schema.decodeSync(PostgresSchemaNameSchema)(config.schema ?? DefaultSchema),
    vectorSearch: vectorSearch.mode,
    approximate: vectorSearch.mode === "approximate" ? vectorSearch.index : undefined,
    searchSettings: timeout === undefined
      ? []
      : [["statement_timeout", String(Schema.decodeSync(SearchTimeoutSchema)(timeout))]],
    bestEffortTextSettings: textTimeout === undefined
      ? undefined
      : [["statement_timeout", String(Schema.decodeSync(SearchTimeoutSchema)(textTimeout))]],
  }
}

/** PostgreSQL cancelled the statement at its statement_timeout (SQLSTATE 57014). */
const isStatementTimeout = (cause: unknown): boolean =>
  Predicate.hasProperty(cause, "code") && cause.code === "57014"

const makePostgresStorage = (
  config: PostgresDocumentGraphConfig,
  options: ResolvedPostgresOptions,
  capabilities: Effect.Effect<VectorCapabilities, ProjectionSearchStoreFailed>,
): DocumentGraphStorageService => {
  const { schema, vectorSearch, approximate, searchSettings, bestEffortTextSettings } = options
  const namespace = quoteIdentifier(schema)

  const tables: PostgresTables = {
    revisions: `${namespace}."projected_revisions"`,
    chunks: `${namespace}."projected_chunks"`,
    relations: `${namespace}."graph_relations"`,
    nodes: `${namespace}."graph_nodes"`,
    locks: `${namespace}."mutation_locks"`,
  }

  const storage: DocumentGraphStorageService = {
    loadRevisions: (keys) =>
      Effect.tryPromise({
        try: async () => {
          const rows = await queryRows<RequestedRevisionWithChunkRow>(
            connectionFor(config),
            `WITH requested(document_key, projection_id, request_ordinal) AS (
               SELECT document_key, projection_id, request_ordinal
               FROM unnest($1::text[], $2::text[]) WITH ORDINALITY
                 AS request(document_key, projection_id, request_ordinal)
             )
             SELECT requested.request_ordinal::integer AS request_ordinal,
                    r.revision_token::text AS revision_token,
                    r.revision_hash, r.embedding_profile_id,
                    r.embedding_profile_version, r.embedding_dimensions,
                    c.chunk_id, c.content_hash, c.ordinal
             FROM requested
             INNER JOIN ${tables.revisions} AS r
               ON r.document_key = requested.document_key
              AND r.projection_id = requested.projection_id
             LEFT JOIN ${tables.chunks} AS c
               ON c.document_key = r.document_key
              AND c.projection_id = r.projection_id
             ORDER BY requested.request_ordinal, c.ordinal`,
            [
              keys.map((key) => key.documentKey),
              keys.map((key) => key.projection),
            ],
          )

          const parsed = rows.map((row) =>
            parseRow(
              RequestedRevisionWithChunkRowSchema,
              row,
              "projected revision",
            ),
          )

          const rowsByOrdinal = new Map<number, Array<typeof parsed[number]>>()

          for (const row of parsed) {
            const grouped = rowsByOrdinal.get(row.request_ordinal) ?? []
            grouped.push(row)
            rowsByOrdinal.set(row.request_ordinal, grouped)
          }

          return keys.map((key, index): ProjectionRevisionLookup => {
            const revisionRows = rowsByOrdinal.get(index + 1)

            return {
              key,
              revision: revisionRows === undefined
                ? Option.none()
                : Option.some(indexedRevisionSnapshot(revisionRows)),
            }
          })
        },
        catch: (cause) => indexFailure("load_revisions", cause),
      }),

    replaceRevision: (replacement) =>
      transactionEffect(
        config,
        (client) => replaceInTransaction(client, tables, replacement),
        (cause) =>
          Schema.is(ProjectionIndexConflict)(cause)
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

    searchCandidates: (request) => {
      if (request.scope.target._tag === "NoDocuments") return Effect.succeed([])

      const discovered = vectorSearch === "float64" || !nativeQueryEligible(request.vector)
        ? Effect.succeed(noVectorCapabilities)
        : capabilities

      return discovered.pipe(Effect.flatMap((resolved) => {
        // Small document-key scopes are exact and cheap to score exhaustively,
        // and a very selective filter could exhaust the index scan. Large ones,
        // such as every agency in a country, would read too many embeddings.
        const target = request.scope.target
        const indexed = approximate !== undefined && resolved.approximateIndexReady &&
          Option.isSome(resolved.nativeNamespace) &&
          (target._tag === "AllDocuments" ||
            (target._tag === "DocumentKeys" && target.documentKeys.length > approximate.approximateAboveDocuments)) &&
          request.embeddingProfile.dimensions === approximate.dimensions &&
          queryVectorFitsIndex(request.vector, approximate)

        const plan: SemanticSearchPlan = indexed && Option.isSome(resolved.nativeNamespace)
          ? { _tag: "Approximate", nativeNamespace: resolved.nativeNamespace.value, graphNamespace: namespace, index: approximate }
          : { _tag: "Exhaustive", nativeNamespace: resolved.nativeNamespace }

        const settings: ReadonlyArray<PostgresReadSetting> = plan._tag === "Approximate"
          ? [
              // relaxed_order keeps scanning past candidates that scope filters discard;
              // exact rescoring restores the final order.
              ["hnsw.iterative_scan", "relaxed_order"],
              ["hnsw.ef_search", String(Math.min(1_000, Math.max(plan.index.efSearch, request.candidates * plan.index.overfetch)))],
              ["hnsw.max_scan_tuples", String(plan.index.maxScanTuples)],
              ...searchSettings,
            ]
          : searchSettings

        return Effect.tryPromise({
          try: () => withReadSettings(config, settings, (client) => searchCandidates(client, tables, request, plan)),
          catch: searchFailure,
        })
      }))
    },

    searchTextCandidates: (request) =>
      bestEffortTextSettings === undefined
        ? Effect.tryPromise({
            try: () =>
              withReadSettings(config, searchSettings, (client) => searchTextCandidates(client, tables, request)),
            catch: textSearchFailure,
          })
        : Effect.tryPromise({
            // Only a timeout is tolerated: the lexical channel then contributes
            // nothing and semantic candidates carry the search. Other failures fail.
            try: () =>
              withReadSettings(config, bestEffortTextSettings, (client) => searchTextCandidates(client, tables, request))
                .then((rows) => ({ rows, timedOut: false }))
                .catch((cause: unknown) => {
                  if (isStatementTimeout(cause)) return { rows: [], timedOut: true }
                  throw cause
                }),
            catch: textSearchFailure,
          }).pipe(
            Effect.tap(({ timedOut }) => timedOut
              ? Effect.annotateCurrentSpan("document_graph.text_search.timed_out", true)
              : Effect.void),
            Effect.map(({ rows }) => rows),
          ),

    replaceDocumentTopology: (replacement) =>
      transactionEffect(
        config,
        (client) =>
          replaceOutgoingRelationsInTransaction(
            client,
            tables,
            replacement,
          ),
        (cause) => topologyFailure("replace_document", cause),
      ),

    deleteNode: (input) =>
      transactionEffect(
        config,
        (client) =>
          deleteNodeRelationsInTransaction(
            client,
            tables,
            input,
          ),
        (cause) => topologyFailure("delete_node", cause),
      ),

    pruneTopology: (input) =>
      transactionEffect(
        config,
        (client) =>
          pruneRelationsInTransaction(client, tables, input),
        (cause) => topologyFailure("prune_graph", cause),
      ),

    listNodes: (input) =>
      Effect.tryPromise({
        try: () =>
          listGraphNodes(
            connectionFor(config),
            tables.nodes,
            input,
          ),
        catch: (cause) => topologyFailure("list_nodes", cause),
      }),

    findRelatedNodes: (input) =>
      Effect.tryPromise({
        try: () =>
          findRelatedGraphNodes(connectionFor(config), tables, input),
        catch: (cause) => topologyFailure("find_related", cause),
      }),
  }

  return {
    loadRevisions: namedPostgresOperation(
      "PostgresProjectionIndex.loadRevisions",
      storage.loadRevisions,
    ),
    replaceRevision: namedPostgresOperation(
      "PostgresProjectionIndex.replaceRevision",
      storage.replaceRevision,
    ),
    deleteRevision: namedPostgresOperation(
      "PostgresProjectionIndex.deleteRevision",
      storage.deleteRevision,
    ),
    pruneGraph: namedPostgresOperation(
      "PostgresProjectionIndex.pruneGraph",
      storage.pruneGraph,
    ),
    searchCandidates: namedPostgresOperation(
      "PostgresProjectionSearch.searchCandidates",
      storage.searchCandidates,
    ),
    searchTextCandidates: namedPostgresOperation(
      "PostgresProjectionSearch.searchTextCandidates",
      storage.searchTextCandidates,
    ),
    replaceDocumentTopology: namedPostgresOperation(
      "PostgresGraphTopology.replaceDocumentTopology",
      storage.replaceDocumentTopology,
    ),
    deleteNode: namedPostgresOperation(
      "PostgresGraphTopology.deleteNode",
      storage.deleteNode,
    ),
    pruneTopology: namedPostgresOperation(
      "PostgresGraphTopology.pruneTopology",
      storage.pruneTopology,
    ),
    listNodes: namedPostgresOperation(
      "PostgresGraphTopology.listNodes",
      storage.listNodes,
    ),
    findRelatedNodes: namedPostgresOperation(
      "PostgresGraphTopology.findRelatedNodes",
      storage.findRelatedNodes,
    ),
  }
}

/**
 * Provide durable indexing plus semantic and full-text retrieval through PostgreSQL.
 *
 * The caller owns the Pool or active transaction and remains responsible for
 * its lifecycle. Pool mode owns each operation's transaction; transaction
 * mode uses a savepoint and never commits the caller's transaction. The
 * adapter uses installed pgvector automatically for eligible vectors, with
 * float64 array scoring for other vectors or vectorSearch: "float64".
 * Apply all ordered PostgreSQL migrations before using this Layer.
 */
export const postgresDocumentGraph = (
  config: PostgresDocumentGraphConfig,
): ReturnType<typeof makeDocumentGraphStorage> =>
  Layer.unwrap(Effect.gen(function*() {
    const options = resolvePostgresOptions(config)
    // Exit-aware TTL prevents a transient connection failure from poisoning
    // the capability cache. Concurrent routes share one discovery request.
    const capabilities = yield* Cache.makeWith(
      () => discoverVectorCapabilities(connectionFor(config), options.schema, options.approximate),
      { capacity: 1, timeToLive: (exit) => Exit.isSuccess(exit) ? "5 minutes" : 0 },
    )

    return makeDocumentGraphStorage(makePostgresStorage(config, options, Cache.get(capabilities, "pgvector")))
  }))
