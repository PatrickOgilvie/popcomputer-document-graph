import { Effect, Layer, Option, Schema } from "effect"
import {
  ChunkIdSchema,
  ContentHashSchema,
  DocumentKeySchema,
  ProjectionRevisionHashSchema,
} from "../../document/document-identity.js"
import {
  EmbeddingDimensionsSchema,
  EmbeddingProfileIdSchema,
  EmbeddingProfileVersionSchema,
} from "../../indexing/embedding-provider.js"
import {
  ProjectionMutationIdSchema,
  ProjectionPayloadDigestSchema,
  ProjectionPublicationCoordinator,
  ProjectionPublicationCoordinatorFailed,
  ProjectionPublicationGenerationSchema,
  ProjectionPublicationIdSchema,
  ProjectionPublicationSuperseded,
  type ProjectionPublicationCoordinatorService,
  type ProjectionPublicationHead,
  type ProjectionPublicationIntent,
  type ProjectionPublicationLease,
  type ProjectionPublicationOutcome,
} from "../../indexing/projection-publication.js"
import {
  IndexRevisionTokenSchema,
  ProjectionIndexConflict,
  type IndexedChunkSummary,
  type IndexedRevisionSnapshot,
  type ProjectionIndexKey,
} from "../../indexing/projection-index.js"
import {
  activeMatchesExpectedToken,
  publicationOutcomeFor,
  sameActiveIntent,
  samePendingIntent,
  stalePublicationPlan,
} from "../publication-head.js"
import type {
  DocumentGraphD1Database,
  DocumentGraphD1PreparedStatement,
  DocumentGraphD1Session,
} from "./contract.js"

const PositiveIntegerSchema = Schema.Number.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0),
)

const HeadRowSchema = Schema.Struct({
  document_key: DocumentKeySchema,
  projection_id: Schema.String,
  last_allocated_generation: PositiveIntegerSchema,
  slot_high_water: PositiveIntegerSchema,
  active_status: Schema.Literals(["never", "revision", "deleted"]),
  active_token: Schema.NullOr(IndexRevisionTokenSchema),
  active_mutation_id: Schema.NullOr(Schema.String),
  active_payload_digest: Schema.NullOr(Schema.String),
  pending_mutation_id: Schema.NullOr(Schema.String),
  pending_payload_digest: Schema.NullOr(Schema.String),
  pending_publication_id: Schema.NullOr(Schema.String),
  pending_generation: Schema.NullOr(PositiveIntegerSchema),
  pending_operation: Schema.NullOr(Schema.Literals(["replace", "delete"])),
  pending_slot_high_water: Schema.NullOr(PositiveIntegerSchema),
  pending_lease_expires_at: Schema.NullOr(PositiveIntegerSchema),
})

const RevisionRowSchema = Schema.Struct({
  request_ordinal: PositiveIntegerSchema,
  document_key: DocumentKeySchema,
  projection_id: Schema.String,
  active_token: IndexRevisionTokenSchema,
  revision_hash: ProjectionRevisionHashSchema,
  embedding_profile_id: EmbeddingProfileIdSchema,
  embedding_profile_version: EmbeddingProfileVersionSchema,
  embedding_dimensions: EmbeddingDimensionsSchema,
  chunk_id: Schema.NullOr(ChunkIdSchema),
  content_hash: Schema.NullOr(ContentHashSchema),
  ordinal: Schema.NullOr(PositiveIntegerSchema),
})

const BegunHeadRowSchema = Schema.Struct({
  document_key: DocumentKeySchema,
  projection_id: Schema.String,
  pending_mutation_id: Schema.String,
  pending_payload_digest: ProjectionPayloadDigestSchema,
  pending_publication_id: ProjectionPublicationIdSchema,
  pending_generation: ProjectionPublicationGenerationSchema,
  pending_operation: Schema.Literals(["replace", "delete"]),
  pending_slot_high_water: PositiveIntegerSchema,
})

const StaleRowSchema = Schema.Struct({
  document_key: DocumentKeySchema,
  projection_id: Schema.String,
  document_kind: Schema.String,
  projection_version: Schema.String,
})

const MutationDigestRowSchema = Schema.Struct({
  payload_digest: ProjectionPayloadDigestSchema,
})

const MutationOutcomeRowSchema = Schema.Struct({
  operation: Schema.Literals(["replace", "delete"]),
  next_token: Schema.NullOr(IndexRevisionTokenSchema),
  commit_inserted: PositiveIntegerSchema,
  commit_updated: PositiveIntegerSchema,
  commit_deleted: PositiveIntegerSchema,
  deletion_revisions: PositiveIntegerSchema,
  deletion_chunks: PositiveIntegerSchema,
})

type HeadRow = typeof HeadRowSchema.Encoded

type RevisionRow = typeof RevisionRowSchema.Encoded

type BegunHeadRow = typeof BegunHeadRowSchema.Encoded

type StaleRow = typeof StaleRowSchema.Encoded

type MutationDigestRow = typeof MutationDigestRowSchema.Encoded

/** D1 settings for durable Turbopuffer publication coordination. */
export interface D1ProjectionPublicationConfig {
  readonly database: DocumentGraphD1Database
  /** Immutable physical namespace generation represented by these heads. */
  readonly indexGeneration: string
  /**
   * Diagnostic deadline for an unreconciled publication; defaults to 60
   * seconds. Expiration never transfers publication authority automatically.
   */
  readonly publicationLeaseMilliseconds?: number | undefined
  /** Completed journal entries retained per projection; defaults to 32. */
  readonly retainedPublicationHistory?: number | undefined
}

const decodeRow = <S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- This function is the D1 row I/O boundary and immediately decodes through the supplied schema.
  row: unknown,
  label: string,
): S["Type"] => {
  try {
    return Schema.decodeUnknownSync(schema)(row, {
      onExcessProperty: "error",
    })
  } catch (cause) {
    throw new Error(`D1 returned an invalid ${label}`, { cause })
  }
}

const decodeCoordinatorRow = <S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- This persisted D1 row is immediately decoded through the supplied schema.
  row: unknown,
  label: string,
  operation: ProjectionPublicationCoordinatorFailed["operation"],
): Effect.Effect<S["Type"], ProjectionPublicationCoordinatorFailed> =>
  Effect.try({
    try: () => decodeRow(schema, row, label),
    catch: (cause) => coordinatorFailure(
      operation,
      cause,
      "invalid_stored_state",
    ),
  })

const coordinatorFailure = (
  operation: ProjectionPublicationCoordinatorFailed["operation"],
  cause: unknown,
  reason: ProjectionPublicationCoordinatorFailed["reason"] = "unavailable",
): ProjectionPublicationCoordinatorFailed =>
  new ProjectionPublicationCoordinatorFailed({ operation, reason, cause })

const d1Effect = <A>(
  operation: ProjectionPublicationCoordinatorFailed["operation"],
  run: () => Promise<A>,
): Effect.Effect<A, ProjectionPublicationCoordinatorFailed> =>
  Effect.tryPromise({
    try: run,
    catch: (cause) =>
      coordinatorFailure(
        operation,
        cause,
        cause instanceof Error && cause.message.startsWith("D1 returned")
          ? "invalid_stored_state"
          : "unavailable",
      ),
  })

const strongSession = (
  database: DocumentGraphD1Database,
): DocumentGraphD1Session =>
  database.withSession("first-primary")

const requestedKeysJson = (
  keys: ReadonlyArray<ProjectionIndexKey>,
): string => JSON.stringify(keys.map((key) => ({
  documentKey: key.documentKey,
  projection: key.projection,
})))

const activeFromRow = (
  row: typeof HeadRowSchema.Type,
): ProjectionPublicationHead["active"] => {
  switch (row.active_status) {
    case "never":
      return { _tag: "NeverPublished" }
    case "deleted":
      return { _tag: "Deleted" }
    case "revision":
      if (row.active_token === null) {
        throw new Error("D1 returned a revision head without an active token")
      }

      return {
        _tag: "Revision",
        token: row.active_token,
      }
  }
}

const pendingFromRow = (
  row: typeof HeadRowSchema.Type,
): ProjectionPublicationHead["pending"] => {
  const mutationId = row.pending_mutation_id
  const payloadDigest = row.pending_payload_digest
  const publicationId = row.pending_publication_id
  const generation = row.pending_generation
  const operation = row.pending_operation
  const slotHighWater = row.pending_slot_high_water

  const fields = [
    mutationId,
    payloadDigest,
    publicationId,
    generation,
    operation,
    slotHighWater,
  ]

  if (fields.every((field) => field === null)) return Option.none()

  if (
    mutationId === null || payloadDigest === null ||
    publicationId === null || generation === null ||
    operation === null || slotHighWater === null
  ) {
    throw new Error("D1 returned an incomplete pending publication")
  }

  return Option.some({
    mutationId: Schema.decodeSync(ProjectionMutationIdSchema)(
      mutationId,
    ),
    payloadDigest: Schema.decodeSync(ProjectionPayloadDigestSchema)(
      payloadDigest,
    ),
    publicationId: Schema.decodeSync(ProjectionPublicationIdSchema)(
      publicationId,
    ),
    generation: Schema.decodeSync(ProjectionPublicationGenerationSchema)(
      generation,
    ),
    slotHighWater,
    operation,
  })
}

const headFromRow = (
  row: typeof HeadRowSchema.Type,
): ProjectionPublicationHead => ({
  key: {
    documentKey: row.document_key,
    projection: row.projection_id,
  },
  lastAllocatedGeneration: row.last_allocated_generation,
  slotHighWater: row.slot_high_water,
  active: activeFromRow(row),
  activeMutationId: row.active_mutation_id === null
    ? Option.none()
    : Option.some(Schema.decodeSync(ProjectionMutationIdSchema)(
        row.active_mutation_id,
      )),
  activePayloadDigest: row.active_payload_digest === null
    ? Option.none()
    : Option.some(Schema.decodeSync(ProjectionPayloadDigestSchema)(
        row.active_payload_digest,
      )),
  pending: pendingFromRow(row),
})

const mutationValues = (intent: ProjectionPublicationIntent, now: number) => {
  if (intent._tag === "Replace") {
    return [
      intent.mutationId,
      intent.key.documentKey,
      intent.key.projection,
      intent.payloadDigest,
      "replace",
      Option.getOrNull(intent.expectedToken),
      intent.snapshot.token,
      intent.snapshot.revisionHash,
      intent.snapshot.embeddingProfile.id,
      intent.snapshot.embeddingProfile.version,
      intent.snapshot.embeddingProfile.dimensions,
      intent.catalog.graph,
      intent.catalog.documentKind,
      intent.catalog.projectionVersion,
      intent.liveSlotCount,
      intent.requiredSlotHighWater,
      intent.commit.inserted,
      intent.commit.updated,
      intent.commit.deleted,
      0,
      0,
      now,
    ] as const
  }

  return [
    intent.mutationId,
    intent.key.documentKey,
    intent.key.projection,
    intent.payloadDigest,
    "delete",
    Option.getOrNull(intent.expectedToken),
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    0,
    0,
    0,
    0,
    0,
    intent.deletion.deletedRevisions,
    intent.deletion.deletedChunks,
    now,
  ] as const
}

const stageMutationStatement = (
  database: DocumentGraphD1Database,
  intent: ProjectionPublicationIntent,
  now: number,
): DocumentGraphD1PreparedStatement =>
  database.prepare(
    `INSERT INTO document_graph_projection_mutations (
       mutation_id, document_key, projection_id, payload_digest, operation,
       expected_token, next_token, revision_hash, embedding_profile_id,
       embedding_profile_version, embedding_dimensions, graph_id,
       document_kind, projection_version, live_slot_count,
       required_slot_high_water, commit_inserted,
       commit_updated, commit_deleted, deletion_revisions, deletion_chunks,
       created_at
     ) VALUES (
       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
     ) ON CONFLICT(mutation_id) DO NOTHING`,
  ).bind(...mutationValues(intent, now))

const stageChunksStatement = (
  database: DocumentGraphD1Database,
  intent: ProjectionPublicationIntent,
): DocumentGraphD1PreparedStatement => {
  const chunks = intent._tag === "Replace"
    ? intent.snapshot.chunks.map((chunk, ordinal) => ({
        ordinal,
        chunkId: chunk.chunkId,
        contentHash: chunk.contentHash,
      }))
    : []

  return database.prepare(
    `INSERT INTO document_graph_projection_mutation_chunks
       (mutation_id, ordinal, chunk_id, content_hash)
     SELECT mutation.mutation_id,
       CAST(json_extract(chunk.value, '$.ordinal') AS INTEGER),
       json_extract(chunk.value, '$.chunkId'),
       json_extract(chunk.value, '$.contentHash')
     FROM document_graph_projection_mutations AS mutation
     CROSS JOIN json_each(?) AS chunk
     WHERE mutation.mutation_id = ? AND mutation.payload_digest = ?
     ON CONFLICT(mutation_id, ordinal) DO NOTHING`,
  ).bind(JSON.stringify(chunks), intent.mutationId, intent.payloadDigest)
}

const cleanupUnreferencedMutationStatement = (
  database: DocumentGraphD1Database,
  intent: ProjectionPublicationIntent,
): DocumentGraphD1PreparedStatement =>
  database.prepare(
    `DELETE FROM document_graph_projection_mutations
     WHERE mutation_id = ? AND payload_digest = ?
       AND NOT EXISTS (
         SELECT 1 FROM document_graph_projection_heads AS head
         WHERE head.active_mutation_id =
                 document_graph_projection_mutations.mutation_id
            OR head.pending_mutation_id =
                 document_graph_projection_mutations.mutation_id
       )
       AND NOT EXISTS (
         SELECT 1 FROM document_graph_projection_publications AS publication
         WHERE publication.mutation_id =
                 document_graph_projection_mutations.mutation_id
       )`,
  ).bind(intent.mutationId, intent.payloadDigest)

const prunePublicationHistoryStatement = (input: {
  readonly database: DocumentGraphD1Database
  readonly indexGeneration: string
  readonly key: ProjectionIndexKey
  readonly retainedPublicationHistory: number
}): DocumentGraphD1PreparedStatement =>
  input.database.prepare(
    `DELETE FROM document_graph_projection_publications
     WHERE publication_id IN (
       SELECT publication_id
       FROM document_graph_projection_publications
       WHERE index_generation = ?
         AND document_key = ? AND projection_id = ?
         AND status != 'pending'
       ORDER BY generation DESC
       LIMIT -1 OFFSET ?
     )`,
  ).bind(
    input.indexGeneration,
    input.key.documentKey,
    input.key.projection,
    input.retainedPublicationHistory,
  )

const cleanupProjectionMutationsStatement = (
  database: DocumentGraphD1Database,
  key: ProjectionIndexKey,
): DocumentGraphD1PreparedStatement =>
  database.prepare(
    `DELETE FROM document_graph_projection_mutations
     WHERE document_key = ? AND projection_id = ?
     AND NOT EXISTS (
       SELECT 1 FROM document_graph_projection_heads AS head
       WHERE head.active_mutation_id =
               document_graph_projection_mutations.mutation_id
          OR head.pending_mutation_id =
               document_graph_projection_mutations.mutation_id
     )
     AND NOT EXISTS (
       SELECT 1 FROM document_graph_projection_publications AS publication
       WHERE publication.mutation_id =
               document_graph_projection_mutations.mutation_id
     )`,
  ).bind(key.documentKey, key.projection)

const beginHeadStatement = (input: {
  readonly database: DocumentGraphD1Database
  readonly intent: ProjectionPublicationIntent
  readonly indexGeneration: string
  readonly publicationId: string
  readonly now: number
  readonly leaseExpiresAt: number
}): DocumentGraphD1PreparedStatement => {
  const expected = Option.getOrNull(input.intent.expectedToken)
  const desiredHighWater = input.intent.slotHighWater
  const operation = input.intent._tag === "Replace" ? "replace" : "delete"

  // Expiration is deliberately absent from the UPSERT guard. D1 cannot
  // atomically fence an already-running external TP write, so only an empty
  // pending slot or the exact prepared mutation may retain publication
  // authority here.
  return input.database.prepare(
    `INSERT INTO document_graph_projection_heads (
       document_key, projection_id, index_generation, head_version,
       last_allocated_generation, slot_high_water, active_status,
       pending_mutation_id, pending_payload_digest, pending_publication_id,
       pending_generation, pending_operation, pending_slot_high_water,
       pending_lease_expires_at, updated_at
     )
     SELECT ?, ?, ?, 0, 1, ?, 'never', ?, ?, ?, 1, ?, ?, ?, ?
     FROM document_graph_projection_mutations AS mutation
     WHERE mutation.mutation_id = ? AND mutation.payload_digest = ?
       AND (
         ? IS NULL
         OR EXISTS (
           SELECT 1 FROM document_graph_projection_heads AS current_head
           WHERE current_head.index_generation = ?
             AND current_head.document_key = ?
             AND current_head.projection_id = ?
         )
       )
       AND ? <= ?
     ON CONFLICT(index_generation, document_key, projection_id) DO UPDATE SET
       last_allocated_generation = CASE
         WHEN document_graph_projection_heads.pending_mutation_id =
              excluded.pending_mutation_id
          AND document_graph_projection_heads.pending_payload_digest =
              excluded.pending_payload_digest
           THEN document_graph_projection_heads.last_allocated_generation
         ELSE document_graph_projection_heads.last_allocated_generation + 1
       END,
       slot_high_water = excluded.pending_slot_high_water,
       pending_mutation_id = excluded.pending_mutation_id,
       pending_payload_digest = excluded.pending_payload_digest,
       pending_publication_id = CASE
         WHEN document_graph_projection_heads.pending_mutation_id =
              excluded.pending_mutation_id
          AND document_graph_projection_heads.pending_payload_digest =
              excluded.pending_payload_digest
           THEN document_graph_projection_heads.pending_publication_id
         ELSE excluded.pending_publication_id
       END,
       pending_generation = CASE
         WHEN document_graph_projection_heads.pending_mutation_id =
              excluded.pending_mutation_id
          AND document_graph_projection_heads.pending_payload_digest =
              excluded.pending_payload_digest
           THEN document_graph_projection_heads.pending_generation
         ELSE document_graph_projection_heads.last_allocated_generation + 1
       END,
       pending_operation = excluded.pending_operation,
       pending_slot_high_water = excluded.pending_slot_high_water,
       pending_lease_expires_at = CASE
         WHEN document_graph_projection_heads.pending_mutation_id =
              excluded.pending_mutation_id
          AND document_graph_projection_heads.pending_payload_digest =
              excluded.pending_payload_digest
           THEN document_graph_projection_heads.pending_lease_expires_at
         ELSE excluded.pending_lease_expires_at
       END,
       updated_at = excluded.updated_at
     WHERE document_graph_projection_heads.index_generation =
             excluded.index_generation
       AND (
         (document_graph_projection_heads.pending_mutation_id =
            excluded.pending_mutation_id
          AND document_graph_projection_heads.pending_payload_digest =
            excluded.pending_payload_digest)
         OR document_graph_projection_heads.pending_mutation_id IS NULL
       )
       AND (
         (? IS NULL AND document_graph_projection_heads.active_token IS NULL)
         OR document_graph_projection_heads.active_token = ?
       )
       AND document_graph_projection_heads.slot_high_water <=
             excluded.pending_slot_high_water
       AND excluded.pending_slot_high_water <= ?
     RETURNING document_key, projection_id, pending_mutation_id,
       pending_payload_digest, pending_publication_id, pending_generation,
       pending_operation, pending_slot_high_water`,
  ).bind(
    input.intent.key.documentKey,
    input.intent.key.projection,
    input.indexGeneration,
    desiredHighWater,
    input.intent.mutationId,
    input.intent.payloadDigest,
    input.publicationId,
    operation,
    desiredHighWater,
    input.leaseExpiresAt,
    input.now,
    input.intent.mutationId,
    input.intent.payloadDigest,
    expected,
    input.indexGeneration,
    input.intent.key.documentKey,
    input.intent.key.projection,
    desiredHighWater,
    input.intent.maximumSlotHighWater,
    expected,
    expected,
    input.intent.maximumSlotHighWater,
  )
}

const journalStatement = (input: {
  readonly database: DocumentGraphD1Database
  readonly intent: ProjectionPublicationIntent
  readonly indexGeneration: string
  readonly publicationId: string
  readonly now: number
}): DocumentGraphD1PreparedStatement =>
  input.database.prepare(
    `INSERT INTO document_graph_projection_publications (
       publication_id, mutation_id, document_key, projection_id,
       index_generation, generation, slot_high_water, status, created_at,
       updated_at
     )
     SELECT pending_publication_id, pending_mutation_id, document_key,
       projection_id, index_generation, pending_generation,
       pending_slot_high_water,
       'pending', ?, ?
     FROM document_graph_projection_heads
     WHERE index_generation = ? AND document_key = ? AND projection_id = ?
       AND pending_mutation_id = ? AND pending_payload_digest = ?
     ON CONFLICT(publication_id) DO UPDATE SET updated_at = excluded.updated_at`,
  ).bind(
    input.now,
    input.now,
    input.indexGeneration,
    input.intent.key.documentKey,
    input.intent.key.projection,
    input.intent.mutationId,
    input.intent.payloadDigest,
  )

const leaseFromRow = (
  intent: ProjectionPublicationIntent,
  row: typeof BegunHeadRowSchema.Type,
): ProjectionPublicationLease => ({
  intent,
  publicationId: row.pending_publication_id,
  generation: row.pending_generation,
  slotHighWater: row.pending_slot_high_water,
})

const makeCoordinator = (
  config: D1ProjectionPublicationConfig,
): ProjectionPublicationCoordinatorService => {
  if (
    config.indexGeneration.trim().length === 0 ||
    config.indexGeneration.length > 256
  ) {
    throw new Error(
      "indexGeneration must contain between 1 and 256 characters",
    )
  }

  const leaseMilliseconds = config.publicationLeaseMilliseconds ?? 60_000

  if (!Number.isInteger(leaseMilliseconds) || leaseMilliseconds < 1_000) {
    throw new Error("publicationLeaseMilliseconds must be an integer >= 1000")
  }

  const retainedPublicationHistory = config.retainedPublicationHistory ?? 32

  if (
    !Number.isSafeInteger(retainedPublicationHistory) ||
    retainedPublicationHistory < 1 || retainedPublicationHistory > 1_000
  ) {
    throw new Error(
      "retainedPublicationHistory must be an integer between 1 and 1000",
    )
  }

  const loadHeads: ProjectionPublicationCoordinatorService["loadHeads"] =
    (keys) => d1Effect("load_heads", async () => {
      const result = await strongSession(config.database).prepare(
        `WITH requested AS (
           SELECT CAST(key AS INTEGER) AS request_ordinal,
             json_extract(value, '$.documentKey') AS document_key,
             json_extract(value, '$.projection') AS projection_id
           FROM json_each(?)
         )
         SELECT requested.request_ordinal, head.document_key,
           head.projection_id, head.last_allocated_generation,
           head.slot_high_water, head.active_status, head.active_token,
           head.active_mutation_id,
           active.payload_digest AS active_payload_digest,
           head.pending_mutation_id, head.pending_payload_digest,
           head.pending_publication_id, head.pending_generation,
           head.pending_operation, head.pending_slot_high_water,
           head.pending_lease_expires_at
         FROM requested
         INNER JOIN document_graph_projection_heads AS head
           ON head.index_generation = ?
          AND head.document_key = requested.document_key
          AND head.projection_id = requested.projection_id
         LEFT JOIN document_graph_projection_mutations AS active
           ON active.mutation_id = head.active_mutation_id
         ORDER BY requested.request_ordinal`,
      ).bind(requestedKeysJson(keys), config.indexGeneration).all<HeadRow & {
        readonly request_ordinal: number
      }>()

      const byOrdinal = new Map<number, ProjectionPublicationHead>()

      for (const unknownRow of result.results) {
        const { request_ordinal: unknownOrdinal, ...unknownHead } = unknownRow

        const requestOrdinal = decodeRow(
          PositiveIntegerSchema,
          unknownOrdinal,
          "head ordinal",
        )

        try {
          byOrdinal.set(
            requestOrdinal,
            headFromRow(decodeRow(HeadRowSchema, unknownHead, "head")),
          )
        } catch (cause) {
          throw new Error("D1 returned an invalid publication head", {
            cause,
          })
        }
      }

      return keys.map((key, ordinal) => {
        const head = byOrdinal.get(ordinal)

        return {
          key,
          head: head === undefined ? Option.none() : Option.some(head),
        }
      })
    })

  const loadRevisions: ProjectionPublicationCoordinatorService["loadRevisions"] =
    (keys) => d1Effect("load_revisions", async () => {
      const result = await strongSession(config.database).prepare(
        `WITH requested AS (
           SELECT CAST(key AS INTEGER) AS request_ordinal,
             json_extract(value, '$.documentKey') AS document_key,
             json_extract(value, '$.projection') AS projection_id
           FROM json_each(?)
         )
         SELECT requested.request_ordinal, head.document_key,
           head.projection_id, head.active_token, mutation.revision_hash,
           mutation.embedding_profile_id,
           mutation.embedding_profile_version,
           mutation.embedding_dimensions, chunk.chunk_id,
           chunk.content_hash, chunk.ordinal
         FROM requested
         INNER JOIN document_graph_projection_heads AS head
           ON head.index_generation = ?
          AND head.document_key = requested.document_key
          AND head.projection_id = requested.projection_id
          AND head.active_status = 'revision'
         INNER JOIN document_graph_projection_mutations AS mutation
           ON mutation.mutation_id = head.active_mutation_id
         LEFT JOIN document_graph_projection_mutation_chunks AS chunk
           ON chunk.mutation_id = mutation.mutation_id
         ORDER BY requested.request_ordinal, chunk.ordinal`,
      ).bind(requestedKeysJson(keys), config.indexGeneration).all<RevisionRow>()

      const grouped = new Map<number, Array<typeof RevisionRowSchema.Type>>()

      for (const unknownRow of result.results) {
        const row = decodeRow(RevisionRowSchema, unknownRow, "revision")
        const group = grouped.get(row.request_ordinal) ?? []
        group.push(row)
        grouped.set(row.request_ordinal, group)
      }

      return keys.map((key, ordinal) => {
        const rows = grouped.get(ordinal)

        if (rows === undefined || rows.length === 0) {
          return { key, revision: Option.none() }
        }

        const first = rows[0]

        if (first === undefined || first.chunk_id === null ||
          first.content_hash === null || first.ordinal === null) {
          throw new Error("D1 returned an incomplete active revision")
        }

        const chunks: Array<IndexedChunkSummary> = rows.map((row) => {
          if (row.chunk_id === null || row.content_hash === null ||
            row.ordinal === null) {
            throw new Error("D1 returned an incomplete active chunk")
          }

          return { chunkId: row.chunk_id, contentHash: row.content_hash }
        })

        const [firstChunk, ...remainingChunks] = chunks

        if (firstChunk === undefined) {
          throw new Error("D1 returned an active revision without chunks")
        }

        const snapshot: IndexedRevisionSnapshot = {
          token: first.active_token,
          revisionHash: first.revision_hash,
          embeddingProfile: {
            id: first.embedding_profile_id,
            version: first.embedding_profile_version,
            dimensions: first.embedding_dimensions,
          },
          chunks: [firstChunk, ...remainingChunks],
        }

        return { key, revision: Option.some(snapshot) }
      })
    })

  const loadCommittedOutcome = (intent: ProjectionPublicationIntent) =>
    d1Effect("begin_publication", async () => {
      const unknownRow = await strongSession(config.database).prepare(
        `SELECT operation, next_token, commit_inserted, commit_updated,
           commit_deleted, deletion_revisions, deletion_chunks
         FROM document_graph_projection_mutations
         WHERE mutation_id = ? AND payload_digest = ?`,
      ).bind(intent.mutationId, intent.payloadDigest).first()

      if (unknownRow === null) return Option.none<ProjectionPublicationOutcome>()
      const row = decodeRow(MutationOutcomeRowSchema, unknownRow, "committed outcome")

      if (row.operation === "delete" && intent._tag === "Delete") {
        return Option.some<ProjectionPublicationOutcome>({
          _tag: "Deleted",
          deletion: {
            deletedRevisions: row.deletion_revisions,
            deletedChunks: row.deletion_chunks,
          },
        })
      }

      if (row.operation !== "replace" || intent._tag !== "Replace" || row.next_token === null) {
        throw new Error("D1 returned an inconsistent committed outcome")
      }

      return Option.some<ProjectionPublicationOutcome>({
        _tag: "Replaced",
        commit: {
          token: row.next_token,
          inserted: row.commit_inserted,
          updated: row.commit_updated,
          deleted: row.commit_deleted,
        },
      })
    }).pipe(
      Effect.flatMap(Effect.fromOption(() => new ProjectionIndexConflict({
        documentKey: intent.key.documentKey,
        projection: intent.key.projection,
      }))),
    )

  const beginPublication: ProjectionPublicationCoordinatorService[
    "beginPublication"
  ] = (intent) => Effect.gen(function*() {
    if (
      intent._tag === "Replace" &&
      intent.requiredSlotHighWater > intent.slotHighWater
    ) {
      return yield* coordinatorFailure(
        "begin_publication",
        "The planned slot closure does not cover every required slot",
        "invalid_stored_state",
      )
    }

    if (intent.slotHighWater > intent.maximumSlotHighWater) {
      return yield* coordinatorFailure(
        "begin_publication",
        "The planned slot high-water exceeds the provider closure bound",
        "capacity_exceeded",
      )
    }

    if (
      !Number.isSafeInteger(intent.slotHighWater) ||
      intent.slotHighWater < 0
    ) {
      return yield* coordinatorFailure(
        "begin_publication",
        "The planned slot high-water is invalid",
        "invalid_stored_state",
      )
    }

    const [lookup] = yield* loadHeads([intent.key])
    const head = lookup?.head ?? Option.none()

    if (Option.isSome(head)) {
      if (sameActiveIntent(head.value, intent)) {
        return {
          _tag: "AlreadyCommitted",
          outcome: yield* loadCommittedOutcome(intent),
        } as const
      }

      if (intent._tag === "Delete" &&
        head.value.active._tag === "Deleted" &&
        Option.isNone(head.value.pending)) {
        return {
          _tag: "AlreadyCommitted",
          outcome: {
            _tag: "Deleted",
            deletion: { deletedRevisions: 0, deletedChunks: 0 },
          },
        } as const
      }

      if (samePendingIntent(head.value, intent)) {
        const pending = Option.getOrThrow(head.value.pending)

        return {
          _tag: "Publish",
          lease: {
            intent,
            publicationId: pending.publicationId,
            generation: pending.generation,
            slotHighWater: pending.slotHighWater,
          },
        } as const
      }

      if (
        Option.isNone(head.value.pending) &&
        activeMatchesExpectedToken(head.value, intent) &&
        head.value.slotHighWater > intent.slotHighWater
      ) {
        return yield* stalePublicationPlan(
          intent,
          head.value.slotHighWater,
        )
      }
    }

    const now = Date.now()

    const publicationId = ProjectionPublicationIdSchema.make(
      crypto.randomUUID(),
    )

    const statements = [
      stageMutationStatement(config.database, intent, now),
      stageChunksStatement(config.database, intent),
      beginHeadStatement({
        database: config.database,
        intent,
        indexGeneration: config.indexGeneration,
        publicationId,
        now,
        leaseExpiresAt: now + leaseMilliseconds,
      }),
      journalStatement({
        database: config.database,
        intent,
        indexGeneration: config.indexGeneration,
        publicationId,
        now,
      }),
      cleanupUnreferencedMutationStatement(config.database, intent),
    ]

    const results = yield* d1Effect(
      "begin_publication",
      () => config.database.batch<BegunHeadRow>(statements),
    )

    const begunResult = results[2]
    const begunUnknown = begunResult?.results[0]

    if (begunUnknown !== undefined) {
      const begun = yield* decodeCoordinatorRow(
        BegunHeadRowSchema,
        begunUnknown,
        "begun head",
        "begin_publication",
      )

      yield* d1Effect("begin_publication", () =>
        config.database.prepare(
          `UPDATE document_graph_projection_publications
           SET status = 'superseded', updated_at = ?
           WHERE index_generation = ?
             AND document_key = ? AND projection_id = ?
             AND generation < ? AND status = 'pending'`,
        ).bind(
          now,
          config.indexGeneration,
          intent.key.documentKey,
          intent.key.projection,
          begun.pending_generation,
        ).run().then(() => undefined))

      return {
        _tag: "Publish",
        lease: leaseFromRow(intent, begun),
      } as const
    }

    const digest = yield* d1Effect("begin_publication", () =>
      strongSession(config.database).prepare(
        `SELECT payload_digest
         FROM document_graph_projection_mutations
         WHERE mutation_id = ?`,
      ).bind(intent.mutationId).first<MutationDigestRow>())

    const persistedDigest = digest === null
      ? null
      : yield* decodeCoordinatorRow(
          MutationDigestRowSchema,
          digest,
          "mutation digest",
          "begin_publication",
        )

    if (persistedDigest !== null &&
      persistedDigest.payload_digest !== intent.payloadDigest) {
      return yield* coordinatorFailure(
        "begin_publication",
        "Mutation ID was reused with a different payload",
        "invalid_stored_state",
      )
    }

    const [afterLookup] = yield* loadHeads([intent.key])
    const after = afterLookup?.head ?? Option.none()

    if (Option.isSome(after) && sameActiveIntent(after.value, intent)) {
      return {
        _tag: "AlreadyCommitted",
        outcome: yield* loadCommittedOutcome(intent),
      } as const
    }

    if (Option.isSome(after) && samePendingIntent(after.value, intent)) {
      const pending = Option.getOrThrow(after.value.pending)

      return {
        _tag: "Publish",
        lease: {
          intent,
          publicationId: pending.publicationId,
          generation: pending.generation,
          slotHighWater: pending.slotHighWater,
        },
      } as const
    }

    if (Option.isSome(after) && Option.isSome(after.value.pending)) {
      return yield* coordinatorFailure(
        "begin_publication",
        "A different publication is already pending",
        "publication_in_progress",
      )
    }

    if (Option.isSome(after) &&
      activeMatchesExpectedToken(after.value, intent) &&
      after.value.slotHighWater > intent.slotHighWater) {
      return yield* stalePublicationPlan(intent, after.value.slotHighWater)
    }

    if (Option.isSome(after) &&
      after.value.slotHighWater > intent.maximumSlotHighWater) {
      return yield* coordinatorFailure(
        "begin_publication",
        "The inherited slot high-water exceeds the provider closure bound",
        "capacity_exceeded",
      )
    }

    return yield* new ProjectionIndexConflict({
      documentKey: intent.key.documentKey,
      projection: intent.key.projection,
    })
  })

  const finalizePublication: ProjectionPublicationCoordinatorService[
    "finalizePublication"
  ] = (lease) => Effect.gen(function*() {
    const activeStatus = lease.intent._tag === "Replace"
      ? "revision"
      : "deleted"

    const activeToken = lease.intent._tag === "Replace"
      ? lease.intent.snapshot.token
      : null

    const now = Date.now()

    const results = yield* d1Effect("finalize_publication", () =>
      config.database.batch([
        config.database.prepare(
          `UPDATE document_graph_projection_heads
           SET head_version = head_version + 1,
             active_mutation_id = ?, active_token = ?, active_status = ?,
             pending_mutation_id = NULL, pending_payload_digest = NULL,
             pending_publication_id = NULL, pending_generation = NULL,
             pending_operation = NULL, pending_slot_high_water = NULL,
             pending_lease_expires_at = NULL, updated_at = ?
           WHERE index_generation = ?
             AND document_key = ? AND projection_id = ?
             AND pending_mutation_id = ? AND pending_payload_digest = ?
             AND pending_publication_id = ? AND pending_generation = ?
           RETURNING document_key`,
        ).bind(
          lease.intent.mutationId,
          activeToken,
          activeStatus,
          now,
          config.indexGeneration,
          lease.intent.key.documentKey,
          lease.intent.key.projection,
          lease.intent.mutationId,
          lease.intent.payloadDigest,
          lease.publicationId,
          lease.generation,
        ),
        config.database.prepare(
          `UPDATE document_graph_projection_publications
           SET status = 'committed', updated_at = ?
           WHERE publication_id = ? AND index_generation = ?
             AND EXISTS (
               SELECT 1 FROM document_graph_projection_heads AS head
               WHERE head.index_generation = ?
                 AND head.document_key = ? AND head.projection_id = ?
                 AND head.active_mutation_id = ?
                 AND head.pending_mutation_id IS NULL
             )`,
        ).bind(
          now,
          lease.publicationId,
          config.indexGeneration,
          config.indexGeneration,
          lease.intent.key.documentKey,
          lease.intent.key.projection,
          lease.intent.mutationId,
        ),
        prunePublicationHistoryStatement({
          database: config.database,
          indexGeneration: config.indexGeneration,
          key: lease.intent.key,
          retainedPublicationHistory,
        }),
        cleanupProjectionMutationsStatement(config.database, lease.intent.key),
      ]))

    if ((results[0]?.results.length ?? 0) > 0) return publicationOutcomeFor(lease.intent)

    const [lookup] = yield* loadHeads([lease.intent.key])
    const head = lookup?.head ?? Option.none()

    if (Option.isSome(head) && sameActiveIntent(head.value, lease.intent)) {
      return publicationOutcomeFor(lease.intent)
    }

    return yield* new ProjectionPublicationSuperseded({
      documentKey: lease.intent.key.documentKey,
      projection: lease.intent.key.projection,
    })
  })

  const supersedePublication: ProjectionPublicationCoordinatorService[
    "supersedePublication"
  ] = (lease) => d1Effect("supersede_publication", async () => {
    const now = Date.now()
    await config.database.batch([
      config.database.prepare(
        `UPDATE document_graph_projection_heads
         SET pending_mutation_id = NULL, pending_payload_digest = NULL,
           pending_publication_id = NULL, pending_generation = NULL,
           pending_operation = NULL, pending_slot_high_water = NULL,
           pending_lease_expires_at = NULL, updated_at = ?
         WHERE index_generation = ?
           AND document_key = ? AND projection_id = ?
           AND pending_publication_id = ? AND pending_generation = ?`,
      ).bind(
        now,
        config.indexGeneration,
        lease.intent.key.documentKey,
        lease.intent.key.projection,
        lease.publicationId,
        lease.generation,
      ),
      config.database.prepare(
        `UPDATE document_graph_projection_publications
         SET status = 'superseded', updated_at = ?
         WHERE publication_id = ? AND index_generation = ?
           AND status = 'pending'`,
      ).bind(now, lease.publicationId, config.indexGeneration),
      prunePublicationHistoryStatement({
        database: config.database,
        indexGeneration: config.indexGeneration,
        key: lease.intent.key,
        retainedPublicationHistory,
      }),
      cleanupProjectionMutationsStatement(config.database, lease.intent.key),
    ])
  })

  const listStaleRevisions: ProjectionPublicationCoordinatorService[
    "listStaleRevisions"
  ] = (input) => d1Effect("list_stale", async () => {
    const result = await strongSession(config.database).prepare(
      `SELECT head.document_key, head.projection_id,
         mutation.document_kind, mutation.projection_version
       FROM document_graph_projection_heads AS head
       INNER JOIN document_graph_projection_mutations AS mutation
         ON mutation.mutation_id = head.active_mutation_id
       WHERE head.index_generation = ?
         AND head.active_status = 'revision' AND mutation.graph_id = ?
       ORDER BY head.document_key, head.projection_id`,
    ).bind(config.indexGeneration, input.graph).all<StaleRow>()

    return result.results
      .map((row) => decodeRow(StaleRowSchema, row, "stale revision"))
      .filter((row) => !input.registered.some((registered) =>
        registered.documentKind === row.document_kind &&
        registered.projection === row.projection_id &&
        (registered.projectionVersion === undefined ||
          registered.projectionVersion === row.projection_version)))
      .map((row) => ({
        documentKey: row.document_key,
        projection: row.projection_id,
      }))
  })

  return {
    indexGeneration: config.indexGeneration,
    loadRevisions,
    loadHeads,
    beginPublication,
    finalizePublication,
    supersedePublication,
    listStaleRevisions,
  }
}

/** Provide a D1-backed CAS, inventory, and publication journal coordinator. */
export const d1ProjectionPublicationCoordinator = (
  config: D1ProjectionPublicationConfig,
): Layer.Layer<ProjectionPublicationCoordinator> =>
  Layer.succeed(
    ProjectionPublicationCoordinator,
    makeCoordinator(config),
  )
