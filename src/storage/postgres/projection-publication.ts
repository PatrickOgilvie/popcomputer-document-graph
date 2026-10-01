import { Effect, Layer, Option, Schema } from "effect"
import type { Pool, PoolClient } from "pg"
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
  IndexRevisionTokenSchema,
  ProjectionIndexConflict,
  type IndexedChunkSummary,
  type ProjectionIndexKey,
  type ProjectionRevisionLookup,
} from "../../indexing/projection-index.js"
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
  type ProjectionPublicationHeadLookup,
  type ProjectionPublicationIntent,
  type ProjectionPublicationLease,
  type ProjectionPublicationOutcome,
} from "../../indexing/projection-publication.js"
import {
  activeMatchesExpectedToken,
  publicationOutcomeFor,
  sameActiveIntent,
  samePendingIntent,
  stalePublicationPlan,
} from "../publication-head.js"
import { queryRows } from "./connection.js"
import {
  DefaultPostgresSchema,
  PostgresSchemaNameSchema,
} from "./schema-name.js"

/** PostgreSQL settings for durable remote-index publication coordination. */
export interface PostgresProjectionPublicationConfig {
  /** Pool whose connections this coordinator borrows for its transactions. */
  readonly pool: Pool
  /** Schema holding migration 0006; defaults to `honertia_document_graph`. */
  readonly schema?: string | undefined
  /** Immutable physical index generation represented by these heads. */
  readonly indexGeneration: string
  /**
   * Diagnostic deadline for an unreconciled publication; defaults to 60
   * seconds. Expiration never transfers publication authority automatically.
   */
  readonly publicationLeaseMilliseconds?: number | undefined
  /** Completed journal entries retained per projection; defaults to 32. */
  readonly retainedPublicationHistory?: number | undefined
}

const NonNegativeIntegerSchema = Schema.Number.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0),
)

// bigint columns arrive as decimal text and must stay exact JavaScript integers.
const StoredCounterSchema = Schema.FiniteFromString.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0),
  Schema.makeFilter(Number.isSafeInteger, { title: "SafeInteger" }),
)

const HeadRowSchema = Schema.Struct({
  document_key: DocumentKeySchema,
  projection_id: Schema.String,
  last_allocated_generation: StoredCounterSchema,
  slot_high_water: NonNegativeIntegerSchema,
  active_status: Schema.Literals(["never", "revision", "deleted"]),
  active_token: Schema.NullOr(IndexRevisionTokenSchema),
  active_mutation_id: Schema.NullOr(ProjectionMutationIdSchema),
  active_payload_digest: Schema.NullOr(ProjectionPayloadDigestSchema),
  pending_mutation_id: Schema.NullOr(ProjectionMutationIdSchema),
  pending_payload_digest: Schema.NullOr(ProjectionPayloadDigestSchema),
  pending_publication_id: Schema.NullOr(ProjectionPublicationIdSchema),
  pending_generation: Schema.NullOr(StoredCounterSchema),
  pending_operation: Schema.NullOr(Schema.Literals(["replace", "delete"])),
  pending_slot_high_water: Schema.NullOr(NonNegativeIntegerSchema),
})

const RequestedHeadRowSchema = Schema.Struct({
  ...HeadRowSchema.fields,
  request_ordinal: NonNegativeIntegerSchema,
})

const RevisionRowSchema = Schema.Struct({
  request_ordinal: NonNegativeIntegerSchema,
  active_token: IndexRevisionTokenSchema,
  revision_hash: ProjectionRevisionHashSchema,
  embedding_profile_id: EmbeddingProfileIdSchema,
  embedding_profile_version: EmbeddingProfileVersionSchema,
  embedding_dimensions: EmbeddingDimensionsSchema,
  chunk_id: Schema.NullOr(ChunkIdSchema),
  content_hash: Schema.NullOr(ContentHashSchema),
})

const MutationDigestRowSchema = Schema.Struct({
  payload_digest: ProjectionPayloadDigestSchema,
})

const MutationOutcomeRowSchema = Schema.Struct({
  operation: Schema.Literals(["replace", "delete"]),
  next_token: Schema.NullOr(IndexRevisionTokenSchema),
  commit_inserted: NonNegativeIntegerSchema,
  commit_updated: NonNegativeIntegerSchema,
  commit_deleted: NonNegativeIntegerSchema,
  deletion_revisions: NonNegativeIntegerSchema,
  deletion_chunks: NonNegativeIntegerSchema,
})

const BegunHeadRowSchema = Schema.Struct({
  pending_publication_id: ProjectionPublicationIdSchema,
  pending_generation: StoredCounterSchema.pipe(
    Schema.decodeTo(ProjectionPublicationGenerationSchema),
  ),
})

const StaleRowSchema = Schema.Struct({
  document_key: DocumentKeySchema,
  projection_id: Schema.String,
  document_kind: Schema.String,
  projection_version: Schema.String,
})

/** Stored journal state that no longer satisfies the coordinator's schema. */
class InvalidStoredPublicationState extends Error {}

const decodeStored = <S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- This persisted PostgreSQL row is immediately decoded through the supplied schema.
  row: unknown,
  label: string,
): S["Type"] => {
  try {
    return Schema.decodeUnknownSync(schema)(row, { onExcessProperty: "error" })
  } catch (cause) {
    throw new InvalidStoredPublicationState(
      `PostgreSQL returned an invalid ${label}`,
      { cause },
    )
  }
}

const coordinatorFailure = (
  operation: ProjectionPublicationCoordinatorFailed["operation"],
  cause: unknown,
  reason: ProjectionPublicationCoordinatorFailed["reason"] = "unavailable",
): ProjectionPublicationCoordinatorFailed =>
  new ProjectionPublicationCoordinatorFailed({ operation, reason, cause })

const postgresEffect = <A>(
  operation: ProjectionPublicationCoordinatorFailed["operation"],
  run: () => Promise<A>,
): Effect.Effect<A, ProjectionPublicationCoordinatorFailed> =>
  Effect.tryPromise({
    try: run,
    catch: (cause) => coordinatorFailure(
      operation,
      cause,
      cause instanceof InvalidStoredPublicationState
        ? "invalid_stored_state"
        : "unavailable",
    ),
  })

/** One transaction body's value and whether its writes should persist. */
interface TransactionResult<A> {
  readonly commit: boolean
  readonly value: A
}

const inTransaction = async <A>(
  pool: Pool,
  body: (client: PoolClient) => Promise<TransactionResult<A>>,
): Promise<A> => {
  const client = await pool.connect()

  try {
    await client.query("BEGIN")

    try {
      const result = await body(client)
      await client.query(result.commit ? "COMMIT" : "ROLLBACK")

      return result.value
    } catch (cause) {
      try {
        await client.query("ROLLBACK")
      } catch {
        // Preserve the failure that caused the rollback.
      }

      throw cause
    }
  } finally {
    client.release()
  }
}

const headFromRow = (
  row: typeof HeadRowSchema.Type,
): ProjectionPublicationHead => {
  const active = ((): ProjectionPublicationHead["active"] => {
    switch (row.active_status) {
      case "never":
        return { _tag: "NeverPublished" }
      case "deleted":
        return { _tag: "Deleted" }
      case "revision":
        if (row.active_token === null) {
          throw new InvalidStoredPublicationState(
            "PostgreSQL returned a revision head without an active token",
          )
        }

        return { _tag: "Revision", token: row.active_token }
    }
  })()

  const pendingFields = [
    row.pending_mutation_id,
    row.pending_payload_digest,
    row.pending_publication_id,
    row.pending_generation,
    row.pending_operation,
    row.pending_slot_high_water,
  ]

  if (
    !pendingFields.every((field) => field === null) &&
    pendingFields.some((field) => field === null)
  ) {
    throw new InvalidStoredPublicationState(
      "PostgreSQL returned an incomplete pending publication",
    )
  }

  const pending: ProjectionPublicationHead["pending"] =
    row.pending_mutation_id === null || row.pending_payload_digest === null ||
    row.pending_publication_id === null || row.pending_generation === null ||
    row.pending_operation === null || row.pending_slot_high_water === null
      ? Option.none()
      : Option.some({
          mutationId: row.pending_mutation_id,
          payloadDigest: row.pending_payload_digest,
          publicationId: row.pending_publication_id,
          generation: Schema.decodeSync(ProjectionPublicationGenerationSchema)(
            row.pending_generation,
          ),
          slotHighWater: row.pending_slot_high_water,
          operation: row.pending_operation,
        })

  return {
    key: { documentKey: row.document_key, projection: row.projection_id },
    lastAllocatedGeneration: row.last_allocated_generation,
    slotHighWater: row.slot_high_water,
    active,
    activeMutationId: Option.fromNullOr(row.active_mutation_id),
    activePayloadDigest: Option.fromNullOr(row.active_payload_digest),
    pending,
  }
}

/** Decision reached while the head row is locked inside one transaction. */
type BeginDecision =
  | { readonly _tag: "Committed"; readonly outcome: ProjectionPublicationOutcome }
  | { readonly _tag: "Publish"; readonly lease: ProjectionPublicationLease }
  | { readonly _tag: "InProgress" }
  | { readonly _tag: "Stale"; readonly currentSlotHighWater: number }
  | { readonly _tag: "InheritedCapacityExceeded" }
  | { readonly _tag: "Conflict" }
  | { readonly _tag: "MutationReused" }

const makeCoordinator = (
  config: PostgresProjectionPublicationConfig,
): ProjectionPublicationCoordinatorService => {
  if (
    config.indexGeneration.trim().length === 0 ||
    config.indexGeneration.length > 256
  ) {
    throw new Error("indexGeneration must contain between 1 and 256 characters")
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

  const schema = Schema.decodeSync(PostgresSchemaNameSchema)(
    config.schema ?? DefaultPostgresSchema,
  )

  const namespace = `"${schema}"`
  const mutations = `${namespace}."projection_mutations"`
  const mutationChunks = `${namespace}."projection_mutation_chunks"`
  const heads = `${namespace}."projection_heads"`
  const publications = `${namespace}."projection_publications"`
  const { pool, indexGeneration } = config

  const headColumns = `h.document_key, h.projection_id,
    h.last_allocated_generation::text AS last_allocated_generation,
    h.slot_high_water, h.active_status, h.active_token, h.active_mutation_id,
    active.payload_digest AS active_payload_digest,
    h.pending_mutation_id, h.pending_payload_digest, h.pending_publication_id,
    h.pending_generation::text AS pending_generation, h.pending_operation,
    h.pending_slot_high_water`

  const keyArrays = (keys: ReadonlyArray<ProjectionIndexKey>) => [
    keys.map((key) => key.documentKey),
    keys.map((key) => key.projection),
  ]

  const loadHeads: ProjectionPublicationCoordinatorService["loadHeads"] =
    (keys) => postgresEffect("load_heads", async () => {
      const rows = await queryRows(
        pool,
        `WITH requested AS (
           SELECT document_key, projection_id,
                  (request_ordinal - 1)::integer AS request_ordinal
           FROM unnest($1::text[], $2::text[]) WITH ORDINALITY
             AS request(document_key, projection_id, request_ordinal)
         )
         SELECT requested.request_ordinal, ${headColumns}
         FROM requested
         INNER JOIN ${heads} AS h
           ON h.index_generation = $3
          AND h.document_key = requested.document_key
          AND h.projection_id = requested.projection_id
         LEFT JOIN ${mutations} AS active
           ON active.mutation_id = h.active_mutation_id
         ORDER BY requested.request_ordinal`,
        [...keyArrays(keys), indexGeneration],
      )

      const byOrdinal = new Map<number, ProjectionPublicationHead>()

      for (const unknownRow of rows) {
        const { request_ordinal, ...row } = decodeStored(
          RequestedHeadRowSchema,
          unknownRow,
          "publication head",
        )

        byOrdinal.set(request_ordinal, headFromRow(row))
      }

      return keys.map((key, ordinal): ProjectionPublicationHeadLookup => ({
        key,
        head: Option.fromUndefinedOr(byOrdinal.get(ordinal)),
      }))
    })

  const loadRevisions: ProjectionPublicationCoordinatorService["loadRevisions"] =
    (keys) => postgresEffect("load_revisions", async () => {
      const rows = await queryRows(
        pool,
        `WITH requested AS (
           SELECT document_key, projection_id,
                  (request_ordinal - 1)::integer AS request_ordinal
           FROM unnest($1::text[], $2::text[]) WITH ORDINALITY
             AS request(document_key, projection_id, request_ordinal)
         )
         SELECT requested.request_ordinal, h.active_token,
                mutation.revision_hash, mutation.embedding_profile_id,
                mutation.embedding_profile_version,
                mutation.embedding_dimensions, chunk.chunk_id,
                chunk.content_hash
         FROM requested
         INNER JOIN ${heads} AS h
           ON h.index_generation = $3
          AND h.document_key = requested.document_key
          AND h.projection_id = requested.projection_id
          AND h.active_status = 'revision'
         INNER JOIN ${mutations} AS mutation
           ON mutation.mutation_id = h.active_mutation_id
         LEFT JOIN ${mutationChunks} AS chunk
           ON chunk.mutation_id = mutation.mutation_id
         ORDER BY requested.request_ordinal, chunk.ordinal`,
        [...keyArrays(keys), indexGeneration],
      )

      const grouped = new Map<number, Array<typeof RevisionRowSchema.Type>>()

      for (const unknownRow of rows) {
        const row = decodeStored(RevisionRowSchema, unknownRow, "active revision")
        const group = grouped.get(row.request_ordinal) ?? []
        group.push(row)
        grouped.set(row.request_ordinal, group)
      }

      return keys.map((key, ordinal): ProjectionRevisionLookup => {
        const rows = grouped.get(ordinal)
        const first = rows?.[0]

        if (rows === undefined || first === undefined) {
          return { key, revision: Option.none() }
        }

        const chunks = rows.map((row): IndexedChunkSummary => {
          if (row.chunk_id === null || row.content_hash === null) {
            throw new InvalidStoredPublicationState(
              "PostgreSQL returned an active revision without its chunks",
            )
          }

          return { chunkId: row.chunk_id, contentHash: row.content_hash }
        })

        const [firstChunk, ...remainingChunks] = chunks

        if (firstChunk === undefined) {
          throw new InvalidStoredPublicationState(
            "PostgreSQL returned an active revision without chunks",
          )
        }

        return {
          key,
          revision: Option.some({
            token: first.active_token,
            revisionHash: first.revision_hash,
            embeddingProfile: {
              id: first.embedding_profile_id,
              version: first.embedding_profile_version,
              dimensions: first.embedding_dimensions,
            },
            chunks: [firstChunk, ...remainingChunks],
          }),
        }
      })
    })

  const stageMutation = async (
    client: PoolClient,
    intent: ProjectionPublicationIntent,
  ): Promise<boolean> => {
    const replacement = intent._tag === "Replace" ? intent : undefined

    await client.query(
      `INSERT INTO ${mutations} (
         mutation_id, document_key, projection_id, payload_digest, operation,
         expected_token, next_token, revision_hash, embedding_profile_id,
         embedding_profile_version, embedding_dimensions, graph_id,
         document_kind, projection_version, live_slot_count,
         required_slot_high_water, commit_inserted, commit_updated,
         commit_deleted, deletion_revisions, deletion_chunks
       ) VALUES (
         $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
         $16, $17, $18, $19, $20, $21
       ) ON CONFLICT (mutation_id) DO NOTHING`,
      [
        intent.mutationId,
        intent.key.documentKey,
        intent.key.projection,
        intent.payloadDigest,
        intent._tag === "Replace" ? "replace" : "delete",
        Option.getOrNull(intent.expectedToken),
        replacement?.snapshot.token ?? null,
        replacement?.snapshot.revisionHash ?? null,
        replacement?.snapshot.embeddingProfile.id ?? null,
        replacement?.snapshot.embeddingProfile.version ?? null,
        replacement?.snapshot.embeddingProfile.dimensions ?? null,
        replacement?.catalog.graph ?? null,
        replacement?.catalog.documentKind ?? null,
        replacement?.catalog.projectionVersion ?? null,
        replacement?.liveSlotCount ?? 0,
        replacement?.requiredSlotHighWater ?? 0,
        replacement?.commit.inserted ?? 0,
        replacement?.commit.updated ?? 0,
        replacement?.commit.deleted ?? 0,
        intent._tag === "Delete" ? intent.deletion.deletedRevisions : 0,
        intent._tag === "Delete" ? intent.deletion.deletedChunks : 0,
      ],
    )

    const [stored] = await queryRows(
      client,
      `SELECT payload_digest FROM ${mutations} WHERE mutation_id = $1`,
      [intent.mutationId],
    )

    const digest = decodeStored(MutationDigestRowSchema, stored, "mutation digest")

    if (digest.payload_digest !== intent.payloadDigest) return false

    if (replacement !== undefined) {
      await client.query(
        `INSERT INTO ${mutationChunks}
           (mutation_id, ordinal, chunk_id, content_hash)
         SELECT $1, (chunk.ordinal - 1)::integer, chunk.chunk_id,
                chunk.content_hash
         FROM unnest($2::text[], $3::text[]) WITH ORDINALITY
           AS chunk(chunk_id, content_hash, ordinal)
         ON CONFLICT (mutation_id, ordinal) DO NOTHING`,
        [
          intent.mutationId,
          replacement.snapshot.chunks.map((chunk) => chunk.chunkId),
          replacement.snapshot.chunks.map((chunk) => chunk.contentHash),
        ],
      )
    }

    return true
  }

  const lockHead = async (
    client: PoolClient,
    key: ProjectionIndexKey,
  ): Promise<ProjectionPublicationHead> => {
    await client.query(
      `INSERT INTO ${heads} (index_generation, document_key, projection_id)
       VALUES ($1, $2, $3)
       ON CONFLICT (index_generation, document_key, projection_id) DO NOTHING`,
      [indexGeneration, key.documentKey, key.projection],
    )

    const [row] = await queryRows(
      client,
      `SELECT ${headColumns}
       FROM ${heads} AS h
       LEFT JOIN ${mutations} AS active
         ON active.mutation_id = h.active_mutation_id
       WHERE h.index_generation = $1
         AND h.document_key = $2 AND h.projection_id = $3
       FOR UPDATE OF h`,
      [indexGeneration, key.documentKey, key.projection],
    )

    return headFromRow(decodeStored(HeadRowSchema, row, "publication head"))
  }

  const committedOutcome = async (
    client: PoolClient,
    intent: ProjectionPublicationIntent,
  ): Promise<BeginDecision> => {
    const [unknownRow] = await queryRows(
      client,
      `SELECT operation, next_token, commit_inserted, commit_updated,
              commit_deleted, deletion_revisions, deletion_chunks
       FROM ${mutations}
       WHERE mutation_id = $1 AND payload_digest = $2`,
      [intent.mutationId, intent.payloadDigest],
    )

    if (unknownRow === undefined) return { _tag: "Conflict" }
    const row = decodeStored(MutationOutcomeRowSchema, unknownRow, "committed outcome")

    if (row.operation === "delete" && intent._tag === "Delete") {
      return {
        _tag: "Committed",
        outcome: {
          _tag: "Deleted",
          deletion: {
            deletedRevisions: row.deletion_revisions,
            deletedChunks: row.deletion_chunks,
          },
        },
      }
    }

    if (row.operation !== "replace" || intent._tag !== "Replace" || row.next_token === null) {
      throw new InvalidStoredPublicationState(
        "PostgreSQL returned an inconsistent committed outcome",
      )
    }

    return {
      _tag: "Committed",
      outcome: {
        _tag: "Replaced",
        commit: {
          token: row.next_token,
          inserted: row.commit_inserted,
          updated: row.commit_updated,
          deleted: row.commit_deleted,
        },
      },
    }
  }

  const allocateLease = async (
    client: PoolClient,
    intent: ProjectionPublicationIntent,
    head: ProjectionPublicationHead,
  ): Promise<ProjectionPublicationLease> => {
    // Replanning the pending mutation keeps its publication identity, so any
    // in-flight provider write for it remains the same fenced generation.
    const resumed = Option.getOrUndefined(head.pending)
    const generation = resumed?.generation ?? head.lastAllocatedGeneration + 1

    const [unknownBegun] = await queryRows(
      client,
      `UPDATE ${heads}
       SET last_allocated_generation = $4,
           slot_high_water = $5,
           pending_mutation_id = $6,
           pending_payload_digest = $7,
           pending_publication_id = COALESCE($8, gen_random_uuid()::text),
           pending_generation = $4,
           pending_operation = $9,
           pending_slot_high_water = $5,
           pending_lease_expires_at = COALESCE(
             pending_lease_expires_at,
             now() + make_interval(secs => $10::double precision)
           ),
           updated_at = now()
       WHERE index_generation = $1 AND document_key = $2 AND projection_id = $3
       RETURNING pending_publication_id,
                 pending_generation::text AS pending_generation`,
      [
        indexGeneration,
        intent.key.documentKey,
        intent.key.projection,
        generation,
        intent.slotHighWater,
        intent.mutationId,
        intent.payloadDigest,
        resumed?.publicationId ?? null,
        intent._tag === "Replace" ? "replace" : "delete",
        leaseMilliseconds / 1_000,
      ],
    )

    const begun = decodeStored(BegunHeadRowSchema, unknownBegun, "begun head")

    await client.query(
      `INSERT INTO ${publications} (
         publication_id, mutation_id, index_generation, document_key,
         projection_id, generation, slot_high_water, status
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending')
       ON CONFLICT (publication_id) DO UPDATE SET updated_at = now()`,
      [
        begun.pending_publication_id,
        intent.mutationId,
        indexGeneration,
        intent.key.documentKey,
        intent.key.projection,
        begun.pending_generation,
        intent.slotHighWater,
      ],
    )

    await client.query(
      `UPDATE ${publications}
       SET status = 'superseded', updated_at = now()
       WHERE index_generation = $1 AND document_key = $2
         AND projection_id = $3 AND generation < $4 AND status = 'pending'`,
      [
        indexGeneration,
        intent.key.documentKey,
        intent.key.projection,
        begun.pending_generation,
      ],
    )

    return {
      intent,
      publicationId: begun.pending_publication_id,
      generation: begun.pending_generation,
      slotHighWater: intent.slotHighWater,
    }
  }

  const begin = (
    client: PoolClient,
    intent: ProjectionPublicationIntent,
  ): Promise<TransactionResult<BeginDecision>> => (async () => {
    const rollback = (value: BeginDecision) => ({ commit: false, value })

    if (!await stageMutation(client, intent)) {
      return rollback({ _tag: "MutationReused" })
    }

    const head = await lockHead(client, intent.key)

    if (sameActiveIntent(head, intent)) {
      return rollback(await committedOutcome(client, intent))
    }

    if (
      intent._tag === "Delete" &&
      head.active._tag === "Deleted" &&
      Option.isNone(head.pending)
    ) {
      return rollback({
        _tag: "Committed",
        outcome: {
          _tag: "Deleted",
          deletion: { deletedRevisions: 0, deletedChunks: 0 },
        },
      })
    }

    const pending = Option.getOrUndefined(head.pending)

    if (pending !== undefined && samePendingIntent(head, intent)) {
      return rollback({
        _tag: "Publish",
        lease: {
          intent,
          publicationId: pending.publicationId,
          generation: pending.generation,
          slotHighWater: pending.slotHighWater,
        },
      })
    }

    const pendingIsThisMutation = pending !== undefined &&
      pending.mutationId === intent.mutationId &&
      pending.payloadDigest === intent.payloadDigest

    const expected = activeMatchesExpectedToken(head, intent)

    if (pending === undefined && expected && head.slotHighWater > intent.slotHighWater) {
      return rollback({ _tag: "Stale", currentSlotHighWater: head.slotHighWater })
    }

    if (
      (pending === undefined || pendingIsThisMutation) &&
      expected &&
      head.slotHighWater <= intent.slotHighWater &&
      intent.slotHighWater <= intent.maximumSlotHighWater
    ) {
      const resumable = pendingIsThisMutation
        ? head
        : { ...head, pending: Option.none() }

      return {
        commit: true,
        value: { _tag: "Publish", lease: await allocateLease(client, intent, resumable) },
      }
    }

    if (pending !== undefined) return rollback({ _tag: "InProgress" })

    if (expected && head.slotHighWater > intent.slotHighWater) {
      return rollback({ _tag: "Stale", currentSlotHighWater: head.slotHighWater })
    }

    if (head.slotHighWater > intent.maximumSlotHighWater) {
      return rollback({ _tag: "InheritedCapacityExceeded" })
    }

    return rollback({ _tag: "Conflict" })
  })()

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

    if (!Number.isSafeInteger(intent.slotHighWater) || intent.slotHighWater < 0) {
      return yield* coordinatorFailure(
        "begin_publication",
        "The planned slot high-water is invalid",
        "invalid_stored_state",
      )
    }

    const decision = yield* postgresEffect(
      "begin_publication",
      () => inTransaction(pool, (client) => begin(client, intent)),
    )

    switch (decision._tag) {
      case "Committed":
        return { _tag: "AlreadyCommitted", outcome: decision.outcome } as const
      case "Publish":
        return { _tag: "Publish", lease: decision.lease } as const
      case "InProgress":
        return yield* coordinatorFailure(
          "begin_publication",
          "A different publication is already pending",
          "publication_in_progress",
        )
      case "Stale":
        return yield* stalePublicationPlan(intent, decision.currentSlotHighWater)
      case "InheritedCapacityExceeded":
        return yield* coordinatorFailure(
          "begin_publication",
          "The inherited slot high-water exceeds the provider closure bound",
          "capacity_exceeded",
        )
      case "MutationReused":
        return yield* coordinatorFailure(
          "begin_publication",
          "Mutation ID was reused with a different payload",
          "invalid_stored_state",
        )
      case "Conflict":
        return yield* new ProjectionIndexConflict({
          documentKey: intent.key.documentKey,
          projection: intent.key.projection,
        })
    }
  })

  const retireHistory = async (
    client: PoolClient,
    key: ProjectionIndexKey,
  ): Promise<void> => {
    await client.query(
      `DELETE FROM ${publications}
       WHERE publication_id IN (
         SELECT publication_id FROM ${publications}
         WHERE index_generation = $1 AND document_key = $2
           AND projection_id = $3 AND status <> 'pending'
         ORDER BY generation DESC
         OFFSET $4
       )`,
      [indexGeneration, key.documentKey, key.projection, retainedPublicationHistory],
    )

    await client.query(
      `DELETE FROM ${mutations} AS mutation
       WHERE mutation.document_key = $1 AND mutation.projection_id = $2
         AND NOT EXISTS (
           SELECT 1 FROM ${heads} AS h
           WHERE h.active_mutation_id = mutation.mutation_id
              OR h.pending_mutation_id = mutation.mutation_id
         )
         AND NOT EXISTS (
           SELECT 1 FROM ${publications} AS publication
           WHERE publication.mutation_id = mutation.mutation_id
         )`,
      [key.documentKey, key.projection],
    )
  }

  const finalizePublication: ProjectionPublicationCoordinatorService[
    "finalizePublication"
  ] = (lease) => Effect.gen(function*() {
    const { intent } = lease

    const finalized = yield* postgresEffect("finalize_publication", () =>
      inTransaction(pool, async (client) => {
        const updated = await queryRows(
          client,
          `UPDATE ${heads}
           SET head_version = head_version + 1,
               active_mutation_id = $4, active_token = $5, active_status = $6,
               pending_mutation_id = NULL, pending_payload_digest = NULL,
               pending_publication_id = NULL, pending_generation = NULL,
               pending_operation = NULL, pending_slot_high_water = NULL,
               pending_lease_expires_at = NULL, updated_at = now()
           WHERE index_generation = $1
             AND document_key = $2 AND projection_id = $3
             AND pending_mutation_id = $4 AND pending_payload_digest = $7
             AND pending_publication_id = $8 AND pending_generation = $9
           RETURNING document_key`,
          [
            indexGeneration,
            intent.key.documentKey,
            intent.key.projection,
            intent.mutationId,
            intent._tag === "Replace" ? intent.snapshot.token : null,
            intent._tag === "Replace" ? "revision" : "deleted",
            intent.payloadDigest,
            lease.publicationId,
            lease.generation,
          ],
        )

        if (updated.length === 0) {
          const [unknownHead] = await queryRows(
            client,
            `SELECT ${headColumns}
             FROM ${heads} AS h
             LEFT JOIN ${mutations} AS active
               ON active.mutation_id = h.active_mutation_id
             WHERE h.index_generation = $1
               AND h.document_key = $2 AND h.projection_id = $3`,
            [indexGeneration, intent.key.documentKey, intent.key.projection],
          )

          const head = unknownHead === undefined
            ? undefined
            : headFromRow(decodeStored(HeadRowSchema, unknownHead, "publication head"))

          return {
            commit: false,
            value: head !== undefined && sameActiveIntent(head, intent),
          }
        }

        await client.query(
          `UPDATE ${publications}
           SET status = 'committed', updated_at = now()
           WHERE publication_id = $1 AND index_generation = $2`,
          [lease.publicationId, indexGeneration],
        )

        await retireHistory(client, intent.key)

        return { commit: true, value: true }
      }))

    if (finalized) return publicationOutcomeFor(intent)

    return yield* new ProjectionPublicationSuperseded({
      documentKey: intent.key.documentKey,
      projection: intent.key.projection,
    })
  })

  const supersedePublication: ProjectionPublicationCoordinatorService[
    "supersedePublication"
  ] = (lease) => postgresEffect("supersede_publication", () =>
    inTransaction(pool, async (client) => {
      await client.query(
        `UPDATE ${heads}
         SET pending_mutation_id = NULL, pending_payload_digest = NULL,
             pending_publication_id = NULL, pending_generation = NULL,
             pending_operation = NULL, pending_slot_high_water = NULL,
             pending_lease_expires_at = NULL, updated_at = now()
         WHERE index_generation = $1
           AND document_key = $2 AND projection_id = $3
           AND pending_publication_id = $4 AND pending_generation = $5`,
        [
          indexGeneration,
          lease.intent.key.documentKey,
          lease.intent.key.projection,
          lease.publicationId,
          lease.generation,
        ],
      )

      await client.query(
        `UPDATE ${publications}
         SET status = 'superseded', updated_at = now()
         WHERE publication_id = $1 AND index_generation = $2
           AND status = 'pending'`,
        [lease.publicationId, indexGeneration],
      )

      await retireHistory(client, lease.intent.key)

      return { commit: true, value: undefined }
    }))

  const listStaleRevisions: ProjectionPublicationCoordinatorService[
    "listStaleRevisions"
  ] = (input) => postgresEffect("list_stale", async () => {
    const rows = await queryRows(
      pool,
      `SELECT h.document_key, h.projection_id, mutation.document_kind,
              mutation.projection_version
       FROM ${heads} AS h
       INNER JOIN ${mutations} AS mutation
         ON mutation.mutation_id = h.active_mutation_id
       WHERE h.index_generation = $1
         AND h.active_status = 'revision' AND mutation.graph_id = $2
       ORDER BY h.document_key, h.projection_id`,
      [indexGeneration, input.graph],
    )

    return rows
      .map((row) => decodeStored(StaleRowSchema, row, "stale revision"))
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
    indexGeneration,
    loadRevisions,
    loadHeads,
    beginPublication,
    finalizePublication,
    supersedePublication,
    listStaleRevisions,
  }
}

/**
 * Provide a PostgreSQL-backed CAS, inventory, and publication journal
 * coordinator for projection indexes stored outside PostgreSQL.
 *
 * Every state transition runs in its own pool transaction and locks the
 * affected head row, so concurrent publishers serialize per document
 * projection. Apply migration 0006 before using this Layer.
 */
export const postgresProjectionPublicationCoordinator = (
  config: PostgresProjectionPublicationConfig,
): Layer.Layer<ProjectionPublicationCoordinator> =>
  Layer.succeed(ProjectionPublicationCoordinator, makeCoordinator(config))
