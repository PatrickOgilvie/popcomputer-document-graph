import { Effect, Option, Schema } from "effect"
import type { Pool } from "pg"
import {
  ChunkIdSchema,
  ContentHashSchema,
  DocumentKeySchema,
  ProjectionRevisionHashSchema,
} from "../../document/document-identity.js"
import { JsonValueSchema } from "../../document/json-value.js"
import { parseTextSearchPolicy } from "../../document/text-search-policy.js"
import type { DocumentGraphManifest } from "../../graph/document-graph-schema.js"
import {
  EmbeddingDimensionsSchema,
  EmbeddingProfileIdSchema,
  EmbeddingProfileVersionSchema,
} from "../../indexing/embedding-provider.js"
import {
  ProjectionIndexStore,
  type ProjectedChunkRecord,
  type ProjectionIndexConflict,
  type ProjectionIndexKey,
  type ProjectionIndexStoreFailed,
  type ReplaceProjectedRevision,
} from "../../indexing/projection-index.js"
import { queryRows } from "./connection.js"
import {
  DefaultPostgresSchema,
  PostgresSchemaNameSchema,
} from "./schema-name.js"

/** Reading the PostgreSQL projection index failed. */
export class PostgresProjectionIndexReadFailed extends Schema.TaggedError<
  PostgresProjectionIndexReadFailed
>()("PostgresProjectionIndexReadFailed", {
  reason: Schema.Literals(["unavailable", "invalid_stored_state"]),
  cause: Schema.Unknown,
}) {}

/** Position after the last revision of one page, in document-key order. */
export interface PostgresProjectionIndexCursor {
  readonly documentKey: string
  readonly projection: string
}

/**
 * One stored revision with its chunks and vectors, ready to replace the same
 * revision in another index store once the target's expected token is known.
 */
export type StoredProjectedRevision = Omit<ReplaceProjectedRevision, "expectedToken">

/** One page of stored revisions and the cursor that continues after it. */
export interface PostgresProjectionIndexPage {
  readonly revisions: ReadonlyArray<StoredProjectedRevision>
  /** Revisions whose projection no longer appears in the graph manifest. */
  readonly unregistered: ReadonlyArray<ProjectionIndexKey>
  readonly next: Option.Option<PostgresProjectionIndexCursor>
}

const NonNegativeIntegerSchema = Schema.Number.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0),
)

const RevisionRowSchema = Schema.Struct({
  document_key: DocumentKeySchema,
  projection_id: Schema.String,
  graph_id: Schema.String,
  document_kind: Schema.String,
  encoded_document_id: JsonValueSchema,
  projection_version: Schema.String,
  revision_hash: ProjectionRevisionHashSchema,
  embedding_profile_id: EmbeddingProfileIdSchema,
  embedding_profile_version: EmbeddingProfileVersionSchema,
  embedding_dimensions: EmbeddingDimensionsSchema,
})

const ChunkRowSchema = Schema.Struct({
  document_key: DocumentKeySchema,
  projection_id: Schema.String,
  chunk_id: ChunkIdSchema,
  content_hash: ContentHashSchema,
  ordinal: NonNegativeIntegerSchema,
  section_key: Schema.String,
  section_index: NonNegativeIntegerSchema,
  section_part: NonNegativeIntegerSchema,
  content: Schema.String,
  embedding_content: Schema.String,
  text_context: Schema.NullOr(Schema.String),
  text_label: Schema.NullOr(Schema.String),
  text_content: Schema.String,
  has_metadata: Schema.Boolean,
  metadata: Schema.NullOr(JsonValueSchema),
  embedding: Schema.Array(Schema.Finite),
})

const decodeRows = <S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  rows: ReadonlyArray<unknown>,
): ReadonlyArray<S["Type"]> => {
  try {
    return rows.map((row) =>
      Schema.decodeUnknownSync(schema)(row, { onExcessProperty: "error" }))
  } catch (cause) {
    throw new PostgresProjectionIndexReadFailed({
      reason: "invalid_stored_state",
      cause,
    })
  }
}

const schemaIdentifier = (schema: string | undefined): string =>
  `"${Schema.decodeSync(PostgresSchemaNameSchema)(schema ?? DefaultPostgresSchema)}"`

const revisionColumns = (alias: string): string =>
  ["document_key", "projection_id", "graph_id", "document_kind",
    "encoded_document_id", "projection_version", "revision_hash",
    "embedding_profile_id", "embedding_profile_version", "embedding_dimensions"]
    .map((column) => `${alias}.${column}`)
    .join(", ")

const readFailure = (cause: unknown): PostgresProjectionIndexReadFailed =>
  cause instanceof PostgresProjectionIndexReadFailed
    ? cause
    : new PostgresProjectionIndexReadFailed({ reason: "unavailable", cause })

/**
 * Load the chunks and vectors of stored revisions. Text policy is not stored
 * with revisions, so it comes from the graph manifest; revisions of
 * projections the manifest no longer registers are reported as unregistered.
 */
const loadStoredRevisions = async (
  pool: Pool,
  schema: string,
  manifest: DocumentGraphManifest,
  revisions: ReadonlyArray<typeof RevisionRowSchema.Type>,
): Promise<{
  readonly revisions: ReadonlyArray<StoredProjectedRevision>
  readonly unregistered: ReadonlyArray<ProjectionIndexKey>
}> => {
  if (revisions.length === 0) return { revisions: [], unregistered: [] }

  const chunks = decodeRows(ChunkRowSchema, await queryRows(
    pool,
    `SELECT c.document_key, c.projection_id, c.chunk_id, c.content_hash,
            c.ordinal, c.section_key, c.section_index, c.section_part,
            c.content, c.embedding_content, c.text_context, c.text_label,
            c.text_content, c.has_metadata, c.metadata, c.embedding
     FROM unnest($1::text[], $2::text[]) AS requested(document_key, projection_id)
     INNER JOIN ${schema}."projected_chunks" AS c
       ON c.document_key = requested.document_key
      AND c.projection_id = requested.projection_id
     ORDER BY c.document_key, c.projection_id, c.ordinal`,
    [revisions.map((row) => row.document_key), revisions.map((row) => row.projection_id)],
  ))

  const chunksByRevision = new Map<string, Array<typeof ChunkRowSchema.Type>>()

  for (const chunk of chunks) {
    const revisionKey = `${chunk.document_key}:${chunk.projection_id}`
    const group = chunksByRevision.get(revisionKey) ?? []
    group.push(chunk)
    chunksByRevision.set(revisionKey, group)
  }

  const stored: Array<StoredProjectedRevision> = []
  const unregistered: Array<ProjectionIndexKey> = []

  for (const revision of revisions) {
    const key = { documentKey: revision.document_key, projection: revision.projection_id }

    const projection = manifest.documents
      .find((document) => document.kind === revision.document_kind)
      ?.projections.find((candidate) =>
        candidate.id === revision.projection_id &&
        candidate.version === revision.projection_version)

    const rows = chunksByRevision.get(`${revision.document_key}:${revision.projection_id}`) ?? []

    const records = rows.map((row): ProjectedChunkRecord => ({
      chunkId: row.chunk_id,
      contentHash: row.content_hash,
      ordinal: row.ordinal,
      sectionKey: row.section_key,
      sectionIndex: row.section_index,
      sectionPart: row.section_part,
      content: row.content,
      embeddingContent: row.embedding_content,
      text: {
        context: row.text_context ?? undefined,
        label: row.text_label ?? undefined,
        content: row.text_content,
      },
      metadata: row.has_metadata ? row.metadata ?? null : undefined,
    }))

    const [first, ...rest] = records

    if (projection === undefined || first === undefined) {
      unregistered.push(key)
      continue
    }

    stored.push({
      key,
      encodedTarget: {
        graph: revision.graph_id,
        kind: revision.document_kind,
        id: revision.encoded_document_id,
      },
      projectionVersion: revision.projection_version,
      textPolicy: parseTextSearchPolicy(projection.text),
      revisionHash: revision.revision_hash,
      embeddingProfile: {
        id: revision.embedding_profile_id,
        version: revision.embedding_profile_version,
        dimensions: revision.embedding_dimensions,
      },
      chunks: [first, ...rest],
      embeddings: rows.map((row) => ({ contentHash: row.content_hash, vector: row.embedding })),
    })
  }

  return { revisions: stored, unregistered }
}

/** Revision rows only, without chunks, in document-key order. */
const readRevisionRows = async (
  pool: Pool,
  schema: string,
  graph: string,
  after: PostgresProjectionIndexCursor | null,
  limit: number,
) => decodeRows(RevisionRowSchema, await queryRows(
  pool,
  `SELECT ${revisionColumns("r")}
   FROM ${schema}."projected_revisions" AS r
   WHERE r.graph_id = $1
     AND ($2::text IS NULL OR (r.document_key, r.projection_id) > ($2::text, $3::text))
   ORDER BY r.document_key, r.projection_id
   LIMIT $4`,
  [graph, after?.documentKey ?? null, after?.projection ?? null, limit],
))

/**
 * Read one page of a graph's PostgreSQL projection index, including every
 * chunk's stored vector, in document-key order.
 *
 * Text policy is not stored with revisions, so it comes from the graph
 * manifest; revisions of projections the manifest no longer registers are
 * reported as unregistered instead of being returned.
 */
export const readPostgresProjectionIndexPage = (input: {
  readonly pool: Pool
  readonly schema?: string | undefined
  readonly manifest: DocumentGraphManifest
  readonly after: Option.Option<PostgresProjectionIndexCursor>
  readonly limit: number
}): Effect.Effect<PostgresProjectionIndexPage, PostgresProjectionIndexReadFailed> =>
  Effect.tryPromise({
    try: async () => {
      const schema = schemaIdentifier(input.schema)
      const after = Option.getOrNull(input.after)

      const revisions = await readRevisionRows(input.pool, schema, input.manifest.id, after, input.limit)

      const last = revisions[revisions.length - 1]

      if (last === undefined) {
        return { revisions: [], unregistered: [], next: Option.none() }
      }

      const loaded = await loadStoredRevisions(input.pool, schema, input.manifest, revisions)

      return {
        ...loaded,
        next: Option.some({ documentKey: last.document_key, projection: last.projection_id }),
      }
    },
    catch: readFailure,
  })

const revisionKey = (key: ProjectionIndexKey) => `${key.documentKey}:${key.projection}`

/** Running totals reported after each copied page. */
export interface ProjectionIndexCopyProgress {
  readonly copied: number
  readonly unchanged: number
  readonly unregistered: number
  readonly next: Option.Option<PostgresProjectionIndexCursor>
}

/**
 * Copy a graph's PostgreSQL projection index into the provided
 * {@link ProjectionIndexStore}, reusing every stored vector.
 *
 * Revisions the target already holds with the same revision hash are left
 * alone, and their chunks and vectors are never read, so an interrupted copy
 * resumes cheaply from the start or `after`.
 */
export const copyPostgresProjectionIndex = Effect.fn(
  "PostgresProjectionIndex.copy",
)(function*(input: {
  readonly pool: Pool
  readonly schema?: string | undefined
  readonly manifest: DocumentGraphManifest
  readonly after?: PostgresProjectionIndexCursor | undefined
  /** Revisions read per page; defaults to 100. */
  readonly pageSize?: number
  /** Concurrent replacements within a page; defaults to 8. */
  readonly concurrency?: number
  /** Stop after this many pages, for a partial copy. */
  readonly maximumPages?: number
  readonly onProgress?: (progress: ProjectionIndexCopyProgress) => Effect.Effect<void>
}) {
  const target = yield* ProjectionIndexStore
  const schema = yield* Effect.try({ try: () => schemaIdentifier(input.schema), catch: readFailure })
  let cursor = Option.fromUndefinedOr(input.after)
  let progress: ProjectionIndexCopyProgress = { copied: 0, unchanged: 0, unregistered: 0, next: cursor }

  for (let page = 0; input.maximumPages === undefined || page < input.maximumPages; page += 1) {
    const after = Option.getOrNull(cursor)
    const rows = yield* Effect.tryPromise({
      try: () => readRevisionRows(input.pool, schema, input.manifest.id, after, input.pageSize ?? 100),
      catch: readFailure,
    })
    const [first, ...rest] = rows
    const last = rows[rows.length - 1]

    if (first === undefined || last === undefined) {
      progress = { ...progress, next: Option.none() }
      if (input.onProgress !== undefined) yield* input.onProgress(progress)
      break
    }

    const keyOf = (row: typeof first): ProjectionIndexKey => ({ documentKey: row.document_key, projection: row.projection_id })
    const lookups = yield* target.loadRevisions([keyOf(first), ...rest.map(keyOf)])
    const current = new Map(rows.map((row, index) =>
      [revisionKey(keyOf(row)), lookups[index]?.revision ?? Option.none()] as const))

    // Chunks and vectors are most of a page's bytes, so only revisions the
    // target lacks, or holds at another hash, load them. A resumed or repeated
    // copy reads little more than revision rows.
    const changed = rows.filter((row) => {
      const held = current.get(revisionKey(keyOf(row))) ?? Option.none()
      return !(Option.isSome(held) && held.value.revisionHash === row.revision_hash)
    })

    const loaded = yield* Effect.tryPromise({
      try: () => loadStoredRevisions(input.pool, schema, input.manifest, changed),
      catch: readFailure,
    })

    yield* Effect.forEach(
      loaded.revisions,
      (revision) => target.replaceRevision({
        ...revision,
        expectedToken: Option.map(current.get(revisionKey(revision.key)) ?? Option.none(), (snapshot) => snapshot.token),
      }),
      { concurrency: input.concurrency ?? 8, discard: true },
    )

    progress = {
      copied: progress.copied + loaded.revisions.length,
      unchanged: progress.unchanged + rows.length - changed.length,
      unregistered: progress.unregistered + loaded.unregistered.length,
      next: Option.some({ documentKey: last.document_key, projection: last.projection_id }),
    }

    if (input.onProgress !== undefined) yield* input.onProgress(progress)
    cursor = progress.next
  }

  return progress
})

const ChangeRowSchema = Schema.Struct({
  document_key: DocumentKeySchema,
  projection_id: Schema.String,
  // bigint, kept as text so it round-trips exactly.
  change_sequence: Schema.String.check(Schema.isPattern(/^[0-9]+$/u)),
})

type ChangeRow = typeof ChangeRowSchema.Type

/** A recorded change the target write did not apply; it stays recorded. */
export interface ProjectionIndexMirrorFailure {
  readonly key: ProjectionIndexKey
  readonly error: ProjectionIndexStoreFailed | ProjectionIndexConflict
}

/** Totals from one drain of recorded projection changes. */
export interface ProjectionIndexMirrorProgress {
  /** Revisions written to the target. */
  readonly replaced: number
  /** Revisions deleted from the target because PostgreSQL no longer holds them. */
  readonly deleted: number
  /** Changes the target already reflected. */
  readonly unchanged: number
  /** Changes to projections the manifest no longer registers, cleared unwritten. */
  readonly unregistered: number
  /** Changes left recorded for the next drain because the target write failed. */
  readonly failures: ReadonlyArray<ProjectionIndexMirrorFailure>
  /** False when the drain stopped at `maximumBatches` with changes unread. */
  readonly drained: boolean
}

type MirrorOutcome =
  | { readonly _tag: "replaced" | "deleted" | "unchanged" | "unregistered" }
  | { readonly _tag: "failed"; readonly failure: ProjectionIndexMirrorFailure }

const readChanges = (
  pool: Pool,
  schema: string,
  graph: string,
  after: string | null,
  limit: number,
) =>
  Effect.tryPromise({
    try: async () => decodeRows(ChangeRowSchema, await queryRows(
      pool,
      `SELECT document_key, projection_id, change_sequence::text AS change_sequence
       FROM ${schema}."projection_changes"
       WHERE graph_id = $1 AND ($2::bigint IS NULL OR change_sequence > $2::bigint)
       ORDER BY change_sequence
       LIMIT $3`,
      [graph, after, limit],
    )),
    catch: readFailure,
  })

const readChangedRevisions = (
  pool: Pool,
  schema: string,
  manifest: DocumentGraphManifest,
  changes: ReadonlyArray<ChangeRow>,
) =>
  Effect.tryPromise({
    try: async () => loadStoredRevisions(pool, schema, manifest, decodeRows(RevisionRowSchema, await queryRows(
      pool,
      `SELECT ${revisionColumns("r")}
       FROM unnest($1::text[], $2::text[]) AS requested(document_key, projection_id)
       INNER JOIN ${schema}."projected_revisions" AS r
         ON r.document_key = requested.document_key
        AND r.projection_id = requested.projection_id`,
      [changes.map((change) => change.document_key), changes.map((change) => change.projection_id)],
    ))),
    catch: readFailure,
  })

// A change renewed while the drain ran has a new sequence, so it survives.
const clearChanges = (pool: Pool, schema: string, changes: ReadonlyArray<ChangeRow>) =>
  changes.length === 0 ? Effect.void : Effect.tryPromise({
    try: () => pool.query(
      `DELETE FROM ${schema}."projection_changes" AS c
       USING unnest($1::text[], $2::text[], $3::bigint[])
         AS done(document_key, projection_id, change_sequence)
       WHERE c.document_key = done.document_key
         AND c.projection_id = done.projection_id
         AND c.change_sequence = done.change_sequence`,
      [
        changes.map((change) => change.document_key),
        changes.map((change) => change.projection_id),
        changes.map((change) => change.change_sequence),
      ],
    ),
    catch: readFailure,
  }).pipe(Effect.asVoid)

/**
 * Bring the provided {@link ProjectionIndexStore} up to date with the
 * projection changes PostgreSQL recorded (migration 0007), reusing every
 * stored vector. A changed revision is copied, a deleted one is deleted from
 * the target, and each applied change is cleared.
 *
 * Changes are recorded by a trigger in the writing transaction, so every
 * writer is covered. A failed target write leaves its change recorded and
 * the drain moves on; the next drain retries it. A change renewed while a
 * drain runs is never cleared by it.
 */
export const mirrorPostgresProjectionChanges = Effect.fn(
  "PostgresProjectionIndex.mirror",
)(function*(input: {
  readonly pool: Pool
  readonly schema?: string | undefined
  readonly manifest: DocumentGraphManifest
  /** Changes read per batch; defaults to 100. */
  readonly batchSize?: number
  /** Concurrent target writes within a batch; defaults to 8. */
  readonly concurrency?: number
  /** Stop after this many batches; by default every recorded change is read. */
  readonly maximumBatches?: number
}) {
  const target = yield* ProjectionIndexStore
  const schema = yield* Effect.try({ try: () => schemaIdentifier(input.schema), catch: readFailure })
  const batchSize = input.batchSize ?? 100
  let after: string | null = null
  const totals = { replaced: 0, deleted: 0, unchanged: 0, unregistered: 0 }
  const failures: Array<ProjectionIndexMirrorFailure> = []

  for (let batch = 0; input.maximumBatches === undefined || batch < input.maximumBatches; batch += 1) {
    const changes: ReadonlyArray<ChangeRow> = yield* readChanges(input.pool, schema, input.manifest.id, after, batchSize)
    const [first, ...rest] = changes

    if (first === undefined) return { ...totals, failures, drained: true }

    after = changes[changes.length - 1]?.change_sequence ?? after

    const keys = changes.map((change): ProjectionIndexKey => ({
      documentKey: change.document_key,
      projection: change.projection_id,
    }))

    // Read after the changes, so each copy is at least as new as its change.
    const stored = yield* readChangedRevisions(input.pool, schema, input.manifest, changes)
    const sources = new Map(stored.revisions.map((revision) => [revisionKey(revision.key), revision]))
    const unregistered = new Set(stored.unregistered.map(revisionKey))
    const lookups = yield* target.loadRevisions([
      { documentKey: first.document_key, projection: first.projection_id },
      ...rest.map((change) => ({ documentKey: change.document_key, projection: change.projection_id })),
    ])

    const outcomes = yield* Effect.forEach(keys, (key, index): Effect.Effect<MirrorOutcome> => {
      const current = lookups[index]?.revision ?? Option.none()
      const source = sources.get(revisionKey(key))

      const write: Effect.Effect<MirrorOutcome, ProjectionIndexStoreFailed | ProjectionIndexConflict> =
        unregistered.has(revisionKey(key))
          ? Effect.succeed({ _tag: "unregistered" })
          : source === undefined
            ? Option.isNone(current)
              ? Effect.succeed({ _tag: "unchanged" })
              : target.deleteRevision(key).pipe(Effect.as({ _tag: "deleted" } as const))
            : Option.isSome(current) && current.value.revisionHash === source.revisionHash
              ? Effect.succeed({ _tag: "unchanged" })
              : target.replaceRevision({
                ...source,
                expectedToken: Option.map(current, (snapshot) => snapshot.token),
              }).pipe(Effect.as({ _tag: "replaced" } as const))

      return write.pipe(Effect.catch((error) => Effect.succeed({ _tag: "failed", failure: { key, error } } as const)))
    }, { concurrency: input.concurrency ?? 8 })

    yield* clearChanges(input.pool, schema, changes.filter((_, index) => outcomes[index]?._tag !== "failed"))

    for (const outcome of outcomes) {
      if (outcome._tag === "failed") failures.push(outcome.failure)
      else totals[outcome._tag] += 1
    }

    if (changes.length < batchSize) return { ...totals, failures, drained: true }
  }

  return { ...totals, failures, drained: false }
})
