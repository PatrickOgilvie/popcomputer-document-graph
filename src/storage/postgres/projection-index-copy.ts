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
      const schema = `"${Schema.decodeSync(PostgresSchemaNameSchema)(input.schema ?? DefaultPostgresSchema)}"`
      const after = Option.getOrNull(input.after)

      const revisions = decodeRows(RevisionRowSchema, await queryRows(
        input.pool,
        `SELECT document_key, projection_id, graph_id, document_kind,
                encoded_document_id, projection_version, revision_hash,
                embedding_profile_id, embedding_profile_version,
                embedding_dimensions
         FROM ${schema}."projected_revisions"
         WHERE graph_id = $1
           AND ($2::text IS NULL OR (document_key, projection_id) > ($2::text, $3::text))
         ORDER BY document_key, projection_id
         LIMIT $4`,
        [input.manifest.id, after?.documentKey ?? null, after?.projection ?? null, input.limit],
      ))

      const last = revisions[revisions.length - 1]

      if (last === undefined) {
        return { revisions: [], unregistered: [], next: Option.none() }
      }

      const chunks = decodeRows(ChunkRowSchema, await queryRows(
        input.pool,
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

        const projection = input.manifest.documents
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

      return {
        revisions: stored,
        unregistered,
        next: Option.some({ documentKey: last.document_key, projection: last.projection_id }),
      }
    },
    catch: (cause) => cause instanceof PostgresProjectionIndexReadFailed
      ? cause
      : new PostgresProjectionIndexReadFailed({ reason: "unavailable", cause }),
  })

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
 * alone, so an interrupted copy resumes cheaply from the start or `after`.
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
  let cursor = Option.fromUndefinedOr(input.after)
  let progress: ProjectionIndexCopyProgress = { copied: 0, unchanged: 0, unregistered: 0, next: cursor }

  for (let page = 0; input.maximumPages === undefined || page < input.maximumPages; page += 1) {
    const read = yield* readPostgresProjectionIndexPage({
      pool: input.pool,
      schema: input.schema,
      manifest: input.manifest,
      after: cursor,
      limit: input.pageSize ?? 100,
    })

    const [first, ...rest] = read.revisions
    let copied = 0

    if (first !== undefined) {
      const lookups = yield* target.loadRevisions([first.key, ...rest.map((revision) => revision.key)])

      const outcomes = yield* Effect.forEach(
        read.revisions,
        (revision, index): Effect.Effect<boolean, ProjectionIndexStoreFailed | ProjectionIndexConflict> => {
          const current = lookups[index]?.revision ?? Option.none()

          if (Option.isSome(current) && current.value.revisionHash === revision.revisionHash) {
            return Effect.succeed(false)
          }

          return target.replaceRevision({
            ...revision,
            expectedToken: Option.map(current, (snapshot) => snapshot.token),
          }).pipe(Effect.as(true))
        },
        { concurrency: input.concurrency ?? 8 },
      )

      copied = outcomes.filter(Boolean).length
    }

    progress = {
      copied: progress.copied + copied,
      unchanged: progress.unchanged + read.revisions.length - copied,
      unregistered: progress.unregistered + read.unregistered.length,
      next: read.next,
    }

    if (input.onProgress !== undefined) yield* input.onProgress(progress)
    if (Option.isNone(read.next)) break
    cursor = read.next
  }

  return progress
})
