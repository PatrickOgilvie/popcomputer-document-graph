import { describe, expect, test } from "bun:test"
import { readFile } from "node:fs/promises"
import { Effect, Layer, Option, Schema } from "effect"
import { Pool } from "pg"
import {
  defineDocument,
  defineDocumentGraph,
  defineEmbeddingProfile,
  EmbeddingProvider,
  GraphTopologyStore,
  ProjectionIndexStore,
  ProjectionIndexStoreFailed,
  sectionChunking,
  type EmbeddingProviderService,
  type ProjectionIndexKey,
  type ProjectionIndexStoreService,
} from "../src/adapter.js"
import { inMemoryDocumentGraph } from "../src/in-memory.js"
import { mirrorPostgresProjectionChanges, postgresDocumentGraph } from "../src/postgres.js"

const databaseUrl = Bun.env.TEST_DATABASE_URL

const runIntegrationTests =
  Bun.env.RUN_DOCUMENT_GRAPH_POSTGRES_TESTS === "true" &&
  databaseUrl !== undefined

const NoteId = Schema.Trimmed.check(Schema.isNonEmpty()).pipe(Schema.brand("MirrorNoteId"))

const Note = Schema.Struct({
  id: NoteId,
  title: Schema.Trimmed.check(Schema.isNonEmpty()),
  text: Schema.Trimmed.check(Schema.isNonEmpty()),
})

const graph = defineDocumentGraph({
  id: "postgres-mirror",
  documents: {
    Note: defineDocument({
      id: NoteId,
      value: Note,
      identify: (note) => note.id,
    }).vectorise({
      id: "note-content",
      version: "v1",
      select: (note) => ({
        context: note.title,
        sections: [{ key: "body", label: "Body", content: note.text }],
      }),
      chunking: sectionChunking({ maximumCharacters: 256 }),
    }),
  },
})

const Notes = graph.document("Note")
const NoteContent = Notes.projection("note-content")
const profile = defineEmbeddingProfile({ id: "test:mirror", version: "v1", dimensions: 2 })
const note = (id: string, text: string) => Schema.decodeSync(Note)({ id, title: `Note ${id}`, text })

describe("mirrorPostgresProjectionChanges", () => {
  const integrationTest = runIntegrationTests ? test : test.skip

  integrationTest("mirrors every recorded change into the target and keeps what it could not apply", async () => {
    const schema = `document_graph_${crypto.randomUUID().replaceAll("-", "")}`
    const pool = new Pool({ connectionString: databaseUrl, max: 4 })
    let embedded = 0

    const embeddings: EmbeddingProviderService = {
      profile,
      embedDocuments: (requests) => {
        embedded += requests.length

        return Effect.succeed(requests.map((request) => ({
          contentHash: request.contentHash,
          vector: [1, request.contentHash.charCodeAt(0) / 100],
        })))
      },
      embedQuery: () => Effect.succeed([1, 0]),
    }

    const postgres = Layer.mergeAll(Layer.succeed(EmbeddingProvider, embeddings), postgresDocumentGraph({ pool, schema }))
    const inPostgres = <A, E>(effect: Effect.Effect<A, E, ProjectionIndexStore | GraphTopologyStore | EmbeddingProvider>) =>
      Effect.provide(effect, postgres)

    const pending = () => Effect.promise(async () =>
      (await pool.query(`SELECT count(*)::int AS count FROM "${schema}"."projection_changes"`)).rows[0]?.count)

    const keyOf = (id: string) => NoteContent.project(note(id, "any")).pipe(
      Effect.map((projected): ProjectionIndexKey => ({ documentKey: projected.documentKey, projection: projected.projection.id })),
    )

    const mirror = (options: { readonly batchSize?: number; readonly maximumBatches?: number } = {}) =>
      mirrorPostgresProjectionChanges({ pool, schema, manifest: graph.manifest, ...options })

    try {
      for (const file of [
        "0001_initial.sql",
        "0002_mutation_locks.sql",
        "0003_graph_topology.sql",
        "0004_native_vector_eligibility.sql",
        "0007_projection_changes.sql",
      ]) {
        const migration = await readFile(new URL(`../migrations/postgres/${file}`, import.meta.url), "utf8")
        await pool.query(migration.replaceAll('"honertia_document_graph"', `"${schema}"`))
      }

      const result = await Effect.runPromise(Effect.gen(function*() {
        const target = yield* ProjectionIndexStore
        const [a, b, c] = [yield* keyOf("a"), yield* keyOf("b"), yield* keyOf("c")]
        const hashes = (keys: readonly [ProjectionIndexKey, ...ReadonlyArray<ProjectionIndexKey>]) =>
          target.loadRevisions(keys).pipe(Effect.map((lookups) =>
            lookups.map((lookup) => Option.getOrNull(Option.map(lookup.revision, (revision) => revision.revisionHash)))))
        const source = (keys: readonly [ProjectionIndexKey, ...ReadonlyArray<ProjectionIndexKey>]) =>
          inPostgres(Effect.gen(function*() { return yield* (yield* ProjectionIndexStore).loadRevisions(keys) })).pipe(
            Effect.map((lookups) => lookups.map((lookup) =>
              Option.getOrNull(Option.map(lookup.revision, (revision) => revision.revisionHash)))))

        yield* inPostgres(Effect.forEach(["a", "b", "c"], (id) => NoteContent.index(note(id, `Food photography by ${id}`))))
        const recorded = yield* pending()
        const embeddedBeforeMirror = embedded

        const first = yield* mirror()
        const afterFirst = { pending: yield* pending(), hashes: yield* hashes([a, b, c]), source: yield* source([a, b, c]) }
        const repeat = yield* mirror()

        yield* inPostgres(NoteContent.index(note("b", "Fashion campaign film for b")))
        yield* inPostgres(Notes.remove(note("c", "any").id))
        const update = yield* mirror()
        const afterUpdate = { hashes: yield* hashes([a, b, c]), source: yield* source([a, b, c]) }
        const reusedVectors = embedded === embeddedBeforeMirror + 1

        // A failed write stays recorded and the next drain applies it.
        let failNext = true
        const failing: ProjectionIndexStoreService = {
          ...target,
          replaceRevision: (replacement) => {
            if (!failNext) return target.replaceRevision(replacement)
            failNext = false
            return Effect.fail(new ProjectionIndexStoreFailed({
              operation: "replace_revision", reason: "unavailable", cause: "test outage",
            }))
          },
        }
        yield* inPostgres(NoteContent.index(note("a", "Recipe video for a")))
        const failed = yield* mirror().pipe(Effect.provideService(ProjectionIndexStore, failing))
        const pendingAfterFailure = yield* pending()
        const retried = yield* mirror()

        // A change renewed while the drain runs is not cleared by that drain.
        let renewNext = true
        const renewing: ProjectionIndexStoreService = {
          ...target,
          replaceRevision: (replacement) => Effect.gen(function*() {
            if (renewNext) {
              renewNext = false
              yield* inPostgres(NoteContent.index(note("a", "Newest packaging shoot for a"))).pipe(Effect.orDie)
            }
            return yield* target.replaceRevision(replacement)
          }),
        }
        yield* inPostgres(NoteContent.index(note("a", "Older tabletop film for a")))
        yield* mirror().pipe(Effect.provideService(ProjectionIndexStore, renewing))
        const pendingAfterRenewal = yield* pending()
        const caughtUp = yield* mirror()
        const afterRenewal = { hashes: yield* hashes([a]), source: yield* source([a]) }

        // A batch limit reports changes left unread.
        yield* inPostgres(Effect.forEach(["a", "b"], (id) => NoteContent.index(note(id, `Brand identity for ${id}`))))
        const limited = yield* mirror({ batchSize: 1, maximumBatches: 1 })
        const rest = yield* mirror({ batchSize: 1 })

        return {
          recorded, first, afterFirst, repeat, update, afterUpdate, reusedVectors, failed, pendingAfterFailure,
          retried, pendingAfterRenewal, caughtUp, afterRenewal, limited, rest,
        }
      }).pipe(Effect.provide(Layer.mergeAll(Layer.succeed(EmbeddingProvider, embeddings), inMemoryDocumentGraph()))))

      expect(result.recorded).toBe(3)
      expect(result.first).toMatchObject({ replaced: 3, deleted: 0, unchanged: 0, failures: [], drained: true })
      expect(result.afterFirst.pending).toBe(0)
      expect(result.afterFirst.hashes).toEqual(result.afterFirst.source)
      expect(result.repeat).toMatchObject({ replaced: 0, deleted: 0, unchanged: 0, drained: true })

      expect(result.update).toMatchObject({ replaced: 1, deleted: 1, failures: [] })
      expect(result.afterUpdate.hashes).toEqual(result.afterUpdate.source)
      expect(result.afterUpdate.hashes[2]).toBeNull()
      // Only the edited note was embedded, by its PostgreSQL write; the mirror reused vectors.
      expect(result.reusedVectors).toBe(true)

      expect(result.failed.failures).toHaveLength(1)
      expect(result.failed.failures[0]?.error._tag).toBe("ProjectionIndexStoreFailed")
      expect(result.pendingAfterFailure).toBe(1)
      expect(result.retried).toMatchObject({ replaced: 1, failures: [] })

      expect(result.pendingAfterRenewal).toBe(1)
      expect(result.caughtUp).toMatchObject({ replaced: 1, failures: [] })
      expect(result.afterRenewal.hashes).toEqual(result.afterRenewal.source)

      expect(result.limited).toMatchObject({ replaced: 1, drained: false })
      expect(result.rest).toMatchObject({ replaced: 1, drained: true })
    } finally {
      await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
      await pool.end()
    }
  })
})
