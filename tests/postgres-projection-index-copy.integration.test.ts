import { describe, expect, test } from "bun:test"
import { readFile } from "node:fs/promises"
import { Effect, Layer, Option, Schema } from "effect"
import { Pool } from "pg"
import {
  defineDocument,
  defineDocumentGraph,
  defineEmbeddingProfile,
  EmbeddingProvider,
  ProjectionIndexStore,
  sectionChunking,
  type EmbeddingProviderService,
} from "../src/adapter.js"
import { inMemoryDocumentGraph } from "../src/in-memory.js"
import { copyPostgresProjectionIndex, postgresDocumentGraph } from "../src/postgres.js"

const databaseUrl = Bun.env.TEST_DATABASE_URL

const runIntegrationTests =
  Bun.env.RUN_DOCUMENT_GRAPH_POSTGRES_TESTS === "true" &&
  databaseUrl !== undefined

const NoteId = Schema.Trimmed.check(Schema.isNonEmpty()).pipe(Schema.brand("CopyNoteId"))

const Note = Schema.Struct({
  id: NoteId,
  title: Schema.Trimmed.check(Schema.isNonEmpty()),
  sections: Schema.NonEmptyArray(Schema.Struct({
    key: Schema.Trimmed.check(Schema.isNonEmpty()),
    text: Schema.Trimmed.check(Schema.isNonEmpty()),
    team: Schema.Literals(["food", "fashion"]),
  })),
})

const graph = defineDocumentGraph({
  id: "postgres-copy",
  documents: {
    Note: defineDocument({
      id: NoteId,
      value: Note,
      identify: (note) => note.id,
    }).vectorise({
      id: "note-content",
      version: "v1",
      metadata: Schema.Struct({ team: Schema.Literals(["food", "fashion"]) }),
      select: (note) => {
        const [first, ...rest] = note.sections
        const section = (input: typeof first) => ({
          key: input.key,
          label: input.key,
          content: input.text,
          metadata: { team: input.team },
        })

        return { context: note.title, sections: [section(first), ...rest.map(section)] }
      },
      chunking: sectionChunking({ maximumCharacters: 256 }),
    }),
  },
})

const profile = defineEmbeddingProfile({ id: "test:copy", version: "v1", dimensions: 2 })

const notes = ["a", "b", "c"].map((id, index) => Schema.decodeSync(Note)({
  id,
  title: `Note ${id}`,
  sections: [
    { key: "intro", text: `Introduction to ${id} food photography`, team: "food" },
    { key: "detail", text: `Detail ${index} about fashion campaigns`, team: "fashion" },
  ],
}))

describe("copyPostgresProjectionIndex", () => {
  const integrationTest = runIntegrationTests ? test : test.skip

  integrationTest("copies every revision with its stored vectors and resumes idempotently", async () => {
    const schema = `document_graph_${crypto.randomUUID().replaceAll("-", "")}`
    const pool = new Pool({ connectionString: databaseUrl, max: 4 })
    let embeddedDocuments = 0

    const embeddings: EmbeddingProviderService = {
      profile,
      embedDocuments: (requests) => {
        embeddedDocuments += requests.length

        return Effect.succeed(requests.map((request, index) => ({
          contentHash: request.contentHash,
          vector: [1, index / 10],
        })))
      },
      embedQuery: () => Effect.succeed([1, 0]),
    }

    try {
      for (const file of [
        "0001_initial.sql",
        "0002_mutation_locks.sql",
        "0003_graph_topology.sql",
        "0004_native_vector_eligibility.sql",
      ]) {
        const migration = await readFile(new URL(`../migrations/postgres/${file}`, import.meta.url), "utf8")
        await pool.query(migration.replaceAll('"honertia_document_graph"', `"${schema}"`))
      }

      const Notes = graph.document("Note").projection("note-content")

      const keys = await Effect.runPromise(Effect.gen(function*() {
        const indexed = []

        for (const note of notes) {
          yield* Notes.index(note)
          const projected = yield* Notes.project(note)
          indexed.push({ documentKey: projected.documentKey, projection: projected.projection.id })
        }

        return indexed
      }).pipe(Effect.provide(Layer.mergeAll(
        Layer.succeed(EmbeddingProvider, embeddings),
        postgresDocumentGraph({ pool, schema }),
      ))))

      const embeddedBeforeCopy = embeddedDocuments
      const [firstKey, ...restKeys] = keys
      if (firstKey === undefined) throw new Error("Expected indexed notes")

      const source = await Effect.runPromise(Effect.gen(function*() {
        const store = yield* ProjectionIndexStore
        return yield* store.loadRevisions([firstKey, ...restKeys])
      }).pipe(Effect.provide(postgresDocumentGraph({ pool, schema }))))

      const result = await Effect.runPromise(Effect.gen(function*() {
        const partial = yield* copyPostgresProjectionIndex({
          pool,
          schema,
          manifest: graph.manifest,
          pageSize: 2,
          maximumPages: 1,
        })

        const rest = yield* copyPostgresProjectionIndex({
          pool,
          schema,
          manifest: graph.manifest,
          pageSize: 2,
          ...Option.match(partial.next, { onNone: () => ({}), onSome: (after) => ({ after }) }),
        })

        const repeated = yield* copyPostgresProjectionIndex({ pool, schema, manifest: graph.manifest })
        const store = yield* ProjectionIndexStore
        const copied = yield* store.loadRevisions([firstKey, ...restKeys])

        const hits = yield* graph.document("Note").projection("note-content").search("food photography", {
          strategy: "semantic",
          where: { team: "food" },
          limit: 3,
        })

        return { partial, rest, repeated, copied, hits }
      }).pipe(Effect.provide(Layer.mergeAll(
        Layer.succeed(EmbeddingProvider, embeddings),
        inMemoryDocumentGraph(),
      ))))

      expect(result.partial).toMatchObject({ copied: 2, unchanged: 0, unregistered: 0 })
      expect(result.rest).toMatchObject({ copied: 1, unchanged: 0, next: Option.none() })
      expect(result.repeated).toMatchObject({ copied: 0, unchanged: 3 })
      expect(embeddedDocuments).toBe(embeddedBeforeCopy)

      const summary = (lookups: typeof source) => lookups.map((lookup) => lookup.revision.pipe(
        Option.map((revision) => ({ revisionHash: revision.revisionHash, chunks: revision.chunks })),
        Option.getOrNull,
      ))

      expect(summary(result.copied)).toEqual(summary(source))
      expect(result.hits).toHaveLength(3)
    } finally {
      await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
      await pool.end()
    }
  })
})
