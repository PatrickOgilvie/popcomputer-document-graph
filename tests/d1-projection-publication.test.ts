/// <reference types="@cloudflare/workers-types" />

/* oxlint-disable anti-slop/no-unknown-parameters, anti-slop/no-runtime-typeof, anti-slop/no-unsafe-dictionary-type -- This faithful test binding implements Cloudflare D1's deliberately unknown-valued boundary and narrows every SQLite binding before execution. */

import { describe, expect, test } from "bun:test"
import type { NamespaceWriteParams } from "@turbopuffer/turbopuffer"
import {
  Database,
  type SQLQueryBindings,
} from "bun:sqlite"
import { Deferred, Effect, Fiber, Option, Result, Schema } from "effect"
import { makeProjectionIndexStoreConformanceFixture } from "../src/conformance/projection-index-conformance.js"
import {
  ChunkIdSchema,
  ContentHashSchema,
  makeDocumentKey,
  ProjectionRevisionHashSchema,
} from "../src/document/document-identity.js"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import {
  ProjectionMutationIdSchema,
  ProjectionPayloadDigestSchema,
  ProjectionPublicationCoordinator,
  type ProjectionDeletionIntent,
  type ProjectionReplacementIntent,
} from "../src/indexing/projection-publication.js"
import {
  IndexRevisionTokenSchema,
  ProjectionIndexConflict,
  type ProjectionIndexKey,
} from "../src/indexing/projection-index.js"
import { d1ProjectionPublicationCoordinator } from "../src/storage/d1/projection-publication.js"
import { TurbopufferClient, type TurbopufferClientService } from "../src/storage/turbopuffer/client.js"
import { TurbopufferTransportFailed } from "../src/storage/turbopuffer/errors.js"
import { makeTurbopufferWorkspacePartition } from "../src/storage/turbopuffer/partition.js"
import { makeTurbopufferProjectionIndexStore } from "../src/storage/turbopuffer/projection-index.js"

type SqliteBinding = Exclude<SQLQueryBindings, Record<string, unknown>>

const sqliteBinding = (input: unknown): SqliteBinding => {
  if (
    input === null ||
    typeof input === "string" ||
    typeof input === "number" ||
    typeof input === "boolean" ||
    typeof input === "bigint" ||
    input instanceof Uint8Array
  ) {
    return input
  }

  throw new TypeError("The test D1 binding received an unsupported value")
}

const resultMeta = (
  changes: number,
): D1Meta & Record<string, unknown> => ({
  duration: 0,
  size_after: 0,
  rows_read: 0,
  rows_written: changes,
  last_row_id: 0,
  changed_db: changes > 0,
  changes,
})

class SqliteD1PreparedStatement implements D1PreparedStatement {
  constructor(
    private readonly database: Database,
    private readonly query: string,
    private readonly values: ReadonlyArray<unknown> = [],
  ) {}

  bind(...values: unknown[]): D1PreparedStatement {
    return new SqliteD1PreparedStatement(this.database, this.query, values)
  }

  executeSync<T>(): D1Result<T> {
    const statement = this.database.query<unknown, Array<SqliteBinding>>(
      this.query,
    )

    const rows = statement.all(...this.values.map(sqliteBinding))

    const changes = this.database
      .query<{ readonly changes: number }, []>(
        "SELECT changes() AS changes",
      )
      .get()?.changes ?? 0

    return {
      success: true,
      // SAFETY: This fake implements D1's caller-selected row generic. The
      // production adapter still decodes every returned value from unknown.
      results: rows as Array<T>,
      meta: resultMeta(changes),
    }
  }

  async first<T = Record<string, unknown>>(): Promise<T | null>
  async first<T = unknown>(columnName: string): Promise<T | null>
  async first<T = Record<string, unknown>>(
    columnName?: string,
  ): Promise<T | null> {
    const first = this.executeSync<Record<string, unknown>>().results[0]

    if (first === undefined) return null

    if (columnName === undefined) {
      // SAFETY: D1's first<T>() API delegates selection of the row type to its
      // caller, matching this faithful boundary implementation.
      return first as T
    }

    const value = first[columnName]

    // SAFETY: D1's first<T>(column) API delegates selection of the column type
    // to its caller, matching this faithful boundary implementation.
    return value === undefined ? null : value as T
  }

  async run<T = Record<string, unknown>>(): Promise<D1Result<T>> {
    return this.executeSync<T>()
  }

  async all<T = Record<string, unknown>>(): Promise<D1Result<T>> {
    return this.executeSync<T>()
  }

  async raw<T = unknown[]>(options: {
    readonly columnNames: true
  }): Promise<[string[], ...T[]]>
  async raw<T = unknown[]>(options?: {
    readonly columnNames?: false
  }): Promise<T[]>
  async raw<T = unknown[]>(
    _options?: { readonly columnNames?: boolean },
  ): Promise<T[] | [string[], ...T[]]> {
    throw new Error("Raw D1 rows are outside this adapter's test seam")
  }
}

class SqliteD1Session implements D1DatabaseSession {
  constructor(private readonly owner: SqliteD1Database) {}

  prepare(query: string): D1PreparedStatement {
    return this.owner.prepare(query)
  }

  batch<T = unknown>(
    statements: D1PreparedStatement[],
  ): Promise<D1Result<T>[]> {
    return this.owner.batch(statements)
  }

  getBookmark(): string | null {
    return null
  }
}

class SqliteD1Database implements D1Database {
  readonly sessionConstraints: Array<
    string | undefined
  > = []

  constructor(private readonly sqlite: Database) {}

  prepare(query: string): D1PreparedStatement {
    return new SqliteD1PreparedStatement(this.sqlite, query)
  }

  async batch<T = unknown>(
    statements: D1PreparedStatement[],
  ): Promise<D1Result<T>[]> {
    this.sqlite.exec("BEGIN IMMEDIATE")

    try {
      const results: Array<D1Result<T>> = []

      for (const statement of statements) {
        if (!(statement instanceof SqliteD1PreparedStatement)) {
          throw new TypeError("The test D1 batch received a foreign statement")
        }

        results.push(statement.executeSync<T>())
      }

      this.sqlite.exec("COMMIT")

      return results
    } catch (cause: unknown) {
      this.sqlite.exec("ROLLBACK")
      throw cause
    }
  }

  async exec(query: string): Promise<D1ExecResult> {
    this.sqlite.exec(query)

    return { count: 0, duration: 0 }
  }

  withSession(
    constraintOrBookmark?: D1SessionBookmark,
  ): D1DatabaseSession {
    this.sessionConstraints.push(constraintOrBookmark)

    return new SqliteD1Session(this)
  }

  async dump(): Promise<ArrayBuffer> {
    throw new Error("Database dumps are outside this adapter's test seam")
  }

  execute(query: string, values: ReadonlyArray<unknown> = []): void {
    this.sqlite.query<unknown, Array<SqliteBinding>>(query).run(
      ...values.map(sqliteBinding),
    )
  }

  row<T>(query: string, values: ReadonlyArray<unknown> = []): T | null {
    return this.sqlite.query<T, Array<SqliteBinding>>(query).get(
      ...values.map(sqliteBinding),
    )
  }

  close(): void {
    this.sqlite.close()
  }
}

const makeDatabase = async (): Promise<SqliteD1Database> => {
  const sqlite = new Database(":memory:", { strict: true })
  sqlite.exec("PRAGMA foreign_keys = ON")
  const database = new SqliteD1Database(sqlite)

  const migration = await Bun.file(
    new URL(
      "../migrations/d1/0002_projection_publications.sql",
      import.meta.url,
    ),
  ).text()

  await database.exec(migration)

  return database
}

const documentKey = makeDocumentKey({
  graph: "contracts",
  documentKind: "contract",
  encodedId: { id: "contract-1" },
})

const anotherDocumentKey = makeDocumentKey({
  graph: "contracts",
  documentKind: "invoice",
  encodedId: { id: "invoice-1" },
})

const key: ProjectionIndexKey = { documentKey, projection: "search" }

const anotherKey: ProjectionIndexKey = {
  documentKey: anotherDocumentKey,
  projection: "search",
}

const profile = defineEmbeddingProfile({
  id: "test/d1-publication",
  version: "v1",
  dimensions: 3,
})

const replacementIntent = (input: {
  readonly key?: ProjectionIndexKey
  readonly mutation: string
  readonly digestCharacter: string
  readonly token: string
  readonly expectedToken?: string
  readonly requiredSlotHighWater?: number
  readonly slotHighWater?: number
  readonly maximumSlotHighWater?: number
  readonly documentKind?: string
  readonly projectionVersion?: string
  readonly chunks?: number
}): ProjectionReplacementIntent => {
  const chunks = Array.from({ length: input.chunks ?? 2 }, (_, ordinal) => ({
    chunkId: Schema.decodeSync(ChunkIdSchema)(
      ((ordinal + 1) % 16).toString(16).repeat(64),
    ),
    contentHash: Schema.decodeSync(ContentHashSchema)(
      ((ordinal + 8) % 16).toString(16).repeat(64),
    ),
  }))

  const [firstChunk, ...remainingChunks] = chunks

  if (firstChunk === undefined) throw new Error("A fixture needs one chunk")
  const selectedKey = input.key ?? key
  const requiredSlotHighWater = input.requiredSlotHighWater ?? chunks.length

  return {
    _tag: "Replace",
    key: selectedKey,
    expectedToken: input.expectedToken === undefined
      ? Option.none()
      : Option.some(Schema.decodeSync(IndexRevisionTokenSchema)(
          input.expectedToken,
        )),
    mutationId: Schema.decodeSync(ProjectionMutationIdSchema)(input.mutation),
    payloadDigest: Schema.decodeSync(ProjectionPayloadDigestSchema)(
      input.digestCharacter.repeat(64),
    ),
    snapshot: {
      token: Schema.decodeSync(IndexRevisionTokenSchema)(input.token),
      revisionHash: Schema.decodeSync(ProjectionRevisionHashSchema)(
        input.digestCharacter.repeat(64),
      ),
      embeddingProfile: profile,
      chunks: [firstChunk, ...remainingChunks],
    },
    catalog: {
      graph: "contracts",
      documentKind: input.documentKind ?? "contract",
      projectionVersion: input.projectionVersion ?? "v1",
    },
    liveSlotCount: chunks.length,
    requiredSlotHighWater,
    slotHighWater: input.slotHighWater ?? requiredSlotHighWater,
    maximumSlotHighWater: input.maximumSlotHighWater ?? 100,
    commit: {
      token: Schema.decodeSync(IndexRevisionTokenSchema)(input.token),
      inserted: chunks.length,
      updated: 0,
      deleted: 0,
    },
  }
}

const deletionIntent = (input: {
  readonly mutation: string
  readonly digestCharacter: string
  readonly expectedToken?: string
  readonly deletedRevisions?: number
  readonly deletedChunks?: number
  readonly slotHighWater?: number
}): ProjectionDeletionIntent => ({
  _tag: "Delete",
  key,
  expectedToken: input.expectedToken === undefined
    ? Option.none()
    : Option.some(Schema.decodeSync(IndexRevisionTokenSchema)(
        input.expectedToken,
      )),
  mutationId: Schema.decodeSync(ProjectionMutationIdSchema)(input.mutation),
  payloadDigest: Schema.decodeSync(ProjectionPayloadDigestSchema)(
    input.digestCharacter.repeat(64),
  ),
  deletion: {
    deletedRevisions: input.deletedRevisions ?? 1,
    deletedChunks: input.deletedChunks ?? 2,
  },
  slotHighWater: input.slotHighWater ?? 2,
  maximumSlotHighWater: 100,
})

const runWithCoordinator = <A, E>(
  database: D1Database,
  effect: Effect.Effect<A, E, ProjectionPublicationCoordinator>,
  indexGeneration = "schema-v1",
  retainedPublicationHistory?: number,
): Promise<A> => {
  const requiredConfig = {
    database,
    indexGeneration,
    publicationLeaseMilliseconds: 1_000,
  }

  const config = retainedPublicationHistory === undefined
    ? requiredConfig
    : { ...requiredConfig, retainedPublicationHistory }

  return Effect.runPromise(effect.pipe(
    Effect.provide(d1ProjectionPublicationCoordinator(config)),
  ))
}

describe("D1 projection publication coordinator", () => {
  test.each(["replace", "delete"] as const)("a rejected duplicate cannot retire an in-flight %s publication", async (operation) => {
    const database = await makeDatabase()
    const fixture = makeProjectionIndexStoreConformanceFixture()

    const partition = makeTurbopufferWorkspacePartition({
      deploymentId: "test:publication-overlap",
      endpoint: { _tag: "Region", region: "gcp-us-central1" },
      workspace: "publication-overlap",
      embeddingProfile: fixture.initial.embeddingProfile,
      schemaGeneration: 1,
    })

    try {
      const result = await runWithCoordinator(database, Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const started = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        let writes = 0
        let providerRows: NonNullable<NamespaceWriteParams["upsert_rows"]> = []

        const client: TurbopufferClientService = {
          partition,
          query: () => Effect.die("Unexpected provider read: the replacement supplies every vector"),
          multiQuery: () => Effect.die("Unexpected multi-query"),
          inspectSchema: () => Effect.die("Unexpected schema inspection"),
          updateSchema: () => Effect.die("Unexpected schema update"),
          destroyNamespace: () => Effect.die("Unexpected namespace deletion"),
          write: (request) => Effect.gen(function*() {
            const attempt = ++writes

            if (attempt === 2) {
              yield* Deferred.succeed(started, undefined)
              yield* Deferred.await(release)
            }

            if (attempt === 3) {
              return yield* new TurbopufferTransportFailed({
                operation: "write",
                reason: "rate_limited",
                requestOutcome: "definitely_not_applied",
                cause: "The duplicate request was rejected before applying",
              })
            }

            providerRows = request.upsert_rows ?? []

            return { status: "OK", rows_affected: providerRows.length }
          }),
        }

        const store = yield* makeTurbopufferProjectionIndexStore({ partition }).pipe(
          Effect.provideService(TurbopufferClient, client),
        )

        const initial = yield* store.replaceRevision(fixture.initial)

        const command = operation === "replace"
          ? store.replaceRevision({
              ...fixture.metadataOnly,
              expectedToken: Option.some(initial.token),
              embeddings: fixture.initial.embeddings,
            }).pipe(Effect.asVoid)
          : store.deleteRevision(fixture.initial.key).pipe(Effect.asVoid)

        const original = yield* Effect.forkChild(command.pipe(Effect.result))
        yield* Deferred.await(started)
        const duplicate = yield* command.pipe(Effect.result)
        const [during] = yield* coordinator.loadHeads([fixture.initial.key])
        yield* Deferred.succeed(release, undefined)
        const completed = yield* Fiber.join(original)
        const [after] = yield* coordinator.loadHeads([fixture.initial.key])
        const [revision] = yield* coordinator.loadRevisions([fixture.initial.key])

        return {
          duplicate,
          completed,
          during: Option.getOrThrow(during?.head ?? Option.none()),
          after: Option.getOrThrow(after?.head ?? Option.none()),
          revision: revision?.revision ?? Option.none(),
          providerRows,
          writes,
        }
      }), partition.d1IndexGeneration)

      expect(result.duplicate).toMatchObject({ _tag: "Failure", failure: { reason: "unavailable" } })
      expect(Option.isSome(result.during.pending)).toBe(true)
      expect(Result.isSuccess(result.completed)).toBe(true)
      expect(Option.isNone(result.after.pending)).toBe(true)
      const active = result.revision
      const liveRows = result.providerRows.filter((row) => row["is_live"] === true)

      if (operation === "replace") {
        expect(Option.getOrThrow(active).revisionHash).toBe(fixture.metadataOnly.revisionHash)
        expect(liveRows.map((row) => row["revision_hash"]))
          .toEqual(fixture.metadataOnly.chunks.map(() => fixture.metadataOnly.revisionHash))
      } else {
        expect(Option.isNone(active)).toBe(true)
        expect(liveRows).toHaveLength(0)
      }

      expect(result.writes).toBe(3)
    } finally {
      database.close()
    }
  })

  test("begins and resumes the same durable publication lease", async () => {
    const database = await makeDatabase()

    const intent = replacementIntent({
      mutation: "replace-resume",
      digestCharacter: "a",
      token: "token-a",
      requiredSlotHighWater: 4,
    })

    try {
      const result = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator
          const first = yield* coordinator.beginPublication(intent)
          const resumed = yield* coordinator.beginPublication(intent)
          const heads = yield* coordinator.loadHeads([key, key])

          return { first, resumed, heads }
        }),
      )

      expect(result.first._tag).toBe("Publish")
      expect(result.resumed._tag).toBe("Publish")

      if (result.first._tag !== "Publish" || result.resumed._tag !== "Publish") {
        throw new Error("Expected publication leases")
      }

      const firstPublicationId = result.first.lease.publicationId
      expect(result.resumed.lease.publicationId).toBe(
        firstPublicationId,
      )
      expect(Number(result.resumed.lease.generation)).toBe(1)
      expect(result.resumed.lease.slotHighWater).toBe(4)
      expect(result.heads).toHaveLength(2)
      expect(
        result.heads.every((lookup) =>
          Option.isSome(lookup.head) &&
          Option.isSome(lookup.head.value.pending) &&
          lookup.head.value.pending.value.publicationId ===
            firstPublicationId
        ),
      ).toBe(true)
      expect(database.sessionConstraints.length).toBeGreaterThan(0)
      expect(database.sessionConstraints.every(
        (constraint) => constraint === "first-primary",
      )).toBe(true)
    } finally {
      database.close()
    }
  })

  test("isolates heads and generation counters by physical index generation", async () => {
    const database = await makeDatabase()

    const intent = replacementIntent({
      mutation: "replace-side-by-side",
      digestCharacter: "a",
      token: "token-side-by-side",
      requiredSlotHighWater: 3,
    })

    const publishIn = (indexGeneration: string) => runWithCoordinator(
      database,
      Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const begun = yield* coordinator.beginPublication(intent)

        if (begun._tag !== "Publish") return yield* Effect.die("No lease")
        yield* coordinator.finalizePublication(begun.lease)
        const [lookup] = yield* coordinator.loadHeads([key])

        return { lease: begun.lease, lookup }
      }),
      indexGeneration,
    )

    try {
      const first = await publishIn("schema-v1")
      const second = await publishIn("schema-v2")

      expect(Number(first.lease.generation)).toBe(1)
      expect(Number(second.lease.generation)).toBe(1)
      expect(first.lease.publicationId).not.toBe(second.lease.publicationId)
      expect(Option.isSome(first.lookup?.head ?? Option.none())).toBe(true)
      expect(Option.isSome(second.lookup?.head ?? Option.none())).toBe(true)
      expect(database.row<{ readonly count: number }>(
        `SELECT COUNT(*) AS count
         FROM document_graph_projection_heads
         WHERE document_key = ? AND projection_id = ?`,
        [key.documentKey, key.projection],
      )?.count).toBe(2)
      expect(database.row<{ readonly count: number }>(
        `SELECT COUNT(*) AS count
         FROM document_graph_projection_publications
         WHERE document_key = ? AND projection_id = ? AND generation = 1`,
        [key.documentKey, key.projection],
      )?.count).toBe(2)
    } finally {
      database.close()
    }
  })

  test("finalizes a revision and replays its exact committed outcome", async () => {
    const database = await makeDatabase()

    const intent = replacementIntent({
      mutation: "replace-finalize",
      digestCharacter: "b",
      token: "token-b",
    })

    try {
      const result = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator
          const begun = yield* coordinator.beginPublication(intent)

          if (begun._tag !== "Publish") return yield* Effect.die("No lease")
          const finalized = yield* coordinator.finalizePublication(begun.lease)

          const finalizedAgain = yield* coordinator.finalizePublication(
            begun.lease,
          )

          // The TP adapter replans counts against the now-active revision on
          // retry, but the durable committed outcome must remain the original.
          const replay = yield* coordinator.beginPublication({
            ...intent,
            commit: { ...intent.commit, inserted: 0, updated: intent.liveSlotCount },
          })

          const revisions = yield* coordinator.loadRevisions([key, key])

          return { begun, finalized, finalizedAgain, replay, revisions }
        }),
      )

      expect(result.finalized).toEqual({
        _tag: "Replaced",
        commit: intent.commit,
      })
      expect(result.finalizedAgain).toEqual(result.finalized)
      expect(result.replay).toEqual({
        _tag: "AlreadyCommitted",
        outcome: result.finalized,
      })
      expect(result.revisions).toHaveLength(2)
      expect(result.revisions.every((lookup) =>
        Option.isSome(lookup.revision) &&
        lookup.revision.value.token === intent.snapshot.token &&
        lookup.revision.value.chunks.length === 2
      )).toBe(true)
      expect(database.row<{ readonly status: string }>(
        `SELECT status FROM document_graph_projection_publications
         WHERE publication_id = ?`,
        [result.begun.lease.publicationId],
      )?.status).toBe("committed")
    } finally {
      database.close()
    }
  })

  test("keeps an expired prepared publication authoritative until reconciliation", async () => {
    const database = await makeDatabase()

    const firstIntent = replacementIntent({
      mutation: "replace-takeover-a",
      digestCharacter: "c",
      token: "token-c",
      requiredSlotHighWater: 6,
    })

    const secondIntent = replacementIntent({
      mutation: "replace-takeover-b",
      digestCharacter: "d",
      token: "token-d",
      requiredSlotHighWater: 2,
    })

    try {
      const first = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator

          return yield* coordinator.beginPublication(firstIntent)
        }),
      )

      if (first._tag !== "Publish") throw new Error("Expected first lease")
      database.execute(
        `UPDATE document_graph_projection_heads
         SET pending_lease_expires_at = 0`,
      )

      const result = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator

          const second = yield* coordinator.beginPublication(secondIntent)
            .pipe(Effect.result)

          const resumed = yield* coordinator.beginPublication(firstIntent)

          if (resumed._tag !== "Publish") {
            return yield* Effect.die("Expected the prepared lease to resume")
          }

          const finalized = yield* coordinator.finalizePublication(
            resumed.lease,
          )

          const [lookup] = yield* coordinator.loadHeads([key])

          return { second, resumed, finalized, lookup }
        }),
      )

      expect(Result.isFailure(result.second)).toBe(true)

      if (Result.isFailure(result.second)) {
        expect(result.second.failure).toMatchObject({
          _tag: "ProjectionPublicationCoordinatorFailed",
          operation: "begin_publication",
          reason: "publication_in_progress",
        })
      }

      expect(result.resumed.lease.publicationId).toBe(
        first.lease.publicationId,
      )
      expect(Number(result.resumed.lease.generation)).toBe(1)
      expect(result.resumed.lease.slotHighWater).toBe(6)
      expect(result.finalized).toEqual({
        _tag: "Replaced",
        commit: firstIntent.commit,
      })
      expect(Option.isSome(result.lookup?.head ?? Option.none())).toBe(true)

      if (Option.isSome(result.lookup?.head ?? Option.none())) {
        const head = Option.getOrThrow(result.lookup?.head ?? Option.none())
        expect(head.lastAllocatedGeneration).toBe(1)
        expect(head.slotHighWater).toBe(6)
        expect(head.active).toEqual({
          _tag: "Revision",
          token: firstIntent.snapshot.token,
        })
        expect(Option.isNone(head.pending)).toBe(true)
      }

      expect(database.row<{ readonly status: string }>(
        `SELECT status FROM document_graph_projection_publications
         WHERE publication_id = ?`,
        [first.lease.publicationId],
      )?.status).toBe("committed")
      expect(database.row<{ readonly generation: number }>(
        `SELECT generation FROM document_graph_projection_publications
         WHERE mutation_id = ?`,
        [secondIntent.mutationId],
      )).toBeNull()
      expect(database.row<{ readonly mutation_id: string }>(
        `SELECT mutation_id FROM document_graph_projection_mutations
         WHERE mutation_id = ?`,
        [secondIntent.mutationId],
      )).toBeNull()
      expect(database.row<{ readonly mutation_id: string }>(
        `SELECT mutation_id FROM document_graph_projection_mutation_chunks
         WHERE mutation_id = ?`,
        [secondIntent.mutationId],
      )).toBeNull()
    } finally {
      database.close()
    }
  })

  test("does not attach chunks when a mutation ID collides on payload digest", async () => {
    const database = await makeDatabase()

    const prepared = replacementIntent({
      mutation: "replace-digest-collision",
      digestCharacter: "a",
      token: "token-digest-a",
      chunks: 1,
    })

    const collision = replacementIntent({
      mutation: "replace-digest-collision",
      digestCharacter: "b",
      token: "token-digest-b",
      chunks: 2,
    })

    try {
      const result = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator
          const first = yield* coordinator.beginPublication(prepared)

          const second = yield* coordinator.beginPublication(collision).pipe(
            Effect.result,
          )

          return { first, second }
        }),
      )

      expect(result.first._tag).toBe("Publish")
      expect(Result.isFailure(result.second)).toBe(true)

      if (Result.isFailure(result.second)) {
        expect(result.second.failure).toMatchObject({
          _tag: "ProjectionPublicationCoordinatorFailed",
          operation: "begin_publication",
          reason: "invalid_stored_state",
        })
      }

      expect(database.row<{ readonly chunk_count: number }>(
        `SELECT COUNT(*) AS chunk_count
         FROM document_graph_projection_mutation_chunks
         WHERE mutation_id = ?`,
        [prepared.mutationId],
      )?.chunk_count).toBe(1)
    } finally {
      database.close()
    }
  })

  test("rejects a second live writer and stale revision tokens", async () => {
    const database = await makeDatabase()

    const firstIntent = replacementIntent({
      mutation: "replace-conflict-a",
      digestCharacter: "e",
      token: "token-e",
      slotHighWater: 6,
    })

    const secondIntent = replacementIntent({
      mutation: "replace-conflict-b",
      digestCharacter: "f",
      token: "token-f",
    })

    try {
      const pendingConflict = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator
          const begun = yield* coordinator.beginPublication(firstIntent)

          const conflict = yield* coordinator.beginPublication(secondIntent)
            .pipe(Effect.result)

          return { begun, conflict }
        }),
      )

      expect(Result.isFailure(pendingConflict.conflict)).toBe(true)

      if (Result.isFailure(pendingConflict.conflict)) {
        expect(pendingConflict.conflict.failure._tag).toBe(
          "ProjectionPublicationCoordinatorFailed",
        )

        if (pendingConflict.conflict.failure._tag ===
          "ProjectionPublicationCoordinatorFailed") {
          expect(pendingConflict.conflict.failure.reason).toBe(
            "publication_in_progress",
          )
        }
      }

      if (pendingConflict.begun._tag !== "Publish") {
        throw new Error("Expected first lease")
      }

      const firstLease = pendingConflict.begun.lease

      const cas = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator
          yield* coordinator.finalizePublication(firstLease)

          const stale = replacementIntent({
            mutation: "replace-stale-token",
            digestCharacter: "1",
            token: "token-next",
            expectedToken: "not-token-e",
          })

          return yield* coordinator.beginPublication(stale).pipe(Effect.result)
        }),
      )

      expect(Result.isFailure(cas)).toBe(true)

      if (Result.isFailure(cas)) {
        expect(cas.failure).toBeInstanceOf(ProjectionIndexConflict)
      }
    } finally {
      database.close()
    }
  })

  test("finalizes deletion, clears revision inventory, and replays deletes", async () => {
    const database = await makeDatabase()

    const replacement = replacementIntent({
      mutation: "replace-before-delete",
      digestCharacter: "2",
      token: "token-before-delete",
    })

    const deletion = deletionIntent({
      mutation: "delete-exact",
      digestCharacter: "3",
      expectedToken: "token-before-delete",
    })

    try {
      const result = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator
          const replace = yield* coordinator.beginPublication(replacement)

          if (replace._tag !== "Publish") return yield* Effect.die("No lease")
          yield* coordinator.finalizePublication(replace.lease)
          const begunDelete = yield* coordinator.beginPublication(deletion)

          if (begunDelete._tag !== "Publish") return yield* Effect.die("No delete")

          const finalized = yield* coordinator.finalizePublication(
            begunDelete.lease,
          )

          const exactReplay = yield* coordinator.beginPublication(deletion)

          const otherReplay = yield* coordinator.beginPublication(
            deletionIntent({
              mutation: "delete-after-delete",
              digestCharacter: "4",
            }),
          )

          const [revision] = yield* coordinator.loadRevisions([key])

          return { finalized, exactReplay, otherReplay, revision }
        }),
      )

      expect(result.finalized).toEqual({
        _tag: "Deleted",
        deletion: deletion.deletion,
      })
      expect(result.exactReplay).toEqual({
        _tag: "AlreadyCommitted",
        outcome: result.finalized,
      })
      expect(result.otherReplay).toEqual({
        _tag: "AlreadyCommitted",
        outcome: {
          _tag: "Deleted",
          deletion: { deletedRevisions: 0, deletedChunks: 0 },
        },
      })
      expect(Option.isNone(result.revision?.revision ?? Option.none())).toBe(true)
    } finally {
      database.close()
    }
  })

  test("selects only revisions stale against the compiled graph catalog", async () => {
    const database = await makeDatabase()

    const retained = replacementIntent({
      mutation: "replace-retained",
      digestCharacter: "5",
      token: "token-retained",
      projectionVersion: "v2",
    })

    const stale = replacementIntent({
      key: anotherKey,
      mutation: "replace-stale",
      digestCharacter: "6",
      token: "token-stale",
      documentKind: "invoice",
      projectionVersion: "v1",
    })

    try {
      const keys = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator

          for (const intent of [retained, stale]) {
            const begun = yield* coordinator.beginPublication(intent)

            if (begun._tag !== "Publish") return yield* Effect.die("No lease")
            yield* coordinator.finalizePublication(begun.lease)
          }

          return yield* coordinator.listStaleRevisions({
            graph: "contracts",
            registered: [{
              documentKind: "contract",
              projection: "search",
              projectionVersion: "v2",
            }],
          })
        }),
      )

      expect(keys).toEqual([anotherKey])
    } finally {
      database.close()
    }
  })

  test("rejects a stale planned closure and exactly resumes its replanned lease", async () => {
    const database = await makeDatabase()

    const superseded = replacementIntent({
      mutation: "replace-higher-closure",
      digestCharacter: "7",
      token: "token-higher-closure",
      requiredSlotHighWater: 6,
      slotHighWater: 6,
    })

    const stale = replacementIntent({
      mutation: "replace-stale-closure",
      digestCharacter: "8",
      token: "token-stale-closure",
      requiredSlotHighWater: 2,
      slotHighWater: 2,
    })

    const replanned = replacementIntent({
      mutation: "replace-replanned-closure",
      digestCharacter: "9",
      token: "token-replanned-closure",
      requiredSlotHighWater: 2,
      slotHighWater: 6,
    })

    try {
      const result = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator
          const higher = yield* coordinator.beginPublication(superseded)

          if (higher._tag !== "Publish") return yield* Effect.die("No lease")
          yield* coordinator.supersedePublication(higher.lease)

          const staleResult = yield* coordinator.beginPublication(stale).pipe(
            Effect.result,
          )

          const begun = yield* coordinator.beginPublication(replanned)
          const resumed = yield* coordinator.beginPublication(replanned)
          const [lookup] = yield* coordinator.loadHeads([key])

          return { higher, staleResult, begun, resumed, lookup }
        }),
      )

      expect(Result.isFailure(result.staleResult)).toBe(true)

      if (Result.isFailure(result.staleResult)) {
        expect(result.staleResult.failure).toMatchObject({
          _tag: "ProjectionPublicationPlanStale",
          documentKey: key.documentKey,
          projection: key.projection,
          plannedSlotHighWater: 2,
          currentSlotHighWater: 6,
        })
      }

      expect(result.begun._tag).toBe("Publish")
      expect(result.resumed._tag).toBe("Publish")

      if (result.begun._tag !== "Publish" || result.resumed._tag !== "Publish") {
        throw new Error("Expected publication leases")
      }

      expect(Number(result.begun.lease.generation)).toBe(2)
      expect(result.begun.lease.slotHighWater).toBe(6)
      expect(result.resumed.lease).toEqual(result.begun.lease)
      expect(Option.isSome(result.lookup?.head ?? Option.none())).toBe(true)

      if (Option.isSome(result.lookup?.head ?? Option.none())) {
        const head = Option.getOrThrow(result.lookup?.head ?? Option.none())
        expect(head.lastAllocatedGeneration).toBe(2)
        expect(head.slotHighWater).toBe(6)
        expect(head.pending.pipe(Option.map((pending) => ({
          mutationId: pending.mutationId,
          slotHighWater: pending.slotHighWater,
        })), Option.getOrNull)).toEqual({
          mutationId: replanned.mutationId,
          slotHighWater: 6,
        })
      }

      expect(database.row<{ readonly generation: number }>(
        `SELECT generation FROM document_graph_projection_publications
         WHERE mutation_id = ?`,
        [stale.mutationId],
      )).toBeNull()
    } finally {
      database.close()
    }
  })

  test("rejects inherited and initial slot capacity overflow", async () => {
    const database = await makeDatabase()

    const seed = replacementIntent({
      mutation: "replace-capacity-seed",
      digestCharacter: "7",
      token: "token-capacity-seed",
      requiredSlotHighWater: 6,
    })

    const inheritedOverflow = replacementIntent({
      mutation: "replace-inherited-overflow",
      digestCharacter: "8",
      token: "token-inherited-overflow",
      requiredSlotHighWater: 2,
      slotHighWater: 6,
      maximumSlotHighWater: 4,
    })

    const initialOverflow = replacementIntent({
      key: anotherKey,
      mutation: "replace-oversized",
      digestCharacter: "9",
      token: "token-oversized",
      requiredSlotHighWater: 5,
      maximumSlotHighWater: 4,
    })

    try {
      const result = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator
          const seeded = yield* coordinator.beginPublication(seed)

          if (seeded._tag !== "Publish") return yield* Effect.die("No lease")
          yield* coordinator.supersedePublication(seeded.lease)

          const inherited = yield* coordinator.beginPublication(
            inheritedOverflow,
          ).pipe(Effect.result)

          const initial = yield* coordinator.beginPublication(
            initialOverflow,
          ).pipe(
            Effect.result,
          )

          const heads = yield* coordinator.loadHeads([key, anotherKey])

          return { inherited, initial, heads }
        }),
      )

      expect(Result.isFailure(result.inherited)).toBe(true)

      if (Result.isFailure(result.inherited)) {
        expect(result.inherited.failure._tag).toBe(
          "ProjectionPublicationCoordinatorFailed",
        )

        if (result.inherited.failure._tag ===
          "ProjectionPublicationCoordinatorFailed") {
          expect(result.inherited.failure.reason).toBe("capacity_exceeded")
        }
      }

      expect(Result.isFailure(result.initial)).toBe(true)

      if (Result.isFailure(result.initial)) {
        expect(result.initial.failure._tag).toBe(
          "ProjectionPublicationCoordinatorFailed",
        )

        if (result.initial.failure._tag ===
          "ProjectionPublicationCoordinatorFailed") {
          expect(result.initial.failure.reason).toBe("capacity_exceeded")
        }
      }

      const inheritedHead = result.heads[0]?.head ?? Option.none()
      expect(Option.isSome(inheritedHead)).toBe(true)

      if (Option.isSome(inheritedHead)) {
        expect(inheritedHead.value.slotHighWater).toBe(6)
        expect(inheritedHead.value.lastAllocatedGeneration).toBe(1)
        expect(Option.isNone(inheritedHead.value.pending)).toBe(true)
      }

      expect(Option.isNone(result.heads[1]?.head ?? Option.none())).toBe(true)
    } finally {
      database.close()
    }
  })

  test("classifies malformed persisted heads as invalid stored state", async () => {
    const database = await makeDatabase()

    const intent = replacementIntent({
      mutation: "replace-corrupt",
      digestCharacter: "8",
      token: "token-corrupt",
    })

    try {
      const begun = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator
          const result = yield* coordinator.beginPublication(intent)

          if (result._tag !== "Publish") return yield* Effect.die("No lease")
          yield* coordinator.finalizePublication(result.lease)

          return result
        }),
      )

      expect(begun._tag).toBe("Publish")
      database.execute(
        `UPDATE document_graph_projection_heads SET active_token = ''
         WHERE document_key = ? AND projection_id = ?`,
        [key.documentKey, key.projection],
      )

      const result = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator

          return yield* coordinator.loadHeads([key]).pipe(Effect.result)
        }),
      )

      expect(Result.isFailure(result)).toBe(true)

      if (Result.isFailure(result)) {
        expect(result.failure.reason).toBe("invalid_stored_state")
      }
    } finally {
      database.close()
    }
  })

  test("classifies a malformed active revision token as invalid stored state", async () => {
    const database = await makeDatabase()

    const intent = replacementIntent({
      mutation: "replace-corrupt-revision",
      digestCharacter: "9",
      token: "token-corrupt-revision",
    })

    try {
      await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator
          const result = yield* coordinator.beginPublication(intent)

          if (result._tag !== "Publish") return yield* Effect.die("No lease")
          yield* coordinator.finalizePublication(result.lease)
        }),
      )
      database.execute(
        `UPDATE document_graph_projection_heads SET active_token = ''
         WHERE document_key = ? AND projection_id = ?`,
        [key.documentKey, key.projection],
      )

      const result = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator

          return yield* coordinator.loadRevisions([key]).pipe(Effect.result)
        }),
      )

      expect(Result.isFailure(result)).toBe(true)

      if (Result.isFailure(result)) {
        expect(result.failure.reason).toBe("invalid_stored_state")
      }
    } finally {
      database.close()
    }
  })

  test("classifies a malformed staged mutation during begin as typed invalid state", async () => {
    const database = await makeDatabase()

    const intent = replacementIntent({
      mutation: "replace-corrupt-begin",
      digestCharacter: "a",
      token: "token-corrupt-begin",
    })

    try {
      await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator
          const begun = yield* coordinator.beginPublication(intent)

          if (begun._tag !== "Publish") return yield* Effect.die("No lease")
          yield* coordinator.supersedePublication(begun.lease)
        }),
      )
      database.execute("PRAGMA ignore_check_constraints = ON")
      database.execute(
        `UPDATE document_graph_projection_mutations
         SET payload_digest = 'invalid'
         WHERE mutation_id = ?`,
        [intent.mutationId],
      )

      const result = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator

          return yield* coordinator.beginPublication(intent).pipe(
            Effect.result,
          )
        }),
      )

      expect(Result.isFailure(result)).toBe(true)

      if (Result.isFailure(result)) {
        expect(result.failure).toMatchObject({
          _tag: "ProjectionPublicationCoordinatorFailed",
          operation: "begin_publication",
          reason: "invalid_stored_state",
        })
      }
    } finally {
      database.close()
    }
  })

  test("bounds completed journals and collects their orphaned mutation payloads", async () => {
    const database = await makeDatabase()

    const intents = ["a", "b", "c", "d"].map((digestCharacter, index) => {
      const requiredIntent = {
        mutation: `replace-retention-${index}`,
        digestCharacter,
        token: `token-retention-${index}`,
      }

      return replacementIntent(index === 0
        ? requiredIntent
        : {
            ...requiredIntent,
            expectedToken: `token-retention-${index - 1}`,
          })
    })

    try {
      const generations = await runWithCoordinator(
        database,
        Effect.gen(function*() {
          const coordinator = yield* ProjectionPublicationCoordinator

          return yield* Effect.forEach(intents, (intent) =>
            Effect.gen(function*() {
              const begun = yield* coordinator.beginPublication(intent)

              if (begun._tag !== "Publish") {
                return yield* Effect.die("Expected a publication lease")
              }

              yield* coordinator.finalizePublication(begun.lease)

              return Number(begun.lease.generation)
            }), { concurrency: 1 })
        }),
        "schema-v1",
        2,
      )

      expect(generations).toEqual([1, 2, 3, 4])
      expect(database.row<{ readonly count: number }>(
        `SELECT COUNT(*) AS count
         FROM document_graph_projection_publications`,
      )?.count).toBe(2)
      expect(database.row<{ readonly count: number }>(
        `SELECT COUNT(*) AS count
         FROM document_graph_projection_mutations`,
      )?.count).toBe(2)
      expect(database.row<{ readonly count: number }>(
        `SELECT COUNT(*) AS count
         FROM document_graph_projection_mutation_chunks`,
      )?.count).toBe(4)
    } finally {
      database.close()
    }
  })
})
