import { describe, expect, test } from "bun:test"
import type { NamespaceWriteParams } from "@turbopuffer/turbopuffer"
import { Deferred, Effect, Fiber, Option, Result, Schema } from "effect"
import { makeProjectionIndexStoreConformanceFixture } from "../../src/conformance/projection-index-conformance.js"
import {
  ChunkIdSchema,
  ContentHashSchema,
  makeDocumentKey,
  ProjectionRevisionHashSchema,
} from "../../src/document/document-identity.js"
import { defineEmbeddingProfile } from "../../src/indexing/embedding-provider.js"
import {
  IndexRevisionTokenSchema,
  ProjectionIndexConflict,
  type ProjectionIndexKey,
} from "../../src/indexing/projection-index.js"
import {
  ProjectionMutationIdSchema,
  ProjectionPayloadDigestSchema,
  ProjectionPublicationCoordinator,
  type ProjectionDeletionIntent,
  type ProjectionReplacementIntent,
} from "../../src/indexing/projection-publication.js"
import { TurbopufferClient, type TurbopufferClientService } from "../../src/storage/turbopuffer/client.js"
import { TurbopufferTransportFailed } from "../../src/storage/turbopuffer/errors.js"
import { makeTurbopufferWorkspacePartition } from "../../src/storage/turbopuffer/partition.js"
import { makeTurbopufferProjectionIndexStore } from "../../src/storage/turbopuffer/projection-index.js"

/** Coordinator options a conformance case may vary. */
export interface CoordinatorRunOptions {
  readonly indexGeneration?: string | undefined
  readonly retainedPublicationHistory?: number | undefined
}

/** Durable coordinator under test, backed by storage that persists across runs. */
export interface PublicationCoordinatorHarness {
  /** Run against a coordinator over this harness's store. */
  readonly run: <A, E>(
    effect: Effect.Effect<A, E, ProjectionPublicationCoordinator>,
    options?: CoordinatorRunOptions,
  ) => Promise<A>
  readonly close: () => Promise<void>
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

export const key: ProjectionIndexKey = { documentKey, projection: "search" }

export const anotherKey: ProjectionIndexKey = {
  documentKey: anotherDocumentKey,
  projection: "search",
}

const profile = defineEmbeddingProfile({
  id: "test/publication-coordinator",
  version: "v1",
  dimensions: 3,
})

export const replacementIntent = (input: {
  readonly key?: ProjectionIndexKey
  readonly mutation: string
  readonly digestCharacter: string
  readonly token: string
  readonly expectedToken?: string | undefined
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
  const requiredSlotHighWater = input.requiredSlotHighWater ?? chunks.length

  return {
    _tag: "Replace",
    key: input.key ?? key,
    expectedToken: input.expectedToken === undefined
      ? Option.none()
      : Option.some(Schema.decodeSync(IndexRevisionTokenSchema)(input.expectedToken)),
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

export const deletionIntent = (input: {
  readonly mutation: string
  readonly digestCharacter: string
  readonly expectedToken?: string
}): ProjectionDeletionIntent => ({
  _tag: "Delete",
  key,
  expectedToken: input.expectedToken === undefined
    ? Option.none()
    : Option.some(Schema.decodeSync(IndexRevisionTokenSchema)(input.expectedToken)),
  mutationId: Schema.decodeSync(ProjectionMutationIdSchema)(input.mutation),
  payloadDigest: Schema.decodeSync(ProjectionPayloadDigestSchema)(
    input.digestCharacter.repeat(64),
  ),
  deletion: { deletedRevisions: 1, deletedChunks: 2 },
  slotHighWater: 2,
  maximumSlotHighWater: 100,
})

const failureOf = <A, E>(result: Result.Result<A, E>): E => {
  if (Result.isFailure(result)) return result.failure

  throw new Error("Expected a failure")
}

const leaseOf = <T extends { readonly _tag: string }>(
  begun: T,
): Extract<T, { readonly _tag: "Publish" }> => {
  if (begun._tag !== "Publish") throw new Error("Expected a publication lease")

  // SAFETY: the guard above proved this union member's discriminant.
  return begun as Extract<T, { readonly _tag: "Publish" }>
}

/**
 * Behaviour every durable publication coordinator shares, independent of the
 * database that stores its journal.
 */
export const describePublicationCoordinatorConformance = (
  name: string,
  makeHarness: () => Promise<PublicationCoordinatorHarness>,
  options: { readonly skip?: boolean } = {},
): void => {
  const coordinatorTest = options.skip === true ? test.skip : test

  const withHarness = (
    run: (harness: PublicationCoordinatorHarness) => Promise<void>,
  ) => async () => {
    const harness = await makeHarness()

    try {
      await run(harness)
    } finally {
      await harness.close()
    }
  }

  describe(`${name} publication coordinator conformance`, () => {
    coordinatorTest("begins and resumes the same durable publication lease", withHarness(async (harness) => {
      const intent = replacementIntent({
        mutation: "replace-resume",
        digestCharacter: "a",
        token: "token-a",
        requiredSlotHighWater: 4,
      })

      const result = await harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const first = leaseOf(yield* coordinator.beginPublication(intent))
        const resumed = leaseOf(yield* coordinator.beginPublication(intent))
        const heads = yield* coordinator.loadHeads([key, key])

        return { first, resumed, heads }
      }))

      expect(result.resumed.lease.publicationId).toBe(result.first.lease.publicationId)
      expect(Number(result.resumed.lease.generation)).toBe(1)
      expect(result.resumed.lease.slotHighWater).toBe(4)
      expect(result.heads).toHaveLength(2)
      expect(result.heads.map((lookup) => lookup.head.pipe(
        Option.flatMap((head) => head.pending),
        Option.map((pending) => pending.publicationId),
        Option.getOrNull,
      ))).toEqual([result.first.lease.publicationId, result.first.lease.publicationId])
    }))

    coordinatorTest("isolates heads and generation counters by physical index generation", withHarness(async (harness) => {
      const intent = replacementIntent({
        mutation: "replace-side-by-side",
        digestCharacter: "a",
        token: "token-side-by-side",
        requiredSlotHighWater: 3,
      })

      const publishIn = (indexGeneration: string) => harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const begun = leaseOf(yield* coordinator.beginPublication(intent))
        yield* coordinator.finalizePublication(begun.lease)
        const [lookup] = yield* coordinator.loadHeads([key])

        return { lease: begun.lease, lookup }
      }), { indexGeneration })

      const first = await publishIn("schema-v1")
      const second = await publishIn("schema-v2")

      expect(Number(first.lease.generation)).toBe(1)
      expect(Number(second.lease.generation)).toBe(1)
      expect(first.lease.publicationId).not.toBe(second.lease.publicationId)
      expect(Option.isSome(first.lookup?.head ?? Option.none())).toBe(true)
      expect(Option.isSome(second.lookup?.head ?? Option.none())).toBe(true)
    }))

    coordinatorTest("finalizes a revision and replays its exact committed outcome", withHarness(async (harness) => {
      const intent = replacementIntent({
        mutation: "replace-finalize",
        digestCharacter: "b",
        token: "token-b",
      })

      const result = await harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const begun = leaseOf(yield* coordinator.beginPublication(intent))
        const finalized = yield* coordinator.finalizePublication(begun.lease)
        const finalizedAgain = yield* coordinator.finalizePublication(begun.lease)

        // A retry replans counts against the now-active revision, but the
        // durable committed outcome must remain the original.
        const replay = yield* coordinator.beginPublication({
          ...intent,
          commit: { ...intent.commit, inserted: 0, updated: intent.liveSlotCount },
        })

        const revisions = yield* coordinator.loadRevisions([key, key])

        return { finalized, finalizedAgain, replay, revisions }
      }))

      expect(result.finalized).toEqual({ _tag: "Replaced", commit: intent.commit })
      expect(result.finalizedAgain).toEqual(result.finalized)
      expect(result.replay).toEqual({ _tag: "AlreadyCommitted", outcome: result.finalized })
      expect(result.revisions.map((lookup) => lookup.revision.pipe(
        Option.map((revision) => ({
          token: revision.token,
          revisionHash: revision.revisionHash,
          chunks: revision.chunks,
        })),
        Option.getOrNull,
      ))).toEqual([0, 1].map(() => ({
        token: intent.snapshot.token,
        revisionHash: intent.snapshot.revisionHash,
        chunks: intent.snapshot.chunks,
      })))
    }))

    coordinatorTest("keeps an expired prepared publication authoritative until reconciliation", withHarness(async (harness) => {
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

      const result = await harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const first = leaseOf(yield* coordinator.beginPublication(firstIntent))
        const second = yield* coordinator.beginPublication(secondIntent).pipe(Effect.result)
        const resumed = leaseOf(yield* coordinator.beginPublication(firstIntent))
        const finalized = yield* coordinator.finalizePublication(resumed.lease)
        const [lookup] = yield* coordinator.loadHeads([key])

        return { first, second, resumed, finalized, lookup }
      }))

      expect(failureOf(result.second)).toMatchObject({
        _tag: "ProjectionPublicationCoordinatorFailed",
        operation: "begin_publication",
        reason: "publication_in_progress",
      })
      expect(result.resumed.lease.publicationId).toBe(result.first.lease.publicationId)
      expect(result.finalized).toEqual({ _tag: "Replaced", commit: firstIntent.commit })

      const head = Option.getOrThrow(result.lookup?.head ?? Option.none())
      expect(head.lastAllocatedGeneration).toBe(1)
      expect(head.slotHighWater).toBe(6)
      expect(head.active).toEqual({ _tag: "Revision", token: firstIntent.snapshot.token })
      expect(Option.isNone(head.pending)).toBe(true)
    }))

    coordinatorTest("rejects a mutation ID reused with a different payload", withHarness(async (harness) => {
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

      const result = await harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const first = leaseOf(yield* coordinator.beginPublication(prepared))
        const second = yield* coordinator.beginPublication(collision).pipe(Effect.result)
        yield* coordinator.finalizePublication(first.lease)
        const [revision] = yield* coordinator.loadRevisions([key])

        return { second, revision }
      }))

      expect(failureOf(result.second)).toMatchObject({
        _tag: "ProjectionPublicationCoordinatorFailed",
        operation: "begin_publication",
        reason: "invalid_stored_state",
      })
      expect(Option.getOrThrow(result.revision?.revision ?? Option.none()).chunks)
        .toEqual(prepared.snapshot.chunks)
    }))

    coordinatorTest("rejects a second live writer and stale revision tokens", withHarness(async (harness) => {
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

      const result = await harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const begun = leaseOf(yield* coordinator.beginPublication(firstIntent))
        const pendingConflict = yield* coordinator.beginPublication(secondIntent).pipe(Effect.result)
        yield* coordinator.finalizePublication(begun.lease)

        const staleToken = yield* coordinator.beginPublication(replacementIntent({
          mutation: "replace-stale-token",
          digestCharacter: "1",
          token: "token-next",
          expectedToken: "not-token-e",
          slotHighWater: 6,
        })).pipe(Effect.result)

        return { pendingConflict, staleToken }
      }))

      expect(failureOf(result.pendingConflict)).toMatchObject({
        _tag: "ProjectionPublicationCoordinatorFailed",
        reason: "publication_in_progress",
      })
      expect(failureOf(result.staleToken)).toBeInstanceOf(ProjectionIndexConflict)
    }))

    coordinatorTest("finalizes deletion, clears revision inventory, and replays deletes", withHarness(async (harness) => {
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

      const result = await harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const replace = leaseOf(yield* coordinator.beginPublication(replacement))
        yield* coordinator.finalizePublication(replace.lease)
        const begunDelete = leaseOf(yield* coordinator.beginPublication(deletion))
        const finalized = yield* coordinator.finalizePublication(begunDelete.lease)
        const exactReplay = yield* coordinator.beginPublication(deletion)

        const otherReplay = yield* coordinator.beginPublication(deletionIntent({
          mutation: "delete-after-delete",
          digestCharacter: "4",
        }))

        const [revision] = yield* coordinator.loadRevisions([key])

        return { finalized, exactReplay, otherReplay, revision }
      }))

      expect(result.finalized).toEqual({ _tag: "Deleted", deletion: deletion.deletion })
      expect(result.exactReplay).toEqual({ _tag: "AlreadyCommitted", outcome: result.finalized })
      expect(result.otherReplay).toEqual({
        _tag: "AlreadyCommitted",
        outcome: { _tag: "Deleted", deletion: { deletedRevisions: 0, deletedChunks: 0 } },
      })
      expect(Option.isNone(result.revision?.revision ?? Option.none())).toBe(true)
    }))

    coordinatorTest("selects only revisions stale against the compiled graph catalog", withHarness(async (harness) => {
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

      const keys = await harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator

        for (const intent of [retained, stale]) {
          const begun = leaseOf(yield* coordinator.beginPublication(intent))
          yield* coordinator.finalizePublication(begun.lease)
        }

        return yield* coordinator.listStaleRevisions({
          graph: "contracts",
          registered: [{ documentKind: "contract", projection: "search", projectionVersion: "v2" }],
        })
      }))

      expect(keys).toEqual([anotherKey])
    }))

    coordinatorTest("rejects a stale planned closure and exactly resumes its replanned lease", withHarness(async (harness) => {
      const higher = replacementIntent({
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

      const result = await harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const higherLease = leaseOf(yield* coordinator.beginPublication(higher))
        yield* coordinator.supersedePublication(higherLease.lease)
        const staleResult = yield* coordinator.beginPublication(stale).pipe(Effect.result)
        const begun = leaseOf(yield* coordinator.beginPublication(replanned))
        const resumed = leaseOf(yield* coordinator.beginPublication(replanned))
        const [lookup] = yield* coordinator.loadHeads([key])

        return { staleResult, begun, resumed, lookup }
      }))

      expect(failureOf(result.staleResult)).toMatchObject({
        _tag: "ProjectionPublicationPlanStale",
        documentKey: key.documentKey,
        projection: key.projection,
        plannedSlotHighWater: 2,
        currentSlotHighWater: 6,
      })
      expect(Number(result.begun.lease.generation)).toBe(2)
      expect(result.begun.lease.slotHighWater).toBe(6)
      expect(result.resumed.lease).toEqual(result.begun.lease)

      const head = Option.getOrThrow(result.lookup?.head ?? Option.none())
      expect(head.lastAllocatedGeneration).toBe(2)
      expect(head.slotHighWater).toBe(6)
      expect(head.pending.pipe(
        Option.map((pending) => ({ mutationId: pending.mutationId, slotHighWater: pending.slotHighWater })),
        Option.getOrNull,
      )).toEqual({ mutationId: replanned.mutationId, slotHighWater: 6 })
    }))

    coordinatorTest("rejects inherited and initial slot capacity overflow", withHarness(async (harness) => {
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

      const result = await harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const seeded = leaseOf(yield* coordinator.beginPublication(seed))
        yield* coordinator.supersedePublication(seeded.lease)
        const inherited = yield* coordinator.beginPublication(inheritedOverflow).pipe(Effect.result)
        const initial = yield* coordinator.beginPublication(initialOverflow).pipe(Effect.result)
        const heads = yield* coordinator.loadHeads([key, anotherKey])

        return { inherited, initial, heads }
      }))

      expect(failureOf(result.inherited)).toMatchObject({ reason: "capacity_exceeded" })
      expect(failureOf(result.initial)).toMatchObject({ reason: "capacity_exceeded" })

      const inheritedHead = Option.getOrThrow(result.heads[0]?.head ?? Option.none())
      expect(inheritedHead.slotHighWater).toBe(6)
      expect(inheritedHead.lastAllocatedGeneration).toBe(1)
      expect(Option.isNone(inheritedHead.pending)).toBe(true)
      expect(Option.isNone(result.heads[1]?.head ?? Option.none())).toBe(true)
    }))

    coordinatorTest("reports a lease superseded before finalization", withHarness(async (harness) => {
      const intent = replacementIntent({
        mutation: "replace-superseded",
        digestCharacter: "a",
        token: "token-superseded",
      })

      const result = await harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        const begun = leaseOf(yield* coordinator.beginPublication(intent))
        yield* coordinator.supersedePublication(begun.lease)

        return yield* coordinator.finalizePublication(begun.lease).pipe(Effect.result)
      }))

      expect(failureOf(result)).toMatchObject({
        _tag: "ProjectionPublicationSuperseded",
        documentKey: key.documentKey,
        projection: key.projection,
      })
    }))

    coordinatorTest("keeps publishing after bounded history retires old journal entries", withHarness(async (harness) => {
      const result = await harness.run(Effect.gen(function*() {
        const coordinator = yield* ProjectionPublicationCoordinator
        let token: string | undefined

        for (const [index, digest] of ["1", "2", "3", "4"].entries()) {
          const intent = replacementIntent({
            mutation: `replace-history-${index}`,
            digestCharacter: digest,
            token: `token-history-${index}`,
            expectedToken: token,
          })

          const begun = leaseOf(yield* coordinator.beginPublication(intent))
          yield* coordinator.finalizePublication(begun.lease)
          token = intent.snapshot.token
        }

        const [lookup] = yield* coordinator.loadHeads([key])
        const [revision] = yield* coordinator.loadRevisions([key])

        return { lookup, revision }
      }), { retainedPublicationHistory: 1 })

      const head = Option.getOrThrow(result.lookup?.head ?? Option.none())
      expect(head.lastAllocatedGeneration).toBe(4)
      expect(Option.getOrThrow(result.revision?.revision ?? Option.none()).token)
        .toBe(Schema.decodeSync(IndexRevisionTokenSchema)("token-history-3"))
    }))

    coordinatorTest.each(["replace", "delete"] as const)(
      "a rejected duplicate cannot retire an in-flight Turbopuffer %s publication",
      (operation) => withHarness(async (harness) => {
        const fixture = makeProjectionIndexStoreConformanceFixture()

        const partition = makeTurbopufferWorkspacePartition({
          deploymentId: "test:publication-overlap",
          endpoint: { _tag: "Region", region: "gcp-us-central1" },
          workspace: "publication-overlap",
          embeddingProfile: fixture.initial.embeddingProfile,
          schemaGeneration: 1,
        })

        const result = await harness.run(Effect.gen(function*() {
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
        }), { indexGeneration: partition.d1IndexGeneration })

        expect(result.duplicate).toMatchObject({ _tag: "Failure", failure: { reason: "unavailable" } })
        expect(Option.isSome(result.during.pending)).toBe(true)
        expect(Result.isSuccess(result.completed)).toBe(true)
        expect(Option.isNone(result.after.pending)).toBe(true)
        const liveRows = result.providerRows.filter((row) => row["is_live"] === true)

        if (operation === "replace") {
          expect(Option.getOrThrow(result.revision).revisionHash).toBe(fixture.metadataOnly.revisionHash)
          expect(liveRows.map((row) => row["revision_hash"]))
            .toEqual(fixture.metadataOnly.chunks.map(() => fixture.metadataOnly.revisionHash))
        } else {
          expect(Option.isNone(result.revision)).toBe(true)
          expect(liveRows).toHaveLength(0)
        }

        expect(result.writes).toBe(3)
      })(),
    )
  })
}
