import { Context, type Effect, type Option, Schema } from "effect"
import type {
  IndexedRevisionSnapshot,
  IndexRevisionToken,
  ProjectionIndexCommit,
  ProjectionIndexConflict,
  ProjectionIndexDeletion,
  ProjectionIndexKey,
  ProjectionIndexPrune,
  ProjectionRevisionLookup,
  PruneGraphIndex,
} from "./projection-index.js"

const IdentifierSchema = Schema.Trimmed.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(128),
)

/** Stable identifier for one logical replacement or deletion intent. */
export const ProjectionMutationIdSchema = IdentifierSchema.pipe(
  Schema.brand("ProjectionMutationId"),
)

/** Stable identifier for one logical replacement or deletion intent. */
export type ProjectionMutationId = typeof ProjectionMutationIdSchema.Type

/** Digest of every normalized physical row, including vector bytes. */
export const ProjectionPayloadDigestSchema = Schema.String.check(
  Schema.isPattern(/^[0-9a-f]{64}$/u),
).pipe(Schema.brand("ProjectionPayloadDigest"))

/** Digest of every normalized physical row, including vector bytes. */
export type ProjectionPayloadDigest =
  typeof ProjectionPayloadDigestSchema.Type

/** Identity shared by every row in one physical publication attempt. */
export const ProjectionPublicationIdSchema = IdentifierSchema.pipe(
  Schema.brand("ProjectionPublicationId"),
)

/** Identity shared by every row in one physical publication attempt. */
export type ProjectionPublicationId =
  typeof ProjectionPublicationIdSchema.Type

/** Strictly increasing, never-reused publication generation. */
export const ProjectionPublicationGenerationSchema = Schema.Number.pipe(
  Schema.check(Schema.isInt()),
  Schema.check(Schema.isGreaterThanOrEqualTo(1)),
  Schema.brand("ProjectionPublicationGeneration"),
)

/** Strictly increasing, never-reused publication generation. */
export type ProjectionPublicationGeneration =
  typeof ProjectionPublicationGenerationSchema.Type

/** Control-plane catalog fields required for schema pruning. */
export interface ProjectionPublicationCatalog {
  readonly graph: string
  readonly documentKind: string
  readonly projectionVersion: string
}

/** Complete immutable replacement inventory staged before publication CAS. */
export interface ProjectionReplacementIntent {
  readonly _tag: "Replace"
  readonly key: ProjectionIndexKey
  readonly expectedToken: Option.Option<IndexRevisionToken>
  readonly mutationId: ProjectionMutationId
  readonly payloadDigest: ProjectionPayloadDigest
  readonly snapshot: IndexedRevisionSnapshot
  readonly catalog: ProjectionPublicationCatalog
  readonly liveSlotCount: number
  /** Highest addressed slot plus one for this logical revision. */
  readonly requiredSlotHighWater: number
  /** Complete frozen slot closure represented by this mutation identity. */
  readonly slotHighWater: number
  /** Provider closure bound checked atomically before generation allocation. */
  readonly maximumSlotHighWater: number
  readonly commit: ProjectionIndexCommit
}

/** Immutable logical deletion staged before publication CAS. */
export interface ProjectionDeletionIntent {
  readonly _tag: "Delete"
  readonly key: ProjectionIndexKey
  readonly expectedToken: Option.Option<IndexRevisionToken>
  readonly mutationId: ProjectionMutationId
  readonly payloadDigest: ProjectionPayloadDigest
  readonly deletion: ProjectionIndexDeletion
  /** Complete frozen slot closure represented by this mutation identity. */
  readonly slotHighWater: number
  readonly maximumSlotHighWater: number
}

/** One complete desired publication coordinated through D1. */
export type ProjectionPublicationIntent =
  | ProjectionReplacementIntent
  | ProjectionDeletionIntent

/**
 * Physical publication authority leased from the coordinator.
 *
 * A diagnostic deadline does not revoke this authority. It remains exclusive
 * until this exact publication is finalized or explicitly superseded.
 */
export interface ProjectionPublicationLease {
  readonly intent: ProjectionPublicationIntent
  readonly publicationId: ProjectionPublicationId
  readonly generation: ProjectionPublicationGeneration
  /** Never decreases, including after failed or superseded attempts. */
  readonly slotHighWater: number
}

/** Logical result retained for exact committed replay. */
export type ProjectionPublicationOutcome =
  | {
      readonly _tag: "Replaced"
      readonly commit: ProjectionIndexCommit
    }
  | {
      readonly _tag: "Deleted"
      readonly deletion: ProjectionIndexDeletion
    }

/** Result of beginning or resuming one publication. */
export type BeginProjectionPublication =
  | {
      readonly _tag: "AlreadyCommitted"
      readonly outcome: ProjectionPublicationOutcome
    }
  | {
      readonly _tag: "Publish"
      readonly lease: ProjectionPublicationLease
    }

/** Durable pending state used to reconcile ambiguous provider outcomes. */
export interface PendingProjectionPublication {
  readonly mutationId: ProjectionMutationId
  readonly payloadDigest: ProjectionPayloadDigest
  readonly publicationId: ProjectionPublicationId
  readonly generation: ProjectionPublicationGeneration
  readonly slotHighWater: number
  readonly operation: "replace" | "delete"
}

/** Durable coordinator head for one document/projection identity. */
export interface ProjectionPublicationHead {
  readonly key: ProjectionIndexKey
  readonly lastAllocatedGeneration: number
  readonly slotHighWater: number
  readonly active:
    | { readonly _tag: "NeverPublished" }
    | {
        readonly _tag: "Revision"
        readonly token: IndexRevisionToken
      }
    | { readonly _tag: "Deleted" }
  readonly activeMutationId: Option.Option<ProjectionMutationId>
  readonly activePayloadDigest: Option.Option<ProjectionPayloadDigest>
  readonly pending: Option.Option<PendingProjectionPublication>
}

/** Ordered head lookup preserving duplicate request keys. */
export interface ProjectionPublicationHeadLookup {
  readonly key: ProjectionIndexKey
  readonly head: Option.Option<ProjectionPublicationHead>
}

/** Publication coordinator could not complete a durable state transition. */
export class ProjectionPublicationCoordinatorFailed extends Schema.TaggedError<
  ProjectionPublicationCoordinatorFailed
>()("ProjectionPublicationCoordinatorFailed", {
  operation: Schema.Literals([
    "load_revisions",
    "load_heads",
    "begin_publication",
    "finalize_publication",
    "supersede_publication",
    "list_stale",
  ]),
  reason: Schema.Literals([
    "unavailable",
    "invalid_stored_state",
    "publication_in_progress",
    "publication_in_doubt",
    "capacity_exceeded",
  ]),
  cause: Schema.Unknown,
}) {}

/** A different generation became authoritative before finalization. */
export class ProjectionPublicationSuperseded extends Schema.TaggedError<
  ProjectionPublicationSuperseded
>()("ProjectionPublicationSuperseded", {
  documentKey: Schema.String,
  projection: Schema.String,
}) {}

/** A publication plan was computed before the durable slot closure advanced. */
export class ProjectionPublicationPlanStale extends Schema.TaggedError<
  ProjectionPublicationPlanStale
>()("ProjectionPublicationPlanStale", {
  documentKey: Schema.String,
  projection: Schema.String,
  plannedSlotHighWater: Schema.Number.pipe(
    Schema.check(Schema.isInt()),
    Schema.check(Schema.isGreaterThanOrEqualTo(0)),
  ),
  currentSlotHighWater: Schema.Number.pipe(
    Schema.check(Schema.isInt()),
    Schema.check(Schema.isGreaterThanOrEqualTo(0)),
  ),
}) {}

/** Durable CAS/catalog capability used by remote projection adapters. */
export interface ProjectionPublicationCoordinatorService {
  /** Immutable physical retrieval partition represented by this coordinator. */
  readonly indexGeneration: string

  /** Load active logical snapshots in request order, including duplicates. */
  readonly loadRevisions: (
    keys: readonly [ProjectionIndexKey, ...ReadonlyArray<ProjectionIndexKey>],
  ) => Effect.Effect<
    ReadonlyArray<ProjectionRevisionLookup>,
    ProjectionPublicationCoordinatorFailed
  >

  /** Load generation/high-water/pending state in request order. */
  readonly loadHeads: (
    keys: readonly [ProjectionIndexKey, ...ReadonlyArray<ProjectionIndexKey>],
  ) => Effect.Effect<
    ReadonlyArray<ProjectionPublicationHeadLookup>,
    ProjectionPublicationCoordinatorFailed
  >

  /** Atomically check logical CAS and allocate or resume publication authority. */
  readonly beginPublication: (
    intent: ProjectionPublicationIntent,
  ) => Effect.Effect<
    BeginProjectionPublication,
    | ProjectionPublicationCoordinatorFailed
    | ProjectionPublicationPlanStale
    | ProjectionIndexConflict
  >

  /** Make one successfully published lease the active logical catalog state. */
  readonly finalizePublication: (
    lease: ProjectionPublicationLease,
  ) => Effect.Effect<
    ProjectionPublicationOutcome,
    ProjectionPublicationCoordinatorFailed | ProjectionPublicationSuperseded
  >

  /**
   * Retire publication authority only after provider fencing proves that no
   * attempt sharing this lease can publish. A single rejected request is not
   * sufficient proof. The retired generation is never reused.
   */
  readonly supersedePublication: (
    lease: ProjectionPublicationLease,
  ) => Effect.Effect<void, ProjectionPublicationCoordinatorFailed>

  /** Select active revision keys that no longer match one compiled graph. */
  readonly listStaleRevisions: (
    input: PruneGraphIndex,
  ) => Effect.Effect<
    ReadonlyArray<ProjectionIndexKey>,
    ProjectionPublicationCoordinatorFailed
  >
}

/** Effect service tag for D1-owned publication coordination. */
export class ProjectionPublicationCoordinator extends Context.Service<
  ProjectionPublicationCoordinator,
  ProjectionPublicationCoordinatorService
>()("@popcomputer/document-graph/ProjectionPublicationCoordinator") {}

/** Sum per-revision deletion results returned by convergent graph pruning. */
export const sumProjectionPrune = (
  deletions: ReadonlyArray<ProjectionIndexDeletion>,
): ProjectionIndexPrune =>
  deletions.reduce(
    (total, deletion) => ({
      deletedRevisions: total.deletedRevisions + deletion.deletedRevisions,
      deletedChunks: total.deletedChunks + deletion.deletedChunks,
    }),
    { deletedRevisions: 0, deletedChunks: 0 },
  )
