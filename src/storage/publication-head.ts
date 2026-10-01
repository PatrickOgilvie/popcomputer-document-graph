import { Option } from "effect"
import {
  ProjectionPublicationPlanStale,
  type ProjectionPublicationHead,
  type ProjectionPublicationIntent,
  type ProjectionPublicationOutcome,
} from "../indexing/projection-publication.js"

/** Whether the head already made this exact intent its active state. */
export const sameActiveIntent = (
  head: ProjectionPublicationHead,
  intent: ProjectionPublicationIntent,
): boolean =>
  Option.isSome(head.activeMutationId) &&
  Option.isSome(head.activePayloadDigest) &&
  head.activeMutationId.value === intent.mutationId &&
  head.activePayloadDigest.value === intent.payloadDigest

/** Whether the head holds this exact intent and slot closure as pending. */
export const samePendingIntent = (
  head: ProjectionPublicationHead,
  intent: ProjectionPublicationIntent,
): boolean =>
  Option.isSome(head.pending) &&
  head.pending.value.mutationId === intent.mutationId &&
  head.pending.value.payloadDigest === intent.payloadDigest &&
  head.pending.value.slotHighWater === intent.slotHighWater

/** Whether the active revision is the one the intent was planned against. */
export const activeMatchesExpectedToken = (
  head: ProjectionPublicationHead,
  intent: ProjectionPublicationIntent,
): boolean =>
  Option.match(intent.expectedToken, {
    onNone: () => head.active._tag !== "Revision",
    onSome: (expectedToken) =>
      head.active._tag === "Revision" && head.active.token === expectedToken,
  })

/** Logical outcome recorded once a publication of this intent commits. */
export const publicationOutcomeFor = (
  intent: ProjectionPublicationIntent,
): ProjectionPublicationOutcome =>
  intent._tag === "Replace"
    ? { _tag: "Replaced", commit: intent.commit }
    : { _tag: "Deleted", deletion: intent.deletion }

/** A plan computed before the durable slot closure reached its current value. */
export const stalePublicationPlan = (
  intent: ProjectionPublicationIntent,
  currentSlotHighWater: number,
): ProjectionPublicationPlanStale =>
  new ProjectionPublicationPlanStale({
    documentKey: intent.key.documentKey,
    projection: intent.key.projection,
    plannedSlotHighWater: intent.slotHighWater,
    currentSlotHighWater,
  })
