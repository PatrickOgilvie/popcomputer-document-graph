import { Context, Effect, Schema } from "effect"
import type { ContentHash } from "../document/document-identity.js"

/** Stable identity for one embedding model and configuration. */
export const EmbeddingProfileIdSchema = Schema.Trimmed.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(200),
  Schema.isPattern(/^[a-z0-9]+(?:[._:/-][a-z0-9]+)*$/),
).pipe(
  Schema.brand("EmbeddingProfileId"),
)

/** Stable version for one embedding profile's model and configuration. */
export const EmbeddingProfileVersionSchema =
  Schema.Trimmed.check(
    Schema.isNonEmpty(),
    Schema.isMaxLength(100),
    Schema.isPattern(/^[a-z0-9]+(?:[._-][a-z0-9]+)*$/),
  ).pipe(
    Schema.brand("EmbeddingProfileVersion"),
  )

/** Positive vector dimensionality declared by an embedding profile. */
export const EmbeddingDimensionsSchema = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 1, maximum: 65_536 }),
).pipe(
  Schema.brand("EmbeddingDimensions"),
)

/** Stable identity for one embedding model and configuration. */
export type EmbeddingProfileId = Schema.Schema.Type<
  typeof EmbeddingProfileIdSchema
>

/** Stable version for one embedding profile's model and configuration. */
export type EmbeddingProfileVersion = Schema.Schema.Type<
  typeof EmbeddingProfileVersionSchema
>

/** Positive vector dimensionality declared by an embedding profile. */
export type EmbeddingDimensions = Schema.Schema.Type<
  typeof EmbeddingDimensionsSchema
>

/** Embedding semantics required to decide whether a vector can be reused. */
export interface EmbeddingProfile {
  readonly id: EmbeddingProfileId
  readonly version: EmbeddingProfileVersion
  readonly dimensions: EmbeddingDimensions
}

/** Parse an embedding profile at application composition time. */
export const defineEmbeddingProfile = (input: {
  readonly id: string
  readonly version: string
  readonly dimensions: number
}): EmbeddingProfile => ({
  id: Schema.decodeSync(EmbeddingProfileIdSchema)(input.id),
  version: Schema.decodeSync(EmbeddingProfileVersionSchema)(
    input.version,
  ),
  dimensions: Schema.decodeSync(EmbeddingDimensionsSchema)(
    input.dimensions,
  ),
})

/** One unique piece of content requested from an embedding provider. */
export interface EmbeddingRequest {
  readonly contentHash: ContentHash
  readonly content: string
}

/** One provider vector attributed to the content it embeds. */
export interface EmbeddedContent {
  readonly contentHash: ContentHash
  readonly vector: ReadonlyArray<number>
}

/** An embedding provider could not complete a batch request. */
export class EmbeddingProviderFailed extends Schema.TaggedError<
  EmbeddingProviderFailed
>()("EmbeddingProviderFailed", {
  profile: EmbeddingProfileIdSchema,
  reason: Schema.Literals(["unavailable", "invalid_response"]),
  cause: Schema.Unknown,
}) {}

/** A provider response violated the declared embedding contract. */
export class InvalidEmbeddingOutput extends Schema.TaggedError<
  InvalidEmbeddingOutput
>()("InvalidEmbeddingOutput", {
  profile: EmbeddingProfileIdSchema,
  reason: Schema.Literals([
    "missing_content",
    "unexpected_content",
    "duplicate_content",
    "wrong_dimensions",
    "invalid_number",
  ]),
}) {}

/** External embedding capability consumed by indexing and semantic search. */
export interface EmbeddingProviderService {
  readonly profile: EmbeddingProfile
  readonly embedDocuments: (
    requests: readonly [
      EmbeddingRequest,
      ...ReadonlyArray<EmbeddingRequest>,
    ],
  ) => Effect.Effect<
    ReadonlyArray<EmbeddedContent>,
    EmbeddingProviderFailed
  >
  readonly embedQuery: (
    query: string,
  ) => Effect.Effect<ReadonlyArray<number>, EmbeddingProviderFailed>
}

/** Effect service tag for the configured embedding provider. */
export class EmbeddingProvider extends Context.Service<
  EmbeddingProvider,
  EmbeddingProviderService
>()("@popcomputer/document-graph/EmbeddingProvider") {}
