import { Schema } from "effect"
import {
  sectionChunking,
  type ChunkingStrategyRuntime,
  type SectionChunkingStrategy,
} from "./chunking-strategy.js"
import {
  parseTextSearchPolicy,
  type TextSearchPolicy,
  type TextSearchPolicyInput,
} from "./text-search-policy.js"

/** Stable identifier for one semantic projection of a document. */
export const VectorProjectionIdSchema = Schema.Trimmed.pipe(
  Schema.check(Schema.isNonEmpty()),
  Schema.check(Schema.isMaxLength(100)),
  Schema.check(Schema.isPattern(/^[a-z0-9]+(?:[._-][a-z0-9]+)*$/)),
)

/** Stable version for one semantic projection and its text policy. */
export const VectorProjectionVersionSchema =
  Schema.Trimmed.pipe(
    Schema.check(Schema.isNonEmpty()),
    Schema.check(Schema.isMaxLength(100)),
    Schema.check(
      Schema.isPattern(/^[a-z0-9]+(?:[._-][a-z0-9]+)*$/),
    ),
  )

interface ProjectedSectionFields {
  readonly key: string
  readonly label?: string
  readonly content: string
}

/**
 * One stable, attributed section emitted by a vector projection.
 *
 * A section is the smallest attribution boundary in a projection. If source,
 * trust, visibility, or citation metadata changes within some text, emit
 * separate sections so every derived chunk has exactly one attribution.
 */
export type ProjectedSection<Metadata = never> =
  ProjectedSectionFields &
    ([Metadata] extends [never]
      ? { readonly metadata?: never }
      : { readonly metadata: Metadata })

/**
 * Complete deterministic text projection of one graph document.
 *
 * Readers and source adapters preserve meaningful structure here; chunkers
 * subsequently split within these authored attribution boundaries.
 */
export interface ProjectedDocument<Metadata = never> {
  readonly context?: string
  readonly sections: ReadonlyArray<ProjectedSection<Metadata>>
}

/** Runtime shape retained for every registered vector projection. */
export interface RegisteredVectorProjection {
  readonly id: string
  readonly version: string
  readonly chunking: ChunkingStrategyRuntime
  readonly metadataSchema: Schema.Codec<unknown, unknown> | undefined
  readonly text: TextSearchPolicy
}

/** Deterministic vector projection registered against one document type. */
export interface VectorProjection<
  Value,
  Metadata,
  Id extends string = string,
  Version extends string = string,
  Chunking extends ChunkingStrategyRuntime = ChunkingStrategyRuntime,
> extends RegisteredVectorProjection {
  readonly id: Id
  readonly version: Version
  readonly chunking: Chunking
  readonly select: (value: Value) => ProjectedDocument<Metadata>
}

type MetadataType<MetadataSchema> =
  MetadataSchema extends Schema.Codec<unknown, unknown>
  ? Schema.Schema.Type<MetadataSchema>
  : never

/** Input for one document-owned vector projection. */
export interface DefineVectorProjectionInput<
  Value,
  Id extends string,
  Version extends string,
  MetadataSchema extends Schema.Codec<unknown, unknown> | undefined,
  Chunking extends ChunkingStrategyRuntime,
> {
  readonly id: Id
  readonly version: Version
  readonly metadata?: MetadataSchema
  readonly select: (
    value: Value,
  ) => ProjectedDocument<MetadataType<MetadataSchema>>
  readonly chunking?: Chunking
  readonly text?: TextSearchPolicyInput
}

/** Build one validated vector projection while preserving literal identity. */
export const makeVectorProjection = <
  Value,
  const Id extends string,
  const Version extends string,
  MetadataSchema extends Schema.Codec<unknown, unknown> | undefined = undefined,
  Chunking extends ChunkingStrategyRuntime = SectionChunkingStrategy,
>(
  input: DefineVectorProjectionInput<
    Value,
    Id,
    Version,
    MetadataSchema,
    Chunking
  >,
): VectorProjection<
  Value,
  MetadataType<MetadataSchema>,
  Id,
  Version,
  Chunking
> => {
  Schema.decodeSync(VectorProjectionIdSchema)(input.id)
  Schema.decodeSync(VectorProjectionVersionSchema)(input.version)

  const chunking = input.chunking ?? sectionChunking()
  const text = parseTextSearchPolicy(input.text)

  return {
    id: input.id,
    version: input.version,
    // SAFETY: An omitted strategy always selects SectionChunkingStrategy,
    // which is the default Chunking type parameter.
    chunking: chunking as Chunking,
    metadataSchema: input.metadata,
    text,
    select: input.select,
  }
}
