import { Schema } from "effect"
import {
  JsonValueSchema,
  type JsonValue,
} from "./json-value.js"

/** Stable identifier for one reusable chunking implementation. */
export const ChunkerIdSchema = Schema.NonEmptyTrimmedString.pipe(
  Schema.maxLength(100),
  Schema.pattern(/^[a-z0-9]+(?:[._-][a-z0-9]+)*$/),
)

/** Stable version for one chunking implementation and its semantics. */
export const ChunkerVersionSchema = Schema.NonEmptyTrimmedString.pipe(
  Schema.maxLength(100),
  Schema.pattern(/^[a-z0-9]+(?:[._-][a-z0-9]+)*$/),
)

/** Bounded size of one chunk submitted to an embedding model. */
export const ChunkMaximumCharactersSchema = Schema.Number.pipe(
  Schema.int(),
  Schema.between(128, 16_000),
  Schema.brand("ChunkMaximumCharacters"),
)

/** Bounded size of one chunk submitted to an embedding model. */
export type ChunkMaximumCharacters = Schema.Schema.Type<
  typeof ChunkMaximumCharactersSchema
>

/** One attributed projected section supplied to a chunking strategy. */
export interface ChunkerSection {
  readonly key: string
  readonly label: string | undefined
  readonly content: string
}

/** One section-scoped invocation of a configured chunking strategy. */
export interface ChunkingInput {
  readonly context: string | undefined
  readonly section: ChunkerSection
}

/**
 * One chunk fragment attributed to exactly one projected section.
 *
 * `content` is the focal retrieval text. `embeddingContent` may add repeated
 * context. The graph core owns section attribution and copies the current
 * section's validated metadata onto every returned fragment.
 */
export interface ChunkFragment {
  readonly content: string
  readonly embeddingContent?: string
}

/** Serializable identity and configuration for a chunking strategy. */
export interface ChunkingStrategyDescriptor {
  readonly id: string
  readonly version: string
  readonly config: JsonValue
}

/** Runtime shape retained by every configured chunking strategy. */
export interface ChunkingStrategyShape {
  readonly id: string
  readonly version: string
  readonly config: unknown
  readonly encodedConfig: JsonValue
  readonly maximumCharacters: ChunkMaximumCharacters
  readonly chunk: (
    input: ChunkingInput,
  ) => ReadonlyArray<ChunkFragment>
}

/** One configured, executable, and manifest-safe chunking strategy. */
export interface ChunkingStrategy<
  Id extends string = string,
  Version extends string = string,
  Config = unknown,
> extends ChunkingStrategyShape {
  readonly id: Id
  readonly version: Version
  readonly config: Config
}

/** Input passed to one custom chunking implementation. */
export interface ChunkerInput<Config> {
  readonly context: string | undefined
  readonly section: ChunkerSection
  readonly config: Config
}

/** Definition for one reusable schema-configured chunker. */
export interface DefineChunkerInput<
  Id extends string,
  Version extends string,
  ConfigSchema extends Schema.Schema.AnyNoContext,
> {
  readonly id: Id
  readonly version: Version
  readonly config: ConfigSchema
  readonly maximumCharacters: (
    config: Schema.Schema.Type<ConfigSchema>,
  ) => number
  readonly chunk: (
    input: ChunkerInput<Schema.Schema.Type<ConfigSchema>>,
  ) => ReadonlyArray<ChunkFragment>
}

/** Callable factory produced for one reusable chunking implementation. */
export interface Chunker<
  Id extends string,
  Version extends string,
  ConfigSchema extends Schema.Schema.AnyNoContext,
> {
  /** Parse configuration and create one executable strategy instance. */
  (
    config: Schema.Schema.Encoded<ConfigSchema>,
  ): ChunkingStrategy<
    Id,
    Version,
    Schema.Schema.Type<ConfigSchema>
  >

  readonly id: Id
  readonly version: Version
  readonly configSchema: ConfigSchema
}

/**
 * Define a reusable chunker whose configuration is parsed by Effect Schema.
 *
 * The callback owns text splitting only. The graph core validates its output
 * and attaches graph identity, projection identity, ordinals, and metadata.
 */
export const defineChunker = <
  const Id extends string,
  const Version extends string,
  ConfigSchema extends Schema.Schema.AnyNoContext,
>(
  definition: DefineChunkerInput<Id, Version, ConfigSchema>,
): Chunker<Id, Version, ConfigSchema> => {
  Schema.decodeSync(ChunkerIdSchema)(definition.id)
  Schema.decodeSync(ChunkerVersionSchema)(definition.version)

  const configure = (
    configInput: Schema.Schema.Encoded<ConfigSchema>,
  ): ChunkingStrategy<
    Id,
    Version,
    Schema.Schema.Type<ConfigSchema>
  > => {
    const config = Schema.decodeUnknownSync(definition.config)(
      configInput,
      { onExcessProperty: "error" },
    )
    const encodedConfig = Schema.decodeUnknownSync(JsonValueSchema)(
      Schema.encodeSync(definition.config)(config),
      { onExcessProperty: "error" },
    )
    const maximumCharacters = Schema.decodeSync(
      ChunkMaximumCharactersSchema,
    )(definition.maximumCharacters(config))

    return {
      id: definition.id,
      version: definition.version,
      config,
      encodedConfig,
      maximumCharacters,
      chunk: (input) => definition.chunk({ ...input, config }),
    }
  }

  return Object.assign(configure, {
    id: definition.id,
    version: definition.version,
    configSchema: definition.config,
  })
}

const splitAtReadableBoundaries = (
  value: string,
  maximumCharacters: number,
): ReadonlyArray<string> => {
  const pieces: Array<string> = []
  let remaining = value.trim()

  while (remaining.length > maximumCharacters) {
    const window = remaining.slice(0, maximumCharacters + 1)
    const readableBoundary = Math.max(
      window.lastIndexOf("\n"),
      window.lastIndexOf(" "),
    )
    const minimumReadableBoundary = Math.floor(maximumCharacters * 0.6)
    const end =
      readableBoundary >= minimumReadableBoundary
        ? readableBoundary
        : maximumCharacters

    pieces.push(remaining.slice(0, end).trim())
    remaining = remaining.slice(end).trim()
  }

  if (remaining.length > 0) {
    pieces.push(remaining)
  }

  return pieces
}

const SectionChunkingConfigSchema = Schema.Struct({
  maximumCharacters: ChunkMaximumCharactersSchema,
})

const sectionChunker = defineChunker({
  id: "section",
  version: "v1",
  config: SectionChunkingConfigSchema,
  maximumCharacters: (config) => config.maximumCharacters,
  chunk: ({ context, section, config }) => {
    const fragments: Array<ChunkFragment> = []
    const prefix = [context, section.label]
      .flatMap((part) => (part === undefined ? [] : [part]))
      .join("\n\n")
    const separator = prefix.length === 0 ? "" : "\n\n"
    const bodyBudget =
      config.maximumCharacters - prefix.length - separator.length

    if (bodyBudget < 1) {
      return [
        {
          content: section.content,
          embeddingContent: `${prefix}${separator}${section.content}`,
        },
      ]
    }

    const parts = splitAtReadableBoundaries(
      section.content,
      bodyBudget,
    )
    for (const part of parts) {
      fragments.push(
        prefix.length === 0
          ? { content: part }
          : {
              content: part,
              embeddingContent: `${prefix}${separator}${part}`,
            },
      )
    }

    return fragments
  },
})

/** Configured built-in strategy that keeps authored sections separate. */
export type SectionChunkingStrategy = ReturnType<typeof sectionChunker>

/** Input used to configure deterministic section-aware chunking. */
export interface SectionChunkingInput {
  readonly maximumCharacters?: number
}

/** Define deterministic chunking that never crosses an authored section. */
export const sectionChunking = (
  input: SectionChunkingInput = {},
): SectionChunkingStrategy =>
  sectionChunker({ maximumCharacters: input.maximumCharacters ?? 1_800 })
