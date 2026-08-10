import { Schema } from "effect"

/** Projected text channels understood by storage-neutral lexical search. */
export const TextSearchFieldSchema = Schema.Literal(
  "context",
  "label",
  "content",
)

/** Projected text channel understood by storage-neutral lexical search. */
export type TextSearchField = Schema.Schema.Type<
  typeof TextSearchFieldSchema
>

/** Text configurations supported by the included PostgreSQL adapter. */
export const TextSearchLanguageSchema = Schema.Literal(
  "english",
  "simple",
)

/** Text configuration supported by the included PostgreSQL adapter. */
export type TextSearchLanguage = Schema.Schema.Type<
  typeof TextSearchLanguageSchema
>

/** Bounded relative importance of one projected text channel. */
export const TextSearchWeightSchema = Schema.Number.pipe(
  Schema.finite(),
  Schema.between(0, 100),
  Schema.brand("TextSearchWeight"),
)

/** Bounded relative importance of one projected text channel. */
export type TextSearchWeight = Schema.Schema.Type<
  typeof TextSearchWeightSchema
>

/** Plain application configuration for projection text search. */
export type TextSearchPolicyInput =
  | "disabled"
  | {
      readonly language?: TextSearchLanguage
      readonly weights?: Partial<
        Readonly<Record<TextSearchField, number>>
      >
    }

/** Parsed storage-neutral text-search policy retained by a projection. */
export type TextSearchPolicy =
  | "disabled"
  | {
      readonly _tag: "TextSearch"
      readonly language: TextSearchLanguage
      readonly weights: Readonly<
        Record<TextSearchField, TextSearchWeight>
      >
    }

/** Storage-neutral text channels derived for one attributed chunk. */
export interface ProjectedText {
  readonly context: string | undefined
  readonly label: string | undefined
  readonly content: string
}

const DefaultTextSearchWeights = Object.freeze({
  context: Schema.decodeSync(TextSearchWeightSchema)(2),
  label: Schema.decodeSync(TextSearchWeightSchema)(3),
  content: Schema.decodeSync(TextSearchWeightSchema)(1),
})

const TextSearchWeightsInputSchema = Schema.Struct({
  context: Schema.optional(TextSearchWeightSchema),
  label: Schema.optional(TextSearchWeightSchema),
  content: Schema.optional(TextSearchWeightSchema),
})

const EnabledTextSearchPolicyInputSchema = Schema.Struct({
  language: Schema.optional(TextSearchLanguageSchema),
  weights: Schema.optional(TextSearchWeightsInputSchema),
}).pipe(
  Schema.filter((input) => {
    const context =
      input.weights?.context ?? DefaultTextSearchWeights.context
    const label = input.weights?.label ?? DefaultTextSearchWeights.label
    const content =
      input.weights?.content ?? DefaultTextSearchWeights.content

    return context > 0 || label > 0 || content > 0
  }),
)

const TextSearchPolicyInputSchema = Schema.Union(
  Schema.Literal("disabled"),
  EnabledTextSearchPolicyInputSchema,
)

/** Parse and normalize text policy once at graph-definition time. */
export const parseTextSearchPolicy = (
  input: TextSearchPolicyInput | undefined,
): TextSearchPolicy => {
  const parsed = Schema.decodeUnknownSync(TextSearchPolicyInputSchema)(
    input === undefined ? {} : input,
    { onExcessProperty: "error" },
  )

  if (parsed === "disabled") {
    return parsed
  }

  return Object.freeze({
    _tag: "TextSearch" as const,
    language: parsed.language ?? "english",
    weights: Object.freeze({
      context:
        parsed.weights?.context ?? DefaultTextSearchWeights.context,
      label: parsed.weights?.label ?? DefaultTextSearchWeights.label,
      content:
        parsed.weights?.content ?? DefaultTextSearchWeights.content,
    }),
  })
}
