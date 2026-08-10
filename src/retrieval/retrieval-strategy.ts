import { Schema } from "effect"
import {
  ReciprocalRankConstantSchema,
  RetrievalWeightSchema,
  type ReciprocalRankConstant,
  type RetrievalWeight,
} from "./rank-fusion.js"

/** Plain application strategy input accepted by projection search. */
export type RetrievalStrategyInput =
  | "semantic"
  | "text"
  | "hybrid"
  | {
      readonly mode: "hybrid"
      readonly weights?: {
        readonly semantic?: number
        readonly text?: number
      }
      readonly rankConstant?: number
    }

/** Bounded final result count returned by one retrieval operation. */
export const RetrievalResultLimitSchema = Schema.Number.pipe(
  Schema.int(),
  Schema.between(1, 1_000),
  Schema.brand("RetrievalResultLimit"),
)

/** Bounded final result count returned by one retrieval operation. */
export type RetrievalResultLimit = Schema.Schema.Type<
  typeof RetrievalResultLimitSchema
>

/** Parsed semantic-only retrieval policy. */
export interface SemanticRetrievalStrategy {
  readonly _tag: "Semantic"
  readonly weight: RetrievalWeight
  readonly rankConstant: ReciprocalRankConstant
}

/** Parsed text-only retrieval policy. */
export interface TextRetrievalStrategy {
  readonly _tag: "Text"
  readonly weight: RetrievalWeight
  readonly rankConstant: ReciprocalRankConstant
}

/** Parsed hybrid retrieval policy. */
export interface HybridRetrievalStrategy {
  readonly _tag: "Hybrid"
  readonly weights: {
    readonly semantic: RetrievalWeight
    readonly text: RetrievalWeight
  }
  readonly rankConstant: ReciprocalRankConstant
}

/** Parsed retrieval policy used by projection-search orchestration. */
export type RetrievalStrategy =
  | SemanticRetrievalStrategy
  | TextRetrievalStrategy
  | HybridRetrievalStrategy

const HybridStrategyInputSchema = Schema.Struct({
  mode: Schema.Literal("hybrid"),
  weights: Schema.optional(
    Schema.Struct({
      semantic: Schema.optional(RetrievalWeightSchema),
      text: Schema.optional(RetrievalWeightSchema),
    }),
  ),
  rankConstant: Schema.optional(ReciprocalRankConstantSchema),
})

const DefaultWeight = Schema.decodeSync(RetrievalWeightSchema)(1)
const DefaultRankConstant = Schema.decodeSync(
  ReciprocalRankConstantSchema,
)(60)
const DefaultHybridStrategy = Schema.decodeSync(
  HybridStrategyInputSchema,
)({ mode: "hybrid" })

/** Parse plain strategy input once at the projection-search boundary. */
export const parseRetrievalStrategy = (
  input: RetrievalStrategyInput | undefined,
  textEnabled: boolean,
): RetrievalStrategy => {
  const selected = input ?? (textEnabled ? "hybrid" : "semantic")
  if (selected === "semantic") {
    return {
      _tag: "Semantic",
      weight: DefaultWeight,
      rankConstant: DefaultRankConstant,
    }
  }

  if (selected === "text") {
    if (!textEnabled) {
      throw new Error("Text retrieval is disabled for this projection")
    }
    return {
      _tag: "Text",
      weight: DefaultWeight,
      rankConstant: DefaultRankConstant,
    }
  }

  if (!textEnabled) {
    throw new Error("Hybrid retrieval requires projection text search")
  }

  const parsed =
    selected === "hybrid"
      ? DefaultHybridStrategy
      : Schema.decodeUnknownSync(HybridStrategyInputSchema)(selected, {
          onExcessProperty: "error",
        })

  return {
    _tag: "Hybrid",
    weights: {
      semantic: parsed.weights?.semantic ?? DefaultWeight,
      text: parsed.weights?.text ?? DefaultWeight,
    },
    rankConstant: parsed.rankConstant ?? DefaultRankConstant,
  }
}
