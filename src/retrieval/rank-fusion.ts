import { Effect, Schema } from "effect"

/** Positive weight assigned to one independently ranked retrieval stream. */
export const RetrievalWeightSchema = Schema.Number.check(
  Schema.isFinite(),
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(100),
).pipe(
  Schema.brand("RetrievalWeight"),
)

/** Positive reciprocal-rank constant controlling rank-score decay. */
export const ReciprocalRankConstantSchema = Schema.Number.check(
  Schema.isInt(),
  Schema.isBetween({ minimum: 1, maximum: 10_000 }),
).pipe(
  Schema.brand("ReciprocalRankConstant"),
)

/** Positive weight assigned to one independently ranked retrieval stream. */
export type RetrievalWeight = Schema.Schema.Type<
  typeof RetrievalWeightSchema
>

/** Positive reciprocal-rank constant controlling rank-score decay. */
export type ReciprocalRankConstant = Schema.Schema.Type<
  typeof ReciprocalRankConstantSchema
>

/** One ranked item supplied by a retrieval stream. */
export interface RankFusionItem<Key extends string, Value> {
  readonly key: Key
  readonly value: Value
  /** Raw channel-local score retained for explanation, never score addition. */
  readonly score: number
}

/** One independently ranked input stream. */
export interface RankFusionStream<
  Key extends string,
  Value,
  Signal,
> {
  /** Stable structural identity used for deterministic signal ordering. */
  readonly id: string
  readonly signal: Signal
  readonly weight: RetrievalWeight
  readonly items: ReadonlyArray<RankFusionItem<Key, Value>>
}

/** One stream's explainable contribution to a fused result. */
export interface RankFusionSignal<Signal> {
  readonly stream: Signal
  readonly rank: number
  readonly score: number
  readonly weight: RetrievalWeight
  readonly contribution: number
}

/** One deterministically ranked reciprocal-rank fusion result. */
export interface RankFusionResult<
  Key extends string,
  Value,
  Signal,
> {
  readonly key: Key
  readonly value: Value
  readonly score: number
  readonly signals: readonly [
    RankFusionSignal<Signal>,
    ...ReadonlyArray<RankFusionSignal<Signal>>,
  ]
}

/** A ranked stream violated the fusion input contract. */
export class InvalidRankFusionInput extends Schema.TaggedError<
  InvalidRankFusionInput
>()("InvalidRankFusionInput", {
  stream: Schema.String,
  reason: Schema.Literals(["duplicate_key", "invalid_score"]),
}) {}

interface MutableFusionResult<Key extends string, Value, Signal> {
  readonly key: Key
  value: Value
  selectedStreamId: string
  score: number
  readonly signals: Array<RankFusionSignal<Signal> & { readonly id: string }>
}

/**
 * Fuse independently ranked streams using weighted reciprocal rank.
 *
 * Raw stream scores are retained in signals but never combined. Stream order
 * cannot affect output order, selected values, or signal ordering.
 */
export const weightedReciprocalRankFusion = <
  Key extends string,
  Value,
  Signal,
>(input: {
  readonly rankConstant: ReciprocalRankConstant
  readonly streams: ReadonlyArray<RankFusionStream<Key, Value, Signal>>
}): Effect.Effect<
  ReadonlyArray<RankFusionResult<Key, Value, Signal>>,
  InvalidRankFusionInput
> => {
  const fused = new Map<Key, MutableFusionResult<Key, Value, Signal>>()

  for (const stream of input.streams) {
    const seen = new Set<Key>()
    for (const [index, item] of stream.items.entries()) {
      if (seen.has(item.key)) {
        return Effect.fail(
          new InvalidRankFusionInput({
            stream: stream.id,
            reason: "duplicate_key",
          }),
        )
      }
      seen.add(item.key)

      if (!Number.isFinite(item.score)) {
        return Effect.fail(
          new InvalidRankFusionInput({
            stream: stream.id,
            reason: "invalid_score",
          }),
        )
      }

      const rank = index + 1
      const contribution = stream.weight / (input.rankConstant + rank)
      const signal = {
        id: stream.id,
        stream: stream.signal,
        rank,
        score: item.score,
        weight: stream.weight,
        contribution,
      }
      const current = fused.get(item.key)
      if (current === undefined) {
        fused.set(item.key, {
          key: item.key,
          value: item.value,
          selectedStreamId: stream.id,
          score: contribution,
          signals: [signal],
        })
        continue
      }

      current.score += contribution
      current.signals.push(signal)
      if (stream.id < current.selectedStreamId) {
        current.value = item.value
        current.selectedStreamId = stream.id
      }
    }
  }

  return Effect.succeed(
    Array.from(fused.values())
      .sort(
        (left, right) =>
          right.score - left.score || left.key.localeCompare(right.key),
      )
      .map((result) => {
        const orderedSignals = result.signals
          .sort((left, right) => left.id.localeCompare(right.id))
          .map(({ id: _id, ...signal }) => signal)
        const [first, ...rest] = orderedSignals
        if (first === undefined) {
          throw new Error("A fused result unexpectedly has no signals")
        }

        return {
          key: result.key,
          value: result.value,
          score: result.score,
          signals: [first, ...rest],
        }
      }),
  )
}
