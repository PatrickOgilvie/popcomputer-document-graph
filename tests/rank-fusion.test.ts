import { describe, expect, test } from "bun:test"
import { Effect, Result, Schema } from "effect"
import {
  InvalidRankFusionInput,
  ReciprocalRankConstantSchema,
  RetrievalWeightSchema,
  weightedReciprocalRankFusion,
  type RankFusionStream,
} from "../src/retrieval/rank-fusion.js"

type Item = { readonly label: string }
type Stream = "semantic" | "text"

const rankConstant = Schema.decodeSync(ReciprocalRankConstantSchema)(60)
const weight = (value: number) =>
  Schema.decodeSync(RetrievalWeightSchema)(value)

const stream = (
  id: Stream,
  items: ReadonlyArray<{
    readonly key: string
    readonly score: number
  }>,
  streamWeight = 1,
): RankFusionStream<string, Item, Stream> => ({
  id,
  signal: id,
  weight: weight(streamWeight),
  items: items.map((item) => ({
    key: item.key,
    score: item.score,
    value: { label: item.key },
  })),
})

const fuse = (
  streams: ReadonlyArray<RankFusionStream<string, Item, Stream>>,
) =>
  Effect.runPromise(
    weightedReciprocalRankFusion({ rankConstant, streams }),
  )

describe("weightedReciprocalRankFusion", () => {
  test("strengthens a result found by both streams without adding raw scores", async () => {
    const results = await fuse([
      stream("semantic", [
        { key: "shared", score: 0.01 },
        { key: "semantic-only", score: 10_000 },
      ]),
      stream("text", [
        { key: "shared", score: 50_000 },
        { key: "text-only", score: 0.001 },
      ]),
    ])

    expect(results.map((result) => result.key)).toEqual([
      "shared",
      "semantic-only",
      "text-only",
    ])
    expect(results[0]?.score).toBeCloseTo(2 / 61)
    expect(results[0]?.signals.map((signal) => signal.score)).toEqual([
      0.01,
      50_000,
    ])
  })

  test("treats empty streams as neutral and stream declaration order as irrelevant", async () => {
    const semantic = stream("semantic", [
      { key: "a", score: 0.8 },
      { key: "b", score: 0.7 },
    ])
    const text = stream("text", [])

    const forward = await fuse([semantic, text])
    const reverse = await fuse([text, semantic])

    expect(reverse).toEqual(forward)
    expect(forward.map((result) => result.key)).toEqual(["a", "b"])
  })

  test("uses chunk identity for stable fused ties", async () => {
    const results = await fuse([
      stream("semantic", [{ key: "b", score: 0.5 }]),
      stream("text", [{ key: "a", score: 0.5 }]),
    ])

    expect(results.map((result) => result.key)).toEqual(["a", "b"])
  })

  test("rejects duplicate identities within one stream before fusion", async () => {
    const result = await Effect.runPromise(
      weightedReciprocalRankFusion({
        rankConstant,
        streams: [
          stream("semantic", [
            { key: "duplicate", score: 0.9 },
            { key: "duplicate", score: 0.8 },
          ]),
        ],
      }).pipe(Effect.result),
    )

    expect(result).toEqual(
      Result.fail(
        new InvalidRankFusionInput({
          stream: "semantic",
          reason: "duplicate_key",
        }),
      ),
    )
  })

  test("increasing a stream weight increases its fixed-rank contribution", async () => {
    const low = await fuse([
      stream("semantic", [{ key: "result", score: 0.7 }], 1),
    ])
    const high = await fuse([
      stream("semantic", [{ key: "result", score: 0.7 }], 2),
    ])

    expect(high[0]?.score).toBe(2 * (low[0]?.score ?? 0))
  })
})
