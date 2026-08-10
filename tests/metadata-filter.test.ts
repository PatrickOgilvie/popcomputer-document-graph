import { describe, expect, test } from "bun:test"
import {
  evaluateMetadataFilter,
  metadataAll,
  metadataAny,
  metadataEquals,
  metadataNot,
  metadataOneOf,
  type MetadataFilter,
} from "../src/adapter.js"

describe("metadata filters", () => {
  test("composes all, any, oneOf, and not with deterministic semantics", () => {
    const filter = metadataAll(
      metadataEquals("reviewStatus", "reviewed"),
      metadataAny(
        metadataOneOf("kind", ["challenge", "outcome"]),
        metadataEquals("featured", true),
      ),
      metadataNot(metadataEquals("visibility", "private")),
    )

    expect(
      evaluateMetadataFilter(
        {
          reviewStatus: "reviewed",
          kind: "outcome",
          featured: false,
          visibility: "public",
        },
        filter,
      ),
    ).toBe(true)
    expect(
      evaluateMetadataFilter(
        {
          reviewStatus: "reviewed",
          kind: "outcome",
          featured: false,
          visibility: "private",
        },
        filter,
      ),
    ).toBe(false)
  })

  test("bounds an expression to 100 leaves", () => {
    const leaves = Array.from({ length: 101 }, (_, index) =>
      metadataEquals("position", index),
    )
    const [first, ...rest] = leaves
    if (first === undefined) throw new Error("Missing leaf fixture")

    expect(() => metadataAll(first, ...rest)).toThrow(
      "at most 100 leaves",
    )
  })

  test("bounds an expression to depth 8", () => {
    let filter: MetadataFilter = metadataEquals("visibility", "public")
    for (let depth = 2; depth <= 8; depth += 1) {
      filter = metadataNot(filter)
    }

    expect(() => metadataNot(filter)).toThrow("at most depth 8")
  })
})
