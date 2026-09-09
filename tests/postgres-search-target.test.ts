import { describe, expect, test } from "bun:test"
import { Effect, Schema } from "effect"
import {
  DocumentKeySchema,
} from "../src/document/document-identity.js"
import {
  defineEmbeddingProfile,
} from "../src/indexing/embedding-provider.js"
import { parseTextSearchPolicy } from "../src/document/text-search-policy.js"
import {
  documentKeys,
  makeGraphSearchScope,
  noDocuments,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
  SearchResultCountSchema,
} from "../src/retrieval/graph-retrieval.js"
import {
  postgresDocumentGraph,
  postgresTransactionClient,
} from "../src/postgres.js"

interface QueryCall {
  readonly text: string
  readonly values: ReadonlyArray<unknown>
}

describe("postgres search targets", () => {
  test("compiles document keys and registered versions before candidate limits", async () => {
    const calls: Array<QueryCall> = []
    const transaction = postgresTransactionClient({
      query: (text, values = []) => {
        calls.push({ text, values })
        return Promise.resolve({ rows: [] })
      },
    })
    const targetDocumentKey = Schema.decodeSync(DocumentKeySchema)(
      "a".repeat(64),
    )
    const scope = makeGraphSearchScope(
      "postgres-target-test",
      { target: documentKeys([targetDocumentKey]) },
      [
        {
          documentKind: "Article",
          projection: "content",
          projectionVersion: "v2",
        },
      ],
    )
    const embeddingProfile = defineEmbeddingProfile({
      id: "test:postgres-target",
      version: "v1",
      dimensions: 2,
    })
    const policy = parseTextSearchPolicy(undefined)
    if (policy === "disabled") {
      throw new Error("The default text policy unexpectedly disabled search")
    }
    const candidates = Schema.decodeSync(SearchResultCountSchema)(1)

    await Effect.runPromise(
      Effect.gen(function*() {
        const semantic = yield* ProjectionSearchStore
        const text = yield* ProjectionTextSearchStore
        yield* semantic.searchCandidates({
          vector: [1, 0],
          embeddingProfile,
          scope,
          candidates,
        })
        yield* text.searchTextCandidates({
          query: "distribution",
          policy,
          scope,
          candidates,
        })
        const emptyScope = makeGraphSearchScope("postgres-target-test", {
          target: noDocuments(),
        })
        yield* semantic.searchCandidates({
          vector: [1, 0],
          embeddingProfile,
          scope: emptyScope,
          candidates,
        })
        yield* text.searchTextCandidates({
          query: "distribution",
          policy,
          scope: emptyScope,
          candidates,
        })
      }).pipe(
        Effect.provide(postgresDocumentGraph({ vectorSearch: "float64", transaction })),
      ),
    )

    expect(calls).toHaveLength(2)
    for (const call of calls) {
      expect(call.text).toContain("r.document_key = ANY(")
      expect(call.text).toContain("::char(64)[]")
      expect(call.text).toContain(
        "registered.projection_version = r.projection_version",
      )
      expect(call.values).toContainEqual([targetDocumentKey])
      expect(call.values).toContainEqual(["v2"])
    }
  })
})
