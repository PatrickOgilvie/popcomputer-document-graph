import { describe, expect, test } from "bun:test"
import { Effect, Option, Schema } from "effect"
import {
  ChunkIdSchema,
  ContentHashSchema,
  DocumentKeySchema,
  ProjectionRevisionHashSchema,
} from "../src/document/document-identity.js"
import { parseTextSearchPolicy } from "../src/document/text-search-policy.js"
import { defineEmbeddingProfile } from "../src/indexing/embedding-provider.js"
import {
  ProjectionIndexStore,
  type ReplaceProjectedRevision,
} from "../src/indexing/projection-index.js"
import {
  documentKeys,
  makeGraphSearchScope,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
  SearchResultCountSchema,
} from "../src/retrieval/graph-retrieval.js"
import { inMemoryDocumentGraph } from "../src/in-memory.js"

const GraphId = "in-memory-search-target"

const ProjectionId = "content"

const ProjectionVersion = "v1"

const embeddingProfile = defineEmbeddingProfile({
  id: "test:in-memory-target",
  version: "v1",
  dimensions: 2,
})

const textPolicy = parseTextSearchPolicy(undefined)

if (textPolicy === "disabled") {
  throw new Error("The default text policy unexpectedly disabled search")
}

const makeReplacement = (input: {
  readonly identity: string
  readonly chunkIdentity: string
  readonly revisionIdentity: string
  readonly contentIdentity: string
  readonly content: string
  readonly vector: readonly [number, number]
}): ReplaceProjectedRevision => {
  const documentKey = Schema.decodeSync(DocumentKeySchema)(
    input.identity.repeat(64),
  )

  const contentHash = Schema.decodeSync(ContentHashSchema)(
    input.contentIdentity.repeat(64),
  )

  return {
    key: { documentKey, projection: ProjectionId },
    expectedToken: Option.none(),
    encodedTarget: {
      graph: GraphId,
      kind: "Article",
      id: input.identity,
    },
    projectionVersion: ProjectionVersion,
    textPolicy,
    revisionHash: Schema.decodeSync(ProjectionRevisionHashSchema)(
      input.revisionIdentity.repeat(64),
    ),
    embeddingProfile,
    chunks: [
      {
        chunkId: Schema.decodeSync(ChunkIdSchema)(
          input.chunkIdentity.repeat(64),
        ),
        contentHash,
        ordinal: 0,
        sectionKey: "body",
        sectionIndex: 0,
        sectionPart: 0,
        content: input.content,
        embeddingContent: input.content,
        text: {
          context: undefined,
          label: undefined,
          content: input.content,
        },
        metadata: undefined,
      },
    ],
    embeddings: [{ contentHash, vector: input.vector }],
  }
}

describe("in-memory search targets", () => {
  test("filters document keys before semantic and text candidate limits", async () => {
    const highestScoring = makeReplacement({
      identity: "a",
      chunkIdentity: "b",
      revisionIdentity: "c",
      contentIdentity: "d",
      content: "needle needle needle",
      vector: [1, 0],
    })

    const eligible = makeReplacement({
      identity: "1",
      chunkIdentity: "2",
      revisionIdentity: "3",
      contentIdentity: "4",
      content: "needle",
      vector: [0.8, 0.2],
    })

    const target = documentKeys([eligible.key.documentKey])

    const scope = makeGraphSearchScope(
      GraphId,
      { target },
      [
        {
          documentKind: "Article",
          projection: ProjectionId,
          projectionVersion: ProjectionVersion,
        },
      ],
    )

    const candidates = Schema.decodeSync(SearchResultCountSchema)(1)

    const result = await Effect.runPromise(
      Effect.gen(function*() {
        const index = yield* ProjectionIndexStore
        yield* index.replaceRevision(highestScoring)
        yield* index.replaceRevision(eligible)

        const semanticStore = yield* ProjectionSearchStore
        const textStore = yield* ProjectionTextSearchStore

        const semanticHits = yield* semanticStore.searchCandidates({
          vector: [1, 0],
          embeddingProfile,
          scope,
          candidates,
        })

        const textHits = yield* textStore.searchTextCandidates({
          query: "needle",
          policy: textPolicy,
          scope,
          candidates,
        })

        return { semanticHits, textHits }
      }).pipe(Effect.provide(inMemoryDocumentGraph())),
    )

    expect(result.semanticHits.map((hit) => hit.documentKey)).toEqual([
      eligible.key.documentKey,
    ])
    expect(result.textHits.map((hit) => hit.documentKey)).toEqual([
      eligible.key.documentKey,
    ])
  })
})
