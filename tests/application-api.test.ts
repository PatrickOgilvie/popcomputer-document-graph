import { describe, expect, test } from "bun:test"
import { Effect, Result, Schema } from "effect"
import {
  defineDocument,
  defineDocumentGraph,
  EmbeddingProvider,
  InvalidVectorProjectionOutput,
  ProjectionTextSearchStore,
  type DocumentGraphServices,
  type SearchDocumentGraphError,
} from "../src/index.js"
import { ProjectionSearchStore } from "../src/adapter.js"

const ArticleId = Schema.String.check(Schema.isUUID()).pipe(Schema.brand("ApplicationArticleId"))
const Article = Schema.Struct({
  id: ArticleId,
  title: Schema.Trimmed.check(Schema.isNonEmpty()),
  body: Schema.Trimmed.check(Schema.isNonEmpty()),
})

const ArticleDocument = defineDocument(Article, { id: "id" }).vectorise({
  id: "article-content",
  version: "v1",
  text: {
    language: "english",
    weights: { context: 4, label: 2, content: 1 },
  },
  select: (article) => ({
    context: article.title,
    sections: [{ key: "body", content: article.body }],
  }),
})

const graph = defineDocumentGraph({
  id: "application-api",
  documents: { Article: ArticleDocument },
})

const CollectionId = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("ApplicationCollectionId"),
)
const Collection = Schema.Struct({
  id: CollectionId,
  entries: Schema.Array(Schema.Trimmed.check(Schema.isNonEmpty())),
})
const CollectionDocument = defineDocument(Collection, {
  id: "id",
}).vectorise({
  id: "collection-entries",
  version: "v1",
  select: (collection) => ({
    sections: collection.entries.map((entry, index) => ({
      key: String(index),
      content: entry,
    })),
  }),
})
const collectionGraph = defineDocumentGraph({
  id: "collection-api",
  documents: { Collection: CollectionDocument },
})
const collectionId = Schema.decodeSync(CollectionId)(
  "11111111-1111-4111-8111-111111111111",
)

describe("application API", () => {
  test("uses schema-field identity and default section chunking", () => {
    expect(graph.manifest).toEqual({
      id: "application-api",
      documents: [
        {
          kind: "Article",
          projections: [
            {
              id: "article-content",
              version: "v1",
              chunking: {
                id: "section",
                version: "v1",
                config: { maximumCharacters: 1_800 },
              },
              text: {
                language: "english",
                weights: { context: 4, label: 2, content: 1 },
              },
            },
          ],
        },
      ],
      relations: [],
    })
  })

  test("binds operations to document and projection handles", () => {
    const article = graph.document("Article")
    const content = article.projection("article-content")

    expect(article.kind).toBe("Article")
    expect(content.id).toBe("article-content")
    expect(content.version).toBe("v1")
  })

  test("exposes one minimal compiled graph surface", () => {
    expect(Object.keys(graph).sort()).toEqual([
      "document",
      "id",
      "manifest",
      "parseReference",
      "reconcileIndex",
      "retrieval",
      "search",
    ])
  })

  test("accepts sections produced by an ordinary array transformation", async () => {
    const revision = await Effect.runPromise(
      collectionGraph
        .document("Collection")
        .projection("collection-entries")
        .project({
          id: collectionId,
          entries: ["First entry", "Second entry"],
        }),
    )

    expect(revision.chunks.map((chunk) => chunk.content)).toEqual([
      "First entry",
      "Second entry",
    ])
  })

  test("rejects an empty projected section array as a typed failure", async () => {
    const result = await Effect.runPromise(
      collectionGraph
        .document("Collection")
        .projection("collection-entries")
        .project({ id: collectionId, entries: [] })
        .pipe(Effect.result),
    )

    expect(result).toEqual(
      Result.fail(
        new InvalidVectorProjectionOutput({
          graph: "collection-api",
          documentKind: "Collection",
          projection: "collection-entries",
          reason: "empty_sections",
        }),
      ),
    )
  })
})

if (import.meta.url === "") {
  type AppEffectServices = DocumentGraphServices
  const searchAction: Effect.Effect<
    unknown,
    SearchDocumentGraphError,
    AppEffectServices
  > = graph.search("query")
  void searchAction

  const textSearchAction: Effect.Effect<
    unknown,
    SearchDocumentGraphError,
    ProjectionTextSearchStore
  > = graph
    .document("Article")
    .projection("article-content")
    .search("query", { strategy: "text" })
  void textSearchAction

  const semanticSearchAction: Effect.Effect<
    unknown,
    SearchDocumentGraphError,
    EmbeddingProvider | ProjectionSearchStore
  > = graph
    .document("Article")
    .projection("article-content")
    .search("query", { strategy: "semantic" })
  void semanticSearchAction

  const hybridSearchAction: Effect.Effect<
    unknown,
    SearchDocumentGraphError,
    EmbeddingProvider | ProjectionSearchStore | ProjectionTextSearchStore
  > = graph
    .document("Article")
    .projection("article-content")
    .search("query", {
      strategy: {
        mode: "hybrid",
        weights: { semantic: 1, text: 2 },
        rankConstant: 60,
      },
      candidates: { semantic: 60, text: 40 },
      limit: 12,
    })
  void hybridSearchAction

  // @ts-expect-error The application facade has no inert graph version.
  void graph.version

  // @ts-expect-error Relations are absent until traversal is implemented.
  void graph.relate

  // @ts-expect-error Storage-oriented scope construction is adapter-only.
  void graph.scope

  // @ts-expect-error Graph-wide filters infer registered document kinds.
  graph.search("query", { include: ["Unknown"] })

  defineDocument(Article, { id: "id" }).vectorise({
    id: "invalid-text-field",
    version: "v1",
    select: (article) => ({
      sections: [{ key: "body", content: article.body }],
    }),
    text: {
      // @ts-expect-error Search weights use projected text channels only.
      weights: { title: 2 },
    },
  })

}
