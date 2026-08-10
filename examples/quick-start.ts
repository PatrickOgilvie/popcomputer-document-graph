import { Schema } from "effect"
import {
  defineDocument,
  defineDocumentGraph,
} from "@popcomputer/document-graph"

/** Article input accepted by indexing boundaries. */
export const ArticleSchema = Schema.Struct({
  id: Schema.UUID,
  title: Schema.NonEmptyTrimmedString,
  body: Schema.NonEmptyTrimmedString,
})

const ArticleDocument = defineDocument(ArticleSchema, { id: "id" }).vectorise({
  id: "content",
  version: "v1",
  select: (article) => ({
    context: article.title,
    sections: [
      {
        key: "body",
        content: article.body,
      },
    ],
  }),
})

/** Document graph used by the quick-start example. */
export const KnowledgeGraph = defineDocumentGraph({
  id: "knowledge-base",
  documents: { Article: ArticleDocument },
})

/** Operations over complete Article documents. */
export const Articles = KnowledgeGraph.document("Article")

/** Search and indexing operations for the Article content projection. */
export const ArticleContent = Articles.projection("content")

/** Index every projection and outgoing relation owned by one Article. */
export const indexArticle = (
  article: Schema.Schema.Type<typeof ArticleSchema>,
) => Articles.index(article)

/** Search Article content using the projection's default hybrid strategy. */
export const searchArticles = (query: string) =>
  ArticleContent.search(query, { limit: 10 })
