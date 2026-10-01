import type { NamespaceMultiQueryParams } from "@turbopuffer/turbopuffer"
import { Schema } from "effect"
import type { TextSearchPolicy } from "../../document/text-search-policy.js"
import type {
  GraphSearchScope,
  SearchResultCount,
} from "../../retrieval/graph-retrieval.js"
import { TurbopufferFullTextAttributes } from "./config.js"
import {
  compileTurbopufferMetadataFilter,
  type TurbopufferFilter,
} from "./metadata-terms.js"
import type { TurbopufferWorkspacePartition } from "./partition.js"
import { TurbopufferSearchResultAttributes } from "./row-codec.js"
import { encodeTurbopufferVector } from "./vector-encoding.js"

/** Namespace partition fields that every retrieval query must constrain. */
export type TurbopufferQueryPartition = TurbopufferWorkspacePartition

/** Consistency accepted for one provider query or multi-query snapshot. */
export const TurbopufferQueryConsistencySchema = Schema.Literals([
  "strong",
  "eventual",
])

/** Consistency requested for one provider query or multi-query snapshot. */
export type TurbopufferQueryConsistency =
  typeof TurbopufferQueryConsistencySchema.Type

/** One provider query whose serialized shape is covered by contract tests. */
export type TurbopufferSerializedQuery = NamespaceMultiQueryParams.Query

/** Explicit empty search plan that must not issue a provider request. */
export interface TurbopufferNoDocumentsQuery {
  readonly _tag: "NoDocuments"
}

/** One executable semantic or text provider request. */
export interface TurbopufferExecutableQuery {
  readonly _tag: "Query"
  readonly query: TurbopufferSerializedQuery
}

/** Compiled single-channel query with an explicit empty-target branch. */
export type CompiledTurbopufferQuery =
  | TurbopufferNoDocumentsQuery
  | TurbopufferExecutableQuery

/** Two same-snapshot channel queries retained separately for package-owned RRF. */
export interface TurbopufferHybridMultiQuery {
  readonly _tag: "MultiQuery"
  readonly request: {
    readonly queries: [
      TurbopufferSerializedQuery,
      TurbopufferSerializedQuery,
    ]
    readonly consistency: {
      readonly level: TurbopufferQueryConsistency
    }
  }
}

/** Compiled hybrid query with an explicit empty-target branch. */
export type CompiledTurbopufferHybridQuery =
  | TurbopufferNoDocumentsQuery
  | TurbopufferHybridMultiQuery

const noDocuments = (): TurbopufferNoDocumentsQuery => ({
  _tag: "NoDocuments",
})

const registeredProjectionFilter = (
  scope: GraphSearchScope,
): TurbopufferFilter | undefined => {
  if (scope.registered === undefined) return undefined

  const alternatives = scope.registered.map((registered) => {
    const filters: Array<TurbopufferFilter> = [
      ["document_kind", "Eq", registered.documentKind],
      ["projection_id", "Eq", registered.projection],
    ]

    if (registered.projectionVersion !== undefined) {
      filters.push([
        "projection_version",
        "Eq",
        registered.projectionVersion,
      ])
    }

    return ["And", filters] satisfies TurbopufferFilter
  })

  return ["Or", alternatives]
}

const compileCommonFilter = (input: {
  readonly scope: GraphSearchScope
  readonly partition: TurbopufferQueryPartition
}): TurbopufferFilter | undefined => {
  if (
    input.scope.target._tag === "NoDocuments" ||
    input.scope.registered?.length === 0
  ) {
    return undefined
  }

  const filters: Array<TurbopufferFilter> = [
    ["row_kind", "Eq", "slot"],
    ["is_live", "Eq", true],
    ["partition_id", "Eq", input.partition.identity],
    ["graph_id", "Eq", input.scope.graph],
    [
      "embedding_profile_id",
      "Eq",
      input.partition.embeddingProfile.id,
    ],
    [
      "embedding_profile_version",
      "Eq",
      input.partition.embeddingProfile.version,
    ],
    ["schema_generation", "Eq", input.partition.schemaGeneration],
  ]

  const registered = registeredProjectionFilter(input.scope)

  if (registered !== undefined) filters.push(registered)

  if (input.scope.includeDocumentKinds.length > 0) {
    filters.push([
      "document_kind",
      "In",
      [...input.scope.includeDocumentKinds],
    ])
  }

  if (input.scope.excludeDocumentKinds.length > 0) {
    filters.push([
      "document_kind",
      "NotIn",
      [...input.scope.excludeDocumentKinds],
    ])
  }

  if (input.scope.includeProjections.length > 0) {
    filters.push([
      "projection_id",
      "In",
      [...input.scope.includeProjections],
    ])
  }

  if (input.scope.excludeProjections.length > 0) {
    filters.push([
      "projection_id",
      "NotIn",
      [...input.scope.excludeProjections],
    ])
  }

  if (input.scope.target._tag === "DocumentKeys") {
    filters.push([
      "document_key",
      "In",
      [...input.scope.target.documentKeys],
    ])
  }

  filters.push(
    ...input.scope.where.map(compileTurbopufferMetadataFilter),
  )

  return ["And", filters]
}

const executableQuery = (
  query: TurbopufferSerializedQuery,
): TurbopufferExecutableQuery => ({ _tag: "Query", query })

/**
 * How many rows a query returns. A per-document cap stops one long document
 * from filling a channel, so the same budget reaches more documents.
 */
const resultLimit = (
  candidates: SearchResultCount,
  chunksPerDocument: number | undefined,
): Pick<TurbopufferSerializedQuery, "top_k" | "limit"> =>
  chunksPerDocument === undefined
    ? { top_k: candidates }
    : {
        limit: {
          total: candidates,
          per: { attributes: ["document_key"], limit: chunksPerDocument },
        },
      }

/** Compile ANN retrieval with cosine distance and all graph constraints pushed down. */
export const compileTurbopufferSemanticQuery = (input: {
  readonly scope: GraphSearchScope
  readonly partition: TurbopufferQueryPartition
  readonly queryVector: ReadonlyArray<number>
  readonly candidates: SearchResultCount
  readonly chunksPerDocument?: number | undefined
}): CompiledTurbopufferQuery => {
  const filters = compileCommonFilter(input)

  if (filters === undefined) return noDocuments()

  if (
    input.queryVector.length !== input.partition.embeddingProfile.dimensions ||
    input.queryVector.some((component) => !Number.isFinite(component))
  ) {
    throw new Error(
      "Turbopuffer query vectors must match the embedding profile dimensions",
    )
  }

  return executableQuery({
    distance_metric: "cosine_distance",
    // SAFETY: the API accepts any vector as base64 little-endian float32, in
    // queries as in writes, and ranks identically; the SDK types only the
    // JSON array form. The encoded vector is a quarter of the request bytes.
    // oxlint-disable-next-line anti-slop/no-chained-type-assertions -- The SDK's RankBy type cannot express the documented base64 vector form, so the wire value is asserted at this single provider boundary.
    rank_by: [
      "vector",
      "ANN",
      encodeTurbopufferVector(input.queryVector),
    ] as unknown as NonNullable<TurbopufferSerializedQuery["rank_by"]>,
    filters,
    ...resultLimit(input.candidates, input.chunksPerDocument),
    include_attributes: [...TurbopufferSearchResultAttributes],
  })
}

type EnabledTextSearchPolicy = Exclude<TextSearchPolicy, "disabled">

type Bm25Clause = [attribute: string, operator: "BM25", query: string]

type WeightedBm25Clause = [
  operator: "Product",
  weight: number,
  clause: Bm25Clause,
]

const compileWeightedBm25 = (
  query: string,
  policy: EnabledTextSearchPolicy,
): [operator: "Sum", clauses: Array<WeightedBm25Clause>] => {
  const fields = TurbopufferFullTextAttributes[policy.language]
  const clauses: Array<WeightedBm25Clause> = []

  for (const field of ["context", "label", "content"] as const) {
    const weight = policy.weights[field]

    if (weight > 0) {
      clauses.push([
        "Product",
        weight,
        [fields[field], "BM25", query],
      ])
    }
  }

  return ["Sum", clauses]
}

/**
 * A web-search style text query (the syntax PostgreSQL's
 * `websearch_to_tsquery` accepts) reduced to what BM25 can evaluate.
 */
export interface TurbopufferTextMatch {
  /** Every positive word, quoted or not, ranked by BM25. */
  readonly terms: string
  /**
   * Alternatives a hit must satisfy, each requiring all its quoted phrases.
   * Empty when any alternative is unquoted words, which BM25 already ranks.
   */
  readonly phraseAlternatives: ReadonlyArray<ReadonlyArray<string>>
}

const WordCharacter = /[\p{L}\p{N}]/u

/** Parse quoted phrases, `or` alternatives, and `-` exclusions from a query. */
export const parseTurbopufferTextMatch = (
  query: string,
): TurbopufferTextMatch => {
  const terms: Array<string> = []
  const alternatives: Array<{ phrases: Array<string>; words: number }> = [
    { phrases: [], words: 0 },
  ]

  let index = 0

  while (index < query.length) {
    const character = query.charAt(index)

    if (/\s/u.test(character)) {
      index += 1
      continue
    }

    const excluded = character === "-"
    if (excluded) index += 1
    const current = alternatives[alternatives.length - 1]

    if (query.charAt(index) === "\"") {
      const close = query.indexOf("\"", index + 1)
      const phrase = query.slice(index + 1, close === -1 ? undefined : close).trim()
      index = close === -1 ? query.length : close + 1

      if (!excluded && WordCharacter.test(phrase) && current !== undefined) {
        terms.push(phrase)
        current.phrases.push(phrase)
      }

      continue
    }

    let end = index

    while (end < query.length && !/[\s"]/u.test(query.charAt(end))) end += 1
    const word = query.slice(index, end)
    index = end

    if (!excluded && word.toLowerCase() === "or") {
      if (current !== undefined && (current.phrases.length > 0 || current.words > 0)) {
        alternatives.push({ phrases: [], words: 0 })
      }

      continue
    }

    if (!excluded && WordCharacter.test(word) && current !== undefined) {
      terms.push(word)
      current.words += 1
    }
  }

  const matched = alternatives.filter((alternative) =>
    alternative.phrases.length > 0 || alternative.words > 0)

  return {
    terms: terms.join(" "),
    phraseAlternatives: matched.length > 0 &&
        matched.every((alternative) => alternative.phrases.length > 0)
      ? matched.map((alternative) => alternative.phrases)
      : [],
  }
}

const compilePhraseFilter = (
  alternatives: ReadonlyArray<ReadonlyArray<string>>,
  policy: EnabledTextSearchPolicy,
): TurbopufferFilter | undefined => {
  if (alternatives.length === 0) return undefined
  const fields = TurbopufferFullTextAttributes[policy.language]

  const phraseAnywhere = (phrase: string): TurbopufferFilter => [
    "Or",
    [fields.context, fields.label, fields.content].map(
      (field): TurbopufferFilter => [field, "ContainsTokenSequence", phrase],
    ),
  ]

  return [
    "Or",
    alternatives.map((phrases): TurbopufferFilter => [
      "And",
      phrases.map(phraseAnywhere),
    ]),
  ]
}

/**
 * Compile weighted BM25 retrieval against only the projection's FTS policy.
 *
 * BM25 ranks every positive word. When the query is made of quoted phrases,
 * as in `"food photography" OR "recipe video"`, a hit must also contain one
 * alternative's phrases, matching PostgreSQL's web-search semantics.
 */
export const compileTurbopufferTextQuery = (input: {
  readonly scope: GraphSearchScope
  readonly partition: TurbopufferQueryPartition
  readonly query: string
  readonly policy: EnabledTextSearchPolicy
  readonly candidates: SearchResultCount
  readonly chunksPerDocument?: number | undefined
}): CompiledTurbopufferQuery => {
  const filters = compileCommonFilter(input)

  if (filters === undefined) return noDocuments()
  const match = parseTurbopufferTextMatch(input.query)
  const phrases = compilePhraseFilter(match.phraseAlternatives, input.policy)

  return executableQuery({
    rank_by: compileWeightedBm25(
      match.terms.length > 0 ? match.terms : input.query,
      input.policy,
    ),
    filters: phrases === undefined ? filters : ["And", [filters, phrases]],
    ...resultLimit(input.candidates, input.chunksPerDocument),
    include_attributes: [...TurbopufferSearchResultAttributes],
  })
}

/** Compile one same-snapshot request while preserving separate channel ranks. */
export const compileTurbopufferHybridQuery = (input: {
  readonly scope: GraphSearchScope
  readonly partition: TurbopufferQueryPartition
  readonly query: string
  readonly queryVector: ReadonlyArray<number>
  readonly policy: EnabledTextSearchPolicy
  readonly semanticCandidates: SearchResultCount
  readonly textCandidates: SearchResultCount
  readonly consistency?: TurbopufferQueryConsistency | undefined
  readonly chunksPerDocument?: number | undefined
}): CompiledTurbopufferHybridQuery => {
  const semantic = compileTurbopufferSemanticQuery({
    scope: input.scope,
    partition: input.partition,
    queryVector: input.queryVector,
    candidates: input.semanticCandidates,
    chunksPerDocument: input.chunksPerDocument,
  })

  if (semantic._tag === "NoDocuments") return semantic

  const text = compileTurbopufferTextQuery({
    scope: input.scope,
    partition: input.partition,
    query: input.query,
    policy: input.policy,
    candidates: input.textCandidates,
    chunksPerDocument: input.chunksPerDocument,
  })

  if (text._tag === "NoDocuments") return text

  return {
    _tag: "MultiQuery",
    request: {
      queries: [semantic.query, text.query],
      consistency: { level: input.consistency ?? "strong" },
    },
  }
}
