import { Effect, Layer, Option, Result, Schema } from "effect"
import { makeRequestCoalescer } from "../postgres/coalesce.js"
import {
  ProjectionPublicationCoordinator,
  ProjectionPublicationCoordinatorFailed,
  type ProjectionPublicationCoordinatorService,
} from "../../indexing/projection-publication.js"
import type {
  IndexedRevisionSnapshot,
  ProjectionIndexKey,
} from "../../indexing/projection-index.js"
import {
  ProjectionHybridSearchStore,
  ProjectionSearchStore,
  ProjectionSearchStoreFailed,
  ProjectionTextSearchStore,
  ProjectionTextSearchStoreFailed,
  type CandidateFields,
  type HybridCandidateRequest,
  type HybridCandidateResult,
  type ProjectionHybridSearchStoreService,
  type ProjectionSearchStoreService,
  type ProjectionTextSearchStoreService,
  type SemanticCandidateRequest,
  type SemanticSearchCandidate,
  type TextCandidateRequest,
  type TextSearchCandidate,
} from "../../retrieval/graph-retrieval.js"
import {
  TurbopufferClient,
  type TurbopufferClientService,
} from "./client.js"
import {
  InvalidTurbopufferConfiguration,
  InvalidTurbopufferResponse,
  TurbopufferTransportFailed,
} from "./errors.js"
import {
  turbopufferWorkspacePartitionsEqual,
  validateTurbopufferWorkspacePartition,
  type TurbopufferWorkspacePartition,
} from "./partition.js"
import {
  compileTurbopufferHybridQuery,
  compileTurbopufferSemanticQuery,
  compileTurbopufferTextQuery,
  TurbopufferQueryConsistencySchema,
  type CompiledTurbopufferQuery,
  type TurbopufferQueryConsistency,
  type TurbopufferSerializedQuery,
} from "./query-compiler.js"
import {
  decodeTurbopufferSearchResultRow,
  scoreTurbopufferBm25,
  scoreTurbopufferCosineDistance,
  type DecodedTurbopufferSearchResultRow,
} from "./row-codec.js"

const QueryResponseSchema = Schema.Struct({
  rows: Schema.Array(Schema.Unknown),
})

const MultiQueryResultSchema = Schema.Struct({
  rows: Schema.Array(Schema.Unknown),
})

const MultiQueryResponseSchema = Schema.Struct({
  results: Schema.Array(MultiQueryResultSchema),
})

const parseQueryConsistency = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Runtime configuration is decoded immediately with the closed consistency schema.
  input: unknown,
): TurbopufferQueryConsistency => {
  try {
    return Schema.decodeUnknownSync(TurbopufferQueryConsistencySchema)(input)
  } catch {
    throw new InvalidTurbopufferConfiguration({
      field: "consistency",
      reason: "invalid_value",
    })
  }
}

/** Immutable namespace partition and read consistency used by retrieval. */
export interface TurbopufferProjectionSearchConfig {
  /** Canonical workspace, vector-space, and schema partition. */
  readonly partition: TurbopufferWorkspacePartition
  /**
   * Defaults to strong reads for search and same-snapshot multi-query.
   * Strong reads add an object-storage round trip to see the latest writes.
   */
  readonly consistency?: TurbopufferQueryConsistency | undefined
  /**
   * How candidates are confirmed current. `"journal"` (default) checks every
   * result against the publication coordinator. `"provider"` trusts live
   * rows: each publication writes a document's marker, live slots and
   * tombstones in one fenced request, so live rows are always one complete
   * revision. It keeps the coordinator's database off the search path.
   */
  readonly candidateVerification?: "journal" | "provider" | undefined
  /** Most chunks one document contributes to a channel's candidates. */
  readonly chunksPerDocument?: number | undefined
  /**
   * Merge searches that arrive together into shared multi-queries of up to
   * 16 subqueries, one HTTP request each. Build the Layer per request where
   * the runtime scopes I/O to a request, as Cloudflare Workers does.
   */
  readonly coalesceSearches?: TurbopufferSearchCoalescing | undefined
}

/** How long searches wait to share a request, and how many may share one. */
export interface TurbopufferSearchCoalescing {
  /** Defaults to 2 ms. */
  readonly windowMilliseconds?: number | undefined
}

/** Turbopuffer's limit on subqueries in one multi-query request. */
const MaximumSubqueries = 16

/** All retrieval capabilities implemented over one Turbopuffer namespace. */
export interface TurbopufferProjectionSearchStores
  extends ProjectionSearchStoreService,
    ProjectionTextSearchStoreService,
    ProjectionHybridSearchStoreService {}

const invalidResponse = (
  operation: InvalidTurbopufferResponse["operation"],
  cause: unknown,
): InvalidTurbopufferResponse =>
  new InvalidTurbopufferResponse({
    operation,
    reason: "invalid_shape",
    cause,
  })

const decodeQueryRows = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- This provider response boundary immediately decodes the query envelope with QueryResponseSchema.
  response: unknown,
): Effect.Effect<ReadonlyArray<unknown>, InvalidTurbopufferResponse> =>
  Schema.decodeUnknownEffect(QueryResponseSchema)(response).pipe(
    Effect.mapError((cause) => invalidResponse("query", cause)),
    Effect.map((decoded) => decoded.rows),
  )

const decodeMultiQueryResults = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- This provider response boundary immediately decodes the multi-query envelope with MultiQueryResponseSchema.
  response: unknown,
  expected: number,
): Effect.Effect<ReadonlyArray<ReadonlyArray<unknown>>, InvalidTurbopufferResponse> =>
  Schema.decodeUnknownEffect(MultiQueryResponseSchema)(response).pipe(
    Effect.mapError((cause) => invalidResponse("multi_query", cause)),
    Effect.flatMap((decoded) =>
      decoded.results.length === expected
        ? Effect.succeed(decoded.results.map((result) => result.rows))
        : Effect.fail(invalidResponse(
            "multi_query",
            "Expected one result for every subquery",
          ))),
  )

/** Rows for each subquery, in request order. */
type QueryExecutor = (
  queries: readonly [TurbopufferSerializedQuery, ...ReadonlyArray<TurbopufferSerializedQuery>],
) => Effect.Effect<
  ReadonlyArray<ReadonlyArray<unknown>>,
  TurbopufferTransportFailed | InvalidTurbopufferResponse
>

const directExecutor = (
  client: TurbopufferClientService,
  consistency: TurbopufferQueryConsistency,
): QueryExecutor => (queries) => {
  const [only, ...rest] = queries

  return rest.length === 0
    ? client.query({ ...only, consistency: { level: consistency } }).pipe(
        Effect.flatMap(decodeQueryRows),
        Effect.map((rows) => [rows]),
      )
    : client.multiQuery({ queries: [...queries], consistency: { level: consistency } }).pipe(
        Effect.flatMap((response) => decodeMultiQueryResults(response, queries.length)),
      )
}

/**
 * Pack the subqueries of searches that arrive together into multi-queries of
 * at most 16, run those requests concurrently, and hand each search its own
 * rows. A failed request fails only the searches packed into it.
 */
const coalescedExecutor = (
  client: TurbopufferClientService,
  consistency: TurbopufferQueryConsistency,
  windowMilliseconds: number,
): QueryExecutor => {
  type Outcome = Result.Result<
    ReadonlyArray<ReadonlyArray<unknown>>,
    TurbopufferTransportFailed | InvalidTurbopufferResponse
  >

  const coalescer = makeRequestCoalescer<ReadonlyArray<TurbopufferSerializedQuery>, Outcome>({
    windowMilliseconds,
    maximumBatch: MaximumSubqueries,
    run: async (_key, searches) => {
      const requests: Array<Array<number>> = []
      let subqueries = MaximumSubqueries

      searches.forEach((queries, index) => {
        const current = requests[requests.length - 1]

        if (current === undefined || subqueries + queries.length > MaximumSubqueries) {
          requests.push([index])
          subqueries = queries.length
        } else {
          current.push(index)
          subqueries += queries.length
        }
      })

      const outcomes = new Array<Outcome>(searches.length)

      await Effect.runPromise(Effect.forEach(requests, (members) => {
        const queries = members.flatMap((index) => searches[index] ?? [])

        return client.multiQuery({ queries, consistency: { level: consistency } }).pipe(
          Effect.flatMap((response) => decodeMultiQueryResults(response, queries.length)),
          Effect.result,
          Effect.map((result) => {
            let offset = 0

            for (const index of members) {
              const count = searches[index]?.length ?? 0

              outcomes[index] = Result.map(result, (rows) => rows.slice(offset, offset + count))
              offset += count
            }
          }),
        )
      }, { concurrency: "unbounded", discard: true }))

      return outcomes
    },
  })

  return (queries) => Effect.tryPromise({
    try: () => coalescer.submit("search", queries),
    catch: (cause) => new TurbopufferTransportFailed({
      operation: "multi_query",
      reason: "unavailable",
      requestOutcome: "unknown",
      cause,
    }),
  }).pipe(Effect.flatMap((outcome) => Result.isSuccess(outcome)
    ? Effect.succeed(outcome.success)
    : Effect.fail(outcome.failure)))
}

interface DecodedCandidate {
  readonly candidate: CandidateFields
  readonly contentHash: DecodedTurbopufferSearchResultRow["contentHash"]
  readonly rowIndex: number
}

const decodeCandidates = (input: {
  readonly operation: "query" | "multi_query"
  readonly rows: ReadonlyArray<unknown>
  readonly partition: TurbopufferWorkspacePartition
  readonly score: (providerScore: number) => number
}): Effect.Effect<ReadonlyArray<DecodedCandidate>, InvalidTurbopufferResponse> =>
  Effect.forEach(input.rows, (row, rowIndex) =>
    decodeTurbopufferSearchResultRow(row, rowIndex).pipe(
      Effect.mapError((error) =>
        input.operation === "query"
          ? error
          : new InvalidTurbopufferResponse({
              operation: "multi_query",
              reason: error.reason,
              rowIndex,
              cause: error,
            })),
      Effect.flatMap((decoded) => {
        if (decoded.partitionIdentity !== input.partition.identity) {
          return Effect.fail(new InvalidTurbopufferResponse({
            operation: input.operation,
            reason: "invalid_row",
            rowIndex,
            cause: "Provider row belongs to a different workspace partition",
          }))
        }

        return Effect.succeed({
          candidate: {
            ...candidateFields(decoded),
            score: input.score(decoded.providerScore),
          },
          contentHash: decoded.contentHash,
          rowIndex,
        })
      }),
    ),
  )

const normalizeCandidates = (
  decoded: ReadonlyArray<DecodedCandidate>,
): ReadonlyArray<CandidateFields> =>
  decoded
    .map((item) => item.candidate)
    .sort((left, right) =>
      right.score - left.score || left.chunkId.localeCompare(right.chunkId)
    )

const candidateFields = (
  row: DecodedTurbopufferSearchResultRow,
): Omit<CandidateFields, "score"> => ({
  chunkId: row.chunkId,
  documentKey: row.documentKey,
  reference: row.reference,
  projection: row.projection,
  revisionHash: row.revisionHash,
  sectionKey: row.sectionKey,
  sectionPart: row.sectionPart,
  content: row.content,
  metadata: row.metadata,
})

const projectionKeyIdentity = (key: ProjectionIndexKey): string =>
  JSON.stringify([key.documentKey, key.projection])

const invalidCommittedCandidate = (
  operation: "query" | "multi_query",
  rowIndex: number | undefined,
  cause: string,
): InvalidTurbopufferResponse => {
  if (rowIndex === undefined) {
    return new InvalidTurbopufferResponse({
      operation,
      reason: "invalid_row",
      cause,
    })
  }

  return new InvalidTurbopufferResponse({
    operation,
    reason: "invalid_row",
    rowIndex,
    cause,
  })
}

const verifyCommittedCandidates = (input: {
  readonly operation: "query" | "multi_query"
  readonly candidates: ReadonlyArray<DecodedCandidate>
  readonly coordinator: ProjectionPublicationCoordinatorService
}): Effect.Effect<
  void,
  InvalidTurbopufferResponse | ProjectionPublicationCoordinatorFailed
> =>
  Effect.gen(function*() {
    if (input.candidates.length === 0) return

    const keys: Array<ProjectionIndexKey> = []
    const seen = new Set<string>()

    for (const decoded of input.candidates) {
      const key = {
        documentKey: decoded.candidate.documentKey,
        projection: decoded.candidate.projection.id,
      }

      const identity = projectionKeyIdentity(key)

      if (!seen.has(identity)) {
        seen.add(identity)
        keys.push(key)
      }
    }

    const [first, ...rest] = keys

    if (first === undefined) return
    const lookups = yield* input.coordinator.loadRevisions([first, ...rest])

    if (lookups.length !== keys.length) {
      return yield* invalidCommittedCandidate(
        input.operation,
        undefined,
        "The publication coordinator returned an incomplete revision batch",
      )
    }

    const revisions = new Map<string, IndexedRevisionSnapshot>()

    for (let index = 0; index < keys.length; index += 1) {
      const expected = keys[index]
      const lookup = lookups[index]

      if (
        expected === undefined ||
        lookup === undefined ||
        lookup.key.documentKey !== expected.documentKey ||
        lookup.key.projection !== expected.projection ||
        Option.isNone(lookup.revision)
      ) {
        return yield* invalidCommittedCandidate(
          input.operation,
          undefined,
          "The publication coordinator did not return the active requested revision",
        )
      }

      revisions.set(
        projectionKeyIdentity(expected),
        lookup.revision.value,
      )
    }

    for (const decoded of input.candidates) {
      const revision = revisions.get(projectionKeyIdentity({
        documentKey: decoded.candidate.documentKey,
        projection: decoded.candidate.projection.id,
      }))

      const chunk = revision?.chunks.find(
        (item) => item.chunkId === decoded.candidate.chunkId,
      )

      if (
        revision === undefined ||
        revision.revisionHash !== decoded.candidate.revisionHash ||
        chunk === undefined ||
        chunk.contentHash !== decoded.contentHash
      ) {
        return yield* invalidCommittedCandidate(
          input.operation,
          decoded.rowIndex,
          "The provider row does not match the active committed revision",
        )
      }
    }
  })

const searchFailureReason = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- This boundary maps provider and coordinator failures into the public search contract.
  error: unknown,
): "unavailable" | "invalid_stored_state" => {
  if (Schema.is(TurbopufferTransportFailed)(error)) return "unavailable"

  if (Schema.is(ProjectionPublicationCoordinatorFailed)(error)) {
    return error.reason === "invalid_stored_state"
      ? "invalid_stored_state"
      : "unavailable"
  }

  return "invalid_stored_state"
}

const semanticFailure = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Effect.try catches an unknown defect here and this boundary distinguishes the typed provider failure with its runtime schema.
  error: unknown,
): ProjectionSearchStoreFailed =>
  Schema.is(ProjectionSearchStoreFailed)(error)
    ? error
    : new ProjectionSearchStoreFailed({
        reason: searchFailureReason(error),
        cause: error,
      })

const textFailure = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Effect.try catches an unknown defect here and this boundary distinguishes the typed provider failure with its runtime schema.
  error: unknown,
): ProjectionTextSearchStoreFailed =>
  Schema.is(ProjectionTextSearchStoreFailed)(error)
    ? error
    : new ProjectionTextSearchStoreFailed({
        reason: searchFailureReason(error),
        cause: error,
      })

const compileSemantic = (
  config: TurbopufferProjectionSearchConfig,
  request: SemanticCandidateRequest,
): Effect.Effect<CompiledTurbopufferQuery, ProjectionSearchStoreFailed> =>
  Effect.try({
    try: () => {
      const compiled = compileTurbopufferSemanticQuery({
        scope: request.scope,
        partition: config.partition,
        queryVector: request.vector,
        candidates: request.candidates,
        chunksPerDocument: config.chunksPerDocument,
      })

      if (
        compiled._tag !== "NoDocuments" &&
        (request.embeddingProfile.id !==
            config.partition.embeddingProfile.id ||
          request.embeddingProfile.version !==
            config.partition.embeddingProfile.version ||
          request.embeddingProfile.dimensions !==
            config.partition.embeddingProfile.dimensions)
      ) {
        throw new Error(
          "The semantic query embedding profile does not match the Turbopuffer namespace",
        )
      }

      return compiled
    },
    catch: semanticFailure,
  })

const compileText = (
  config: TurbopufferProjectionSearchConfig,
  request: TextCandidateRequest,
): Effect.Effect<CompiledTurbopufferQuery, ProjectionTextSearchStoreFailed> =>
  Effect.try({
    try: () =>
      compileTurbopufferTextQuery({
        scope: request.scope,
        partition: config.partition,
        query: request.query,
        policy: request.policy,
        candidates: request.candidates,
        chunksPerDocument: config.chunksPerDocument,
      }),
    catch: textFailure,
  })

/**
 * Build semantic, lexical, and same-snapshot hybrid stores over one client and
 * its D1 publication coordinator.
 * Graph targets are compiled into provider filters before either channel's top-k.
 */
export const makeTurbopufferProjectionSearchStores = (input: {
  readonly client: TurbopufferClientService
  readonly coordinator: ProjectionPublicationCoordinatorService
  readonly config: TurbopufferProjectionSearchConfig
}): TurbopufferProjectionSearchStores => {
  const partition = validateTurbopufferWorkspacePartition(
    input.config.partition,
  )

  const clientPartition = validateTurbopufferWorkspacePartition(
    input.client.partition,
  )

  if (
    !turbopufferWorkspacePartitionsEqual(partition, clientPartition) ||
    input.coordinator.indexGeneration !== partition.d1IndexGeneration
  ) {
    throw new InvalidTurbopufferConfiguration({
      field: "partition",
      reason: "mismatch",
    })
  }

  const consistency = parseQueryConsistency(
    input.config.consistency ?? "strong",
  )

  const verification = input.config.candidateVerification ?? "journal"
  const chunksPerDocument = input.config.chunksPerDocument

  if (
    chunksPerDocument !== undefined &&
    (!Number.isSafeInteger(chunksPerDocument) || chunksPerDocument < 1)
  ) {
    throw new InvalidTurbopufferConfiguration({
      field: "chunks_per_document",
      reason: "invalid_value",
    })
  }

  const coalescing = input.config.coalesceSearches
  const windowMilliseconds = coalescing?.windowMilliseconds ?? 2

  if (!Number.isSafeInteger(windowMilliseconds) || windowMilliseconds < 0 || windowMilliseconds > 100) {
    throw new InvalidTurbopufferConfiguration({
      field: "coalesce_searches",
      reason: "invalid_value",
    })
  }

  const execute = coalescing === undefined
    ? directExecutor(input.client, consistency)
    : coalescedExecutor(input.client, consistency, windowMilliseconds)

  const operation = coalescing === undefined ? "query" as const : "multi_query" as const

  const verified = (
    candidates: ReadonlyArray<DecodedCandidate>,
  ): Effect.Effect<
    ReadonlyArray<CandidateFields>,
    InvalidTurbopufferResponse | ProjectionPublicationCoordinatorFailed
  > =>
    verification === "provider"
      ? Effect.succeed(normalizeCandidates(candidates))
      : verifyCommittedCandidates({
          operation,
          candidates,
          coordinator: input.coordinator,
        }).pipe(Effect.map(() => normalizeCandidates(candidates)))

  const singleChannel = <E>(
    compiled: CompiledTurbopufferQuery,
    score: (providerScore: number) => number,
    failure: (
      error:
        | TurbopufferTransportFailed
        | InvalidTurbopufferResponse
        | ProjectionPublicationCoordinatorFailed,
    ) => E,
  ): Effect.Effect<ReadonlyArray<CandidateFields>, E> => {
    if (compiled._tag === "NoDocuments") return Effect.succeed([])

    return execute([compiled.query]).pipe(
      Effect.flatMap(([rows]) => decodeCandidates({
        operation,
        rows: rows ?? [],
        partition,
        score,
      })),
      Effect.flatMap(verified),
      Effect.mapError(failure),
    )
  }

  const searchCandidates = (
    request: SemanticCandidateRequest,
  ): Effect.Effect<
    ReadonlyArray<SemanticSearchCandidate>,
    ProjectionSearchStoreFailed
  > =>
    compileSemantic(input.config, request).pipe(
      Effect.flatMap((compiled) =>
        singleChannel(compiled, scoreTurbopufferCosineDistance, semanticFailure)),
    )

  const searchTextCandidates = (
    request: TextCandidateRequest,
  ): Effect.Effect<
    ReadonlyArray<TextSearchCandidate>,
    ProjectionTextSearchStoreFailed
  > =>
    compileText(input.config, request).pipe(
      Effect.flatMap((compiled) =>
        singleChannel(compiled, scoreTurbopufferBm25, textFailure)),
    )

  const searchHybridCandidates = (
    request: HybridCandidateRequest,
  ): Effect.Effect<
    HybridCandidateResult,
    ProjectionSearchStoreFailed | ProjectionTextSearchStoreFailed
  > =>
    Effect.try({
      try: () => {
        const compiled = compileTurbopufferHybridQuery({
          scope: request.scope,
          partition,
          query: request.query,
          queryVector: request.vector,
          policy: request.textPolicy,
          semanticCandidates: request.semanticCandidates,
          textCandidates: request.textCandidates,
          consistency,
          chunksPerDocument,
        })

        if (
          compiled._tag !== "NoDocuments" &&
          (request.embeddingProfile.id !==
            partition.embeddingProfile.id ||
            request.embeddingProfile.version !==
              partition.embeddingProfile.version ||
            request.embeddingProfile.dimensions !==
              partition.embeddingProfile.dimensions)
        ) {
          throw new Error(
            "The hybrid query embedding profile does not match the Turbopuffer namespace",
          )
        }

        return compiled
      },
      catch: semanticFailure,
    }).pipe(
      Effect.flatMap((compiled) => {
        if (compiled._tag === "NoDocuments") {
          return Effect.succeed({ semantic: [], text: [] })
        }

        const [semanticQuery, textQuery] = compiled.request.queries

        return execute([semanticQuery, textQuery]).pipe(
          Effect.mapError(semanticFailure),
          Effect.flatMap(([semanticRows, textRows]) =>
            Effect.all({
              semantic: decodeCandidates({
                operation: "multi_query",
                rows: semanticRows ?? [],
                partition,
                score: scoreTurbopufferCosineDistance,
              }).pipe(Effect.mapError(semanticFailure)),
              text: decodeCandidates({
                operation: "multi_query",
                rows: textRows ?? [],
                partition,
                score: scoreTurbopufferBm25,
              }).pipe(Effect.mapError(textFailure)),
            })),
          Effect.flatMap((candidates) =>
            verification === "provider"
              ? Effect.succeed({
                  semantic: normalizeCandidates(candidates.semantic),
                  text: normalizeCandidates(candidates.text),
                })
              : verifyCommittedCandidates({
                  operation: "multi_query",
                  candidates: [
                    ...candidates.semantic,
                    ...candidates.text,
                  ],
                  coordinator: input.coordinator,
                }).pipe(
                  Effect.mapError(semanticFailure),
                  Effect.map(() => ({
                    semantic: normalizeCandidates(candidates.semantic),
                    text: normalizeCandidates(candidates.text),
                  })),
                )),
        )
      }),
    )

  return {
    searchCandidates,
    searchTextCandidates,
    searchHybridCandidates,
  }
}

/** Provide Turbopuffer search from the partition-bound client and coordinator. */
export const turbopufferProjectionSearch = (
  config: TurbopufferProjectionSearchConfig,
): Layer.Layer<
  | ProjectionSearchStore
  | ProjectionTextSearchStore
  | ProjectionHybridSearchStore,
  InvalidTurbopufferConfiguration,
  ProjectionPublicationCoordinator | TurbopufferClient
> => {
  const stores = Effect.all({
    client: TurbopufferClient,
    coordinator: ProjectionPublicationCoordinator,
  }).pipe(Effect.flatMap(({ client, coordinator }) =>
    Effect.try({
      try: () => makeTurbopufferProjectionSearchStores({
        client,
        coordinator,
        config,
      }),
      catch: (cause) =>
        Schema.is(InvalidTurbopufferConfiguration)(cause)
          ? cause
          : new InvalidTurbopufferConfiguration({
              field: "partition",
              reason: "invalid_value",
            }),
    })))

  return Layer.mergeAll(
    Layer.effect(ProjectionSearchStore, stores),
    Layer.effect(ProjectionTextSearchStore, stores),
    Layer.effect(ProjectionHybridSearchStore, stores),
  )
}
