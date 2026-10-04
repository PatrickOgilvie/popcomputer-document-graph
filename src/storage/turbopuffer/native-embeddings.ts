import { Effect, Schema } from "effect"
import type { ContentHash } from "../../document/document-identity.js"
import {
  EmbeddingProviderFailed,
  type EmbeddedContent,
  type EmbeddingProfile,
  type EmbeddingProviderService,
  type EmbeddingRequest,
} from "../../indexing/embedding-provider.js"
import type { TurbopufferClientService } from "./client.js"
import { TurbopufferMaximumQueryRows } from "./config.js"
import {
  InvalidTurbopufferConfiguration,
  type TurbopufferTransportFailed,
} from "./errors.js"

/** Settings for document embeddings computed by a Turbopuffer managed model. */
export interface TurbopufferNativeEmbeddingConfig {
  /**
   * Vector space the model produces. Reuse an existing profile only when the
   * model is the one that embedded the stored vectors; Turbopuffer may serve
   * it at another precision, so measure before mixing providers.
   */
  readonly profile: EmbeddingProfile
  /** Managed model name, such as `qwen/qwen3-embedding-0p6b`. */
  readonly model: string
  /**
   * Client over a namespace used only for these embeddings, never a
   * projection index namespace. Turbopuffer fixes the namespace's model on
   * its first write and rejects any other.
   */
  readonly client: TurbopufferClientService
  /**
   * Turbopuffer embeds queries only inside its own searches, so query vectors
   * come from the provider that embeds them for this profile today.
   */
  readonly embedQuery: EmbeddingProviderService["embedQuery"]
  /** Documents per write and read; defaults to 256. */
  readonly batchSize?: number | undefined
}

const defaultBatchSize = 256

const StoredEmbeddingsSchema = Schema.Struct({
  rows: Schema.Array(Schema.Struct({
    id: Schema.String,
    model: Schema.optional(Schema.NullOr(Schema.String)),
    vector: Schema.optional(Schema.NullOr(Schema.Array(Schema.Finite))),
  })),
})

const NotFoundSchema = Schema.Struct({ status: Schema.Literal(404) })

const isMissingNamespace = (error: TurbopufferTransportFailed): boolean =>
  error.reason === "rejected" && Schema.is(NotFoundSchema)(error.cause)

const ModelSchema = Schema.Trimmed.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(200),
)

/**
 * Document embeddings from a Turbopuffer managed model.
 *
 * Turbopuffer embeds a string attribute when it is written and returns the
 * stored vector to queries, but has no endpoint that only embeds. Each
 * document is therefore written to the configured namespace under its
 * content hash, and its vector is read back. Indexing receives the vectors as
 * from any other provider, so stores and the publication path are unchanged.
 *
 * Content already in the namespace for the same model is read, not embedded
 * again, which makes retried indexing free. Rows stay in the namespace, as
 * Turbopuffer always keeps the source text beside a native embedding.
 */
export const makeTurbopufferNativeEmbeddingProvider = (
  config: TurbopufferNativeEmbeddingConfig,
): EmbeddingProviderService => {
  const batchSize = config.batchSize ?? defaultBatchSize

  if (
    !Number.isSafeInteger(batchSize) ||
    batchSize < 1 ||
    batchSize > TurbopufferMaximumQueryRows
  ) {
    throw new InvalidTurbopufferConfiguration({
      field: "batch_size",
      reason: "invalid_value",
    })
  }

  if (!Schema.is(ModelSchema)(config.model)) {
    throw new InvalidTurbopufferConfiguration({
      field: "embedding_model",
      reason: "invalid_value",
    })
  }

  const { profile, model, client } = config

  const failed = (
    reason: EmbeddingProviderFailed["reason"],
    cause: unknown,
  ) => new EmbeddingProviderFailed({ profile: profile.id, reason, cause })

  // Sent with every write: identical settings are accepted, and a namespace
  // created for another model rejects the write instead of mixing spaces.
  const schema = {
    content: {
      type: "string",
      filterable: false,
      embed: { model, attribute: "vector", dims: profile.dimensions },
    },
    vector: { type: `[${profile.dimensions}]f32`, ann: true },
    model: { type: "string", filterable: false },
  } as const

  const read = (ids: ReadonlyArray<ContentHash>) =>
    client.query({
      filters: ["id", "In", [...ids]],
      rank_by: ["id", "asc"],
      limit: ids.length,
      include_attributes: ["vector", "model"],
      // A vector written a moment ago must be visible to this read.
      consistency: { level: "strong" },
    }).pipe(
      // The namespace is created by its first write; before then nothing is stored.
      Effect.catchIf(isMissingNamespace, () => Effect.succeed({ rows: [] })),
      Effect.mapError((cause) => failed("unavailable", cause)),
      Effect.flatMap((response) =>
        Schema.decodeUnknownEffect(StoredEmbeddingsSchema)(response).pipe(
          Effect.mapError((cause) => failed("invalid_response", cause)),
        )),
      Effect.map(({ rows }) => {
        const vectors = new Map<string, ReadonlyArray<number>>()

        for (const row of rows) {
          if (row.model !== model || row.vector == null) continue
          if (row.vector.length !== profile.dimensions) continue
          vectors.set(row.id, row.vector)
        }

        return vectors
      }),
    )

  const embedBatch = Effect.fn("TurbopufferNativeEmbeddings.embedBatch")(
    function*(requests: ReadonlyArray<EmbeddingRequest>) {
      const ids = requests.map(({ contentHash }) => contentHash)
      const stored = yield* read(ids)
      const missing = requests.filter(({ contentHash }) => !stored.has(contentHash))

      if (missing.length > 0) {
        yield* client.write({
          upsert_rows: missing.map(({ contentHash, content }) => ({
            id: contentHash,
            content,
            model,
          })),
          distance_metric: "cosine_distance",
          schema,
        }).pipe(Effect.mapError((cause) => failed("unavailable", cause)))

        const written = yield* read(missing.map(({ contentHash }) => contentHash))
        for (const [id, vector] of written) stored.set(id, vector)
      }

      return yield* Effect.forEach(requests, ({ contentHash }) => {
        const vector = stored.get(contentHash)

        return vector === undefined
          ? Effect.fail(failed(
            "invalid_response",
            new Error("Turbopuffer returned no vector for embedded content"),
          ))
          : Effect.succeed<EmbeddedContent>({ contentHash, vector })
      })
    },
  )

  return {
    profile,
    embedDocuments: (requests) => {
      const unique = [
        ...new Map(requests.map((request) => [request.contentHash, request])).values(),
      ]
      const batches = Array.from(
        { length: Math.ceil(unique.length / batchSize) },
        (_, index) => unique.slice(index * batchSize, (index + 1) * batchSize),
      )

      // One vector per content hash, as indexing requires.
      return Effect.forEach(batches, embedBatch, { concurrency: 1 }).pipe(
        Effect.map((embedded) => embedded.flat()),
      )
    },
    embedQuery: config.embedQuery,
  }
}
