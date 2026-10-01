import Turbopuffer, {
  APIConnectionTimeoutError,
  APIError,
  type ClientOptions,
  type NamespaceMultiQueryParams,
  type NamespaceQueryParams,
  type NamespaceUpdateSchemaParams,
  type NamespaceWriteParams,
} from "@turbopuffer/turbopuffer"
import {
  Context,
  Duration,
  Effect,
  Layer,
  Redacted,
  Schedule,
  Schema,
} from "effect"
import {
  InvalidTurbopufferConfiguration,
  TurbopufferTransportFailed,
} from "./errors.js"
import {
  validateTurbopufferWorkspacePartition,
  type TurbopufferWorkspacePartition,
} from "./partition.js"

/** Explicit official-client settings; credentials never enter telemetry. */
export interface TurbopufferClientConfig {
  readonly apiKey: Redacted.Redacted<string>
  /** Canonical deployment, endpoint, workspace, vector, and schema partition. */
  readonly partition: TurbopufferWorkspacePartition
  readonly timeoutMilliseconds?: number | undefined
  /** Effect-owned retries after the initial request; defaults to two. */
  readonly retries?: number | undefined
  readonly fetch?: NonNullable<ClientOptions["fetch"]> | undefined
  /**
   * Gzip request bodies. Worth it where upload bandwidth, not CPU, limits
   * bulk writes; on Workers the SDK compresses in JavaScript. Defaults off.
   */
  readonly compression?: boolean | undefined
  /**
   * Ask for gzipped responses; defaults on. Search responses carry chunk
   * text and shrink about fourfold, and every runtime decompresses natively.
   * The SDK otherwise requests uncompressed responses unless `compression`
   * also gzips requests.
   */
  readonly compressResponses?: boolean | undefined
}

/** Narrow provider operations used by publication, search, and administration. */
export interface TurbopufferClientService {
  readonly partition: TurbopufferWorkspacePartition
  readonly write: (
    request: Omit<NamespaceWriteParams, "namespace">,
  ) => Effect.Effect<unknown, TurbopufferTransportFailed>
  readonly query: (
    request: Omit<NamespaceQueryParams, "namespace">,
  ) => Effect.Effect<unknown, TurbopufferTransportFailed>
  readonly multiQuery: (
    request: Omit<NamespaceMultiQueryParams, "namespace">,
  ) => Effect.Effect<unknown, TurbopufferTransportFailed>
  readonly inspectSchema: () => Effect.Effect<
    unknown,
    TurbopufferTransportFailed
  >
  readonly updateSchema: (
    request: Omit<NamespaceUpdateSchemaParams, "namespace">,
  ) => Effect.Effect<unknown, TurbopufferTransportFailed>
  readonly destroyNamespace: () => Effect.Effect<
    unknown,
    TurbopufferTransportFailed
  >
}

/** Official client operations kept outside the narrow service seam. */
export interface OfficialTurbopufferClient extends TurbopufferClientService {
  /**
   * Ask Turbopuffer to load the namespace into cache, for example when a user
   * opens search. Free when it is already warm.
   */
  readonly warmCache: () => Effect.Effect<void, TurbopufferTransportFailed>
}

/** Effect service tag for the narrow Turbopuffer transport boundary. */
export class TurbopufferClient extends Context.Service<
  TurbopufferClient,
  TurbopufferClientService
>()("@popcomputer/document-graph/TurbopufferClient") {}

const RetryableTransportReasonSchema = Schema.Literals([
  "unavailable",
  "timed_out",
  "rate_limited",
  "conflict",
])

const maximumRetryDelayMilliseconds = 30 * 60 * 1_000

interface ProviderRetryMetadata {
  readonly providerRetryDirective?: "retry" | "do_not_retry"
  readonly providerRetryAfterMilliseconds?: number
  readonly providerRetryAtEpochMilliseconds?: number
}

interface ProviderRetryMetadataBuilder {
  providerRetryDirective?: "retry" | "do_not_retry"
  providerRetryAfterMilliseconds?: number
  providerRetryAtEpochMilliseconds?: number
}

const parseNonNegativeDecimal = (value: string): number | undefined => {
  const canonical = value.trim()

  if (!/^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(canonical)) return undefined
  const parsed = Number(canonical)

  return Number.isFinite(parsed) ? parsed : undefined
}

const parseProviderRetryMetadata = (
  headers: Headers | undefined,
): ProviderRetryMetadata => {
  if (headers === undefined) return {}

  const metadata: ProviderRetryMetadataBuilder = {}
  const shouldRetry = headers.get("x-should-retry")

  if (shouldRetry === "true") metadata.providerRetryDirective = "retry"

  if (shouldRetry === "false") {
    metadata.providerRetryDirective = "do_not_retry"
  }

  const retryAfterMilliseconds = headers.get("retry-after-ms")

  if (retryAfterMilliseconds !== null) {
    const parsed = parseNonNegativeDecimal(retryAfterMilliseconds)

    if (parsed !== undefined) {
      metadata.providerRetryAfterMilliseconds = parsed

      return metadata
    }
  }

  const retryAfter = headers.get("retry-after")

  if (retryAfter === null) return metadata

  const seconds = parseNonNegativeDecimal(retryAfter)

  if (seconds !== undefined) {
    const milliseconds = seconds * 1_000

    if (Number.isFinite(milliseconds)) {
      metadata.providerRetryAfterMilliseconds = milliseconds
    }

    return metadata
  }

  const epochMilliseconds = /[A-Za-z]/.test(retryAfter)
    ? Date.parse(retryAfter)
    : Number.NaN

  if (Number.isFinite(epochMilliseconds)) {
    metadata.providerRetryAtEpochMilliseconds = epochMilliseconds
  }

  return metadata
}

const classifyTransportFailure = (
  operation: TurbopufferTransportFailed["operation"],
  cause: unknown,
): TurbopufferTransportFailed => {
  if (cause instanceof APIConnectionTimeoutError) {
    return new TurbopufferTransportFailed({
      operation,
      reason: "timed_out",
      requestOutcome: "unknown",
      cause,
    })
  }

  if (cause instanceof APIError) {
    const status = cause.status

    return new TurbopufferTransportFailed({
      operation,
      reason: status === 401
        ? "authentication_failed"
        : status === 403
          ? "permission_denied"
          : status === 409
            ? "conflict"
            : status === 429
              ? "rate_limited"
              : status === 408 || status === 504
                ? "timed_out"
                : status !== undefined && status >= 400 && status < 500
                  ? "rejected"
                  : "unavailable",
      requestOutcome: status !== undefined && status >= 400 && status < 500 &&
          status !== 408
        ? "definitely_not_applied"
        : "unknown",
      ...parseProviderRetryMetadata(cause.headers),
      cause,
    })
  }

  return new TurbopufferTransportFailed({
    operation,
    reason: cause instanceof Error && cause.name === "AbortError"
      ? "timed_out"
      : "unavailable",
    requestOutcome: "unknown",
    cause,
  })
}

interface UnknownOutcomeTransportFailureFields {
  operation: TurbopufferTransportFailed["operation"]
  reason: TurbopufferTransportFailed["reason"]
  requestOutcome: "unknown"
  providerRetryDirective?: "retry" | "do_not_retry"
  providerRetryAfterMilliseconds?: number
  providerRetryAtEpochMilliseconds?: number
  cause: unknown
}

const withUnknownRequestOutcome = (
  error: TurbopufferTransportFailed,
): TurbopufferTransportFailed => {
  if (error.requestOutcome === "unknown") return error

  const fields: UnknownOutcomeTransportFailureFields = {
    operation: error.operation,
    reason: error.reason,
    requestOutcome: "unknown",
    cause: error.cause,
  }

  if (error.providerRetryDirective !== undefined) {
    fields.providerRetryDirective = error.providerRetryDirective
  }

  if (error.providerRetryAfterMilliseconds !== undefined) {
    fields.providerRetryAfterMilliseconds =
      error.providerRetryAfterMilliseconds
  }

  if (error.providerRetryAtEpochMilliseconds !== undefined) {
    fields.providerRetryAtEpochMilliseconds =
      error.providerRetryAtEpochMilliseconds
  }

  return new TurbopufferTransportFailed(fields)
}

const retrying = <A>(
  retries: number,
  effect: Effect.Effect<A, TurbopufferTransportFailed>,
): Effect.Effect<A, TurbopufferTransportFailed> =>
  retries === 0
    ? effect
    : Effect.suspend(() => {
        let observedAmbiguousOutcome = false

        const observed = effect.pipe(
          Effect.catch((error) =>
            Effect.sync(() => {
              if (error.requestOutcome !== "definitely_not_applied") {
                observedAmbiguousOutcome = true
              }
            }).pipe(Effect.andThen(Effect.fail(error))),
          ),
        )

        return observed.pipe(
          Effect.retry({
            schedule: Schedule.exponential("100 millis").pipe(
              Schedule.jittered,
              Schedule.upTo({ times: retries }),
              Schedule.passthrough,
              Schedule.modifyDelay(({ duration, input, now }) => {
                const relativeDelay = input.providerRetryAfterMilliseconds

                const absoluteDelay =
                  input.providerRetryAtEpochMilliseconds === undefined
                    ? undefined
                    : Math.max(
                        0,
                        input.providerRetryAtEpochMilliseconds - now,
                      )

                const providerDelay = relativeDelay ?? absoluteDelay

                const combinedDelay = providerDelay === undefined
                  ? Duration.toMillis(duration)
                  : Math.max(Duration.toMillis(duration), providerDelay)

                return Effect.succeed(
                  Duration.millis(
                    Math.min(
                      maximumRetryDelayMilliseconds,
                      combinedDelay,
                    ),
                  ),
                )
              }),
            ),
            while: (error) => {
              if (error.providerRetryDirective === "do_not_retry") {
                return false
              }

              if (error.providerRetryDirective === "retry") return true

              return Schema.is(RetryableTransportReasonSchema)(error.reason)
            },
          }),
          Effect.mapError((error) =>
            observedAmbiguousOutcome
              ? withUnknownRequestOutcome(error)
              : error),
        )
      })

const checkedInteger = (
  value: number,
  field: "retries" | "timeout_milliseconds",
  minimum: number,
): number => {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new InvalidTurbopufferConfiguration({
      field,
      reason: "invalid_value",
    })
  }

  return value
}

const TurbopufferApiKeySchema = Schema.Trimmed.check(Schema.isNonEmpty())

const invalidApiKey = (): Error =>
  new InvalidTurbopufferConfiguration({
    field: "api_key",
    reason: "invalid_value",
  })

const unwrapCanonicalApiKey = (
  apiKey: Redacted.Redacted<string>,
): string => {
  if (!Redacted.isRedacted(apiKey)) {
    throw invalidApiKey()
  }

  try {
    return Schema.decodeSync(TurbopufferApiKeySchema)(
      Redacted.value(apiKey),
    )
  } catch {
    throw invalidApiKey()
  }
}

/** Build the official SDK transport with SDK retries and logging disabled. */
export const makeOfficialTurbopufferClient = (
  config: TurbopufferClientConfig,
): OfficialTurbopufferClient => {
  const partition = validateTurbopufferWorkspacePartition(config.partition)
  const namespace = partition.namespace
  const retries = checkedInteger(config.retries ?? 2, "retries", 0)

  const timeout = checkedInteger(
    config.timeoutMilliseconds ?? 60_000,
    "timeout_milliseconds",
    1,
  )

  const apiKey = unwrapCanonicalApiKey(config.apiKey)

  const clientOptions: ClientOptions = {
    apiKey,
    defaultNamespace: namespace,
    // The SDK merges TURBOPUFFER_CUSTOM_HEADERS after its auth headers.
    // Reassert the canonical key here so ambient headers cannot replace it.
    defaultHeaders: config.compressResponses === false
      ? { authorization: `Bearer ${apiKey}` }
      : { authorization: `Bearer ${apiKey}`, "accept-encoding": "gzip" },
    maxRetries: 0,
    timeout,
    logLevel: "off",
    compression: config.compression ?? false,
  }

  if (partition.endpoint._tag === "Region") {
    clientOptions.region = partition.endpoint.region
    // The SDK otherwise falls back to TURBOPUFFER_BASE_URL, which could route
    // this deployment-scoped partition to an unrelated ambient endpoint.
    clientOptions.baseURL = null
  } else {
    clientOptions.baseURL = partition.endpoint.baseURL
    // The SDK otherwise falls back to TURBOPUFFER_REGION, which would make a
    // custom endpoint depend on ambient process configuration.
    clientOptions.region = null
  }

  if (config.fetch !== undefined) clientOptions.fetch = config.fetch
  const client = new Turbopuffer(clientOptions)
  const remote = client.namespace(namespace)

  const request = <A>(
    operation: TurbopufferTransportFailed["operation"],
    run: (signal: AbortSignal) => Promise<A>,
  ): Effect.Effect<A, TurbopufferTransportFailed> =>
    retrying(
      retries,
      Effect.tryPromise({
        try: (signal) => run(signal),
        catch: (cause) => classifyTransportFailure(operation, cause),
      }),
    )

  return {
    partition,
    write: (input) => request(
      "write",
      (signal) => remote.write(
        { ...input, namespace },
        { signal, maxRetries: 0 },
      ),
    ),
    query: (input) => request(
      "query",
      (signal) => remote.query({ ...input, namespace }, { signal }),
    ),
    multiQuery: (input) => request(
      "multi_query",
      (signal) => remote.multiQuery({ ...input, namespace }, { signal }),
    ),
    inspectSchema: () => request(
      "inspect_schema",
      (signal) => remote.schema({}, { signal }),
    ),
    updateSchema: (input) => request(
      "update_schema",
      (signal) => remote.updateSchema({ ...input, namespace }, { signal }),
    ),
    destroyNamespace: () => request(
      "destroy_namespace",
      (signal) => remote.deleteAll({}, { signal }),
    ),
    warmCache: () => request(
      "hint_cache_warm",
      (signal) => remote.hintCacheWarm({}, { signal }),
    ).pipe(Effect.asVoid),
  }
}

/** Provide the official Turbopuffer SDK through the narrow Effect boundary. */
export const officialTurbopufferClient = (
  config: TurbopufferClientConfig,
): Layer.Layer<TurbopufferClient> =>
  Layer.succeed(TurbopufferClient, makeOfficialTurbopufferClient(config))
