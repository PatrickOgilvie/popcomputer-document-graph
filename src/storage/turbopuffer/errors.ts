import { Schema } from "effect"

/** A Turbopuffer adapter setting failed validation before any request ran. */
export class InvalidTurbopufferConfiguration extends Schema.TaggedError<
  InvalidTurbopufferConfiguration
>()("InvalidTurbopufferConfiguration", {
  field: Schema.Literals([
    "namespace",
    "schema_generation",
    "vector_dimensions",
    "workspace",
    "deployment_id",
    "endpoint",
    "partition",
    "api_key",
    "retries",
    "timeout_milliseconds",
    "maximum_slots_per_revision",
    "maximum_publication_bytes",
    "publication_lease_milliseconds",
    "retained_publication_history",
    "consistency",
    "coalesce_writes",
  ]),
  reason: Schema.Literals(["invalid_value", "mismatch"]),
}) {}

/** A Turbopuffer request failed before a valid provider response was decoded. */
export class TurbopufferTransportFailed extends Schema.TaggedError<
  TurbopufferTransportFailed
>()("TurbopufferTransportFailed", {
  operation: Schema.Literals([
    "write",
    "query",
    "multi_query",
    "inspect_schema",
    "update_schema",
    "destroy_namespace",
  ]),
  reason: Schema.Literals([
    "unavailable",
    "timed_out",
    "rate_limited",
    "conflict",
    "authentication_failed",
    "permission_denied",
    "rejected",
  ]),
  /** Whether the provider response proves that the requested mutation did not apply. */
  requestOutcome: Schema.optional(
    Schema.Literals(["definitely_not_applied", "unknown"]),
  ),
  /** Explicit retry decision supplied by the provider response. */
  providerRetryDirective: Schema.optional(
    Schema.Literals(["retry", "do_not_retry"]),
  ),
  /** Relative provider backoff supplied by `retry-after-ms` or Retry-After. */
  providerRetryAfterMilliseconds: Schema.optional(
    Schema.Number.pipe(
      Schema.check(
        Schema.isFinite(),
        Schema.isGreaterThanOrEqualTo(0),
      ),
    ),
  ),
  /** Absolute provider backoff supplied by an HTTP-date Retry-After value. */
  providerRetryAtEpochMilliseconds: Schema.optional(
    Schema.Number.pipe(Schema.check(Schema.isFinite())),
  ),
  cause: Schema.Unknown,
}) {}

/** A successful Turbopuffer response did not satisfy the adapter contract. */
export class InvalidTurbopufferResponse extends Schema.TaggedError<
  InvalidTurbopufferResponse
>()("InvalidTurbopufferResponse", {
  operation: Schema.Literals([
    "write",
    "query",
    "multi_query",
    "inspect_schema",
  ]),
  reason: Schema.Literals([
    "invalid_shape",
    "invalid_row",
    "invalid_score",
    "partial_write",
  ]),
  rowIndex: Schema.optional(Schema.Number.pipe(Schema.check(Schema.isInt()))),
  cause: Schema.Unknown,
}) {}

/** The live namespace schema differs from the compiled adapter manifest. */
export class TurbopufferSchemaMismatch extends Schema.TaggedError<
  TurbopufferSchemaMismatch
>()("TurbopufferSchemaMismatch", {
  namespace: Schema.String,
  reason: Schema.Literals([
    "missing_attribute",
    "unexpected_attribute",
    "incompatible_attribute",
    "distance_metric",
  ]),
  attribute: Schema.optional(Schema.String),
}) {}

/** One atomic publication would exceed an adapter or provider request bound. */
export class TurbopufferMutationTooLarge extends Schema.TaggedError<
  TurbopufferMutationTooLarge
>()("TurbopufferMutationTooLarge", {
  rows: Schema.Number.pipe(Schema.check(Schema.isInt())),
  bytes: Schema.Number.pipe(Schema.check(Schema.isInt())),
  maximumBytes: Schema.Number.pipe(Schema.check(Schema.isInt())),
}) {}
