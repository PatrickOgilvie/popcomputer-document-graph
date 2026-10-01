import { Effect, Schema } from "effect"
import {
  JsonValueSchema,
  type JsonValue,
} from "../../document/json-value.js"
import { TurbopufferClient } from "./client.js"
import { compileTurbopufferSchemaManifest } from "./config.js"
import {
  InvalidTurbopufferResponse,
  type TurbopufferTransportFailed,
} from "./errors.js"

const NamespaceSchemaResponseSchema = Schema.Record(
  Schema.String,
  JsonValueSchema,
)

/** Provider-normalized schema returned by an explicit namespace inspection. */
export interface TurbopufferNamespaceSchemaInspection {
  readonly format: "honertia.document-graph/turbopuffer-schema-inspection-v1"
  readonly namespace: string
  readonly attributes: Readonly<Record<string, JsonValue>>
}

/** Update the pinned document-graph schema in an existing namespace. */
export const updateTurbopufferNamespaceSchema = Effect.fn(
  "TurbopufferNamespace.updateSchema",
)(function*() {
  const client = yield* TurbopufferClient

  const manifest = compileTurbopufferSchemaManifest(
    client.partition.embeddingProfile.dimensions,
    client.partition.vectorElementType,
  )

  yield* client.updateSchema({ schema: { ...manifest.attributes } })
})

/** Inspect a namespace schema without mutating or implicitly bootstrapping it. */
export const inspectTurbopufferNamespaceSchema: Effect.Effect<
  TurbopufferNamespaceSchemaInspection,
  TurbopufferTransportFailed | InvalidTurbopufferResponse,
  TurbopufferClient
> = Effect.gen(function*() {
  const client = yield* TurbopufferClient
  const response = yield* client.inspectSchema()

  const attributes = yield* Schema.decodeUnknownEffect(
    NamespaceSchemaResponseSchema,
  )(response).pipe(
    Effect.mapError((cause) =>
      new InvalidTurbopufferResponse({
        operation: "inspect_schema",
        reason: "invalid_shape",
        cause,
      })),
  )

  return {
    format: "honertia.document-graph/turbopuffer-schema-inspection-v1",
    namespace: client.partition.namespace,
    attributes,
  }
})

/** Destroy a namespace only when invoked by an explicit administration flow. */
export const destroyTurbopufferNamespace: Effect.Effect<
  void,
  TurbopufferTransportFailed,
  TurbopufferClient
> = Effect.gen(function*() {
  const client = yield* TurbopufferClient
  yield* client.destroyNamespace()
})
