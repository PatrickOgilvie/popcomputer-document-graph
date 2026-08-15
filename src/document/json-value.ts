import { Schema } from "effect"

/** JSON-safe value retained in manifests, metadata, and storage records. */
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | ReadonlyArray<JsonValue>
  | { readonly [key: string]: JsonValue }

/** Parser for values that can safely cross storage and manifest boundaries. */
export const JsonValueSchema: Schema.Codec<JsonValue> = Schema.Union([
  Schema.Null,
  Schema.Boolean,
  Schema.Finite,
  Schema.String,
  Schema.Array(
    Schema.suspend((): Schema.Codec<JsonValue> => JsonValueSchema),
  ),
  Schema.Record(
    Schema.String,
    Schema.suspend((): Schema.Codec<JsonValue> => JsonValueSchema),
  ),
])
