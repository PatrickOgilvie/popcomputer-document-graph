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
export const JsonValueSchema: Schema.Schema<JsonValue> = Schema.Union(
  Schema.Null,
  Schema.Boolean,
  Schema.JsonNumber,
  Schema.String,
  Schema.Array(Schema.suspend(() => JsonValueSchema)),
  Schema.Record({
    key: Schema.String,
    value: Schema.suspend(() => JsonValueSchema),
  }),
)
