import { Schema } from "effect"
import {
  JsonValueSchema,
  type JsonValue,
} from "../../document/json-value.js"
import type {
  MetadataFilter,
  MetadataSearchValue,
} from "../../retrieval/metadata-filter.js"
import { hashTurbopufferIdentity } from "./identity.js"

/** Provider attribute containing exact, type-sensitive metadata terms. */
export const TurbopufferMetadataTermsAttribute = "metadata_terms"

/** Exact filter grammar emitted by the document-graph Turbopuffer adapter. */
export type TurbopufferFilter =
  | [attribute: string, operator: "Eq", value: unknown]
  | [attribute: string, operator: "In", values: Array<unknown>]
  | [attribute: string, operator: "NotIn", values: Array<unknown>]
  | [attribute: string, operator: "Contains", value: unknown]
  | [attribute: string, operator: "ContainsAny", values: Array<unknown>]
  | [attribute: string, operator: "ContainsTokenSequence", phrase: string]
  | [operator: "And", filters: Array<TurbopufferFilter>]
  | [operator: "Or", filters: Array<TurbopufferFilter>]
  | [operator: "Not", filter: TurbopufferFilter]

/** Hash one top-level metadata comparison into a bounded provider term. */
export const makeTurbopufferMetadataTerm = (
  key: string,
  value: MetadataSearchValue,
): string =>
  hashTurbopufferIdentity([
    "honertia.turbopuffer-metadata-term",
    1,
    key,
    value,
  ])

const MetadataSearchValueSchema = Schema.Union([
  Schema.Null,
  Schema.Boolean,
  Schema.Finite,
  Schema.String,
])

const MetadataRecordSchema = Schema.Record(Schema.String, JsonValueSchema)

/** Compile all searchable top-level metadata fields retained on one row. */
export const encodeTurbopufferMetadataTerms = (
  metadata: JsonValue | undefined,
): ReadonlyArray<string> => {
  if (metadata === undefined || !Schema.is(MetadataRecordSchema)(metadata)) {
    return []
  }

  const terms: Array<string> = []

  for (const key of Object.keys(metadata).sort()) {
    const value = metadata[key]

    if (value !== undefined && Schema.is(MetadataSearchValueSchema)(value)) {
      terms.push(makeTurbopufferMetadataTerm(key, value))
    }
  }

  return terms
}

/** Compile one storage-neutral metadata expression into Turbopuffer filters. */
export const compileTurbopufferMetadataFilter = (
  filter: MetadataFilter,
): TurbopufferFilter => {
  switch (filter._tag) {
    case "Equals":
      return [
        TurbopufferMetadataTermsAttribute,
        "Contains",
        makeTurbopufferMetadataTerm(filter.key, filter.value),
      ]
    case "OneOf":
      return [
        TurbopufferMetadataTermsAttribute,
        "ContainsAny",
        filter.values.map((value) =>
          makeTurbopufferMetadataTerm(filter.key, value),
        ),
      ]
    case "All":
      return [
        "And",
        filter.filters.map(compileTurbopufferMetadataFilter),
      ]
    case "Any":
      return [
        "Or",
        filter.filters.map(compileTurbopufferMetadataFilter),
      ]
    case "Not":
      return ["Not", compileTurbopufferMetadataFilter(filter.filter)]
  }
}
