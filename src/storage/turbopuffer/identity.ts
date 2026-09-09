import { sha256 } from "@noble/hashes/sha2.js"
import { bytesToHex, utf8ToBytes } from "@noble/hashes/utils.js"
import { Schema } from "effect"
import type { DocumentKey } from "../../document/document-identity.js"
import type { JsonValue } from "../../document/json-value.js"

const JsonPrimitiveSchema = Schema.Union([
  Schema.Null,
  Schema.Boolean,
  Schema.Finite,
  Schema.String,
])

const primitiveJson = (
  value: null | boolean | number | string,
): string => {
  const encoded = JSON.stringify(value)
  if (encoded === undefined) {
    throw new Error("A JSON primitive unexpectedly failed to encode")
  }
  return encoded
}

/** Encode JSON with recursively sorted object keys for stable provider values. */
export const canonicalTurbopufferJson = (value: JsonValue): string => {
  if (Schema.is(JsonPrimitiveSchema)(value)) {
    return primitiveJson(value)
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalTurbopufferJson).join(",")}]`
  }

  // SAFETY: JsonValue contains only primitives, arrays, and string-keyed records.
  const record = value as Readonly<Record<string, JsonValue>>
  return `{${Object.keys(record)
    .sort()
    .map((key) => {
      const item = record[key]
      if (item === undefined) {
        throw new Error(
          "A parsed JSON object unexpectedly contained undefined",
        )
      }
      return `${primitiveJson(key)}:${canonicalTurbopufferJson(item)}`
    })
    .join(",")}}`
}

/** Hash one canonical, domain-separated adapter identity. */
export const hashTurbopufferIdentity = (value: JsonValue): string =>
  bytesToHex(sha256(utf8ToBytes(canonicalTurbopufferJson(value))))

/** A provider document ID guaranteed to fit Turbopuffer's 64-byte bound. */
export const TurbopufferPhysicalRowIdSchema = Schema.String.pipe(
  Schema.check(Schema.isNonEmpty()),
  Schema.check(
    Schema.makeFilter(
      (value) => utf8ToBytes(value).byteLength <= 64,
      { title: "TurbopufferPhysicalRowId" },
    ),
  ),
  Schema.brand("TurbopufferPhysicalRowId"),
)

/** A provider document ID guaranteed to fit Turbopuffer's 64-byte bound. */
export type TurbopufferPhysicalRowId = typeof TurbopufferPhysicalRowIdSchema.Type

/** Fixed-width identity used when deriving a namespace from logical scope. */
export const TurbopufferNamespaceIdentitySchema = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
  Schema.brand("TurbopufferNamespaceIdentity"),
)

/** Fixed-width identity used when deriving a namespace from logical scope. */
export type TurbopufferNamespaceIdentity =
  typeof TurbopufferNamespaceIdentitySchema.Type

/** Logical document/projection address shared by its marker and stable slots. */
export interface TurbopufferProjectionAddress {
  readonly partitionIdentity: TurbopufferNamespaceIdentity
  readonly documentKey: DocumentKey
  readonly projection: string
}

/** Derive the namespace identity for one isolated retrieval-compatible space. */
export const makeTurbopufferNamespaceIdentity = (input: {
  readonly deploymentId: string
  readonly endpoint:
    | { readonly _tag: "Region"; readonly region: string }
    | { readonly _tag: "Custom"; readonly baseURL: string }
  readonly workspace: string
  readonly embeddingProfile: {
    readonly id: string
    readonly version: string
    readonly dimensions: number
  }
  readonly schemaGeneration: number
}): TurbopufferNamespaceIdentity =>
  Schema.decodeSync(TurbopufferNamespaceIdentitySchema)(
    hashTurbopufferIdentity([
      "honertia.turbopuffer-namespace",
      2,
      input.deploymentId,
      input.endpoint._tag,
      input.endpoint._tag === "Region"
        ? input.endpoint.region
        : input.endpoint.baseURL,
      input.workspace,
      input.embeddingProfile.id,
      input.embeddingProfile.version,
      input.embeddingProfile.dimensions,
      input.schemaGeneration,
    ]),
  )

/** Derive the stable physical marker ID for one logical projection address. */
export const makeTurbopufferMarkerRowId = (
  address: TurbopufferProjectionAddress,
): TurbopufferPhysicalRowId =>
  Schema.decodeSync(TurbopufferPhysicalRowIdSchema)(
    hashTurbopufferIdentity([
      "honertia.turbopuffer-marker",
      1,
      address.partitionIdentity,
      address.documentKey,
      address.projection,
    ]),
  )

/** Derive one stable physical slot ID, independent of publication generation. */
export const makeTurbopufferSlotRowId = (
  address: TurbopufferProjectionAddress,
  slotOrdinal: number,
): TurbopufferPhysicalRowId => {
  if (!Number.isSafeInteger(slotOrdinal) || slotOrdinal < 0) {
    throw new Error("Turbopuffer slot ordinals must be non-negative integers")
  }

  return Schema.decodeSync(TurbopufferPhysicalRowIdSchema)(
    hashTurbopufferIdentity([
      "honertia.turbopuffer-slot",
      1,
      address.partitionIdentity,
      address.documentKey,
      address.projection,
      slotOrdinal,
    ]),
  )
}
