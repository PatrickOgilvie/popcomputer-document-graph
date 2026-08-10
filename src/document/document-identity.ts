import { sha256 } from "@noble/hashes/sha2.js"
import { bytesToHex, utf8ToBytes } from "@noble/hashes/utils.js"
import { Effect, Schema } from "effect"
import type { ChunkingStrategyDescriptor } from "./chunking-strategy.js"
import { JsonValueSchema, type JsonValue } from "./json-value.js"
import type {
  ProjectedText,
  TextSearchPolicy,
} from "./text-search-policy.js"

/** Lowercase SHA-256 digest encoded as 64 hexadecimal characters. */
export const Sha256HexSchema = Schema.String.pipe(
  Schema.length(64),
  Schema.pattern(/^[0-9a-f]{64}$/),
)

/** Stable storage identity for one graph document node. */
export const DocumentKeySchema = Sha256HexSchema.pipe(
  Schema.brand("DocumentKey"),
)

/** Stable storage identity for one projected chunk location. */
export const ChunkIdSchema = Sha256HexSchema.pipe(
  Schema.brand("ChunkId"),
)

/** SHA-256 digest of the exact text submitted for embedding. */
export const ContentHashSchema = Sha256HexSchema.pipe(
  Schema.brand("ContentHash"),
)

/** SHA-256 digest of one complete projected document revision. */
export const ProjectionRevisionHashSchema = Sha256HexSchema.pipe(
  Schema.brand("ProjectionRevisionHash"),
)

/** Stable storage identity for one graph document node. */
export type DocumentKey = Schema.Schema.Type<typeof DocumentKeySchema>

/** Stable storage identity for one projected chunk location. */
export type ChunkId = Schema.Schema.Type<typeof ChunkIdSchema>

/** SHA-256 digest of the exact text submitted for embedding. */
export type ContentHash = Schema.Schema.Type<typeof ContentHashSchema>

/** SHA-256 digest of one complete projected document revision. */
export type ProjectionRevisionHash = Schema.Schema.Type<
  typeof ProjectionRevisionHashSchema
>

/** A document ID could not be encoded into a canonical manifest value. */
export class InvalidDocumentIdentity extends Schema.TaggedError<
  InvalidDocumentIdentity
>()("InvalidDocumentIdentity", {
  graph: Schema.String,
  documentKind: Schema.String,
  reason: Schema.Literal("cannot_encode_id", "id_not_json"),
}) {}

const primitiveJson = (
  value: null | boolean | number | string,
): string => {
  const encoded = JSON.stringify(value)
  if (encoded === undefined) {
    throw new Error("A JSON primitive unexpectedly failed to encode")
  }

  return encoded
}

const canonicalJson = (value: JsonValue): string => {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string"
  ) {
    return primitiveJson(value)
  }

  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`
  }

  // SAFETY: JsonValue has only primitive, array, and record branches. The
  // primitive and array branches were eliminated above.
  const record = value as Readonly<Record<string, JsonValue>>
  const entries: Array<string> = []
  for (const key of Object.keys(record).sort()) {
    const item = record[key]
    if (item === undefined) {
      throw new Error("A parsed JSON object unexpectedly contained undefined")
    }

    entries.push(`${primitiveJson(key)}:${canonicalJson(item)}`)
  }

  return `{${entries.join(",")}}`
}

const sha256Hex = (value: string): string =>
  bytesToHex(sha256(utf8ToBytes(value)))

const hashJson = (value: JsonValue): string =>
  sha256Hex(canonicalJson(value))

/** Encode one parsed document ID through its owning Effect Schema. */
export const encodeDocumentId = <
  IdSchema extends Schema.Schema.AnyNoContext,
>(
  graph: string,
  documentKind: string,
  schema: IdSchema,
  id: Schema.Schema.Type<IdSchema>,
): Effect.Effect<JsonValue, InvalidDocumentIdentity> =>
  Schema.encode(schema)(id).pipe(
    Effect.mapError(
      () =>
        new InvalidDocumentIdentity({
          graph,
          documentKind,
          reason: "cannot_encode_id",
        }),
    ),
    Effect.flatMap((encoded) =>
      Schema.decodeUnknown(JsonValueSchema)(encoded, {
        onExcessProperty: "error",
      }).pipe(
        Effect.mapError(
          () =>
            new InvalidDocumentIdentity({
              graph,
              documentKind,
              reason: "id_not_json",
            }),
        ),
      ),
    ),
  )

/** Derive one fixed-width key from a graph node's encoded identity. */
export const makeDocumentKey = (input: {
  readonly graph: string
  readonly documentKind: string
  readonly encodedId: JsonValue
}): DocumentKey =>
  Schema.decodeSync(DocumentKeySchema)(
    hashJson([
      "honertia.document-key",
      1,
      input.graph,
      input.documentKind,
      input.encodedId,
    ]),
  )

/** Derive a stable key for one logical chunk location. */
export const makeChunkId = (input: {
  readonly documentKey: DocumentKey
  readonly projection: string
  readonly sectionKey: string
  readonly sectionPart: number
}): ChunkId =>
  Schema.decodeSync(ChunkIdSchema)(
    hashJson([
      "honertia.chunk-id",
      1,
      input.documentKey,
      input.projection,
      input.sectionKey,
      input.sectionPart,
    ]),
  )

/** Hash the exact content submitted to an embedding provider. */
export const makeContentHash = (content: string): ContentHash =>
  Schema.decodeSync(ContentHashSchema)(sha256Hex(content))

/** Storage-relevant fields from one chunk entering a revision hash. */
export interface ProjectionRevisionChunk {
  readonly chunkId: ChunkId
  readonly ordinal: number
  readonly sectionIndex: number
  readonly sectionPart: number
  readonly content: string
  readonly contentHash: ContentHash
  readonly text: ProjectedText
  readonly metadata: JsonValue | undefined
}

/** Derive a hash for one complete projection and chunking result. */
export const makeProjectionRevisionHash = (input: {
  readonly documentKey: DocumentKey
  readonly projection: {
    readonly id: string
    readonly version: string
  }
  readonly chunking: ChunkingStrategyDescriptor
  readonly search: {
    readonly text: TextSearchPolicy
  }
  readonly chunks: ReadonlyArray<ProjectionRevisionChunk>
}): ProjectionRevisionHash =>
  Schema.decodeSync(ProjectionRevisionHashSchema)(
    hashJson({
      format: "honertia.projection-revision/v2",
      documentKey: input.documentKey,
      projection: {
        id: input.projection.id,
        version: input.projection.version,
      },
      chunking: {
        id: input.chunking.id,
        version: input.chunking.version,
        config: input.chunking.config,
      },
      search: {
        text:
          input.search.text === "disabled"
            ? "disabled"
            : {
                language: input.search.text.language,
                weights: input.search.text.weights,
              },
        // Retain the v2 identity shape for existing non-fuzzy revisions while
        // the unimplemented fuzzy policy is absent from the public API.
        fuzzy: null,
      },
      chunks: input.chunks.map((chunk) => ({
        chunkId: chunk.chunkId,
        ordinal: chunk.ordinal,
        sectionIndex: chunk.sectionIndex,
        sectionPart: chunk.sectionPart,
        content: chunk.content,
        contentHash: chunk.contentHash,
        text: {
          context: chunk.text.context ?? null,
          label: chunk.text.label ?? null,
          content: chunk.text.content,
        },
        metadata: chunk.metadata ?? null,
      })),
    }),
  )
