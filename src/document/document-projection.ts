import { Array as EffectArray, Effect, Schema } from "effect"
import type { ChunkingInput } from "./chunking-strategy.js"
import type {
  DocumentDefinitions,
  DocumentKind,
  DocumentValue,
} from "./document-definition.js"
import type { DocumentReference } from "./document-reference.js"
import { JsonValueSchema, type JsonValue } from "./json-value.js"
import {
  makeChunkId,
  makeContentHash,
  makeProjectionRevisionHash,
  type ChunkId,
  type ContentHash,
  type DocumentKey,
  type InvalidDocumentIdentity,
  type ProjectionRevisionHash,
} from "./document-identity.js"
import {
  InvalidDocumentValue,
  parseDocumentInstance,
  type EncodedDocumentReference,
  type ParsedDocumentInstance,
} from "./document-instance.js"
import type {
  ProjectedDocument,
  VectorProjection,
  VectorProjectionShape,
} from "./vector-projection.js"
import type { ProjectedText } from "./text-search-policy.js"

/** Projection identifiers registered by one document definition. */
export type DocumentProjectionId<
  Documents extends DocumentDefinitions,
  Kind extends DocumentKind<Documents>,
> = Documents[Kind]["projections"][number]["id"]

type ProjectionFor<
  Documents extends DocumentDefinitions,
  Kind extends DocumentKind<Documents>,
  ProjectionId extends DocumentProjectionId<Documents, Kind>,
> = Extract<
  Documents[Kind]["projections"][number],
  { readonly id: ProjectionId }
>

type ProjectionMetadata<Projection> =
  Projection extends VectorProjection<
    infer _Value,
    infer Metadata,
    infer _Id,
    infer _Version
  >
    ? Metadata
    : never

interface ProjectedChunkFields<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Kind extends DocumentKind<Documents>,
  ProjectionId extends DocumentProjectionId<Documents, Kind>,
> {
  readonly target: DocumentReference<GraphId, Documents, Kind>
  readonly projection: {
    readonly id: ProjectionId
    readonly version: ProjectionFor<
      Documents,
      Kind,
      ProjectionId
    >["version"]
  }
  readonly documentKey: DocumentKey
  readonly revisionHash: ProjectionRevisionHash
  readonly chunkId: ChunkId
  readonly contentHash: ContentHash
  readonly ordinal: number
  readonly sectionKey: string
  readonly sectionIndex: number
  readonly sectionPart: number
  readonly content: string
  readonly embeddingContent: string
  readonly text: ProjectedText
}

/**
 * One deterministic, attributed unit ready for embedding.
 *
 * `sectionKey` identifies the authored attribution boundary. Its validated
 * metadata is carried unchanged to every chunk part derived from that section.
 */
export type ProjectedChunk<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Kind extends DocumentKind<Documents>,
  ProjectionId extends DocumentProjectionId<Documents, Kind>,
> = ProjectedChunkFields<GraphId, Documents, Kind, ProjectionId> &
  ([ProjectionMetadata<
    ProjectionFor<Documents, Kind, ProjectionId>
  >] extends [never]
    ? { readonly metadata?: never }
    : {
        readonly metadata: ProjectionMetadata<
          ProjectionFor<Documents, Kind, ProjectionId>
        > & JsonValue
      })

/**
 * Complete atomic replacement for one document projection in an index.
 *
 * All chunks share the same document, projection, and revision identities.
 */
export interface ProjectedRevision<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Kind extends DocumentKind<Documents>,
  ProjectionId extends DocumentProjectionId<Documents, Kind>,
> {
  readonly target: DocumentReference<GraphId, Documents, Kind>
  readonly encodedTarget: EncodedDocumentReference & {
    readonly graph: GraphId
    readonly kind: Kind
  }
  readonly documentKey: DocumentKey
  readonly projection: {
    readonly id: ProjectionId
    readonly version: ProjectionFor<
      Documents,
      Kind,
      ProjectionId
    >["version"]
  }
  readonly revisionHash: ProjectionRevisionHash
  readonly chunks: readonly [
    ProjectedChunk<GraphId, Documents, Kind, ProjectionId>,
    ...ReadonlyArray<
      ProjectedChunk<GraphId, Documents, Kind, ProjectionId>
    >,
  ]
}

/** A projection returned sections that did not satisfy its declared contract. */
export class InvalidVectorProjectionOutput extends Schema.TaggedError<
  InvalidVectorProjectionOutput
>()("InvalidVectorProjectionOutput", {
  graph: Schema.String,
  documentKind: Schema.String,
  projection: Schema.String,
  reason: Schema.Literal(
    "invalid_shape",
    "empty_sections",
    "missing_metadata",
    "unexpected_metadata",
    "invalid_metadata",
    "duplicate_section_key",
  ),
}) {}

/** A valid projection cannot be represented by its declared chunking policy. */
export class DocumentChunkingFailed extends Schema.TaggedError<
  DocumentChunkingFailed
>()("DocumentChunkingFailed", {
  graph: Schema.String,
  documentKind: Schema.String,
  projection: Schema.String,
  reason: Schema.Literal(
    "invalid_output",
    "chunk_exceeds_maximum",
  ),
}) {}

/** Expected failures produced while projecting and chunking one document. */
export type ProjectDocumentError =
  | InvalidDocumentValue
  | InvalidDocumentIdentity
  | InvalidVectorProjectionOutput
  | DocumentChunkingFailed

const ProjectedSectionOutputSchema = Schema.Struct({
  key: Schema.NonEmptyTrimmedString.pipe(Schema.maxLength(200)),
  label: Schema.optional(
    Schema.NonEmptyTrimmedString.pipe(Schema.maxLength(200)),
  ),
  content: Schema.NonEmptyTrimmedString,
  metadata: Schema.optional(Schema.Unknown),
})

const ProjectedDocumentOutputSchema = Schema.Struct({
  context: Schema.optional(Schema.NonEmptyTrimmedString),
  sections: Schema.Array(ProjectedSectionOutputSchema),
})

const ChunkFragmentOutputSchema = Schema.Struct({
  content: Schema.NonEmptyTrimmedString,
  embeddingContent: Schema.optional(Schema.NonEmptyTrimmedString),
})

const ChunkerOutputSchema = Schema.NonEmptyArray(
  ChunkFragmentOutputSchema,
)

interface ProjectedSectionCandidate {
  readonly key: string
  readonly label?: string | undefined
  readonly content: string
  readonly metadata?: unknown
}

interface ProjectedDocumentCandidate {
  readonly context?: string | undefined
  readonly sections: ReadonlyArray<ProjectedSectionCandidate>
}

interface ParsedProjectedSection {
  readonly key: string
  readonly label?: string | undefined
  readonly content: string
  readonly metadata?: JsonValue
}

interface ParsedProjectedDocument {
  readonly context?: string | undefined
  readonly sections: readonly [
    ParsedProjectedSection,
    ...ReadonlyArray<ParsedProjectedSection>,
  ]
}

interface ExecutableVectorProjection extends VectorProjectionShape {
  readonly select: (value: unknown) => unknown
}

const projectionOutputError = (
  graph: string,
  documentKind: string,
  projection: string,
  reason: InvalidVectorProjectionOutput["reason"],
): InvalidVectorProjectionOutput =>
  new InvalidVectorProjectionOutput({
    graph,
    documentKind,
    projection,
    reason,
  })

const documentChunkingError = (
  graph: string,
  documentKind: string,
  projection: string,
  reason: DocumentChunkingFailed["reason"],
): DocumentChunkingFailed =>
  new DocumentChunkingFailed({
    graph,
    documentKind,
    projection,
    reason,
  })

const parseProjectionMetadata = (
  graph: string,
  documentKind: string,
  projection: ExecutableVectorProjection,
  document: ProjectedDocumentCandidate,
): Effect.Effect<ParsedProjectedDocument, InvalidVectorProjectionOutput> =>
  Effect.forEach(document.sections, (section) => {
    if (projection.metadataSchema === undefined) {
      if (section.metadata !== undefined) {
        return Effect.fail(
          projectionOutputError(
            graph,
            documentKind,
            projection.id,
            "unexpected_metadata",
          ),
        )
      }

      return Effect.succeed({
        key: section.key,
        label: section.label,
        content: section.content,
      })
    }

    if (section.metadata === undefined) {
      return Effect.fail(
        projectionOutputError(
          graph,
          documentKind,
          projection.id,
          "missing_metadata",
        ),
      )
    }

    return Schema.validate(projection.metadataSchema)(section.metadata, {
      onExcessProperty: "error",
    }).pipe(
      Effect.mapError(() =>
        projectionOutputError(
          graph,
          documentKind,
          projection.id,
          "invalid_metadata",
        ),
      ),
      Effect.flatMap((metadata) =>
        Schema.validate(JsonValueSchema)(metadata, {
          onExcessProperty: "error",
        }).pipe(
          Effect.mapError(() =>
            projectionOutputError(
              graph,
              documentKind,
              projection.id,
              "invalid_metadata",
            ),
          ),
        ),
      ),
      Effect.map((metadata) => ({
        key: section.key,
        label: section.label,
        content: section.content,
        metadata,
      })),
    )
  }).pipe(
    Effect.flatMap((sections) => {
      if (!EffectArray.isNonEmptyReadonlyArray(sections)) {
        return Effect.fail(
          projectionOutputError(
            graph,
            documentKind,
            projection.id,
            "empty_sections",
          ),
        )
      }

      const sectionKeys = new Set<string>()
      for (const section of sections) {
        if (sectionKeys.has(section.key)) {
          return Effect.fail(
            projectionOutputError(
              graph,
              documentKind,
              projection.id,
              "duplicate_section_key",
            ),
          )
        }

        sectionKeys.add(section.key)
      }

      return Effect.succeed({
        context: document.context,
        sections,
      })
    }),
  )

interface UnidentifiedChunk {
  readonly target: DocumentReference<string, DocumentDefinitions>
  readonly projection: {
    readonly id: string
    readonly version: string
  }
  readonly ordinal: number
  readonly sectionKey: string
  readonly sectionIndex: number
  readonly sectionPart: number
  readonly content: string
  readonly embeddingContent: string
  readonly text: ProjectedText
  readonly metadata?: JsonValue
}

const chunkProjectedDocument = (
  graph: string,
  documentKind: string,
  target: DocumentReference<string, DocumentDefinitions>,
  projection: ExecutableVectorProjection,
  document: ParsedProjectedDocument,
): Effect.Effect<
  readonly [UnidentifiedChunk, ...ReadonlyArray<UnidentifiedChunk>],
  DocumentChunkingFailed
> => {
  return Effect.gen(function*() {
    const chunks: Array<UnidentifiedChunk> = []

    for (const [sectionIndex, section] of document.sections.entries()) {
      const input: ChunkingInput = {
        context: document.context,
        section: {
          key: section.key,
          label: section.label,
          content: section.content,
        },
      }
      const output = yield* Effect.try({
        try: () => projection.chunking.chunk(input),
        catch: () =>
          documentChunkingError(
            graph,
            documentKind,
            projection.id,
            "invalid_output",
          ),
      })
      const fragments = yield* Schema.validate(ChunkerOutputSchema)(
        output,
        { onExcessProperty: "error" },
      ).pipe(
        Effect.mapError(() =>
          documentChunkingError(
            graph,
            documentKind,
            projection.id,
            "invalid_output",
          ),
        ),
      )

      for (const [sectionPart, fragment] of fragments.entries()) {
        const embeddingContent =
          fragment.embeddingContent ?? fragment.content
        if (
          embeddingContent.length >
          projection.chunking.maximumCharacters
        ) {
          return yield* Effect.fail(
            documentChunkingError(
              graph,
              documentKind,
              projection.id,
              "chunk_exceeds_maximum",
            ),
          )
        }

        const base = {
          target,
          projection: {
            id: projection.id,
            version: projection.version,
          },
          ordinal: chunks.length,
          sectionKey: section.key,
          sectionIndex,
          sectionPart,
          content: fragment.content,
          embeddingContent,
          text: {
            context: document.context,
            label: section.label,
            content: fragment.content,
          },
        }

        chunks.push(
          section.metadata === undefined
            ? base
            : { ...base, metadata: section.metadata },
        )
      }
    }

    if (!EffectArray.isNonEmptyReadonlyArray(chunks)) {
      return yield* Effect.dieMessage(
        "Parsed non-empty sections unexpectedly produced no chunks",
      )
    }

    return chunks
  })
}

interface IdentifiedChunk extends UnidentifiedChunk {
  readonly documentKey: DocumentKey
  readonly revisionHash: ProjectionRevisionHash
  readonly chunkId: ChunkId
  readonly contentHash: ContentHash
}

const identifyChunks = (
  documentKey: DocumentKey,
  projection: ExecutableVectorProjection,
  chunks: readonly [
    UnidentifiedChunk,
    ...ReadonlyArray<UnidentifiedChunk>,
  ],
): readonly [IdentifiedChunk, ...ReadonlyArray<IdentifiedChunk>] => {
  const withChunkIdentity = EffectArray.map(chunks, (chunk) => ({
    ...chunk,
    documentKey,
    chunkId: makeChunkId({
      documentKey,
      projection: projection.id,
      sectionKey: chunk.sectionKey,
      sectionPart: chunk.sectionPart,
    }),
    contentHash: makeContentHash(chunk.embeddingContent),
  }))
  const revisionHash = makeProjectionRevisionHash({
    documentKey,
    projection: {
      id: projection.id,
      version: projection.version,
    },
    chunking: {
      id: projection.chunking.id,
      version: projection.chunking.version,
      config: projection.chunking.encodedConfig,
    },
    search: {
      text: projection.text,
    },
    chunks: EffectArray.map(withChunkIdentity, (chunk) => ({
      chunkId: chunk.chunkId,
      ordinal: chunk.ordinal,
      sectionIndex: chunk.sectionIndex,
      sectionPart: chunk.sectionPart,
      content: chunk.content,
      contentHash: chunk.contentHash,
      text: chunk.text,
      metadata: chunk.metadata,
    })),
  })
  return EffectArray.map(withChunkIdentity, (chunk) => ({
    ...chunk,
    revisionHash,
  }))
}

/** Project and deterministically chunk one already parsed document instance. */
export const projectParsedDocument = <
  const GraphId extends string,
  const Documents extends DocumentDefinitions,
  Kind extends DocumentKind<Documents>,
  ProjectionId extends DocumentProjectionId<Documents, Kind>,
>(
  graph: GraphId,
  documents: Documents,
  documentKind: Kind,
  projectionId: ProjectionId,
  document: ParsedDocumentInstance<
    GraphId,
    Kind,
    Documents[Kind]
  >,
): Effect.Effect<
  ProjectedRevision<GraphId, Documents, Kind, ProjectionId>,
  InvalidVectorProjectionOutput | DocumentChunkingFailed
> => {
  const definition = documents[documentKind]
  if (definition === undefined) {
    return Effect.dieMessage(
      `Unknown document kind ${documentKind} for graph ${graph}`,
    )
  }

  const selectedProjection = definition.projections.find(
    (candidate) => candidate.id === projectionId,
  )

  if (selectedProjection === undefined) {
    return Effect.dieMessage(
      `Unknown projection ${projectionId} for document kind ${documentKind}`,
    )
  }

  // SAFETY: the projection was selected from this document definition after
  // the type system restricted its ID to that definition's projection tuple.
  const projection =
    selectedProjection as unknown as ExecutableVectorProjection

  return Effect.gen(function*() {
    // SAFETY: parseDocumentInstance used this exact document definition and
    // kind before constructing the structurally equivalent public reference.
    const target = document.reference as DocumentReference<
      GraphId,
      Documents,
      Kind
    >
    const output = yield* Effect.sync(() =>
      projection.select(document.value),
    )
    const projectedDocument = yield* Schema.validate(
      ProjectedDocumentOutputSchema,
    )(output, { onExcessProperty: "error" }).pipe(
      Effect.mapError(() =>
        projectionOutputError(
          graph,
          documentKind,
          projection.id,
          "invalid_shape",
        ),
      ),
    )
    const parsedDocument = yield* parseProjectionMetadata(
      graph,
      documentKind,
      projection,
      projectedDocument,
    )
    const chunks = yield* chunkProjectedDocument(
      graph,
      documentKind,
      target,
      projection,
      parsedDocument,
    )
    const identifiedChunks = identifyChunks(
      document.documentKey,
      projection,
      chunks,
    )

    // SAFETY: the selected projection fixes the public metadata and version
    // types. The document ID, metadata, chunks, and hashes were parsed or
    // constructed above before this precise public type is restored.
    return {
      target,
      encodedTarget: document.encodedReference,
      documentKey: document.documentKey,
      projection: identifiedChunks[0].projection,
      revisionHash: identifiedChunks[0].revisionHash,
      chunks: identifiedChunks,
    } as ProjectedRevision<GraphId, Documents, Kind, ProjectionId>
  })
}

/**
 * Validate, project, and deterministically chunk one schema-defined document.
 *
 * Selector exceptions are defects; schema, metadata, and chunk-policy
 * rejections remain typed expected failures.
 */
export const projectDocument = <
  const GraphId extends string,
  const Documents extends DocumentDefinitions,
  Kind extends DocumentKind<Documents>,
  ProjectionId extends DocumentProjectionId<Documents, Kind>,
>(
  graph: GraphId,
  documents: Documents,
  documentKind: Kind,
  projectionId: ProjectionId,
  value: DocumentValue<Documents, Kind>,
): Effect.Effect<
  ProjectedRevision<GraphId, Documents, Kind, ProjectionId>,
  ProjectDocumentError
> => {
  const definition = documents[documentKind]
  if (definition === undefined) {
    return Effect.dieMessage(
      `Unknown document kind ${documentKind} for graph ${graph}`,
    )
  }

  return parseDocumentInstance({
    graph,
    documentKind,
    definition,
    value,
  }).pipe(
    Effect.flatMap((document) =>
      projectParsedDocument(
        graph,
        documents,
        documentKind,
        projectionId,
        document,
      ),
    ),
  )
}
