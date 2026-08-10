import { describe, expect, test } from "bun:test"
import { Effect, Either, Schema } from "effect"
import {
  ChunkMaximumCharactersSchema,
  DocumentChunkingFailed,
  defineChunker,
  defineDocument,
  defineDocumentGraph,
  InvalidDocumentGraphDefinition,
  InvalidDocumentReference,
  InvalidDocumentValue,
  InvalidVectorProjectionOutput,
  sectionChunking,
} from "../src/adapter.js"

const AgencyId = Schema.UUID.pipe(Schema.brand("AgencyId"))
const WorkId = Schema.UUID.pipe(Schema.brand("WorkId"))

const AgencySchema = Schema.Struct({
  id: AgencyId,
  name: Schema.NonEmptyTrimmedString,
  summary: Schema.NonEmptyTrimmedString,
})

const WorkSchema = Schema.Struct({
  id: WorkId,
  title: Schema.NonEmptyTrimmedString,
  evidence: Schema.NonEmptyArray(
    Schema.Struct({
      id: Schema.NonEmptyTrimmedString,
      kind: Schema.Literal("challenge", "outcome"),
      text: Schema.NonEmptyTrimmedString,
    }),
  ),
})

const WorkMetadata = Schema.Struct({
  kind: Schema.Literal("challenge", "outcome"),
})

const ParagraphChunkingConfig = Schema.Struct({
  maximumCharacters: ChunkMaximumCharactersSchema,
  separator: Schema.NonEmptyTrimmedString,
})

const paragraphChunking = defineChunker({
  id: "paragraph",
  version: "v1",
  config: ParagraphChunkingConfig,
  maximumCharacters: (config) => config.maximumCharacters,
  chunk: ({ context, section, config }) =>
    section.content.split(config.separator).map((paragraph) => ({
      content: paragraph.trim(),
      embeddingContent: [context, section.label, paragraph.trim()]
        .flatMap((part) =>
          part === undefined || part.length === 0 ? [] : [part],
        )
        .join("\n\n"),
    })),
})

const AgencyDocument = defineDocument({
  id: AgencyId,
  value: AgencySchema,
  identify: (agency) => agency.id,
})

const WorkDocument = defineDocument({
  id: WorkId,
  value: WorkSchema,
  identify: (work) => work.id,
}).vectorise({
  id: "work-evidence",
  version: "v1",
  metadata: WorkMetadata,
  select: (work) => ({
    context: work.title,
    sections: work.evidence.map((evidence) => ({
      key: evidence.id,
      label: evidence.kind,
      content: evidence.text,
      metadata: { kind: evidence.kind },
    })),
  }),
  chunking: sectionChunking({ maximumCharacters: 128 }),
})

const graph = defineDocumentGraph({
  id: "catalog",
  documents: {
    Agency: AgencyDocument,
    Work: WorkDocument,
  },
})
const AgencyNode = graph.document("Agency")
const WorkNode = graph.document("Work")
const WorkEvidence = WorkNode.projection("work-evidence")

const agencyId = Schema.decodeSync(AgencyId)(
  "11111111-1111-4111-8111-111111111111",
)
const workId = Schema.decodeSync(WorkId)(
  "22222222-2222-4222-8222-222222222222",
)

describe("defineDocumentGraph", () => {
  test("rejects duplicate projection IDs while compiling the graph", () => {
    const duplicateId: string = "work-evidence"
    const duplicateDocument = WorkDocument.vectorise({
      id: duplicateId,
      version: "v2",
      metadata: WorkMetadata,
      select: (work) => ({
        sections: work.evidence.map((item) => ({
          key: item.id,
          content: item.text,
          metadata: { kind: item.kind },
        })),
      }),
      chunking: sectionChunking({ maximumCharacters: 128 }),
    })

    expect(() =>
      defineDocumentGraph({
        id: "duplicate-catalog",
        documents: { Work: duplicateDocument },
      }),
    ).toThrow(
      new InvalidDocumentGraphDefinition({
        reason: "duplicate_projection",
        documentKind: "Work",
        projection: "work-evidence",
      }),
    )
  })

  test("constructs typed references through document handles", () => {
    expect(AgencyNode.ref(agencyId)).toEqual({
      graph: "catalog",
      kind: "Agency",
      id: agencyId,
    })
    expect(WorkNode.ref(workId)).toEqual({
      graph: "catalog",
      kind: "Work",
      id: workId,
    })
  })

  test("parses unknown references through the selected identity schema", async () => {
    const result = await Effect.runPromise(
      graph.parseReference({
        graph: "catalog",
        kind: "Work",
        id: workId,
      }),
    )

    expect(result).toEqual({
      graph: "catalog",
      kind: "Work",
      id: workId,
    })
  })

  test("rejects references for another graph", async () => {
    const result = await Effect.runPromise(
      graph.parseReference({
        graph: "another-graph",
        kind: "Work",
        id: workId,
      }).pipe(Effect.either),
    )

    expect(result).toEqual(
      Either.left(
        new InvalidDocumentReference({ reason: "wrong_graph" }),
      ),
    )
  })

  test("retains active vectorisation policy in a stable manifest", () => {
    expect(graph.manifest).toEqual({
      id: "catalog",
      documents: [
        { kind: "Agency", projections: [] },
        {
          kind: "Work",
          projections: [
            {
              id: "work-evidence",
              version: "v1",
              chunking: {
                id: "section",
                version: "v1",
                config: { maximumCharacters: 128 },
              },
              text: {
                language: "english",
                weights: { context: 2, label: 3, content: 1 },
              },
            },
          ],
        },
      ],
      relations: [],
    })
  })

  test("keeps projection input and metadata inferred from document schemas", async () => {
    const work = Schema.decodeSync(WorkSchema)({
      id: workId,
      title: "Challenger launch",
      evidence: [
        {
          id: "outcome",
          kind: "outcome",
          text: "Reached national retail distribution.",
        },
      ],
    })
    const revision = await Effect.runPromise(WorkEvidence.project(work))

    expect(revision.chunks[0]).toMatchObject({
      sectionKey: "outcome",
      content: "Reached national retail distribution.",
      text: {
        context: "Challenger launch",
        label: "outcome",
        content: "Reached national retail distribution.",
      },
      metadata: { kind: "outcome" },
    })
  })

  test("rejects invalid text policy at graph definition", () => {
    expect(() =>
      defineDocument(WorkSchema, { id: "id" }).vectorise({
        id: "invalid-text-policy",
        version: "v1",
        text: {
          weights: { context: 0, label: 0, content: 0 },
        },
        select: (work) => ({
          sections: [{ key: "body", content: work.title }],
        }),
      }),
    ).toThrow()

  })

  test("projects deterministic chunks without crossing authored sections", async () => {
    const value = Schema.decodeSync(WorkSchema)({
      id: workId,
      title: "Challenger launch",
      evidence: [
        {
          id: "challenge",
          kind: "challenge",
          text: "Legacy distribution made national growth difficult. "
            .repeat(5)
            .trim(),
        },
        {
          id: "outcome",
          kind: "outcome",
          text: "Reached national retail distribution. ".repeat(5).trim(),
        },
      ],
    })
    const target = WorkNode.ref(workId)

    const firstRevision = await Effect.runPromise(
      WorkEvidence.project(value),
    )
    const secondRevision = await Effect.runPromise(
      WorkEvidence.project(value),
    )
    const documentKey = await Effect.runPromise(WorkNode.key(workId))
    const first = firstRevision.chunks

    expect(firstRevision).toEqual(secondRevision)
    expect(firstRevision.documentKey).toBe(documentKey)
    expect(firstRevision.encodedTarget).toEqual({
      graph: "catalog",
      kind: "Work",
      id: workId,
    })
    expect(first.every((chunk) => chunk.documentKey === documentKey)).toBe(
      true,
    )
    expect(first.every((chunk) => chunk.revisionHash === first[0].revisionHash))
      .toBe(true)
    expect(new Set(first.map((chunk) => chunk.chunkId)).size).toBe(
      first.length,
    )
    expect(first.length).toBeGreaterThan(2)
    expect(
      first.every((chunk) => chunk.content.length <= 128),
    ).toBe(true)
    expect(first.map((chunk) => chunk.ordinal)).toEqual(
      first.map((_, index) => index),
    )
    expect(
      first
        .filter((chunk) => chunk.sectionKey === "challenge")
        .every(
          (chunk) =>
            chunk.sectionIndex === 0 &&
            chunk.metadata.kind === "challenge",
        ),
    ).toBe(true)
    expect(
      first
        .filter((chunk) => chunk.sectionKey === "outcome")
        .every(
          (chunk) =>
            chunk.sectionIndex === 1 &&
            chunk.metadata.kind === "outcome",
        ),
    ).toBe(true)
  })

  test("executes a schema-configured custom chunker through the graph", async () => {
    const ParagraphWorkDocument = defineDocument({
      id: WorkId,
      value: WorkSchema,
      identify: (work) => work.id,
    }).vectorise({
      id: "work-paragraphs",
      version: "v1",
      metadata: WorkMetadata,
      select: (work) => ({
        context: work.title,
        sections: [
          {
            key: work.evidence[0].id,
            label: work.evidence[0].kind,
            content: work.evidence[0].text,
            metadata: { kind: work.evidence[0].kind },
          },
        ],
      }),
      chunking: paragraphChunking({
        maximumCharacters: 128,
        separator: "||",
      }),
    })
    const paragraphGraph = defineDocumentGraph({
      id: "paragraph-catalog",
      documents: { Work: ParagraphWorkDocument },
    })
    const ParagraphWork = paragraphGraph.document("Work")
    const WorkParagraphs = ParagraphWork.projection("work-paragraphs")
    const value = Schema.decodeSync(WorkSchema)({
      id: workId,
      title: "Challenger launch",
      evidence: [
        {
          id: "outcome",
          kind: "outcome",
          text: "First paragraph||Second paragraph",
        },
      ],
    })
    const target = ParagraphWork.ref(workId)

    const revision = await Effect.runPromise(
      WorkParagraphs.project(value),
    )
    const chunks = revision.chunks

    const storageIdentity = chunks.map(
      ({ documentKey, revisionHash, chunkId, contentHash, ...chunk }) =>
        chunk,
    )

    expect(storageIdentity).toEqual([
      {
        target,
        projection: { id: "work-paragraphs", version: "v1" },
        ordinal: 0,
        sectionKey: "outcome",
        sectionIndex: 0,
        sectionPart: 0,
        content: "First paragraph",
        embeddingContent:
          "Challenger launch\n\noutcome\n\nFirst paragraph",
        text: {
          context: "Challenger launch",
          label: "outcome",
          content: "First paragraph",
        },
        metadata: { kind: "outcome" },
      },
      {
        target,
        projection: { id: "work-paragraphs", version: "v1" },
        ordinal: 1,
        sectionKey: "outcome",
        sectionIndex: 0,
        sectionPart: 1,
        content: "Second paragraph",
        embeddingContent:
          "Challenger launch\n\noutcome\n\nSecond paragraph",
        text: {
          context: "Challenger launch",
          label: "outcome",
          content: "Second paragraph",
        },
        metadata: { kind: "outcome" },
      },
    ])
    expect(new Set(chunks.map((chunk) => chunk.documentKey)).size).toBe(1)
    expect(new Set(chunks.map((chunk) => chunk.revisionHash)).size).toBe(1)
    expect(new Set(chunks.map((chunk) => chunk.chunkId)).size).toBe(2)
    expect(new Set(chunks.map((chunk) => chunk.contentHash)).size).toBe(2)
    expect(paragraphGraph.manifest.documents[0]?.projections[0]).toEqual({
      id: "work-paragraphs",
      version: "v1",
      chunking: {
        id: "paragraph",
        version: "v1",
        config: {
          maximumCharacters: 128,
          separator: "||",
        },
      },
      text: {
        language: "english",
        weights: { context: 2, label: 3, content: 1 },
      },
    })
  })

  test("parses custom chunker configuration when it is configured", () => {
    expect(() =>
      paragraphChunking({
        maximumCharacters: 127,
        separator: "||",
      }),
    ).toThrow()
  })

  test("keeps logical identities stable while content revisions change", async () => {
    const target = WorkNode.ref(workId)
    const makeValue = (text: string) =>
      Schema.decodeSync(WorkSchema)({
        id: workId,
        title: "Challenger launch",
        evidence: [{ id: "outcome", kind: "outcome", text }],
      })

    const beforeRevision = await Effect.runPromise(
      WorkEvidence.project(
        makeValue("Reached national retail distribution."),
      ),
    )
    const afterRevision = await Effect.runPromise(
      WorkEvidence.project(
        makeValue("Reached international retail distribution."),
      ),
    )
    const before = beforeRevision.chunks[0]
    const after = afterRevision.chunks[0]

    expect(before.documentKey).toBe(after.documentKey)
    expect(before.documentKey).toBe(
      await Effect.runPromise(WorkNode.key(target.id)),
    )
    expect(before.chunkId).toBe(after.chunkId)
    expect(before.contentHash).not.toBe(after.contentHash)
    expect(before.revisionHash).not.toBe(after.revisionHash)
  })

  test("separates focal content from contextual embedding content", async () => {
    const makeValue = (title: string) =>
      Schema.decodeSync(WorkSchema)({
        id: workId,
        title,
        evidence: [
          {
            id: "outcome",
            kind: "outcome",
            text: "Reached national retail distribution.",
          },
        ],
      })

    const before = await Effect.runPromise(
      WorkEvidence.project(makeValue("Challenger launch")),
    )
    const after = await Effect.runPromise(
      WorkEvidence.project(makeValue("National launch")),
    )
    const beforeChunk = before.chunks[0]
    const afterChunk = after.chunks[0]

    expect(beforeChunk.content).toBe(afterChunk.content)
    expect(beforeChunk.embeddingContent).not.toBe(
      afterChunk.embeddingContent,
    )
    expect(beforeChunk.chunkId).toBe(afterChunk.chunkId)
    expect(beforeChunk.contentHash).not.toBe(afterChunk.contentHash)
    expect(before.revisionHash).not.toBe(after.revisionHash)
  })

  test("revises metadata without invalidating reusable embedding content", async () => {
    const MetadataValue = Schema.Struct({
      id: WorkId,
      text: Schema.NonEmptyTrimmedString,
      reviewStatus: Schema.Literal("source", "reviewed"),
    })
    const Metadata = Schema.Struct({
      reviewStatus: Schema.Literal("source", "reviewed"),
    })
    const MetadataDocument = defineDocument({
      id: WorkId,
      value: MetadataValue,
      identify: (value) => value.id,
    }).vectorise({
      id: "metadata-revision",
      version: "v1",
      metadata: Metadata,
      select: (value) => ({
        sections: [
          {
            key: "body",
            content: value.text,
            metadata: { reviewStatus: value.reviewStatus },
          },
        ],
      }),
      chunking: sectionChunking({ maximumCharacters: 128 }),
    })
    const metadataGraph = defineDocumentGraph({
      id: "metadata-revision-test",
      documents: { Work: MetadataDocument },
    })
    const MetadataProjection = metadataGraph
      .document("Work")
      .projection("metadata-revision")
    const makeValue = (reviewStatus: "source" | "reviewed") =>
      Schema.decodeSync(MetadataValue)({
        id: workId,
        text: "The embedding text is unchanged.",
        reviewStatus,
      })

    const beforeRevision = await Effect.runPromise(
      MetadataProjection.project(makeValue("source")),
    )
    const afterRevision = await Effect.runPromise(
      MetadataProjection.project(makeValue("reviewed")),
    )
    const before = beforeRevision.chunks[0]
    const after = afterRevision.chunks[0]

    expect(before.documentKey).toBe(after.documentKey)
    expect(before.chunkId).toBe(after.chunkId)
    expect(before.contentHash).toBe(after.contentHash)
    expect(before.metadata).toEqual({ reviewStatus: "source" })
    expect(after.metadata).toEqual({ reviewStatus: "reviewed" })
    expect(before.revisionHash).not.toBe(after.revisionHash)
  })

  test("includes projected text and search policy in revision identity", async () => {
    const SearchValue = Schema.Struct({
      id: WorkId,
      title: Schema.NonEmptyTrimmedString,
      label: Schema.NonEmptyTrimmedString,
      text: Schema.NonEmptyTrimmedString,
    })
    const focalContentChunking = defineChunker({
      id: "focal-content",
      version: "v1",
      config: Schema.Struct({
        maximumCharacters: ChunkMaximumCharactersSchema,
      }),
      maximumCharacters: (config) => config.maximumCharacters,
      chunk: ({ section }) => [{
        content: section.content,
        embeddingContent: section.content,
      }],
    })({ maximumCharacters: 128 })
    const makeSearchGraph = (contextWeight: number) => {
      const SearchDocument = defineDocument(SearchValue, {
        id: "id",
      }).vectorise({
        id: "searchable-content",
        version: "v1",
        text: {
          weights: { context: contextWeight, label: 3, content: 1 },
        },
        select: (value) => ({
          context: value.title,
          sections: [
            {
              key: "body",
              label: value.label,
              content: value.text,
            },
          ],
        }),
        chunking: focalContentChunking,
      })

      return defineDocumentGraph({
        id: "search-policy-identity",
        documents: { Work: SearchDocument },
      })
    }
    const firstGraph = makeSearchGraph(2)
    const secondGraph = makeSearchGraph(5)
    const firstProjection = firstGraph
      .document("Work")
      .projection("searchable-content")
    const secondProjection = secondGraph
      .document("Work")
      .projection("searchable-content")
    const makeValue = (label: string) =>
      Schema.decodeSync(SearchValue)({
        id: workId,
        title: "Challenger launch",
        label,
        text: "Reached national retail distribution.",
      })

    const before = await Effect.runPromise(
      firstProjection.project(makeValue("Outcome")),
    )
    const relabelled = await Effect.runPromise(
      firstProjection.project(makeValue("Result")),
    )
    const reweighted = await Effect.runPromise(
      secondProjection.project(makeValue("Outcome")),
    )
    const beforeChunk = before.chunks[0]
    const relabelledChunk = relabelled.chunks[0]
    const reweightedChunk = reweighted.chunks[0]

    expect(beforeChunk.text).toEqual({
      context: "Challenger launch",
      label: "Outcome",
      content: "Reached national retail distribution.",
    })
    expect(relabelledChunk.text.label).toBe("Result")
    expect(beforeChunk.chunkId).toBe(relabelledChunk.chunkId)
    expect(beforeChunk.contentHash).toBe(relabelledChunk.contentHash)
    expect(before.revisionHash).not.toBe(relabelled.revisionHash)
    expect(beforeChunk.chunkId).toBe(reweightedChunk.chunkId)
    expect(beforeChunk.contentHash).toBe(reweightedChunk.contentHash)
    expect(beforeChunk.text).toEqual(reweightedChunk.text)
    expect(before.revisionHash).not.toBe(reweighted.revisionHash)
  })

  test("hashes the exact embedding text with SHA-256", async () => {
    const HashId = Schema.UUID.pipe(Schema.brand("HashId"))
    const HashValue = Schema.Struct({ id: HashId, text: Schema.String })
    const HashDocument = defineDocument({
      id: HashId,
      value: HashValue,
      identify: (value) => value.id,
    }).vectorise({
      id: "body",
      version: "v1",
      select: (value) => ({
        sections: [{ key: "body", content: value.text }],
      }),
      chunking: sectionChunking({ maximumCharacters: 128 }),
    })
    const hashGraph = defineDocumentGraph({
      id: "hash-test",
      documents: { Hash: HashDocument },
    })
    const HashBody = hashGraph.document("Hash").projection("body")
    const id = Schema.decodeSync(HashId)(
      "33333333-3333-4333-8333-333333333333",
    )
    const value = Schema.decodeSync(HashValue)({ id, text: "abc" })

    const revision = await Effect.runPromise(
      HashBody.project(value),
    )
    const chunk = revision.chunks[0]

    expect(chunk.content).toBe("abc")
    expect(String(chunk.contentHash)).toBe(
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    )
  })

  test("includes chunking configuration in the revision hash", async () => {
    const makeDocument = (maximumCharacters: number) =>
      defineDocument({
        id: WorkId,
        value: WorkSchema,
        identify: (work) => work.id,
      }).vectorise({
        id: "work-evidence",
        version: "v1",
        select: (work) => ({
          sections: [{ key: "title", content: work.title }],
        }),
        chunking: sectionChunking({ maximumCharacters }),
      })
    const makeGraph = (maximumCharacters: number) =>
      defineDocumentGraph({
        id: "configuration-test",
        documents: { Work: makeDocument(maximumCharacters) },
      })
    const value = Schema.decodeSync(WorkSchema)({
      id: workId,
      title: "Challenger launch",
      evidence: [
        { id: "outcome", kind: "outcome", text: "An outcome." },
      ],
    })
    const firstGraph = makeGraph(128)
    const secondGraph = makeGraph(256)
    const FirstEvidence = firstGraph
      .document("Work")
      .projection("work-evidence")
    const SecondEvidence = secondGraph
      .document("Work")
      .projection("work-evidence")
    const firstRevision = await Effect.runPromise(
      FirstEvidence.project(value),
    )
    const secondRevision = await Effect.runPromise(
      SecondEvidence.project(value),
    )
    const first = firstRevision.chunks[0]
    const second = secondRevision.chunks[0]

    expect(first.documentKey).toBe(second.documentKey)
    expect(first.chunkId).toBe(second.chunkId)
    expect(first.contentHash).toBe(second.contentHash)
    expect(first.revisionHash).not.toBe(second.revisionHash)
  })

  test("rejects duplicate section keys before deriving chunk identities", async () => {
    const DuplicateSectionDocument = defineDocument({
      id: WorkId,
      value: WorkSchema,
      identify: (work) => work.id,
    }).vectorise({
      id: "duplicate-sections",
      version: "v1",
      select: (work) => ({
        sections: [
          { key: "body", content: work.title },
          { key: "body", content: work.evidence[0].text },
        ],
      }),
      chunking: sectionChunking({ maximumCharacters: 128 }),
    })
    const duplicateGraph = defineDocumentGraph({
      id: "duplicate-test",
      documents: { Work: DuplicateSectionDocument },
    })
    const value = Schema.decodeSync(WorkSchema)({
      id: workId,
      title: "Challenger launch",
      evidence: [
        { id: "outcome", kind: "outcome", text: "An outcome." },
      ],
    })

    const result = await Effect.runPromise(
      duplicateGraph
        .document("Work")
        .projection("duplicate-sections")
        .project(value)
        .pipe(Effect.either),
    )

    expect(result).toEqual(
      Either.left(
        new InvalidVectorProjectionOutput({
          graph: "duplicate-test",
          documentKind: "Work",
          projection: "duplicate-sections",
          reason: "duplicate_section_key",
        }),
      ),
    )
  })

  test("rejects chunker configuration that cannot enter the manifest", () => {
    const nonSerializableChunking = defineChunker({
      id: "non-serializable",
      version: "v1",
      config: Schema.Struct({
        maximumCharacters: ChunkMaximumCharactersSchema,
        value: Schema.Unknown,
      }),
      maximumCharacters: (config) => config.maximumCharacters,
      chunk: () => [{ content: "Chunk" }],
    })

    expect(() =>
      nonSerializableChunking({
        maximumCharacters: 128,
        value: () => "not JSON",
      }),
    ).toThrow()
  })

  test("invokes a custom chunker independently for each projected section", async () => {
    const sectionScopedChunking = defineChunker({
      id: "section-scoped",
      version: "v1",
      config: Schema.Struct({
        maximumCharacters: ChunkMaximumCharactersSchema,
      }),
      maximumCharacters: (config) => config.maximumCharacters,
      chunk: ({ context, section }) => [{
        content: section.content,
        embeddingContent: [context, section.key, section.content].join(" | "),
      }],
    })
    const SectionScopedWorkDocument = defineDocument({
      id: WorkId,
      value: WorkSchema,
      identify: (work) => work.id,
    }).vectorise({
      id: "section-scoped",
      version: "v1",
      select: (work) => ({
        context: work.title,
        sections: work.evidence.map((evidence) => ({
          key: evidence.id,
          label: evidence.kind,
          content: evidence.text,
        })),
      }),
      chunking: sectionScopedChunking({ maximumCharacters: 128 }),
    })
    const sectionScopedGraph = defineDocumentGraph({
      id: "section-scoped-catalog",
      documents: { Work: SectionScopedWorkDocument },
    })
    const value = Schema.decodeSync(WorkSchema)({
      id: workId,
      title: "Challenger launch",
      evidence: [
        {
          id: "challenge",
          kind: "challenge",
          text: "Needed stronger retail awareness.",
        },
        {
          id: "outcome",
          kind: "outcome",
          text: "Reached national retail distribution.",
        },
      ],
    })

    const revision = await Effect.runPromise(
      sectionScopedGraph
        .document("Work")
        .projection("section-scoped")
        .project(value),
    )

    expect(revision.chunks.map((chunk) => ({
      sectionKey: chunk.sectionKey,
      sectionIndex: chunk.sectionIndex,
      sectionPart: chunk.sectionPart,
      embeddingContent: chunk.embeddingContent,
    }))).toEqual([
      {
        sectionKey: "challenge",
        sectionIndex: 0,
        sectionPart: 0,
        embeddingContent:
          "Challenger launch | challenge | Needed stronger retail awareness.",
      },
      {
        sectionKey: "outcome",
        sectionIndex: 1,
        sectionPart: 0,
        embeddingContent:
          "Challenger launch | outcome | Reached national retail distribution.",
      },
    ])
  })

  test("returns a typed failure when custom chunker code throws", async () => {
    const throwingChunking = defineChunker({
      id: "throwing",
      version: "v1",
      config: Schema.Struct({
        maximumCharacters: ChunkMaximumCharactersSchema,
      }),
      maximumCharacters: (config) => config.maximumCharacters,
      chunk: () => {
        throw new Error("custom implementation failed")
      },
    })
    const ThrowingWorkDocument = defineDocument({
      id: WorkId,
      value: WorkSchema,
      identify: (work) => work.id,
    }).vectorise({
      id: "throwing",
      version: "v1",
      select: (work) => ({
        sections: [{ key: "title", content: work.title }],
      }),
      chunking: throwingChunking({ maximumCharacters: 128 }),
    })
    const throwingGraph = defineDocumentGraph({
      id: "throwing-catalog",
      documents: { Work: ThrowingWorkDocument },
    })
    const value = Schema.decodeSync(WorkSchema)({
      id: workId,
      title: "Challenger launch",
      evidence: [{
        id: "outcome",
        kind: "outcome",
        text: "Reached national retail distribution.",
      }],
    })

    const result = await Effect.runPromise(
      throwingGraph
        .document("Work")
        .projection("throwing")
        .project(value)
        .pipe(Effect.either),
    )

    expect(result).toEqual(
      Either.left(
        new DocumentChunkingFailed({
          graph: "throwing-catalog",
          documentKind: "Work",
          projection: "throwing",
          reason: "invalid_output",
        }),
      ),
    )
  })

  test("rejects a document value before invoking its projection", async () => {
    const result = await Effect.runPromise(
      WorkEvidence
        .project({
          id: workId,
          title: "",
          evidence: [
            {
              id: "outcome",
              kind: "outcome",
              text: "Reached national retail distribution.",
            },
          ],
        })
        .pipe(Effect.either),
    )

    expect(result).toEqual(
      Either.left(
        new InvalidDocumentValue({
          graph: "catalog",
          documentKind: "Work",
          reason: "schema_rejected",
        }),
      ),
    )
  })

  test("rejects projection metadata that violates its schema", async () => {
    const StrictMetadata = Schema.Struct({
      code: Schema.String.pipe(Schema.maxLength(3)),
    })
    const StrictWorkDocument = defineDocument({
      id: WorkId,
      value: WorkSchema,
      identify: (work) => work.id,
    }).vectorise({
      id: "strict-metadata",
      version: "v1",
      metadata: StrictMetadata,
      select: (work) => ({
        sections: [
          {
            key: "title",
            content: work.title,
            metadata: { code: work.title },
          },
        ],
      }),
      chunking: sectionChunking({ maximumCharacters: 128 }),
    })
    const strictGraph = defineDocumentGraph({
      id: "strict-catalog",
      documents: { Work: StrictWorkDocument },
    })
    const value = Schema.decodeSync(WorkSchema)({
      id: workId,
      title: "Too long",
      evidence: [
        {
          id: "outcome",
          kind: "outcome",
          text: "Reached national retail distribution.",
        },
      ],
    })

    const result = await Effect.runPromise(
      strictGraph
        .document("Work")
        .projection("strict-metadata")
        .project(value)
        .pipe(Effect.either),
    )

    expect(result).toEqual(
      Either.left(
        new InvalidVectorProjectionOutput({
          graph: "strict-catalog",
          documentKind: "Work",
          projection: "strict-metadata",
          reason: "invalid_metadata",
        }),
      ),
    )
  })

  test("returns a typed failure when the repeated prefix consumes the chunk", async () => {
    const value = Schema.decodeSync(WorkSchema)({
      id: workId,
      title: "x".repeat(120),
      evidence: [
        {
          id: "outcome",
          kind: "outcome",
          text: "Reached national retail distribution.",
        },
      ],
    })

    const result = await Effect.runPromise(
      WorkEvidence
        .project(value)
        .pipe(Effect.either),
    )

    expect(result).toEqual(
      Either.left(
        new DocumentChunkingFailed({
          graph: "catalog",
          documentKind: "Work",
          projection: "work-evidence",
          reason: "chunk_exceeds_maximum",
        }),
      ),
    )
  })
})

const workProjectionVersion: "v1" = WorkEvidence.version

const agencyValue = Schema.decodeSync(AgencySchema)({
  id: agencyId,
  name: "Example agency",
  summary: "An example agency profile.",
})

if (false) {
  // @ts-expect-error A Work reference requires a WorkId.
  WorkNode.ref(agencyId)

  // @ts-expect-error Agency does not register the work-evidence projection.
  AgencyNode.projection("work-evidence")

  // @ts-expect-error The Work projection accepts the Work document value.
  WorkEvidence.project(agencyValue)

  // @ts-expect-error Custom chunker configuration requires a separator.
  paragraphChunking({ maximumCharacters: 128 })

  defineChunker({
    id: "invalid-whole-document-chunker",
    version: "v1",
    config: Schema.Struct({
      maximumCharacters: ChunkMaximumCharactersSchema,
    }),
    maximumCharacters: (config) => config.maximumCharacters,
    // @ts-expect-error Custom chunkers receive one section, not the document.
    chunk: ({ document }) => [{ content: document.sections[0].content }],
  })

  WorkDocument.vectorise({
    // @ts-expect-error Projection IDs must be unique within a document.
    id: "work-evidence",
    version: "v2",
    metadata: WorkMetadata,
    select: (work) => ({
      sections: [{
        key: work.evidence[0].id,
        content: work.evidence[0].text,
        metadata: { kind: work.evidence[0].kind },
      }],
    }),
    chunking: sectionChunking({ maximumCharacters: 128 }),
  })
}

void workProjectionVersion
