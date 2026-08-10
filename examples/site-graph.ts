import { Schema } from "effect"
import {
  ChunkMaximumCharactersSchema,
  defineChunker,
  defineDocument,
  defineDocumentGraph,
  hydrateGrounding,
  type SearchHit,
} from "@popcomputer/document-graph"

const AgencyId = Schema.UUID.pipe(Schema.brand("AgencyId"))
const WorkId = Schema.UUID.pipe(Schema.brand("WorkId"))

const Agency = Schema.Struct({
  id: AgencyId,
  name: Schema.NonEmptyTrimmedString,
  summary: Schema.NonEmptyTrimmedString,
})

const Evidence = Schema.Struct({
  id: Schema.NonEmptyTrimmedString,
  kind: Schema.Literal("challenge", "approach", "outcome"),
  text: Schema.NonEmptyTrimmedString,
})

/** Work input accepted by the example's indexing boundary. */
export const WorkSchema = Schema.Struct({
  id: WorkId,
  title: Schema.NonEmptyTrimmedString,
  agencyIds: Schema.Array(AgencyId),
  evidence: Schema.NonEmptyArray(Evidence),
})

const EvidenceMetadata = Schema.Struct({
  kind: Evidence.fields.kind,
})

const fixedWindowChunking = defineChunker({
  id: "fixed-window",
  version: "v1",
  config: Schema.Struct({
    maximumCharacters: ChunkMaximumCharactersSchema,
  }),
  maximumCharacters: (config) => config.maximumCharacters,
  chunk: ({ section, config }) => {
    const fragments = []

    for (
      let offset = 0;
      offset < section.content.length;
      offset += config.maximumCharacters
    ) {
      const content = section.content
        .slice(offset, offset + config.maximumCharacters)
        .trim()

      if (content.length > 0) {
        fragments.push({ content })
      }
    }

    return fragments
  },
})

const AgencyDocument = defineDocument(Agency, { id: "id" }).vectorise({
  id: "agency-profile",
  version: "v1",
  text: {
    weights: { context: 4, label: 2, content: 1 },
  },
  select: (agency) => ({
    context: agency.name,
    sections: [
      {
        key: "profile",
        label: "Agency profile",
        content: agency.summary,
      },
    ],
  }),
  chunking: fixedWindowChunking({ maximumCharacters: 1_800 }),
})

const WorkDocument = defineDocument(WorkSchema, { id: "id" }).vectorise({
  id: "work-evidence",
  version: "v1",
  metadata: EvidenceMetadata,
  select: (work) => ({
    context: work.title,
    sections: work.evidence.map((evidence) => ({
      key: evidence.id,
      label: evidence.kind,
      content: evidence.text,
      metadata: { kind: evidence.kind },
    })),
  }),
})

/** The complete typed document graph used by the package examples. */
export const SiteGraph = defineDocumentGraph({
  id: "site-content",
  documents: {
    Agency: AgencyDocument,
    Work: WorkDocument,
  },
  relations: (relation) => ({
    deliveredBy: relation({
      from: "Work",
      to: "Agency",
      version: "v1",
      select: (work) => work.agencyIds,
    }),
  }),
})

/** Operations bound to Work documents in the example graph. */
export const WorkNode = SiteGraph.document("Work")

/** Operations bound to Agency documents in the example graph. */
export const AgencyNode = SiteGraph.document("Agency")

/** Search and indexing operations for attributed Work evidence. */
export const WorkEvidence = WorkNode.projection("work-evidence")

/** Search operations for Agency profile text. */
export const AgencyProfile = AgencyNode.projection("agency-profile")

/**
 * Rank Agency nodes using their profiles and evidence from related Work.
 *
 * This named retrieval policy belongs to the application. The package infers
 * its legal routes and result types from SiteGraph.
 */
export const FindAgencies = SiteGraph.retrieval({
  target: "Agency",
  routes: [
    AgencyProfile,
    WorkEvidence.through("deliveredBy", {
      weight: 2,
      neighboursPerSource: 5,
    }),
  ],
  maximumEvidencePerTarget: 3,
})

/** Turn one parsed Work value into typed, embedding-ready chunks. */
export const prepareWork = (
  value: Schema.Schema.Type<typeof WorkSchema>,
) =>
  WorkEvidence.project(value)

/** Delta-index every Work projection using provided Effect Layers. */
export const indexWork = (
  value: Schema.Schema.Type<typeof WorkSchema>,
) =>
  WorkNode.index(value)

/** Search Work evidence through provided embedding and search-store Layers. */
export const searchWork = (query: string) =>
  WorkEvidence.search(query, {
    where: { kind: "outcome" },
    limit: 10,
  })

/** Search the same evidence lexically without requiring query embeddings. */
export const searchWorkByText = (query: string) =>
  WorkEvidence.search(query, {
    strategy: "text",
    where: { kind: "outcome" },
    limit: 10,
  })

/** Rank Agencies using direct profiles and evidence from related Work. */
export const findAgencies = (query: string) =>
  FindAgencies.search(query, { limit: 6 })

/** Load the complete attributed section through the application hydrator. */
export const hydrateWorkHit = (hit: SearchHit) =>
  hydrateGrounding(hit, { level: "section" })

/** Derive the same stable node key without loading or projecting the Work. */
export const identifyWork = (id: Schema.Schema.Type<typeof WorkId>) =>
  WorkNode.key(id)

/** Find Agency references directly related to one Work graph node. */
export const findWorkAgencies = (
  id: Schema.Schema.Type<typeof WorkId>,
) => WorkNode.neighbours(id, { via: "deliveredBy" })

/** Find Work references that point to one Agency through deliveredBy. */
export const findAgencyWork = (
  id: Schema.Schema.Type<typeof AgencyId>,
) =>
  AgencyNode.neighbours(id, {
    via: "deliveredBy",
    direction: "incoming",
  })
