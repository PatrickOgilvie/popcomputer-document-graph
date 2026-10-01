import { Layer, type Redacted } from "effect"
import {
  EmbeddingProvider,
  type EmbeddingProviderService,
} from "../../indexing/embedding-provider.js"
import { ProjectionIndexStore } from "../../indexing/projection-index.js"
import type {
  ProjectionHybridSearchStore,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
} from "../../retrieval/graph-retrieval.js"
import type { GraphTopologyStore } from "../../graph/graph-topology.js"
import {
  d1ProjectionPublicationCoordinator,
} from "../d1/projection-publication.js"
import type { DocumentGraphD1Database } from "../d1/contract.js"
import { d1GraphTopology } from "../d1/runtime.js"
import {
  officialTurbopufferClient,
  type TurbopufferClientConfig,
} from "./client.js"
import { InvalidTurbopufferConfiguration } from "./errors.js"
import {
  makeTurbopufferWorkspacePartition,
  type TurbopufferEndpointInput,
  type TurbopufferWorkspacePartition,
} from "./partition.js"
import {
  turbopufferProjectionSearch,
  type TurbopufferProjectionSearchConfig,
} from "./projection-search.js"
import type { TurbopufferQueryConsistency } from "./query-compiler.js"
import type { TurbopufferVectorElementType } from "./config.js"
import type { ProjectionPublicationCoordinator } from "../../indexing/projection-publication.js"
import {
  makeTurbopufferProjectionIndexStore,
  type TurbopufferProjectionIndexConfig,
  type TurbopufferWriteCoalescing,
} from "./projection-index.js"

/** Turbopuffer transport, partition, publication, and retrieval settings. */
export interface TurbopufferD1WorkspaceProviderConfig {
  /** Redacted credential for the explicitly identified deployment. */
  readonly apiKey: Redacted.Redacted<string>
  /** Stable, non-secret account or deployment identity. */
  readonly deploymentId: string
  /** Explicit region or custom endpoint included in the physical partition. */
  readonly endpoint: TurbopufferEndpointInput
  /** Immutable provider schema generation. */
  readonly schemaGeneration: number
  /** Dense vector element type, fixed per namespace; defaults to `f32`. */
  readonly vectorElementType?: TurbopufferVectorElementType | undefined
  /** Per-request transport timeout; defaults to 60 seconds. */
  readonly timeoutMilliseconds?: number | undefined
  /** Effect-owned retries after the initial request; defaults to two. */
  readonly retries?: number | undefined
  /** Optional explicit fetch implementation for the current runtime. */
  readonly fetch?: TurbopufferClientConfig["fetch"]
  /** Gzip request bodies, for bulk writes limited by upload bandwidth. */
  readonly compression?: boolean | undefined
  /** Maximum stable slots for one logical revision; defaults to 10,000. */
  readonly maximumSlotsPerRevision?: number | undefined
  /** Maximum serialized atomic publication bytes; defaults to 4 MiB. */
  readonly maximumPublicationBytes?: number | undefined
  /** Share one provider write among publications that arrive together. */
  readonly coalesceWrites?: TurbopufferWriteCoalescing | undefined
  /** Retrieval consistency; defaults to strong. */
  readonly consistency?: TurbopufferQueryConsistency | undefined
  /** How search confirms candidates are current; defaults to the journal. */
  readonly candidateVerification?: TurbopufferProjectionSearchConfig["candidateVerification"]
  /** Most chunks one document contributes to a channel's candidates. */
  readonly chunksPerDocument?: number | undefined
  /** Share multi-query requests among searches that arrive together. */
  readonly coalesceSearches?: TurbopufferProjectionSearchConfig["coalesceSearches"]
  /** Diagnostic deadline for an unreconciled publication. */
  readonly publicationLeaseMilliseconds?: number | undefined
  /** Completed publication journal entries retained per projection. */
  readonly retainedPublicationHistory?: number | undefined
}

/** Explicit application-composition input for one workspace data plane. */
export interface TurbopufferD1WorkspaceConfig {
  /** Stable logical workspace identity. */
  readonly workspace: string
  /** Raw workspace-local D1 binding, accepted only at this composition seam. */
  readonly database: DocumentGraphD1Database
  /** Embedding capability whose profile defines the physical vector space. */
  readonly embeddings: EmbeddingProviderService
  /** Provider and publication settings for the workspace. */
  readonly turbopuffer: TurbopufferD1WorkspaceProviderConfig
}

/** Cohesive capabilities exposed by the composed workspace data plane. */
export type TurbopufferD1WorkspaceServices =
  | EmbeddingProvider
  | GraphTopologyStore
  | ProjectionIndexStore
  | ProjectionSearchStore
  | ProjectionTextSearchStore
  | ProjectionHybridSearchStore

/** Derived physical identity plus the fully wired workspace Layer. */
export interface TurbopufferD1Workspace {
  /** Canonical identity shared by the client, rows, search, and D1 journal. */
  readonly partition: TurbopufferWorkspacePartition
  /** Index, topology, semantic, lexical, and hybrid application capabilities. */
  readonly layer: Layer.Layer<
    TurbopufferD1WorkspaceServices,
    InvalidTurbopufferConfiguration
  >
}

const publicationCoordinator = (input: {
  readonly database: DocumentGraphD1Database
  readonly indexGeneration: string
  readonly publicationLeaseMilliseconds: number | undefined
  readonly retainedPublicationHistory: number | undefined
}) => {
  if (
    input.publicationLeaseMilliseconds !== undefined &&
    (!Number.isSafeInteger(input.publicationLeaseMilliseconds) ||
      input.publicationLeaseMilliseconds < 1_000)
  ) {
    throw new InvalidTurbopufferConfiguration({
      field: "publication_lease_milliseconds",
      reason: "invalid_value",
    })
  }

  if (
    input.retainedPublicationHistory !== undefined &&
    (!Number.isSafeInteger(input.retainedPublicationHistory) ||
      input.retainedPublicationHistory < 1 ||
      input.retainedPublicationHistory > 1_000)
  ) {
    throw new InvalidTurbopufferConfiguration({
      field: "retained_publication_history",
      reason: "invalid_value",
    })
  }

  if (input.publicationLeaseMilliseconds === undefined) {
    return input.retainedPublicationHistory === undefined
      ? d1ProjectionPublicationCoordinator({
          database: input.database,
          indexGeneration: input.indexGeneration,
        })
      : d1ProjectionPublicationCoordinator({
          database: input.database,
          indexGeneration: input.indexGeneration,
          retainedPublicationHistory: input.retainedPublicationHistory,
        })
  }

  return input.retainedPublicationHistory === undefined
    ? d1ProjectionPublicationCoordinator({
        database: input.database,
        indexGeneration: input.indexGeneration,
        publicationLeaseMilliseconds: input.publicationLeaseMilliseconds,
      })
    : d1ProjectionPublicationCoordinator({
        database: input.database,
        indexGeneration: input.indexGeneration,
        publicationLeaseMilliseconds: input.publicationLeaseMilliseconds,
        retainedPublicationHistory: input.retainedPublicationHistory,
      })
}

/** Turbopuffer index and retrieval capabilities provided by one deployment. */
export type TurbopufferProviderServices =
  | ProjectionIndexStore
  | ProjectionSearchStore
  | ProjectionTextSearchStore
  | ProjectionHybridSearchStore

/** Derive the physical partition one provider configuration addresses. */
export const turbopufferProviderPartition = (input: {
  readonly workspace: string
  readonly embeddingProfile: EmbeddingProviderService["profile"]
  readonly turbopuffer: TurbopufferD1WorkspaceProviderConfig
}): TurbopufferWorkspacePartition =>
  makeTurbopufferWorkspacePartition({
    workspace: input.workspace,
    deploymentId: input.turbopuffer.deploymentId,
    endpoint: input.turbopuffer.endpoint,
    embeddingProfile: input.embeddingProfile,
    schemaGeneration: input.turbopuffer.schemaGeneration,
    vectorElementType: input.turbopuffer.vectorElementType,
  })

/**
 * Turbopuffer index and retrieval over one partition, coordinated by the
 * supplied publication journal.
 */
export const turbopufferProviderStorage = (input: {
  readonly partition: TurbopufferWorkspacePartition
  readonly turbopuffer: TurbopufferD1WorkspaceProviderConfig
  readonly coordinator: Layer.Layer<ProjectionPublicationCoordinator>
}): Layer.Layer<TurbopufferProviderServices, InvalidTurbopufferConfiguration> => {
  const client = officialTurbopufferClient({
    apiKey: input.turbopuffer.apiKey,
    partition: input.partition,
    timeoutMilliseconds: input.turbopuffer.timeoutMilliseconds,
    retries: input.turbopuffer.retries,
    fetch: input.turbopuffer.fetch,
    compression: input.turbopuffer.compression,
  })

  const indexConfig: TurbopufferProjectionIndexConfig = {
    partition: input.partition,
    maximumSlotsPerRevision: input.turbopuffer.maximumSlotsPerRevision,
    maximumPublicationBytes: input.turbopuffer.maximumPublicationBytes,
    coalesceWrites: input.turbopuffer.coalesceWrites,
  }

  const searchConfig: TurbopufferProjectionSearchConfig = {
    partition: input.partition,
    consistency: input.turbopuffer.consistency,
    candidateVerification: input.turbopuffer.candidateVerification,
    chunksPerDocument: input.turbopuffer.chunksPerDocument,
    coalesceSearches: input.turbopuffer.coalesceSearches,
  }

  return Layer.mergeAll(
    Layer.effect(
      ProjectionIndexStore,
      makeTurbopufferProjectionIndexStore(indexConfig),
    ),
    turbopufferProjectionSearch(searchConfig),
  ).pipe(Layer.provide(Layer.mergeAll(client, input.coordinator)))
}

/**
 * Compose one workspace-local D1 database and one Turbopuffer deployment.
 *
 * The returned Layer hides transport and publication-coordinator mechanics.
 * Callers provide only the resulting application capabilities to graph flows.
 */
export const makeTurbopufferD1Workspace = (
  config: TurbopufferD1WorkspaceConfig,
): TurbopufferD1Workspace => {
  const partition = turbopufferProviderPartition({
    workspace: config.workspace,
    embeddingProfile: config.embeddings.profile,
    turbopuffer: config.turbopuffer,
  })

  const coordinator = publicationCoordinator({
    database: config.database,
    indexGeneration: partition.d1IndexGeneration,
    publicationLeaseMilliseconds:
      config.turbopuffer.publicationLeaseMilliseconds,
    retainedPublicationHistory: config.turbopuffer.retainedPublicationHistory,
  })

  return {
    partition,
    layer: Layer.mergeAll(
      Layer.succeed(EmbeddingProvider, config.embeddings),
      d1GraphTopology({ database: config.database }),
      turbopufferProviderStorage({
        partition,
        turbopuffer: config.turbopuffer,
        coordinator,
      }),
    ),
  }
}
