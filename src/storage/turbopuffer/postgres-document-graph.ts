import { Effect, Layer } from "effect"
import type { Pool } from "pg"
import { GraphTopologyStore } from "../../graph/graph-topology.js"
import type { EmbeddingProfile } from "../../indexing/embedding-provider.js"
import type { PostgresSearchCoalescing } from "../postgres/connection.js"
import { postgresProjectionPublicationCoordinator } from "../postgres/projection-publication.js"
import { postgresDocumentGraph } from "../postgres/runtime.js"
import type { InvalidTurbopufferConfiguration } from "./errors.js"
import type { TurbopufferWorkspacePartition } from "./partition.js"
import {
  turbopufferProviderPartition,
  turbopufferProviderStorage,
  type TurbopufferD1WorkspaceProviderConfig,
  type TurbopufferProviderServices,
} from "./workspace.js"

/** PostgreSQL side of a Turbopuffer-backed document graph. */
export interface TurbopufferPostgresStorageConfig {
  /** Shared pool; topology reads and journal transactions borrow from it. */
  readonly pool: Pool
  /** Schema holding the package migrations; defaults to `honertia_document_graph`. */
  readonly schema?: string | undefined
  /** Merge concurrent topology reads into shared statements. */
  readonly coalesceSearches?: PostgresSearchCoalescing | undefined
}

/** Explicit composition input for PostgreSQL topology with Turbopuffer retrieval. */
export interface TurbopufferPostgresDocumentGraphConfig {
  /** Stable logical identity of this graph's retrieval space. */
  readonly workspace: string
  /** Vector space of every stored and queried embedding. */
  readonly embeddingProfile: EmbeddingProfile
  readonly postgres: TurbopufferPostgresStorageConfig
  /** Provider and publication settings. */
  readonly turbopuffer: TurbopufferD1WorkspaceProviderConfig
}

/** Capabilities of PostgreSQL topology plus Turbopuffer index and retrieval. */
export type TurbopufferPostgresDocumentGraphServices =
  | GraphTopologyStore
  | TurbopufferProviderServices

/** Derived physical identity plus the fully wired Layer. */
export interface TurbopufferPostgresDocumentGraph {
  readonly partition: TurbopufferWorkspacePartition
  readonly layer: Layer.Layer<
    TurbopufferPostgresDocumentGraphServices,
    InvalidTurbopufferConfiguration
  >
}

/**
 * Keep graph topology and the publication journal in PostgreSQL while chunks,
 * vectors, and full-text indexes live in Turbopuffer.
 *
 * Apply PostgreSQL migrations through 0006 first. Retrieval verifies every
 * Turbopuffer candidate against the journal, so search reads PostgreSQL once
 * per non-empty result in addition to any topology traversal.
 */
export const makeTurbopufferPostgresDocumentGraph = (
  config: TurbopufferPostgresDocumentGraphConfig,
): TurbopufferPostgresDocumentGraph => {
  const partition = turbopufferProviderPartition({
    workspace: config.workspace,
    embeddingProfile: config.embeddingProfile,
    turbopuffer: config.turbopuffer,
  })

  const coordinator = postgresProjectionPublicationCoordinator({
    pool: config.postgres.pool,
    schema: config.postgres.schema,
    indexGeneration: partition.d1IndexGeneration,
    publicationLeaseMilliseconds: config.turbopuffer.publicationLeaseMilliseconds,
    retainedPublicationHistory: config.turbopuffer.retainedPublicationHistory,
  })

  // The PostgreSQL storage Layer also provides index and search capabilities;
  // only its topology store is exposed, so Turbopuffer owns every chunk.
  const topology = Layer.effect(
    GraphTopologyStore,
    Effect.gen(function*() {
      return yield* GraphTopologyStore
    }),
  ).pipe(Layer.provide(postgresDocumentGraph({
    pool: config.postgres.pool,
    schema: config.postgres.schema,
    coalesceSearches: config.postgres.coalesceSearches,
  })))

  return {
    partition,
    layer: Layer.mergeAll(
      topology,
      turbopufferProviderStorage({
        partition,
        turbopuffer: config.turbopuffer,
        coordinator,
      }),
    ),
  }
}
