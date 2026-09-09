import { Layer } from "effect"
import {
  ProjectionSearchStore,
  ProjectionTextSearchStore,
  type ProjectionSearchStoreService,
  type ProjectionTextSearchStoreService,
} from "../retrieval/graph-retrieval.js"
import {
  ProjectionIndexStore,
  type ProjectionIndexStoreService,
} from "../indexing/projection-index.js"
import {
  GraphTopologyStore,
  type GraphTopologyStoreService,
} from "../graph/graph-topology.js"

/** Cohesive storage implementation supporting both indexing and retrieval. */
export interface DocumentGraphStorageService
  extends ProjectionIndexStoreService,
    ProjectionSearchStoreService,
    ProjectionTextSearchStoreService,
    GraphTopologyStoreService {}

/**
 * Provide one storage implementation through the independent index and search
 * capabilities consumed by the core workflows.
 */
export const makeDocumentGraphStorage = (
  storage: DocumentGraphStorageService,
): Layer.Layer<
  | ProjectionIndexStore
  | ProjectionSearchStore
  | ProjectionTextSearchStore
  | GraphTopologyStore
> =>
  Layer.mergeAll(
    Layer.succeed(ProjectionIndexStore, storage),
    Layer.succeed(ProjectionSearchStore, storage),
    Layer.succeed(ProjectionTextSearchStore, storage),
    Layer.succeed(GraphTopologyStore, storage),
  )
