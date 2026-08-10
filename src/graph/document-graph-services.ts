import type {
  ProjectionSearchStore,
  ProjectionTextSearchStore,
} from "../retrieval/graph-retrieval.js"
import type { GroundingHydrator } from "../retrieval/grounding.js"
import type {
  ProjectionIndexStore,
} from "../indexing/projection-index.js"
import type { EmbeddingProvider } from "../indexing/embedding-provider.js"
import type { GraphRelationStore } from "./graph-relation.js"

/** Services a Honertia application may add to its shared Effect environment. */
export type DocumentGraphServices =
  | EmbeddingProvider
  | ProjectionIndexStore
  | ProjectionSearchStore
  | ProjectionTextSearchStore
  | GraphRelationStore
  | GroundingHydrator
