import { Layer } from "effect"
import type { ProjectionPublicationCoordinator } from "./indexing/projection-publication.js"
import { ProjectionIndexStore } from "./indexing/projection-index.js"
import type { TurbopufferClient } from "./storage/turbopuffer/client.js"
import {
  InvalidTurbopufferConfiguration,
} from "./storage/turbopuffer/errors.js"
import {
  makeTurbopufferProjectionIndexStore,
  type TurbopufferProjectionIndexConfig,
} from "./storage/turbopuffer/projection-index.js"

/** Provide the D1-coordinated Turbopuffer projection index capability. */
export const turbopufferProjectionIndex = (
  config: TurbopufferProjectionIndexConfig,
): Layer.Layer<
  ProjectionIndexStore,
  InvalidTurbopufferConfiguration,
  ProjectionPublicationCoordinator | TurbopufferClient
> => Layer.effect(
  ProjectionIndexStore,
  makeTurbopufferProjectionIndexStore(config),
)

export {
  officialTurbopufferClient,
  makeOfficialTurbopufferClient,
  TurbopufferClient,
  type TurbopufferClientConfig,
  type TurbopufferClientService,
} from "./storage/turbopuffer/client.js"

export {
  compileTurbopufferSchemaManifest,
  namespaceFromTurbopufferIdentity,
  parseTurbopufferNamespace,
  parseTurbopufferSchemaGeneration,
  parseTurbopufferVectorDimensions,
  TurbopufferFilterableAttributes,
  TurbopufferFullTextAttributes,
  TurbopufferMaximumAttributeBytes,
  TurbopufferMaximumDocumentBytes,
  TurbopufferMaximumFilterableValueBytes,
  TurbopufferMaximumQueryRows,
  TurbopufferMaximumVectorDimensions,
  TurbopufferMaximumWriteBytes,
  TurbopufferNamespaceSchema,
  TurbopufferSchemaGenerationSchema,
  TurbopufferVectorDimensionsSchema,
  type TurbopufferNamespace,
  type TurbopufferSchemaGeneration,
  type TurbopufferSchemaManifest,
  type TurbopufferVectorDimensions,
} from "./storage/turbopuffer/config.js"

export {
  InvalidTurbopufferConfiguration,
  InvalidTurbopufferResponse,
  TurbopufferMutationTooLarge,
  TurbopufferSchemaMismatch,
  TurbopufferTransportFailed,
} from "./storage/turbopuffer/errors.js"

export {
  canonicalTurbopufferJson,
  hashTurbopufferIdentity,
  makeTurbopufferMarkerRowId,
  makeTurbopufferNamespaceIdentity,
  makeTurbopufferSlotRowId,
  TurbopufferNamespaceIdentitySchema,
  TurbopufferPhysicalRowIdSchema,
  type TurbopufferNamespaceIdentity,
  type TurbopufferPhysicalRowId,
  type TurbopufferProjectionAddress,
} from "./storage/turbopuffer/identity.js"

export {
  makeTurbopufferWorkspacePartition,
  turbopufferWorkspacePartitionsEqual,
  TurbopufferBaseURLSchema,
  TurbopufferDeploymentIdSchema,
  TurbopufferD1IndexGenerationSchema,
  TurbopufferEndpointSchema,
  TurbopufferRegionSchema,
  TurbopufferWorkspaceIdSchema,
  TurbopufferWorkspacePartitionSchema,
  validateTurbopufferWorkspacePartition,
  type TurbopufferBaseURL,
  type TurbopufferDeploymentId,
  type TurbopufferD1IndexGeneration,
  type TurbopufferEndpoint,
  type TurbopufferEndpointInput,
  type TurbopufferRegion,
  type TurbopufferWorkspaceId,
  type TurbopufferWorkspacePartition,
} from "./storage/turbopuffer/partition.js"

export {
  makeTurbopufferD1Workspace,
  type TurbopufferD1Workspace,
  type TurbopufferD1WorkspaceConfig,
  type TurbopufferD1WorkspaceProviderConfig,
  type TurbopufferD1WorkspaceServices,
} from "./storage/turbopuffer/workspace.js"

export {
  destroyTurbopufferNamespace,
  inspectTurbopufferNamespaceSchema,
  updateTurbopufferNamespaceSchema,
  type TurbopufferNamespaceSchemaInspection,
} from "./storage/turbopuffer/namespace.js"

export {
  compileTurbopufferMetadataFilter,
  encodeTurbopufferMetadataTerms,
  makeTurbopufferMetadataTerm,
  TurbopufferMetadataTermsAttribute,
  type TurbopufferFilter,
} from "./storage/turbopuffer/metadata-terms.js"

export {
  makeTurbopufferProjectionIndexStore,
  type TurbopufferProjectionIndexConfig,
} from "./storage/turbopuffer/projection-index.js"

export {
  makeTurbopufferProjectionSearchStores,
  turbopufferProjectionSearch,
  type TurbopufferProjectionSearchConfig,
  type TurbopufferProjectionSearchStores,
} from "./storage/turbopuffer/projection-search.js"

export {
  compileTurbopufferHybridQuery,
  compileTurbopufferSemanticQuery,
  compileTurbopufferTextQuery,
  TurbopufferQueryConsistencySchema,
  type CompiledTurbopufferHybridQuery,
  type CompiledTurbopufferQuery,
  type TurbopufferQueryConsistency,
  type TurbopufferQueryPartition,
} from "./storage/turbopuffer/query-compiler.js"

export {
  decodeTurbopufferSearchResultRow,
  makeTurbopufferDummyVector,
  makeTurbopufferLiveSlotRow,
  makeTurbopufferMarkerRow,
  makeTurbopufferTombstoneRow,
  scoreTurbopufferBm25,
  scoreTurbopufferCosineDistance,
  TurbopufferLiveSlotRowSchema,
  TurbopufferMarkerRowSchema,
  TurbopufferPublicationRowSchema,
  TurbopufferRowKindSchema,
  TurbopufferSearchResultAttributes,
  TurbopufferSearchResultRowSchema,
  TurbopufferTombstoneRowSchema,
  type DecodedTurbopufferSearchResultRow,
  type TurbopufferLiveSlotRow,
  type TurbopufferMarkerRow,
  type TurbopufferPublicationRow,
  type TurbopufferPublicationRowContext,
  type TurbopufferRowKind,
  type TurbopufferTombstoneRow,
} from "./storage/turbopuffer/row-codec.js"
