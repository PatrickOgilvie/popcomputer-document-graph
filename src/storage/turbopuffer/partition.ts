import { Schema } from "effect"
import {
  EmbeddingProfileIdSchema,
  EmbeddingProfileVersionSchema,
  type EmbeddingProfile,
} from "../../indexing/embedding-provider.js"
import {
  namespaceFromTurbopufferIdentity,
  parseTurbopufferSchemaGeneration,
  parseTurbopufferVectorDimensions,
  TurbopufferNamespaceSchema,
  TurbopufferSchemaGenerationSchema,
  TurbopufferVectorDimensionsSchema,
} from "./config.js"
import { InvalidTurbopufferConfiguration } from "./errors.js"
import {
  makeTurbopufferNamespaceIdentity,
  TurbopufferNamespaceIdentitySchema,
  type TurbopufferNamespaceIdentity,
} from "./identity.js"

/** Stable workspace identity used to isolate one customer's provider state. */
export const TurbopufferWorkspaceIdSchema = Schema.Trimmed.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(512),
).pipe(Schema.brand("TurbopufferWorkspaceId"))

/** Stable workspace identity used to isolate one customer's provider state. */
export type TurbopufferWorkspaceId =
  typeof TurbopufferWorkspaceIdSchema.Type

/** Stable, non-secret identity for one Turbopuffer account or deployment. */
export const TurbopufferDeploymentIdSchema = Schema.Trimmed.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(512),
).pipe(Schema.brand("TurbopufferDeploymentId"))

/** Stable, non-secret identity for one Turbopuffer account or deployment. */
export type TurbopufferDeploymentId =
  typeof TurbopufferDeploymentIdSchema.Type

/** Explicit Turbopuffer region selected for a regional API deployment. */
export const TurbopufferRegionSchema = Schema.Trimmed.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(128),
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._-]*$/),
).pipe(Schema.brand("TurbopufferRegion"))

/** Explicit Turbopuffer region selected for a regional API deployment. */
export type TurbopufferRegion = typeof TurbopufferRegionSchema.Type

const canonicalCustomBaseURL = (input: string): string | undefined => {
  try {
    const parsed = new URL(input)

    if (
      parsed.protocol !== "https:" ||
      parsed.username !== "" ||
      parsed.password !== "" ||
      parsed.search !== "" ||
      parsed.hash !== ""
    ) {
      return undefined
    }

    const path = parsed.pathname.replace(/\/+$/, "")

    return `${parsed.origin}${path}`
  } catch {
    return undefined
  }
}

/** Canonical secret-free base URL for a custom Turbopuffer-compatible API. */
export const TurbopufferBaseURLSchema = Schema.Trimmed.check(
  Schema.isNonEmpty(),
  Schema.isMaxLength(2_048),
  Schema.makeFilter(
    (value) => canonicalCustomBaseURL(value) === value,
    { title: "CanonicalTurbopufferBaseURL" },
  ),
).pipe(Schema.brand("TurbopufferBaseURL"))

/** Canonical secret-free base URL for a custom Turbopuffer-compatible API. */
export type TurbopufferBaseURL = typeof TurbopufferBaseURLSchema.Type

const TurbopufferRegionEndpointSchema = Schema.TaggedStruct("Region", {
  region: TurbopufferRegionSchema,
})

const TurbopufferCustomEndpointSchema = Schema.TaggedStruct("Custom", {
  baseURL: TurbopufferBaseURLSchema,
})

/** Explicit physical endpoint used by the Turbopuffer SDK transport. */
export const TurbopufferEndpointSchema = Schema.Union([
  TurbopufferRegionEndpointSchema,
  TurbopufferCustomEndpointSchema,
])

/** Explicit physical endpoint used by the Turbopuffer SDK transport. */
export type TurbopufferEndpoint = typeof TurbopufferEndpointSchema.Type

/** Unparsed endpoint accepted at the application composition boundary. */
export type TurbopufferEndpointInput =
  | { readonly _tag: "Region"; readonly region: string }
  | { readonly _tag: "Custom"; readonly baseURL: string }

/** D1 coordinator generation paired with exactly one physical TP namespace. */
export const TurbopufferD1IndexGenerationSchema = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^turbopuffer-v2-[0-9a-f]{64}$/)),
  Schema.brand("TurbopufferD1IndexGeneration"),
)

/** D1 coordinator generation paired with exactly one physical TP namespace. */
export type TurbopufferD1IndexGeneration =
  typeof TurbopufferD1IndexGenerationSchema.Type

const EmbeddingProfileSchema = Schema.Struct({
  id: EmbeddingProfileIdSchema,
  version: EmbeddingProfileVersionSchema,
  dimensions: TurbopufferVectorDimensionsSchema,
})

const WorkspacePartitionFieldsSchema = Schema.Struct({
  workspace: TurbopufferWorkspaceIdSchema,
  deploymentId: TurbopufferDeploymentIdSchema,
  endpoint: TurbopufferEndpointSchema,
  embeddingProfile: EmbeddingProfileSchema,
  schemaGeneration: TurbopufferSchemaGenerationSchema,
  identity: TurbopufferNamespaceIdentitySchema,
  namespace: TurbopufferNamespaceSchema,
  d1IndexGeneration: TurbopufferD1IndexGenerationSchema,
})

type WorkspacePartitionFields =
  typeof WorkspacePartitionFieldsSchema.Type

const d1IndexGenerationFromIdentity = (
  identity: TurbopufferNamespaceIdentity,
): TurbopufferD1IndexGeneration =>
  Schema.decodeSync(TurbopufferD1IndexGenerationSchema)(
    `turbopuffer-v2-${identity}`,
  )

const expectedPartitionFields = (input: {
  readonly workspace: TurbopufferWorkspaceId
  readonly deploymentId: TurbopufferDeploymentId
  readonly endpoint: TurbopufferEndpoint
  readonly embeddingProfile: EmbeddingProfile
  readonly schemaGeneration: typeof TurbopufferSchemaGenerationSchema.Type
}): Pick<
  WorkspacePartitionFields,
  "identity" | "namespace" | "d1IndexGeneration"
> => {
  const identity = makeTurbopufferNamespaceIdentity(input)

  return {
    identity,
    namespace: namespaceFromTurbopufferIdentity({ identity }),
    d1IndexGeneration: d1IndexGenerationFromIdentity(identity),
  }
}

const hasDerivedPartitionIdentity = (
  partition: WorkspacePartitionFields,
): boolean => {
  const expected = expectedPartitionFields(partition)

  return partition.identity === expected.identity &&
    partition.namespace === expected.namespace &&
    partition.d1IndexGeneration === expected.d1IndexGeneration
}

/**
 * One self-consistent physical partition shared by TP transport, rows, search,
 * and D1 publication coordination.
 */
export const TurbopufferWorkspacePartitionSchema =
  WorkspacePartitionFieldsSchema.pipe(
    Schema.check(Schema.makeFilter(hasDerivedPartitionIdentity, {
      title: "DerivedTurbopufferWorkspacePartition",
    })),
    Schema.brand("TurbopufferWorkspacePartition"),
  )

/**
 * One self-consistent physical partition shared by TP transport, rows, search,
 * and D1 publication coordination.
 */
export type TurbopufferWorkspacePartition =
  typeof TurbopufferWorkspacePartitionSchema.Type

const parseWorkspaceId = (workspace: string): TurbopufferWorkspaceId => {
  try {
    return Schema.decodeSync(TurbopufferWorkspaceIdSchema)(workspace)
  } catch {
    throw new InvalidTurbopufferConfiguration({
      field: "workspace",
      reason: "invalid_value",
    })
  }
}

const parseDeploymentId = (
  deploymentId: string,
): TurbopufferDeploymentId => {
  try {
    return Schema.decodeSync(TurbopufferDeploymentIdSchema)(deploymentId)
  } catch {
    throw new InvalidTurbopufferConfiguration({
      field: "deployment_id",
      reason: "invalid_value",
    })
  }
}

const parseEndpoint = (
  endpoint: TurbopufferEndpointInput,
): TurbopufferEndpoint => {
  try {
    if (endpoint._tag === "Region") {
      return Schema.decodeSync(TurbopufferEndpointSchema)(endpoint, {
        onExcessProperty: "error",
      })
    }

    const baseURL = canonicalCustomBaseURL(endpoint.baseURL)

    if (baseURL === undefined) {
      throw new Error("Invalid custom Turbopuffer base URL")
    }

    return Schema.decodeSync(TurbopufferEndpointSchema)({
      _tag: "Custom",
      baseURL,
    }, { onExcessProperty: "error" })
  } catch {
    throw new InvalidTurbopufferConfiguration({
      field: "endpoint",
      reason: "invalid_value",
    })
  }
}

/** Derive all physical isolation identities from one logical workspace scope. */
export const makeTurbopufferWorkspacePartition = (input: {
  readonly workspace: string
  readonly deploymentId: string
  readonly endpoint: TurbopufferEndpointInput
  readonly embeddingProfile: EmbeddingProfile
  readonly schemaGeneration: number
}): TurbopufferWorkspacePartition => {
  const workspace = parseWorkspaceId(input.workspace)
  const deploymentId = parseDeploymentId(input.deploymentId)
  const endpoint = parseEndpoint(input.endpoint)

  const embeddingProfile = {
    id: input.embeddingProfile.id,
    version: input.embeddingProfile.version,
    dimensions: parseTurbopufferVectorDimensions(
      input.embeddingProfile.dimensions,
    ),
  }

  const schemaGeneration = parseTurbopufferSchemaGeneration(
    input.schemaGeneration,
  )

  const expected = expectedPartitionFields({
    workspace,
    deploymentId,
    endpoint,
    embeddingProfile,
    schemaGeneration,
  })

  try {
    return Schema.decodeSync(TurbopufferWorkspacePartitionSchema)({
      workspace,
      deploymentId,
      endpoint,
      embeddingProfile,
      schemaGeneration,
      ...expected,
    }, { onExcessProperty: "error" })
  } catch {
    throw new InvalidTurbopufferConfiguration({
      field: "partition",
      reason: "invalid_value",
    })
  }
}

/** Revalidate a partition at an external composition boundary. */
export const validateTurbopufferWorkspacePartition = (
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- This is the external composition boundary and immediately decodes the complete branded partition.
  partition: unknown,
): TurbopufferWorkspacePartition => {
  try {
    return Schema.decodeUnknownSync(TurbopufferWorkspacePartitionSchema)(
      partition,
      { onExcessProperty: "error" },
    )
  } catch {
    throw new InvalidTurbopufferConfiguration({
      field: "partition",
      reason: "invalid_value",
    })
  }
}

/** Compare two already-validated physical partitions by canonical identity. */
export const turbopufferWorkspacePartitionsEqual = (
  left: TurbopufferWorkspacePartition,
  right: TurbopufferWorkspacePartition,
): boolean => left.identity === right.identity
