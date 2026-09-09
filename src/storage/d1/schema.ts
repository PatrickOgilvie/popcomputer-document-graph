import { sql } from "drizzle-orm"
import {
  check,
  foreignKey,
  index,
  integer,
  primaryKey,
  sqliteTable,
  text,
  unique,
} from "drizzle-orm/sqlite-core"
import type { JsonValue } from "../../document/json-value.js"
import type { GraphNodeState } from "../../graph/graph-topology.js"

/** Canonical graph nodes stored in one workspace-local D1 database. */
export const documentGraphNodes = sqliteTable(
  "document_graph_nodes",
  {
    graphId: text("graph_id").notNull(),
    documentKey: text("document_key").notNull(),
    documentKind: text("document_kind").notNull(),
    encodedDocumentId: text("encoded_document_id", { mode: "json" })
      .$type<JsonValue>()
      .notNull(),
    nodeState: text("node_state")
      .$type<GraphNodeState>()
      .notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.graphId, table.documentKey] }),
    check(
      "document_graph_nodes_document_key_sha256",
      sql`length(${table.documentKey}) = 64 AND ${table.documentKey} NOT GLOB '*[^0-9a-f]*'`,
    ),
    check(
      "document_graph_nodes_reference_not_empty",
      sql`length(trim(${table.graphId})) > 0 AND length(trim(${table.documentKind})) > 0`,
    ),
    check(
      "document_graph_nodes_encoded_id_json",
      sql`json_valid(${table.encodedDocumentId})`,
    ),
    check(
      "document_graph_nodes_state",
      sql`${table.nodeState} IN ('Referenced', 'Materialized')`,
    ),
    index("document_graph_nodes_catalog_idx").on(
      table.graphId,
      table.documentKind,
      table.nodeState,
      table.documentKey,
    ),
  ],
)

/** Directed, schema-versioned edges between canonical D1 graph nodes. */
export const documentGraphRelations = sqliteTable(
  "document_graph_relations",
  {
    graphId: text("graph_id").notNull(),
    relationId: text("relation_id").notNull(),
    relationVersion: text("relation_version").notNull(),
    sourceDocumentKey: text("source_document_key").notNull(),
    sourceDocumentKind: text("source_document_kind").notNull(),
    targetDocumentKey: text("target_document_key").notNull(),
    targetDocumentKind: text("target_document_kind").notNull(),
  },
  (table) => [
    primaryKey({
      columns: [
        table.graphId,
        table.relationId,
        table.sourceDocumentKey,
        table.targetDocumentKey,
      ],
    }),
    check(
      "document_graph_relations_reference_not_empty",
      sql`length(trim(${table.graphId})) > 0 AND length(trim(${table.relationId})) > 0 AND length(trim(${table.relationVersion})) > 0 AND length(trim(${table.sourceDocumentKind})) > 0 AND length(trim(${table.targetDocumentKind})) > 0`,
    ),
    foreignKey({
      columns: [table.graphId, table.sourceDocumentKey],
      foreignColumns: [documentGraphNodes.graphId, documentGraphNodes.documentKey],
      name: "document_graph_relations_source_node_fk",
    }).onDelete("cascade"),
    foreignKey({
      columns: [table.graphId, table.targetDocumentKey],
      foreignColumns: [documentGraphNodes.graphId, documentGraphNodes.documentKey],
      name: "document_graph_relations_target_node_fk",
    }).onDelete("cascade"),
    index("document_graph_relations_outgoing_idx").on(
      table.graphId,
      table.sourceDocumentKey,
      table.sourceDocumentKind,
      table.relationId,
      table.relationVersion,
      table.targetDocumentKind,
      table.targetDocumentKey,
    ),
    index("document_graph_relations_incoming_idx").on(
      table.graphId,
      table.targetDocumentKey,
      table.targetDocumentKind,
      table.relationId,
      table.relationVersion,
      table.sourceDocumentKind,
      table.sourceDocumentKey,
    ),
  ],
)

/** Immutable logical mutation payloads staged before remote publication. */
export const documentGraphProjectionMutations = sqliteTable(
  "document_graph_projection_mutations",
  {
    mutationId: text("mutation_id").primaryKey(),
    documentKey: text("document_key").notNull(),
    projectionId: text("projection_id").notNull(),
    payloadDigest: text("payload_digest").notNull(),
    operation: text("operation").$type<"replace" | "delete">().notNull(),
    expectedToken: text("expected_token"),
    nextToken: text("next_token"),
    revisionHash: text("revision_hash"),
    embeddingProfileId: text("embedding_profile_id"),
    embeddingProfileVersion: text("embedding_profile_version"),
    embeddingDimensions: integer("embedding_dimensions"),
    graphId: text("graph_id"),
    documentKind: text("document_kind"),
    projectionVersion: text("projection_version"),
    liveSlotCount: integer("live_slot_count").notNull(),
    requiredSlotHighWater: integer("required_slot_high_water").notNull(),
    commitInserted: integer("commit_inserted").notNull(),
    commitUpdated: integer("commit_updated").notNull(),
    commitDeleted: integer("commit_deleted").notNull(),
    deletionRevisions: integer("deletion_revisions").notNull(),
    deletionChunks: integer("deletion_chunks").notNull(),
    createdAt: integer("created_at").notNull(),
  },
  (table) => [
    check(
      "document_graph_projection_mutations_operation",
      sql`${table.operation} IN ('replace', 'delete')`,
    ),
    check(
      "document_graph_projection_mutations_document_key",
      sql`length(${table.documentKey}) = 64`,
    ),
    check(
      "document_graph_projection_mutations_payload_digest",
      sql`length(${table.payloadDigest}) = 64`,
    ),
    check(
      "document_graph_projection_mutations_live_slots",
      sql`${table.liveSlotCount} >= 0`,
    ),
    check(
      "document_graph_projection_mutations_slot_high_water",
      sql`${table.requiredSlotHighWater} >= ${table.liveSlotCount}`,
    ),
    unique("document_graph_projection_mutations_identity").on(
      table.documentKey,
      table.projectionId,
      table.mutationId,
    ),
  ],
)

/** Ordered chunk inventory retained for exact logical-revision replay. */
export const documentGraphProjectionMutationChunks = sqliteTable(
  "document_graph_projection_mutation_chunks",
  {
    mutationId: text("mutation_id").notNull(),
    ordinal: integer("ordinal").notNull(),
    chunkId: text("chunk_id").notNull(),
    contentHash: text("content_hash").notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.mutationId, table.ordinal] }),
    unique("document_graph_projection_mutation_chunks_identity").on(
      table.mutationId,
      table.chunkId,
    ),
    foreignKey({
      columns: [table.mutationId],
      foreignColumns: [documentGraphProjectionMutations.mutationId],
      name: "document_graph_projection_mutation_chunks_mutation_fk",
    }).onDelete("cascade"),
    check(
      "document_graph_projection_mutation_chunks_ordinal",
      sql`${table.ordinal} >= 0`,
    ),
    check(
      "document_graph_projection_mutation_chunks_chunk_id",
      sql`length(${table.chunkId}) = 64`,
    ),
    check(
      "document_graph_projection_mutation_chunks_content_hash",
      sql`length(${table.contentHash}) = 64`,
    ),
  ],
)

/** D1-authoritative logical head and pending publication for one partition. */
export const documentGraphProjectionHeads = sqliteTable(
  "document_graph_projection_heads",
  {
    documentKey: text("document_key").notNull(),
    projectionId: text("projection_id").notNull(),
    indexGeneration: text("index_generation").notNull(),
    headVersion: integer("head_version").notNull().default(0),
    lastAllocatedGeneration: integer("last_allocated_generation")
      .notNull()
      .default(0),
    slotHighWater: integer("slot_high_water").notNull().default(0),
    activeMutationId: text("active_mutation_id"),
    activeToken: text("active_token"),
    activeStatus: text("active_status")
      .$type<"never" | "revision" | "deleted">()
      .notNull()
      .default("never"),
    pendingMutationId: text("pending_mutation_id"),
    pendingPayloadDigest: text("pending_payload_digest"),
    pendingPublicationId: text("pending_publication_id"),
    pendingGeneration: integer("pending_generation"),
    pendingOperation: text("pending_operation").$type<"replace" | "delete">(),
    pendingSlotHighWater: integer("pending_slot_high_water"),
    pendingLeaseExpiresAt: integer("pending_lease_expires_at"),
    updatedAt: integer("updated_at").notNull(),
  },
  (table) => [
    primaryKey({
      columns: [table.indexGeneration, table.documentKey, table.projectionId],
    }),
    foreignKey({
      columns: [table.activeMutationId],
      foreignColumns: [documentGraphProjectionMutations.mutationId],
      name: "document_graph_projection_heads_active_mutation_fk",
    }),
    foreignKey({
      columns: [table.pendingMutationId],
      foreignColumns: [documentGraphProjectionMutations.mutationId],
      name: "document_graph_projection_heads_pending_mutation_fk",
    }),
    check(
      "document_graph_projection_heads_document_key",
      sql`length(${table.documentKey}) = 64`,
    ),
    check(
      "document_graph_projection_heads_projection_id",
      sql`length(${table.projectionId}) > 0`,
    ),
    check(
      "document_graph_projection_heads_index_generation",
      sql`length(trim(${table.indexGeneration})) BETWEEN 1 AND 256`,
    ),
    check(
      "document_graph_projection_heads_generations",
      sql`${table.lastAllocatedGeneration} >= 0 AND ${table.slotHighWater} >= 0`,
    ),
    check(
      "document_graph_projection_heads_active_state",
      sql`(
        (${table.activeStatus} = 'never' AND ${table.activeMutationId} IS NULL AND ${table.activeToken} IS NULL)
        OR (${table.activeStatus} = 'revision' AND ${table.activeMutationId} IS NOT NULL AND ${table.activeToken} IS NOT NULL)
        OR (${table.activeStatus} = 'deleted' AND ${table.activeMutationId} IS NOT NULL AND ${table.activeToken} IS NULL)
      )`,
    ),
    check(
      "document_graph_projection_heads_pending_state",
      sql`(
        (${table.pendingMutationId} IS NULL AND ${table.pendingPayloadDigest} IS NULL AND ${table.pendingPublicationId} IS NULL AND ${table.pendingGeneration} IS NULL AND ${table.pendingOperation} IS NULL AND ${table.pendingSlotHighWater} IS NULL AND ${table.pendingLeaseExpiresAt} IS NULL)
        OR (${table.pendingMutationId} IS NOT NULL AND ${table.pendingPayloadDigest} IS NOT NULL AND ${table.pendingPublicationId} IS NOT NULL AND ${table.pendingGeneration} > 0 AND ${table.pendingOperation} IS NOT NULL AND ${table.pendingSlotHighWater} >= 0 AND ${table.pendingLeaseExpiresAt} IS NOT NULL)
      )`,
    ),
    index("document_graph_projection_heads_catalog_idx").on(
      table.indexGeneration,
      table.activeStatus,
      table.activeMutationId,
    ),
    index("document_graph_projection_heads_active_mutation_idx").on(
      table.activeMutationId,
    ),
    index("document_graph_projection_heads_pending_mutation_idx").on(
      table.pendingMutationId,
    ),
  ],
)

/** Durable audit journal for pending, committed, and superseded publications. */
export const documentGraphProjectionPublications = sqliteTable(
  "document_graph_projection_publications",
  {
    publicationId: text("publication_id").primaryKey(),
    mutationId: text("mutation_id").notNull(),
    documentKey: text("document_key").notNull(),
    projectionId: text("projection_id").notNull(),
    indexGeneration: text("index_generation").notNull(),
    generation: integer("generation").notNull(),
    slotHighWater: integer("slot_high_water").notNull(),
    status: text("status")
      .$type<"pending" | "committed" | "superseded">()
      .notNull(),
    createdAt: integer("created_at").notNull(),
    updatedAt: integer("updated_at").notNull(),
  },
  (table) => [
    foreignKey({
      columns: [table.mutationId],
      foreignColumns: [documentGraphProjectionMutations.mutationId],
      name: "document_graph_projection_publications_mutation_fk",
    }),
    check(
      "document_graph_projection_publications_document_key",
      sql`length(${table.documentKey}) = 64`,
    ),
    check(
      "document_graph_projection_publications_projection_id",
      sql`length(${table.projectionId}) > 0`,
    ),
    check(
      "document_graph_projection_publications_index_generation",
      sql`length(trim(${table.indexGeneration})) BETWEEN 1 AND 256`,
    ),
    check(
      "document_graph_projection_publications_status",
      sql`${table.status} IN ('pending', 'committed', 'superseded')`,
    ),
    check(
      "document_graph_projection_publications_generation",
      sql`${table.generation} > 0`,
    ),
    check(
      "document_graph_projection_publications_slot_high_water",
      sql`${table.slotHighWater} >= 0`,
    ),
    unique("document_graph_projection_publications_generation_identity").on(
      table.indexGeneration,
      table.documentKey,
      table.projectionId,
      table.generation,
    ),
    index("document_graph_projection_publications_pending_idx").on(
      table.status,
      table.updatedAt,
    ),
    index("document_graph_projection_publications_mutation_idx").on(
      table.mutationId,
    ),
  ],
)

/** Drizzle schema supplied when composing the D1 topology adapter. */
export const d1GraphTopologySchema = {
  documentGraphNodes,
  documentGraphRelations,
}

/** Complete fixed package-owned schema for one workspace-local D1 database. */
export const d1DocumentGraphSchema = {
  ...d1GraphTopologySchema,
  documentGraphProjectionMutations,
  documentGraphProjectionMutationChunks,
  documentGraphProjectionHeads,
  documentGraphProjectionPublications,
}
