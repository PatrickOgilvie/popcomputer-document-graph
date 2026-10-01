-- Publication journal for projection indexes stored outside PostgreSQL, such
-- as Turbopuffer. PostgreSQL owns the logical catalog: which revision of each
-- document projection is active, which physical publication may write it, and
-- the chunk inventory used to verify retrieval candidates.

CREATE TABLE IF NOT EXISTS "honertia_document_graph"."projection_mutations" (
  "mutation_id" text PRIMARY KEY,
  "document_key" char(64) NOT NULL,
  "projection_id" text NOT NULL,
  "payload_digest" char(64) NOT NULL,
  "operation" text NOT NULL,
  "expected_token" text,
  "next_token" text,
  "revision_hash" char(64),
  "embedding_profile_id" text,
  "embedding_profile_version" text,
  "embedding_dimensions" integer,
  "graph_id" text,
  "document_kind" text,
  "projection_version" text,
  "live_slot_count" integer NOT NULL,
  "required_slot_high_water" integer NOT NULL,
  "commit_inserted" integer NOT NULL,
  "commit_updated" integer NOT NULL,
  "commit_deleted" integer NOT NULL,
  "deletion_revisions" integer NOT NULL,
  "deletion_chunks" integer NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT "projection_mutations_operation"
    CHECK ("operation" IN ('replace', 'delete')),
  CONSTRAINT "projection_mutations_identifier_length"
    CHECK (length(btrim("mutation_id")) BETWEEN 1 AND 128),
  CONSTRAINT "projection_mutations_document_key_sha256"
    CHECK ("document_key" ~ '^[0-9a-f]{64}$'),
  CONSTRAINT "projection_mutations_payload_digest_sha256"
    CHECK ("payload_digest" ~ '^[0-9a-f]{64}$'),
  CONSTRAINT "projection_mutations_counts"
    CHECK (
      "live_slot_count" >= 0 AND
      "required_slot_high_water" >= "live_slot_count" AND
      "commit_inserted" >= 0 AND "commit_updated" >= 0 AND
      "commit_deleted" >= 0 AND "deletion_revisions" >= 0 AND
      "deletion_chunks" >= 0
    ),
  CONSTRAINT "projection_mutations_replacement_catalog"
    CHECK (
      "operation" = 'delete' OR (
        "next_token" IS NOT NULL AND
        "revision_hash" ~ '^[0-9a-f]{64}$' AND
        "embedding_profile_id" IS NOT NULL AND
        "embedding_profile_version" IS NOT NULL AND
        "embedding_dimensions" > 0 AND
        "graph_id" IS NOT NULL AND
        "document_kind" IS NOT NULL AND
        "projection_version" IS NOT NULL
      )
    )
);

CREATE INDEX IF NOT EXISTS "projection_mutations_projection_idx"
  ON "honertia_document_graph"."projection_mutations"
  ("document_key", "projection_id");

CREATE TABLE IF NOT EXISTS "honertia_document_graph"."projection_mutation_chunks" (
  "mutation_id" text NOT NULL
    REFERENCES "honertia_document_graph"."projection_mutations" ("mutation_id")
    ON DELETE CASCADE,
  "ordinal" integer NOT NULL,
  "chunk_id" char(64) NOT NULL,
  "content_hash" char(64) NOT NULL,
  PRIMARY KEY ("mutation_id", "ordinal"),
  UNIQUE ("mutation_id", "chunk_id"),
  CONSTRAINT "projection_mutation_chunks_ordinal" CHECK ("ordinal" >= 0),
  CONSTRAINT "projection_mutation_chunks_chunk_id_sha256"
    CHECK ("chunk_id" ~ '^[0-9a-f]{64}$'),
  CONSTRAINT "projection_mutation_chunks_content_hash_sha256"
    CHECK ("content_hash" ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS "honertia_document_graph"."projection_heads" (
  "index_generation" text NOT NULL,
  "document_key" char(64) NOT NULL,
  "projection_id" text NOT NULL,
  "head_version" bigint NOT NULL DEFAULT 0,
  "last_allocated_generation" bigint NOT NULL DEFAULT 0,
  "slot_high_water" integer NOT NULL DEFAULT 0,
  "active_status" text NOT NULL DEFAULT 'never',
  "active_mutation_id" text
    REFERENCES "honertia_document_graph"."projection_mutations" ("mutation_id"),
  "active_token" text,
  "pending_mutation_id" text
    REFERENCES "honertia_document_graph"."projection_mutations" ("mutation_id"),
  "pending_payload_digest" char(64),
  "pending_publication_id" text,
  "pending_generation" bigint,
  "pending_operation" text,
  "pending_slot_high_water" integer,
  "pending_lease_expires_at" timestamptz,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("index_generation", "document_key", "projection_id"),
  CONSTRAINT "projection_heads_index_generation_length"
    CHECK (length(btrim("index_generation")) BETWEEN 1 AND 256),
  CONSTRAINT "projection_heads_document_key_sha256"
    CHECK ("document_key" ~ '^[0-9a-f]{64}$'),
  CONSTRAINT "projection_heads_counters"
    CHECK ("last_allocated_generation" >= 0 AND "slot_high_water" >= 0),
  CONSTRAINT "projection_heads_active"
    CHECK (
      ("active_status" = 'never'
        AND "active_mutation_id" IS NULL AND "active_token" IS NULL)
      OR ("active_status" = 'revision'
        AND "active_mutation_id" IS NOT NULL AND "active_token" IS NOT NULL)
      OR ("active_status" = 'deleted'
        AND "active_mutation_id" IS NOT NULL AND "active_token" IS NULL)
    ),
  CONSTRAINT "projection_heads_pending"
    CHECK (
      ("pending_mutation_id" IS NULL
        AND "pending_payload_digest" IS NULL
        AND "pending_publication_id" IS NULL
        AND "pending_generation" IS NULL
        AND "pending_operation" IS NULL
        AND "pending_slot_high_water" IS NULL
        AND "pending_lease_expires_at" IS NULL)
      OR ("pending_mutation_id" IS NOT NULL
        AND "pending_payload_digest" IS NOT NULL
        AND "pending_publication_id" IS NOT NULL
        AND "pending_generation" > 0
        AND "pending_operation" IN ('replace', 'delete')
        AND "pending_slot_high_water" >= 0
        AND "pending_lease_expires_at" IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS "projection_heads_catalog_idx"
  ON "honertia_document_graph"."projection_heads"
  ("index_generation", "active_status", "active_mutation_id");

CREATE INDEX IF NOT EXISTS "projection_heads_active_mutation_idx"
  ON "honertia_document_graph"."projection_heads" ("active_mutation_id");

CREATE INDEX IF NOT EXISTS "projection_heads_pending_mutation_idx"
  ON "honertia_document_graph"."projection_heads" ("pending_mutation_id");

CREATE TABLE IF NOT EXISTS "honertia_document_graph"."projection_publications" (
  "publication_id" text PRIMARY KEY,
  "mutation_id" text NOT NULL
    REFERENCES "honertia_document_graph"."projection_mutations" ("mutation_id"),
  "index_generation" text NOT NULL,
  "document_key" char(64) NOT NULL,
  "projection_id" text NOT NULL,
  "generation" bigint NOT NULL,
  "slot_high_water" integer NOT NULL,
  "status" text NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  UNIQUE ("index_generation", "document_key", "projection_id", "generation"),
  CONSTRAINT "projection_publications_status"
    CHECK ("status" IN ('pending', 'committed', 'superseded')),
  CONSTRAINT "projection_publications_generation" CHECK ("generation" > 0),
  CONSTRAINT "projection_publications_slot_high_water"
    CHECK ("slot_high_water" >= 0)
);

CREATE INDEX IF NOT EXISTS "projection_publications_mutation_idx"
  ON "honertia_document_graph"."projection_publications" ("mutation_id");
