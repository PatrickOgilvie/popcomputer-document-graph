CREATE SCHEMA IF NOT EXISTS "honertia_document_graph";

CREATE TABLE IF NOT EXISTS "honertia_document_graph"."projected_revisions" (
  "document_key" char(64) NOT NULL,
  "projection_id" text NOT NULL,
  "graph_id" text NOT NULL,
  "document_kind" text NOT NULL,
  "encoded_document_id" jsonb NOT NULL,
  "projection_version" text NOT NULL,
  "revision_hash" char(64) NOT NULL,
  "embedding_profile_id" text NOT NULL,
  "embedding_profile_version" text NOT NULL,
  "embedding_dimensions" integer NOT NULL,
  "revision_token" bigint NOT NULL DEFAULT 1,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("document_key", "projection_id"),
  UNIQUE ("document_key", "projection_id", "embedding_dimensions"),
  CONSTRAINT "projected_revisions_document_key_sha256"
    CHECK ("document_key" ~ '^[0-9a-f]{64}$'),
  CONSTRAINT "projected_revisions_revision_hash_sha256"
    CHECK ("revision_hash" ~ '^[0-9a-f]{64}$'),
  CONSTRAINT "projected_revisions_projection_id_length"
    CHECK (length("projection_id") BETWEEN 1 AND 100),
  CONSTRAINT "projected_revisions_projection_id_format"
    CHECK ("projection_id" ~ '^[a-z0-9]+([._-][a-z0-9]+)*$'),
  CONSTRAINT "projected_revisions_projection_version_length"
    CHECK (length("projection_version") BETWEEN 1 AND 100),
  CONSTRAINT "projected_revisions_projection_version_format"
    CHECK ("projection_version" ~ '^[a-z0-9]+([._-][a-z0-9]+)*$'),
  CONSTRAINT "projected_revisions_reference_not_empty"
    CHECK (length(btrim("graph_id")) > 0 AND length(btrim("document_kind")) > 0),
  CONSTRAINT "projected_revisions_embedding_dimensions"
    CHECK ("embedding_dimensions" BETWEEN 1 AND 65536),
  CONSTRAINT "projected_revisions_revision_token"
    CHECK ("revision_token" > 0)
);

CREATE INDEX IF NOT EXISTS "projected_revisions_scope_idx"
  ON "honertia_document_graph"."projected_revisions"
  ("graph_id", "document_kind", "projection_id");

CREATE INDEX IF NOT EXISTS "projected_revisions_embedding_profile_idx"
  ON "honertia_document_graph"."projected_revisions"
  ("embedding_profile_id", "embedding_profile_version", "embedding_dimensions");

CREATE TABLE IF NOT EXISTS "honertia_document_graph"."projected_chunks" (
  "chunk_id" char(64) PRIMARY KEY,
  "document_key" char(64) NOT NULL,
  "projection_id" text NOT NULL,
  "ordinal" integer NOT NULL,
  "section_key" text NOT NULL,
  "section_index" integer NOT NULL,
  "section_part" integer NOT NULL,
  "content_hash" char(64) NOT NULL,
  "content" text NOT NULL,
  "embedding_content" text NOT NULL,
  "text_context" text,
  "text_label" text,
  "text_content" text NOT NULL,
  "text_search_english" tsvector GENERATED ALWAYS AS (
    to_tsvector(
      'english'::regconfig,
      coalesce("text_context", '') || ' ' ||
      coalesce("text_label", '') || ' ' || "text_content"
    )
  ) STORED,
  "text_search_simple" tsvector GENERATED ALWAYS AS (
    to_tsvector(
      'simple'::regconfig,
      coalesce("text_context", '') || ' ' ||
      coalesce("text_label", '') || ' ' || "text_content"
    )
  ) STORED,
  "has_metadata" boolean NOT NULL,
  "metadata" jsonb,
  "embedding_dimensions" integer NOT NULL,
  "embedding" double precision[] NOT NULL,
  CONSTRAINT "projected_chunks_revision_fk"
    FOREIGN KEY (
      "document_key",
      "projection_id",
      "embedding_dimensions"
    )
    REFERENCES "honertia_document_graph"."projected_revisions"
      ("document_key", "projection_id", "embedding_dimensions")
    ON DELETE CASCADE,
  CONSTRAINT "projected_chunks_content_hash_sha256"
    CHECK ("content_hash" ~ '^[0-9a-f]{64}$'),
  CONSTRAINT "projected_chunks_chunk_id_sha256"
    CHECK ("chunk_id" ~ '^[0-9a-f]{64}$'),
  CONSTRAINT "projected_chunks_non_negative_positions"
    CHECK ("ordinal" >= 0 AND "section_index" >= 0 AND "section_part" >= 0),
  CONSTRAINT "projected_chunks_content_not_empty"
    CHECK (
      length(btrim("section_key")) > 0 AND
      length(btrim("content")) > 0 AND
      length(btrim("embedding_content")) > 0 AND
      length(btrim("text_content")) > 0
    ),
  CONSTRAINT "projected_chunks_embedding_dimensions"
    CHECK (
      "embedding_dimensions" BETWEEN 1 AND 65536 AND
      cardinality("embedding") = "embedding_dimensions"
    ),
  CONSTRAINT "projected_chunks_metadata_presence"
    CHECK (
      ("has_metadata" AND "metadata" IS NOT NULL) OR
      (NOT "has_metadata" AND "metadata" IS NULL)
    ),
  UNIQUE ("document_key", "projection_id", "ordinal"),
  UNIQUE ("document_key", "projection_id", "section_key", "section_part")
);

CREATE INDEX IF NOT EXISTS "projected_chunks_revision_idx"
  ON "honertia_document_graph"."projected_chunks"
  ("document_key", "projection_id");

CREATE INDEX IF NOT EXISTS "projected_chunks_metadata_gin_idx"
  ON "honertia_document_graph"."projected_chunks"
  USING gin ("metadata" jsonb_path_ops)
  WHERE "has_metadata";

CREATE INDEX IF NOT EXISTS "projected_chunks_text_english_gin_idx"
  ON "honertia_document_graph"."projected_chunks"
  USING gin ("text_search_english");

CREATE INDEX IF NOT EXISTS "projected_chunks_text_simple_gin_idx"
  ON "honertia_document_graph"."projected_chunks"
  USING gin ("text_search_simple");

CREATE TABLE IF NOT EXISTS "honertia_document_graph"."graph_relations" (
  "graph_id" text NOT NULL,
  "relation_id" text NOT NULL,
  "relation_version" text NOT NULL,
  "source_document_key" char(64) NOT NULL,
  "source_document_kind" text NOT NULL,
  "encoded_source_document_id" jsonb NOT NULL,
  "target_document_key" char(64) NOT NULL,
  "target_document_kind" text NOT NULL,
  "encoded_target_document_id" jsonb NOT NULL,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (
    "graph_id",
    "relation_id",
    "source_document_key",
    "target_document_key"
  ),
  CONSTRAINT "graph_relations_source_key_sha256"
    CHECK ("source_document_key" ~ '^[0-9a-f]{64}$'),
  CONSTRAINT "graph_relations_target_key_sha256"
    CHECK ("target_document_key" ~ '^[0-9a-f]{64}$'),
  CONSTRAINT "graph_relations_relation_id_length"
    CHECK (length("relation_id") BETWEEN 1 AND 100),
  CONSTRAINT "graph_relations_relation_id_format"
    CHECK ("relation_id" ~ '^[A-Za-z0-9]+([._-][A-Za-z0-9]+)*$'),
  CONSTRAINT "graph_relations_relation_version_length"
    CHECK (length("relation_version") BETWEEN 1 AND 100),
  CONSTRAINT "graph_relations_relation_version_format"
    CHECK ("relation_version" ~ '^[a-z0-9]+([._-][a-z0-9]+)*$'),
  CONSTRAINT "graph_relations_reference_not_empty"
    CHECK (
      length(btrim("graph_id")) > 0 AND
      length(btrim("source_document_kind")) > 0 AND
      length(btrim("target_document_kind")) > 0
    )
);

CREATE INDEX IF NOT EXISTS "graph_relations_outgoing_idx"
  ON "honertia_document_graph"."graph_relations" (
    "graph_id",
    "source_document_kind",
    "source_document_key",
    "relation_id",
    "relation_version",
    "target_document_kind",
    "target_document_key"
  );

CREATE INDEX IF NOT EXISTS "graph_relations_incoming_idx"
  ON "honertia_document_graph"."graph_relations" (
    "graph_id",
    "target_document_kind",
    "target_document_key",
    "relation_id",
    "relation_version",
    "source_document_kind",
    "source_document_key"
  );
