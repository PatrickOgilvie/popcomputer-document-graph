CREATE TABLE IF NOT EXISTS document_graph_projection_mutations (
  mutation_id TEXT PRIMARY KEY,
  document_key TEXT NOT NULL,
  projection_id TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  operation TEXT NOT NULL CHECK (operation IN ('replace', 'delete')),
  expected_token TEXT,
  next_token TEXT,
  revision_hash TEXT,
  embedding_profile_id TEXT,
  embedding_profile_version TEXT,
  embedding_dimensions INTEGER,
  graph_id TEXT,
  document_kind TEXT,
  projection_version TEXT,
  live_slot_count INTEGER NOT NULL,
  required_slot_high_water INTEGER NOT NULL,
  commit_inserted INTEGER NOT NULL,
  commit_updated INTEGER NOT NULL,
  commit_deleted INTEGER NOT NULL,
  deletion_revisions INTEGER NOT NULL,
  deletion_chunks INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  CHECK (length(document_key) = 64),
  CHECK (length(payload_digest) = 64),
  CHECK (live_slot_count >= 0),
  CHECK (required_slot_high_water >= live_slot_count),
  UNIQUE (document_key, projection_id, mutation_id)
) STRICT;

CREATE TABLE IF NOT EXISTS document_graph_projection_mutation_chunks (
  mutation_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  chunk_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  PRIMARY KEY (mutation_id, ordinal),
  UNIQUE (mutation_id, chunk_id),
  FOREIGN KEY (mutation_id)
    REFERENCES document_graph_projection_mutations(mutation_id)
    ON DELETE CASCADE,
  CHECK (ordinal >= 0),
  CHECK (length(chunk_id) = 64),
  CHECK (length(content_hash) = 64)
) STRICT;

CREATE TABLE IF NOT EXISTS document_graph_projection_heads (
  document_key TEXT NOT NULL,
  projection_id TEXT NOT NULL,
  index_generation TEXT NOT NULL,
  head_version INTEGER NOT NULL DEFAULT 0,
  last_allocated_generation INTEGER NOT NULL DEFAULT 0,
  slot_high_water INTEGER NOT NULL DEFAULT 0,
  active_mutation_id TEXT,
  active_token TEXT,
  active_status TEXT NOT NULL DEFAULT 'never'
    CHECK (active_status IN ('never', 'revision', 'deleted')),
  pending_mutation_id TEXT,
  pending_payload_digest TEXT,
  pending_publication_id TEXT,
  pending_generation INTEGER,
  pending_operation TEXT CHECK (pending_operation IN ('replace', 'delete')),
  pending_slot_high_water INTEGER,
  pending_lease_expires_at INTEGER,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (index_generation, document_key, projection_id),
  FOREIGN KEY (active_mutation_id)
    REFERENCES document_graph_projection_mutations(mutation_id),
  FOREIGN KEY (pending_mutation_id)
    REFERENCES document_graph_projection_mutations(mutation_id),
  CHECK (length(document_key) = 64),
  CHECK (length(projection_id) > 0),
  CHECK (length(trim(index_generation)) BETWEEN 1 AND 256),
  CHECK (last_allocated_generation >= 0),
  CHECK (slot_high_water >= 0),
  CHECK (
    (active_status = 'never'
      AND active_mutation_id IS NULL
      AND active_token IS NULL)
    OR
    (active_status = 'revision'
      AND active_mutation_id IS NOT NULL
      AND active_token IS NOT NULL)
    OR
    (active_status = 'deleted'
      AND active_mutation_id IS NOT NULL
      AND active_token IS NULL)
  ),
  CHECK (
    (pending_mutation_id IS NULL
      AND pending_payload_digest IS NULL
      AND pending_publication_id IS NULL
      AND pending_generation IS NULL
      AND pending_operation IS NULL
      AND pending_slot_high_water IS NULL
      AND pending_lease_expires_at IS NULL)
    OR
    (pending_mutation_id IS NOT NULL
      AND pending_payload_digest IS NOT NULL
      AND pending_publication_id IS NOT NULL
      AND pending_generation > 0
      AND pending_operation IS NOT NULL
      AND pending_slot_high_water >= 0
      AND pending_lease_expires_at IS NOT NULL)
  )
) STRICT;

CREATE INDEX IF NOT EXISTS document_graph_projection_heads_catalog_idx
  ON document_graph_projection_heads(
    index_generation,
    active_status,
    active_mutation_id
  );

CREATE INDEX IF NOT EXISTS document_graph_projection_heads_active_mutation_idx
  ON document_graph_projection_heads(active_mutation_id);

CREATE INDEX IF NOT EXISTS document_graph_projection_heads_pending_mutation_idx
  ON document_graph_projection_heads(pending_mutation_id);

CREATE TABLE IF NOT EXISTS document_graph_projection_publications (
  publication_id TEXT PRIMARY KEY,
  mutation_id TEXT NOT NULL,
  document_key TEXT NOT NULL,
  projection_id TEXT NOT NULL,
  index_generation TEXT NOT NULL,
  generation INTEGER NOT NULL,
  slot_high_water INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'committed', 'superseded')),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (mutation_id)
    REFERENCES document_graph_projection_mutations(mutation_id),
  CHECK (length(document_key) = 64),
  CHECK (length(projection_id) > 0),
  CHECK (length(trim(index_generation)) BETWEEN 1 AND 256),
  UNIQUE (index_generation, document_key, projection_id, generation),
  CHECK (generation > 0),
  CHECK (slot_high_water >= 0)
) STRICT;

CREATE INDEX IF NOT EXISTS document_graph_projection_publications_pending_idx
  ON document_graph_projection_publications(status, updated_at);

CREATE INDEX IF NOT EXISTS document_graph_projection_publications_mutation_idx
  ON document_graph_projection_publications(mutation_id);
