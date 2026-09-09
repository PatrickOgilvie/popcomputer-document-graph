CREATE TABLE IF NOT EXISTS "honertia_document_graph"."graph_nodes" (
  "graph_id" text NOT NULL,
  "document_key" char(64) NOT NULL,
  "document_kind" text NOT NULL,
  "encoded_document_id" jsonb NOT NULL,
  "node_state" text NOT NULL,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("graph_id", "document_key"),
  CONSTRAINT "graph_nodes_document_key_sha256"
    CHECK ("document_key" ~ '^[0-9a-f]{64}$'),
  CONSTRAINT "graph_nodes_reference_not_empty"
    CHECK (
      length(btrim("graph_id")) > 0 AND
      length(btrim("document_kind")) > 0
    ),
  CONSTRAINT "graph_nodes_state"
    CHECK ("node_state" IN ('Referenced', 'Materialized'))
);

CREATE INDEX IF NOT EXISTS "graph_nodes_catalog_idx"
  ON "honertia_document_graph"."graph_nodes"
  ("graph_id", "document_kind", "node_state", "document_key");

INSERT INTO "honertia_document_graph"."graph_nodes"
  (graph_id, document_key, document_kind, encoded_document_id, node_state)
SELECT DISTINCT ON (graph_id, document_key)
       graph_id, document_key, document_kind, encoded_document_id,
       'Materialized'
FROM "honertia_document_graph"."projected_revisions"
ORDER BY graph_id, document_key, projection_id
ON CONFLICT (graph_id, document_key) DO UPDATE
  SET document_kind = EXCLUDED.document_kind,
      encoded_document_id = EXCLUDED.encoded_document_id,
      node_state = 'Materialized',
      updated_at = now();

INSERT INTO "honertia_document_graph"."graph_nodes"
  (graph_id, document_key, document_kind, encoded_document_id, node_state)
SELECT DISTINCT ON (graph_id, source_document_key)
       graph_id, source_document_key, source_document_kind,
       encoded_source_document_id, 'Materialized'
FROM "honertia_document_graph"."graph_relations"
ORDER BY graph_id, source_document_key, relation_id, target_document_key
ON CONFLICT (graph_id, document_key) DO UPDATE
  SET document_kind = EXCLUDED.document_kind,
      encoded_document_id = EXCLUDED.encoded_document_id,
      node_state = 'Materialized',
      updated_at = now();

INSERT INTO "honertia_document_graph"."graph_nodes"
  (graph_id, document_key, document_kind, encoded_document_id, node_state)
SELECT DISTINCT ON (graph_id, target_document_key)
       graph_id, target_document_key, target_document_kind,
       encoded_target_document_id, 'Referenced'
FROM "honertia_document_graph"."graph_relations"
ORDER BY graph_id, target_document_key, relation_id, source_document_key
ON CONFLICT (graph_id, document_key) DO UPDATE
  SET document_kind = EXCLUDED.document_kind,
      encoded_document_id = EXCLUDED.encoded_document_id,
      node_state = CASE
        WHEN "honertia_document_graph"."graph_nodes"."node_state" =
          'Materialized' THEN 'Materialized'
        ELSE 'Referenced'
      END,
      updated_at = now();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'graph_relations_source_node_fk'
      AND conrelid =
        '"honertia_document_graph"."graph_relations"'::regclass
  ) THEN
    ALTER TABLE "honertia_document_graph"."graph_relations"
      ADD CONSTRAINT "graph_relations_source_node_fk"
      FOREIGN KEY (graph_id, source_document_key)
      REFERENCES "honertia_document_graph"."graph_nodes"
        (graph_id, document_key)
      ON DELETE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'graph_relations_target_node_fk'
      AND conrelid =
        '"honertia_document_graph"."graph_relations"'::regclass
  ) THEN
    ALTER TABLE "honertia_document_graph"."graph_relations"
      ADD CONSTRAINT "graph_relations_target_node_fk"
      FOREIGN KEY (graph_id, target_document_key)
      REFERENCES "honertia_document_graph"."graph_nodes"
        (graph_id, document_key)
      ON DELETE CASCADE;
  END IF;
END $$;

-- Historical 0.3 storage could not represent materialized nodes with neither
-- a projection nor an outgoing relation. Re-index canonical source documents
-- after this migration to reconstruct those nodes.
