PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS document_graph_nodes (
  graph_id TEXT NOT NULL,
  document_key TEXT NOT NULL,
  document_kind TEXT NOT NULL,
  encoded_document_id TEXT NOT NULL,
  node_state TEXT NOT NULL,
  PRIMARY KEY (graph_id, document_key),
  CONSTRAINT document_graph_nodes_document_key_sha256
    CHECK (
      length(document_key) = 64 AND
      document_key NOT GLOB '*[^0-9a-f]*'
    ),
  CONSTRAINT document_graph_nodes_reference_not_empty
    CHECK (
      length(trim(graph_id)) > 0 AND
      length(trim(document_kind)) > 0
    ),
  CONSTRAINT document_graph_nodes_encoded_id_json
    CHECK (json_valid(encoded_document_id)),
  CONSTRAINT document_graph_nodes_state
    CHECK (node_state IN ('Referenced', 'Materialized'))
);

CREATE INDEX IF NOT EXISTS document_graph_nodes_catalog_idx
  ON document_graph_nodes (
    graph_id,
    document_kind,
    node_state,
    document_key
  );

CREATE TABLE IF NOT EXISTS document_graph_relations (
  graph_id TEXT NOT NULL,
  relation_id TEXT NOT NULL,
  relation_version TEXT NOT NULL,
  source_document_key TEXT NOT NULL,
  source_document_kind TEXT NOT NULL,
  target_document_key TEXT NOT NULL,
  target_document_kind TEXT NOT NULL,
  PRIMARY KEY (
    graph_id,
    relation_id,
    source_document_key,
    target_document_key
  ),
  CONSTRAINT document_graph_relations_reference_not_empty
    CHECK (
      length(trim(graph_id)) > 0 AND
      length(trim(relation_id)) > 0 AND
      length(trim(relation_version)) > 0 AND
      length(trim(source_document_kind)) > 0 AND
      length(trim(target_document_kind)) > 0
    ),
  CONSTRAINT document_graph_relations_source_node_fk
    FOREIGN KEY (graph_id, source_document_key)
    REFERENCES document_graph_nodes (graph_id, document_key)
    ON DELETE CASCADE,
  CONSTRAINT document_graph_relations_target_node_fk
    FOREIGN KEY (graph_id, target_document_key)
    REFERENCES document_graph_nodes (graph_id, document_key)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS document_graph_relations_outgoing_idx
  ON document_graph_relations (
    graph_id,
    source_document_key,
    source_document_kind,
    relation_id,
    relation_version,
    target_document_kind,
    target_document_key
  );

CREATE INDEX IF NOT EXISTS document_graph_relations_incoming_idx
  ON document_graph_relations (
    graph_id,
    target_document_key,
    target_document_kind,
    relation_id,
    relation_version,
    source_document_kind,
    source_document_key
  );
