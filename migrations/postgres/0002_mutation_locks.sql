CREATE TABLE IF NOT EXISTS "honertia_document_graph"."mutation_locks" (
  "mutation_kind" text NOT NULL,
  "scope_key" text NOT NULL,
  "member_key" text NOT NULL,
  CONSTRAINT "mutation_locks_identity" PRIMARY KEY ("mutation_kind", "scope_key", "member_key"),
  CONSTRAINT "mutation_locks_mutation_kind"
    CHECK ("mutation_kind" IN ('projection', 'relations'))
);
