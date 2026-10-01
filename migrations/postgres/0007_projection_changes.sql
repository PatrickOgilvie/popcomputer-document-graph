-- Projection changes awaiting a mirror, such as a Turbopuffer copy of the
-- index. Every write to projected_revisions, from any writer, marks its
-- document projection here in the same transaction, and
-- mirrorPostgresProjectionChanges drains the marks. A projection holds one
-- row however often it changes, so the set stays no larger than the index
-- when nothing drains it.

CREATE SEQUENCE IF NOT EXISTS "honertia_document_graph"."projection_change_sequence";

CREATE TABLE IF NOT EXISTS "honertia_document_graph"."projection_changes" (
  "document_key" char(64) NOT NULL,
  "projection_id" text NOT NULL,
  "graph_id" text NOT NULL,
  -- Renewed on every change, so a drain that read an older value leaves a
  -- newer change in place.
  "change_sequence" bigint NOT NULL,
  "changed_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("document_key", "projection_id")
);

CREATE INDEX IF NOT EXISTS "projection_changes_graph_sequence_idx"
  ON "honertia_document_graph"."projection_changes"
  ("graph_id", "change_sequence");

CREATE OR REPLACE FUNCTION "honertia_document_graph"."record_projection_change"()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  changed record;
BEGIN
  IF TG_OP = 'DELETE' THEN
    changed := OLD;
  ELSE
    changed := NEW;
  END IF;

  INSERT INTO "honertia_document_graph"."projection_changes"
    ("document_key", "projection_id", "graph_id", "change_sequence", "changed_at")
  VALUES (
    changed.document_key,
    changed.projection_id,
    changed.graph_id,
    nextval('"honertia_document_graph"."projection_change_sequence"'),
    now()
  )
  ON CONFLICT ("document_key", "projection_id") DO UPDATE SET
    "graph_id" = EXCLUDED."graph_id",
    "change_sequence" = EXCLUDED."change_sequence",
    "changed_at" = EXCLUDED."changed_at";

  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS "projected_revisions_record_change"
  ON "honertia_document_graph"."projected_revisions";

CREATE TRIGGER "projected_revisions_record_change"
  AFTER INSERT OR UPDATE OR DELETE
  ON "honertia_document_graph"."projected_revisions"
  FOR EACH ROW
  EXECUTE FUNCTION "honertia_document_graph"."record_projection_change"();
