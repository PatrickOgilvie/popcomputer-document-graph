-- Keep canonical embeddings as float64 arrays. This extension-free predicate
-- records whether native float32 cosine accumulation is safe (up to 16k terms).
-- Evaluate it on writes, not for every candidate in every semantic search.
CREATE OR REPLACE FUNCTION "honertia_document_graph"."native_vector_eligible"(embedding double precision[])
RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$
  SELECT cardinality(embedding) BETWEEN 1 AND 16000
     AND array_ndims(embedding) = 1
     AND coalesce(bool_and(component IS NOT NULL), false)
     AND coalesce(max(abs(component)) BETWEEN 1e-18 AND 1e15, false)
  FROM unnest(embedding) AS component
$$;

ALTER TABLE "honertia_document_graph"."projected_chunks"
  ADD COLUMN IF NOT EXISTS "embedding_native_eligible" boolean
  GENERATED ALWAYS AS (
    "honertia_document_graph"."native_vector_eligible"("embedding")
  ) STORED;
