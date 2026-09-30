-- Guards the optional halfvec HNSW expression index built by
-- postgresVectorIndexSql. halfvec holds magnitudes up to 65,504, so indexing a
-- larger component would make the index cast fail and reject the write. The
-- smallest normal half (2^-14) keeps every indexed vector non-zero after
-- conversion. The index predicate evaluates this once per write; searches that
-- use the index never evaluate it.
CREATE OR REPLACE FUNCTION "honertia_document_graph"."native_halfvec_eligible"(embedding double precision[])
RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$
  SELECT cardinality(embedding) BETWEEN 1 AND 4000
     AND array_ndims(embedding) = 1
     AND coalesce(bool_and(component IS NOT NULL), false)
     AND coalesce(max(abs(component)) BETWEEN 6.103515625e-05 AND 65504, false)
  FROM unnest(embedding) AS component
$$;
