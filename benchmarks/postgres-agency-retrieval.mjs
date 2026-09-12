import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { performance } from "node:perf_hooks"
import { resolve } from "node:path"
import { pathToFileURL } from "node:url"
import { isDeepStrictEqual } from "node:util"
import { Client } from "pg"
import { Effect, Layer, Schema } from "effect"

// This historical runner rewrites the pre-optimization SQL. Supply the saved
// baseline package; use postgres-agency-production.mjs for current production.
assert(process.env.BENCHMARK_BASELINE_PACKAGE, "BENCHMARK_BASELINE_PACKAGE must point to the pre-optimization package")

const baseline = (name) => pathToFileURL(resolve(process.env.BENCHMARK_BASELINE_PACKAGE, "dist", name)).href

const { defineDocument, defineDocumentGraph, defineEmbeddingProfile, EmbeddingProvider } = await import(baseline("index.js"))

const { postgresDocumentGraph, postgresTransactionClient } = await import(baseline("postgres.js"))

const { makeChunkId, makeDocumentKey } = await import(baseline("document/document-identity.js"))

// Experiments only: real migrations and public graph retrieval, with SQL
// variants supplied through the PostgreSQL connection seam. Every fixture
// table and experimental schema change is rolled back at the end.
assert(process.env.TEST_DATABASE_URL, "TEST_DATABASE_URL is required")

const dimensions = 1_024

const measuredRuns = 5

const agencies = 1_000

const caseStudies = 9_000

const candidates = 50

const graphId = "agency-benchmark"

const schema = `agency_benchmark_${process.pid}`

const table = (name) => `"${schema}"."${name}"`

const client = new Client({ connectionString: process.env.TEST_DATABASE_URL })

const vector = Array.from({ length: dimensions }, (_, index) => Math.cos(index + 1))

const profile = defineEmbeddingProfile({ id: "bench", version: "v1", dimensions })

const embeddings = Layer.succeed(EmbeddingProvider, {
  profile,
  embedQuery: () => Effect.succeed(vector),
  embedDocuments: () => Effect.die(new Error("The read benchmark must not embed documents")),
})

const Agency = defineDocument(Schema.Struct({
  id: Schema.Number,
  title: Schema.String,
  body: Schema.String,
}), { id: "id" }).vectorise({
  id: "profile",
  version: "v1",
  select: (document) => ({ sections: [{ key: "body", content: document.body }] }),
})

const CaseStudy = defineDocument(Schema.Struct({
  id: Schema.Number,
  title: Schema.String,
  body: Schema.String,
  agencyIds: Schema.Array(Schema.Number),
}), { id: "id" }).vectorise({
  id: "evidence",
  version: "v1",
  select: (document) => ({ sections: [{ key: "body", content: document.body }] }),
})

const graph = defineDocumentGraph({
  id: graphId,
  documents: { Agency, CaseStudy },
  relations: (relation) => ({ deliveredBy: relation({
    from: "CaseStudy", to: "Agency", version: "v1", select: (document) => document.agencyIds,
  }) }),
})

const agencyProfile = graph.document("Agency").projection("profile")

const caseEvidence = graph.document("CaseStudy").projection("evidence")

const retrieval = (strategy) => graph.retrieval({
  target: "Agency",
  routes: [agencyProfile, caseEvidence.through("deliveredBy", { neighboursPerSource: 2 })],
  strategy,
  candidates: { semantic: candidates, text: candidates },
  maximumEvidencePerTarget: 3,
})

const plans = new Map(["semantic", "text", "hybrid"].map((strategy) => [strategy, retrieval(strategy)]))

const documents = []

const agencyKeys = new Map()

for (const kind of ["Agency", "CaseStudy"]) {
  const count = kind === "Agency" ? agencies : caseStudies

  for (let id = 1; id <= count; id += 1) {
    const agencyId = kind === "Agency" ? id : Math.floor((id - 1) / 9) + 1
    const relevant = kind === "Agency" ? id % 37 === 0 : agencyId % 10 === 0
    const projection = kind === "Agency" ? "profile" : "evidence"
    const key = makeDocumentKey({ graph: graphId, documentKind: kind, encodedId: id })

    if (kind === "Agency") agencyKeys.set(id, key)
    documents.push({
      id, kind, key, projection, agencyId, relevant,
      chunkId: makeChunkId({ documentKey: key, projection, sectionKey: "body", sectionPart: 0 }),
      content: relevant
        ? `Healthcare brand identity and medical campaign ${kind} ${id}`
        : `Retail transport and hospitality campaign ${kind} ${id}`,
      vectorSeed: kind === "Agency" ? caseStudies + id : id,
    })
  }
}

const edges = documents.filter((document) => document.kind === "CaseStudy").flatMap((document) => {
  const owners = document.id % 50 === 0
    ? [document.agencyId, document.agencyId % agencies + 1]
    : [document.agencyId]

  if (document.id % 100 === 0) owners.push((document.agencyId + 7) % agencies + 1)

  return owners.map((agencyId) => ({
    sourceKey: document.key, sourceId: document.id,
    targetKey: agencyKeys.get(agencyId), targetId: agencyId,
  }))
})

const median = (samples) => [...samples].sort((left, right) => left - right)[Math.floor(samples.length / 2)]

const queryNorm = Math.sqrt(vector.reduce((total, value) => total + value * value, 0))

const unchanged = (text, values) => ({ text, values })

const hoistQueryNorm = (text, values) => {
  if (!text.includes("sqrt(sum(component.query * component.query))")) return { text, values }

  return {
    text: text.replace("sqrt(sum(component.query * component.query))", `$${values.length + 1}::double precision`),
    values: [...values, queryNorm],
  }
}

const storedNorms = (text, values) => {
  const hoisted = hoistQueryNorm(text, values)

  return {
    text: hoisted.text.replace("c.metadata, c.embedding,", "c.metadata, c.embedding, c.embedding_norm,")
      .replace("sqrt(sum(component.stored * component.stored))", "scoped.embedding_norm"),
    values: hoisted.values,
  }
}

const cachedText = (text, values) => ({
  text: text.replaceAll("to_tsvector('english'::regconfig, coalesce(c.text_context, ''))", "c.rank_context_english")
    .replaceAll("to_tsvector('english'::regconfig, coalesce(c.text_label, ''))", "c.rank_label_english")
    .replaceAll("to_tsvector('english'::regconfig, c.text_content)", "c.rank_content_english"),
  values,
})

const nativeCosine = (text, values) => {
  if (!text.includes("FROM unnest(scoped.embedding,")) return { text, values }

  const transformed = text.replace("c.metadata, c.embedding,", "c.metadata, c.embedding_native AS embedding,")
    .replace(/SELECT sum\(component\.stored[\s\S]+?FROM unnest\(scoped\.embedding, \$1::double precision\[\]\) AS component\(stored, query\)/,
      "SELECT 1 - cosine_distance(scoped.embedding, $1::double precision[]::vector) AS score")
    .replace("WHERE similarity.score IS NOT NULL", "WHERE similarity.score IS NOT NULL AND similarity.score <> 'NaN'::double precision")

  assert(!transformed.includes("component.stored"), "Update the native experiment for the new cosine SQL")

  return { text: transformed, values }
}

const variants = [
  { name: "current", transform: unchanged },
  { name: "inline_scope", transform: (text, values) => ({
    text: text.replaceAll("scoped AS MATERIALIZED", "scoped AS NOT MATERIALIZED"), values,
  }) },
  { name: "query_norm_once", transform: hoistQueryNorm, strategies: ["semantic", "hybrid"] },
  { name: "batch_relations", transform: unchanged, batchRelations: true },
  { name: "stored_norms", transform: storedNorms, strategies: ["semantic", "hybrid"] },
  { name: "cached_text", transform: cachedText, strategies: ["text", "hybrid"] },
  { name: "combined", transform: (text, values) => {
    const norms = storedNorms(text, values)

    return cachedText(norms.text, norms.values)
  }, batchRelations: true },
]

const nativeSource = process.env.BENCHMARK_PGVECTOR_SOURCE

if (nativeSource) {
  variants.push({ name: "pgvector_exact", transform: nativeCosine, strategies: ["semantic", "hybrid"], native: true })
  variants.push({ name: "pgvector_exact_combined", transform: (text, values) => {
    const native = nativeCosine(text, values)

    return cachedText(native.text, native.values)
  }, batchRelations: true, strategies: ["semantic", "hybrid"], native: true })
}

const withoutScores = (value) => JSON.parse(JSON.stringify(value, (key, item) => key === "score" ? undefined : item))

await client.connect()

try {
  await client.query("BEGIN")
  await client.query("SET LOCAL statement_timeout = 60000")
  await client.query("SET LOCAL jit = off")

  for (const name of ["0001_initial.sql", "0002_mutation_locks.sql", "0003_graph_topology.sql"]) {
    const migration = await readFile(new URL(`../migrations/postgres/${name}`, import.meta.url), "utf8")
    await client.query(migration.replaceAll('"honertia_document_graph"', `"${schema}"`))
  }

  await client.query(`CREATE TEMP TABLE agency_fixture AS
    SELECT * FROM jsonb_to_recordset($1::jsonb) AS document(
      id integer, kind text, key char(64), projection text, "agencyId" integer,
      relevant boolean, "chunkId" char(64), content text, "vectorSeed" integer
    )`, [JSON.stringify(documents)])
  await client.query(`INSERT INTO ${table("projected_revisions")}
    (document_key, projection_id, graph_id, document_kind, encoded_document_id,
     projection_version, revision_hash, embedding_profile_id, embedding_profile_version, embedding_dimensions)
    SELECT key, projection, $1, kind, to_jsonb(id), 'v1', repeat('a',64), 'bench','v1',$2
    FROM agency_fixture`, [graphId, dimensions])
  await client.query(`INSERT INTO ${table("projected_chunks")}
    (chunk_id,document_key,projection_id,ordinal,section_key,section_index,section_part,
     content_hash,content,embedding_content,text_content,has_metadata,metadata,embedding_dimensions,embedding)
    SELECT "chunkId",key,projection,0,'body',0,0,repeat('b',64),content,content,content,false,null,$1,
      ARRAY(SELECT sin(d * ("vectorSeed" + 1)) +
        (CASE WHEN relevant THEN 1.0 ELSE 0.1 END) * cos(d)
        FROM generate_series(1,$1::int) AS d)
    FROM agency_fixture`, [dimensions])
  await client.query(`INSERT INTO ${table("graph_nodes")}
    (graph_id,document_key,document_kind,encoded_document_id,node_state)
    SELECT $1,key,kind,to_jsonb(id),'Materialized' FROM agency_fixture`, [graphId])
  await client.query(`INSERT INTO ${table("graph_relations")}
    (graph_id,relation_id,relation_version,source_document_key,source_document_kind,encoded_source_document_id,
     target_document_key,target_document_kind,encoded_target_document_id)
    SELECT $1,'deliveredBy','v1',"sourceKey",'CaseStudy',to_jsonb("sourceId"),
      "targetKey",'Agency',to_jsonb("targetId")
    FROM jsonb_to_recordset($2::jsonb) AS edge(
      "sourceKey" char(64),"sourceId" integer,"targetKey" char(64),"targetId" integer)`,
  [graphId, JSON.stringify(edges)])

  for (const name of ["projected_revisions", "projected_chunks", "graph_nodes", "graph_relations"]) {
    await client.query(`ANALYZE ${table(name)}`)
  }

  const originalChunkBytes = Number((await client.query(
    "SELECT pg_total_relation_size($1::regclass) AS bytes", [table("projected_chunks")],
  )).rows[0].bytes)

  const preprocessingStarted = performance.now()
  await client.query(`ALTER TABLE ${table("projected_chunks")}
    ADD COLUMN embedding_norm double precision,
    ADD COLUMN rank_context_english tsvector GENERATED ALWAYS AS
      (to_tsvector('english'::regconfig, coalesce(text_context, ''))) STORED,
    ADD COLUMN rank_label_english tsvector GENERATED ALWAYS AS
      (to_tsvector('english'::regconfig, coalesce(text_label, ''))) STORED,
    ADD COLUMN rank_content_english tsvector GENERATED ALWAYS AS
      (to_tsvector('english'::regconfig, text_content)) STORED`)
  await client.query(`UPDATE ${table("projected_chunks")} AS chunk SET embedding_norm =
    (SELECT sqrt(sum(value * value)) FROM unnest(chunk.embedding) AS value)`)
  const preprocessingMilliseconds = performance.now() - preprocessingStarted
  await client.query(`ANALYZE ${table("projected_chunks")}`)

  const experimentalChunkBytes = Number((await client.query(
    "SELECT pg_total_relation_size($1::regclass) AS bytes", [table("projected_chunks")],
  )).rows[0].bytes)

  if (nativeSource) {
    // Load only the vector type, scalar functions and casts into the disposable
    // schema. No system installation or ANN index is needed for this experiment.
    const nativeSql = (await readFile(resolve(nativeSource, "sql/vector.sql"), "utf8"))
      .split("-- access methods")[0].split("\n").filter((line) => !line.startsWith("\\")).join("\n")
      .replaceAll("MODULE_PATHNAME", resolve(nativeSource, "vector").replaceAll("'", "''"))

    await client.query(`SET LOCAL search_path TO "${schema}", pg_catalog`)
    await client.query(nativeSql)
    await client.query(`ALTER TABLE ${table("projected_chunks")} ADD COLUMN embedding_native vector(${dimensions})`)
    await client.query(`UPDATE ${table("projected_chunks")} SET embedding_native = embedding::vector`)
    await client.query(`ANALYZE ${table("projected_chunks")}`)
  }

  console.error(`Fixture ready: ${documents.length} documents, ${edges.length} case-study → agency relations`)
  const results = []
  const baselines = new Map()
  const capturedQueries = new Map()
  const nativeComparisons = new Map()

  for (const strategy of ["semantic", "text", "hybrid"]) {
    const measurements = variants.flatMap((variant) =>
      !variant.strategies || variant.strategies.includes(strategy)
        ? [{ variant, samples: [], statements: [], statementMilliseconds: [] }]
        : [],
    )

    for (let iteration = 0; iteration <= measuredRuns; iteration += 1) {
      const order = iteration % 2 === 0 ? measurements : [...measurements].reverse()

      for (const measurement of order) {
        const queries = []
        const timings = []
        let pendingQuery = Promise.resolve()

        const execute = (text, values) => {
          const transformed = measurement.variant.transform(text, values)
          queries.push(transformed)

          const next = pendingQuery.then(async () => {
            const started = performance.now()
            const response = await client.query(transformed.text, transformed.values)
            timings.push(performance.now() - started)

            return response
          })

          pendingQuery = next.then(() => undefined, () => undefined)

          return next
        }

        const caseStudyKeys = new Set()
        const relatedNodes = new Map()
        let relationBatch

        const transaction = postgresTransactionClient({ query: async (text, values) => {
          if (measurement.variant.batchRelations && text.includes(`FROM ${table("graph_relations")} AS edge`)) {
            assert.equal(values[0], graphId)
            assert.equal(values[2], "CaseStudy")
            assert.equal(values[3], "deliveredBy")
            assert.equal(values[4], "v1")
            assert.equal(values[5], "Agency")
            assert.equal(values[6], 2)
            assert(caseStudyKeys.has(values[1]), "A relation read escaped the retrieved source population")

            if (!relationBatch) {
              const inner = text.replace("edge.source_document_key = $2", "edge.source_document_key = requested.document_key")
              assert.notEqual(inner, text, "Update the batch experiment for the new relation SQL")

              const batch = `SELECT requested.document_key AS requested_source_key, related.*
                FROM unnest($2::char(64)[]) WITH ORDINALITY AS requested(document_key, position)
                CROSS JOIN LATERAL (${inner}) AS related
                ORDER BY requested.position, related.document_key`

              for (const key of caseStudyKeys) relatedNodes.set(key, [])
              relationBatch = execute(batch, [values[0], [...caseStudyKeys], ...values.slice(2)])
                .then((response) => {
                  for (const row of response.rows) {
                    const { requested_source_key: sourceKey, ...node } = row
                    relatedNodes.get(sourceKey).push(node)
                  }
                })
            }

            await relationBatch

            return { rows: relatedNodes.get(values[1]) }
          }

          const response = await execute(text, values)

          for (const row of response.rows) {
            if (row.document_kind === "CaseStudy") caseStudyKeys.add(row.document_key)
          }

          return response
        } })

        const started = performance.now()

        const output = await Effect.runPromise(plans.get(strategy).search("healthcare brand identity", { limit: 10 }).pipe(
          Effect.provide(Layer.merge(embeddings, postgresDocumentGraph({ transaction, schema }))),
        ))

        const elapsed = performance.now() - started

        if (measurement.variant.name === "current" && !baselines.has(strategy)) {
          baselines.set(strategy, output)
          capturedQueries.set(strategy, queries)
        }

        if (measurement.variant.native) {
          const baseline = baselines.get(strategy)

          const comparison = {
            targetOrderEqual: isDeepStrictEqual(output.map((row) => row.target), baseline.map((row) => row.target)),
            ranksAndEvidenceEqual: isDeepStrictEqual(withoutScores(output), withoutScores(baseline)),
            scoresBitIdentical: isDeepStrictEqual(output, baseline),
            targetIds: output.map((row) => row.target.id),
            top10Overlap: output.filter((row) => baseline.some((candidate) => candidate.target.id === row.target.id)).length,
          }

          nativeComparisons.set(`${strategy}/${measurement.variant.name}`, comparison)
        } else {
          assert.deepEqual(output, baselines.get(strategy), `${strategy}/${measurement.variant.name} changed agency rank or evidence`)
        }

        if (iteration > 0) {
          measurement.samples.push(elapsed)
          measurement.statements.push(queries.length)
          measurement.statementMilliseconds.push(timings.reduce((total, time) => total + time, 0))
        }
      }
    }

    for (const measurement of measurements) {
      const result = {
        strategy, variant: measurement.variant.name,
        medianMilliseconds: median(measurement.samples), samplesMilliseconds: measurement.samples,
        statementCounts: measurement.statements,
        // Execution is explicitly serialized on one connection in every variant.
        summedStatementMilliseconds: measurement.statementMilliseconds,
      }

      results.push(result)
      console.error(`${strategy}/${result.variant}: ${result.medianMilliseconds.toFixed(3)} ms, ${result.statementCounts[0]} statements`)
    }
  }

  const profileOnlyGate = {}

  for (const [strategy, baseline] of baselines) {
    const directQueries = capturedQueries.get(strategy).filter((query) => query.text.includes("scoped") &&
      query.values.some((value) => Array.isArray(value) && value.length === 1 && value[0] === "Agency"))

    assert(directQueries.length > 0, "Missing agency-profile query for the graph guardrail")
    const directKeys = new Set()

    for (const query of directQueries) {
      for (const row of (await client.query(query.text, query.values)).rows) directKeys.add(row.document_key)
    }

    const excluded = baseline.filter((row) => !directKeys.has(makeDocumentKey({
      graph: graphId, documentKind: "Agency", encodedId: row.target.id,
    })))

    profileOnlyGate[strategy] = {
      directCandidateAgencies: directKeys.size,
      top10AgenciesLostByProfileOnlyGate: excluded.map((row) => row.target.id),
    }
    assert(excluded.length > 0, "The fixture must exercise discovery through case studies")
  }

  console.log(JSON.stringify({
    postgres: (await client.query("SHOW server_version")).rows[0].server_version,
    node: process.version, measuredRuns, warmupRuns: 1, jit: false,
    fixture: { documents: documents.length, agencies, caseStudies, relations: edges.length, dimensions, chunksPerDocument: 1 },
    preprocessing: { milliseconds: preprocessingMilliseconds, originalChunkBytes, experimentalChunkBytes,
      note: "Size after ALTER/UPDATE includes dead tuples; this is not a steady-state storage overhead measurement." },
    query: { text: "healthcare brand identity", candidatesPerChannelPerRoute: candidates, resultLimit: 10 },
    nonNativeResultsBitIdentical: true,
    nativeComparisons: Object.fromEntries(nativeComparisons),
    profileOnlyGate,
    results,
    sampleResults: Object.fromEntries(baselines),
    capturedQueries: Object.fromEntries(capturedQueries),
  }, null, 2))
} finally {
  try {
    await client.query("ROLLBACK")
  } finally {
    await client.end()
  }
}
