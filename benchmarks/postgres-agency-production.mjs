import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { performance } from "node:perf_hooks"
import { resolve } from "node:path"
import { pathToFileURL } from "node:url"
import { isDeepStrictEqual } from "node:util"
import { Client } from "pg"
import { Effect, Layer, Schema } from "effect"
import * as current from "../dist/index.js"
import { postgresDocumentGraph, postgresTransactionClient } from "../dist/postgres.js"
import { makeChunkId, makeDocumentKey } from "../dist/document/document-identity.js"

// Measure the production public graph API. No SQL rewriting, private helper
// calls, synthetic relation caches or precomputed native vectors are involved.
// Tables and optional CREATE EXTENSION live inside a rolled-back transaction.
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

const profile = current.defineEmbeddingProfile({ id: "bench", version: "v1", dimensions })

const embeddings = Layer.succeed(current.EmbeddingProvider, {
  profile,
  embedQuery: () => Effect.succeed(vector),
  embedDocuments: () => Effect.die(new Error("The read benchmark must not embed documents")),
})

const makePlans = (api) => {
  const { defineDocument, defineDocumentGraph } = api

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

return new Map(["semantic", "text", "hybrid"].map((strategy) => [strategy, retrieval(strategy)]))
}

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

const withoutScores = (value) => JSON.parse(JSON.stringify(value, (key, item) => key === "score" ? undefined : item))

const plans = makePlans(current)

const variants = [
  { name: "float64", plans, storage: (transaction) => postgresDocumentGraph({ transaction, schema, vectorSearch: "float64" }) },
  { name: "auto", plans, storage: (transaction) => postgresDocumentGraph({ transaction, schema }) },
]

const baselineDirectory = process.env.BENCHMARK_BASELINE_PACKAGE

if (baselineDirectory) {
  const baseline = await import(pathToFileURL(resolve(baselineDirectory, "dist/index.js")).href)
  const postgres = await import(pathToFileURL(resolve(baselineDirectory, "dist/postgres.js")).href)
  variants.unshift({ name: "baseline", plans: makePlans(baseline), storage: (transaction) => postgres.postgresDocumentGraph({ transaction, schema }) })
}

await client.connect()

try {
  await client.query("BEGIN")
  await client.query("SET LOCAL statement_timeout = 60000")
  await client.query("SET LOCAL jit = off")

  for (const name of ["0001_initial.sql", "0002_mutation_locks.sql", "0003_graph_topology.sql", "0004_native_vector_eligibility.sql"]) {
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

  if (process.env.BENCHMARK_ENABLE_PGVECTOR === "true") {
    await client.query(`CREATE EXTENSION vector WITH SCHEMA "${schema}"`)
  }

  const extension = await client.query("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
  console.error(`Fixture ready: ${documents.length} documents, ${edges.length} relations, pgvector=${extension.rows[0]?.extversion ?? "absent"}`)
  const measurements = []
  const comparisons = []

  for (const strategy of ["semantic", "text", "hybrid"]) {
    const reference = new Map()
    const items = variants.map((variant) => ({ variant, samples: [], counts: [] }))

    for (let iteration = 0; iteration <= measuredRuns; iteration += 1) {
      for (const item of iteration % 2 === 0 ? items : [...items].reverse()) {
        let count = 0
        let pending = Promise.resolve()

        const transaction = postgresTransactionClient({ query: (text, values) => {
          count += 1
          // All variants use the same single, explicitly serialized connection.
          const next = pending.then(() => client.query(text, [...(values ?? [])]))
          pending = next.then(() => undefined, () => undefined)

          return next
        } })

        const started = performance.now()

        const result = await Effect.runPromise(item.variant.plans.get(strategy).search("healthcare brand identity", { limit: 10 }).pipe(
          Effect.provide(Layer.merge(embeddings, item.variant.storage(transaction))),
        ))

        const elapsed = performance.now() - started

        if (iteration === 0) reference.set(item.variant.name, result)
        else assert.deepEqual(result, reference.get(item.variant.name), "Repeated production results changed")

        if (iteration > 0) { item.samples.push(elapsed); item.counts.push(count) }
      }
    }

    const baseline = reference.get("baseline") ?? reference.get("float64")

    for (const item of items) {
      const result = reference.get(item.variant.name)

      const comparison = {
        strategy, variant: item.variant.name,
        targetOrderEqual: isDeepStrictEqual(result.map((row) => row.target), baseline.map((row) => row.target)),
        ranksAndEvidenceEqual: isDeepStrictEqual(withoutScores(result), withoutScores(baseline)),
        scoresBitIdentical: isDeepStrictEqual(result, baseline),
      }

      assert(comparison.ranksAndEvidenceEqual, `${strategy}/${item.variant.name} changed fixture ranks or evidence`)

      if (item.variant.name !== "auto" || extension.rows.length === 0) assert(comparison.scoresBitIdentical)
      comparisons.push(comparison)

      const measurement = {
        strategy, variant: item.variant.name,
        medianMilliseconds: median(item.samples), samplesMilliseconds: item.samples,
        statementCounts: item.counts,
      }

      measurements.push(measurement)
      console.error(`${strategy}/${item.variant.name}: ${measurement.medianMilliseconds.toFixed(2)} ms, ${item.counts[0]} statements`)
    }
  }

  console.log(JSON.stringify({
    fixture: { agencies, caseStudies, documents: documents.length, relations: edges.length, dimensions,
      candidatesPerRoutePerChannel: candidates, neighboursPerSource: 2, results: 10 },
    runtime: { node: process.version, postgres: (await client.query("SHOW server_version")).rows[0].server_version,
      pgvector: extension.rows[0]?.extversion ?? null, jit: false },
    measuredRuns, warmupRuns: 1, includesEmbeddingApiLatency: false,
    includesCapabilityDiscovery: true, serializedConnection: true,
    comparisons, measurements,
  }, null, 2))
} finally {
  try { await client.query("ROLLBACK") } finally { await client.end() }
}
