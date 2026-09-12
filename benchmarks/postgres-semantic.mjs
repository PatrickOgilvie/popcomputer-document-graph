import assert from "node:assert/strict"
import { Client } from "pg"
import { Effect, Schema } from "effect"
import { postgresDocumentGraph, postgresTransactionClient } from "../dist/postgres.js"
import { defineEmbeddingProfile } from "../dist/indexing/embedding-provider.js"
import { DocumentKeySchema } from "../dist/document/document-identity.js"
import {
  documentKeys,
  makeGraphSearchScope,
  ProjectionSearchStore,
  SearchResultCountSchema,
} from "../dist/retrieval/graph-retrieval.js"

// Build first, then supply TEST_DATABASE_URL. All data lives in temporary
// tables in this connection; the final rollback leaves the database unchanged.
assert(process.env.TEST_DATABASE_URL, "TEST_DATABASE_URL is required")

const client = new Client({ connectionString: process.env.TEST_DATABASE_URL })

const documents = 10_000

const dimensions = 1_024

const runs = 5

await client.connect()

try {
  await client.query("BEGIN")
  await client.query("SET LOCAL statement_timeout = 60000")
  await client.query("SET LOCAL jit = off")
  await client.query(`CREATE TEMP TABLE projected_revisions (
    document_key char(64), projection_id text, graph_id text, document_kind text,
    encoded_document_id jsonb, projection_version text, revision_hash char(64),
    embedding_profile_id text, embedding_profile_version text, embedding_dimensions integer,
    PRIMARY KEY(document_key, projection_id))`)
  await client.query(`CREATE TEMP TABLE projected_chunks (
    chunk_id char(64) PRIMARY KEY, document_key char(64), projection_id text, section_key text,
    section_part integer, content text, has_metadata boolean, metadata jsonb, embedding double precision[])`)
  await client.query("CREATE INDEX ON projected_chunks(document_key, projection_id)")
  await client.query(`INSERT INTO projected_revisions SELECT lpad(to_hex(i),64,'0'),
    'content','bench','Article',to_jsonb(i),'v1',repeat('a',64),'bench','v1',$2
    FROM generate_series(1,$1::int) AS i`, [documents, dimensions])
  await client.query(`INSERT INTO projected_chunks SELECT document_key,document_key,projection_id,
    'body',0,'Evidence',false,null,
    ARRAY(SELECT sin(d * (encoded_document_id::text::int + 1)) FROM generate_series(1,$1::int) AS d)
    FROM projected_revisions`, [dimensions])
  await client.query("ANALYZE projected_revisions")
  await client.query("ANALYZE projected_chunks")

  const queries = []

  const transaction = postgresTransactionClient({ query: (text, values) => {
    queries.push({ text, values })

    return Promise.resolve({ rows: [] })
  } })

  const vector = Array.from({ length: dimensions }, (_, index) => Math.cos(index + 1))
  const selected = Schema.decodeSync(DocumentKeySchema)("1".padStart(64, "0"))

  for (const target of [undefined, documentKeys([selected])]) {
    await Effect.runPromise(ProjectionSearchStore.pipe(
      Effect.flatMap((store) => store.searchCandidates({
        vector,
        embeddingProfile: defineEmbeddingProfile({ id: "bench", version: "v1", dimensions }),
        scope: makeGraphSearchScope("bench", { target }),
        candidates: Schema.decodeSync(SearchResultCountSchema)(10),
      })),
      Effect.provide(postgresDocumentGraph({ vectorSearch: "float64", transaction, schema: "pg_temp" })),
    ))
  }

  const [current, indexed] = queries
  assert(current && indexed)
  const expansion = "FROM unnest(scoped.embedding, $1::double precision[]) AS component(stored, query)"
  assert(current.text.includes(expansion), "Update the benchmark for the new cosine SQL")

  // Reconstruct the previous positional-join implementation, keeping every
  // other part of the production query, parameters, data, and plan settings equal.
  const previous = {
    ...current,
    text: current.text.replaceAll("component.stored", "stored.value")
      .replaceAll("component.query", "query.value")
      .replace(expansion, `FROM unnest(scoped.embedding) WITH ORDINALITY AS stored(value, ordinal)
        INNER JOIN unnest($1::double precision[]) WITH ORDINALITY AS query(value, ordinal) USING (ordinal)`),
  }

  const cast = {
    ...indexed,
    text: indexed.text.replace(/r.document_key = ANY\((\$\d+)::char\(64\)\[\]\)/,
      "r.document_key::text = ANY($1::text[])"),
  }

  assert.notEqual(cast.text, indexed.text, "Update the benchmark for the new target SQL")

  const cases = [
    { name: "cosine_previous", query: previous, samples: [] },
    { name: "cosine_current", query: current, samples: [] },
    { name: "target_column_cast", query: cast, samples: [] },
    { name: "target_indexed", query: indexed, samples: [] },
  ]

  const plans = new Map()

  for (let iteration = 0; iteration <= runs; iteration += 1) {
    const ordered = iteration % 2 === 0 ? cases : [...cases].reverse()

    for (const item of ordered) {
      const result = await client.query(
        `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${item.query.text}`, item.query.values,
      )

      const plan = result.rows[0]["QUERY PLAN"][0]

      if (iteration > 0) item.samples.push(plan["Execution Time"])
      plans.set(item.name, plan)
    }
  }

  const oldRows = await client.query(previous.text, previous.values)
  const newRows = await client.query(current.text, current.values)
  assert.deepEqual(newRows.rows, oldRows.rows, "Cosine scores or ranking changed")
  const castRows = await client.query(cast.text, cast.values)
  const indexedRows = await client.query(indexed.text, indexed.values)
  assert.deepEqual(indexedRows.rows, castRows.rows, "Target results changed")
  const version = await client.query("SHOW server_version")
  console.log(JSON.stringify({
    postgres: version.rows[0].server_version,
    documents,
    chunksPerDocument: 1,
    dimensions,
    measuredRuns: runs,
    warmupRuns: 1,
    jit: false,
    identicalResults: true,
    measurements: cases.map((item) => ({
      name: item.name,
      medianMilliseconds: [...item.samples].sort((left, right) => left - right)[2],
      samplesMilliseconds: item.samples,
      plan: plans.get(item.name),
    })),
  }, null, 2))
} finally {
  try {
    await client.query("ROLLBACK")
  } finally {
    await client.end()
  }
}
