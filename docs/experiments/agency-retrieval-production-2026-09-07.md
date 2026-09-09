# Production retrieval improvements

7 September 2026. Implemented after the [Agency retrieval experiments](./agency-retrieval-2026-09-07.md), with Agency profiles and related CaseStudy evidence both contributing independently to the final Agency ranking.

## Implemented behaviour

- **Batched graph expansion:** the retrieval workflow groups up to 100 distinct source document keys per relation read. PostgreSQL uses indexed `LATERAL` queries, D1 uses `json_each` with a bounded indexed subquery per source, and in-memory storage scans its edges once per batch. Each source keeps its own neighbour limit, ordering and empty result. Batches within a route run sequentially; up to four routes run concurrently.
- **Automatic native scoring:** PostgreSQL discovers installed pgvector in its actual namespace and checks the executing role's schema, type and required function privileges. An inaccessible extension uses float64 scoring. Concurrent searches share discovery; successful results are cached for five minutes per built layer and transient failures are not cached. `vectorSearch: "float64"` forces the extension-free scorer. Both paths score the eligible population exhaustively before limiting candidates.
- **Safe numeric fallback:** canonical embeddings remain float64 arrays. Migration `0004_native_vector_eligibility.sql` computes an eligibility flag on writes, avoiding repeated eligibility scans during retrieval. Native scoring accepts at most 16,000 dimensions and a maximum absolute component between `1e-18` and `1e15`, bounding float32 accumulation. Other vectors use float64 scoring; zero-norm results remain excluded. Native float32 scores can reorder near ties.
- **Less repeated scoring work:** the float64 query norm is evaluated once. Plain-text ranking reuses existing generated full-text vectors; attributed content retains its separate context, label and content weights.

These choices apply the set-based query execution and explicit scope patterns in [TypeGraph's query compiler](https://github.com/nicia-ai/typegraph/blob/7bb743a5a4ad6ea1b3baf2066b3298ab07189842/packages/typegraph/src/query/compiler/emitter/standard-builders.ts), bounded traversal from [Graph RAG](https://datastax.github.io/graph-rag/reference/graph_retriever/strategies/), and the native cosine implementation in [pgvector 0.8.6](https://github.com/pgvector/pgvector/blob/v0.8.6/src/vector.c). The existing typed graph, rank fusion and evidence provenance remain the public retrieval interface.

## Production measurements

The benchmark calls the public graph API with no SQL rewriting or experimental vector columns. It compares the previously reviewed 0.4.0 package with the implementation in this working tree, using the same database transaction and fixture.

1,000 Agencies, 9,000 CaseStudies, 9,270 relationships, one 1,024-dimensional chunk per document. Both routes use 50 candidates per channel, a two-neighbour bound and ten final results. Node.js 22.22.2, PostgreSQL 17.8, pgvector 0.8.6, local Apple Silicon, JIT disabled. One warmup and five measured runs with alternating variant order. All variants use one serialized database connection. Timings include retrieval, decoding, graph expansion and fusion; they exclude embedding API latency.

| Strategy | Reviewed baseline | Updated float64 | Updated auto with pgvector |
| --- | ---: | ---: | ---: |
| Semantic | 1,095.4 ms / 52 statements | 944.1 ms / 3 statements | **97.5 ms / 4 statements** |
| Lexical | 24.8 ms / 52 statements | 11.1 ms / 3 statements | **11.1 ms / 3 statements** |
| Hybrid | 1,074.9 ms / 104 statements | 953.4 ms / 5 statements | **106.6 ms / 6 statements** |

Automatic native scoring improves semantic retrieval by **11.2×** and hybrid retrieval by **10.1×** on this fixture. Lexical retrieval improves by **2.2×**, and float64 hybrid retrieval improves by **1.13×**. These are local synthetic measurements, not production latency promises.

Every measured search builds a fresh storage layer, so auto includes its first capability lookup. With discovery cached in an already built layer, the retrieval itself uses three semantic or five hybrid statements. Lexical searches do not discover pgvector.

All variants returned identical Agency order, evidence, provenance and ranks. Float64 results retained bit-identical scores; native semantic scores differed slightly. This fixture verifies behaviour preservation, not relevance quality or near-tie stability on an application corpus. Both independent discovery routes remain essential: the earlier experiment showed that gating on Agency profile candidates alone excluded relevant CaseStudy-derived targets.

Samples and comparisons: [agency-retrieval-production-2026-09-07.json](./agency-retrieval-production-2026-09-07.json).

## Verification

`bun run verify` passed with both PostgreSQL and pgvector integration enabled:

- 248 Bun tests passed, one skipped, zero failures; 1,053 assertions across 36 files, including the follow-up review fixes.
- Cloudflare Workers runtime test passed, including ordered duplicate and empty source groups in D1.
- Lint, source and consumer types, package build, and Node ESM smoke test passed.
- PostgreSQL tests cover real migrations, native extension discovery in a quoted schema, restricted-role fallback, scope/version filters, exact native ties, zero vectors, extreme magnitudes, vectors above 16,000 dimensions, and empty scopes.
- Publication regressions use the real D1 coordinator with SQLite and a controlled provider seam. A rejected duplicate retains shared authority while the original replacement or deletion finishes. Exact retry after a definite rejection also succeeds.
- Node pagination excludes retired document kinds after reconciliation, including empty include lists and graphs whose entire document catalog has been retired.
- Lexical tests compare persisted-vector ranking with the original field-by-field expression across both languages, weighted fields, zero weights and phrase queries.
- Batch tests cover empty/missing sources, duplicate input keys, per-source limits, stale relations, malformed adapter output, and a 232-source retrieval split into 100/100/32.
- SQLite's actual query plan uses the node primary-key index and the covering outgoing edge index for the bounded D1 relation query.

The skipped test is live Turbopuffer conformance because no deployment credentials are configured. Protocol and Workers tests passed locally.

## Upgrade and reproduce

Apply all four ordered PostgreSQL migrations. Migration 0004 computes eligibility for existing stored embeddings without regenerating embeddings or requiring pgvector. The runtime never installs extensions or modifies schemas. Custom topology adapters must implement the ordered `documentKeys` / `RelatedGraphNodeSet[]` batch contract described in the README.

```sh
bun run build
TEST_DATABASE_URL=postgresql://localhost/document_graph_test \
  node benchmarks/postgres-agency-production.mjs
```

To include native scoring on a disposable server with pgvector extension files, add `BENCHMARK_ENABLE_PGVECTOR=true`. To compare an earlier build, set `BENCHMARK_BASELINE_PACKAGE` to its unpacked package directory. The baseline used here was the reviewed pre-optimization 0.4.0 tarball; its npm integrity was `sha512-oQPGoRmSLu7dxEDhILf+duFAz4bB6jIyGQ2QZoYrhXXmuevGqY55zlPucYq2FuA8mboiRJRGaQ+r7PP2obCWjQ==`.

For full native integration coverage, supply `RUN_DOCUMENT_GRAPH_POSTGRES_TESTS=true`, `RUN_DOCUMENT_GRAPH_PGVECTOR_TESTS=true`, and `TEST_DATABASE_URL` when running `bun run verify`. Both benchmark fixtures and the native integration extension are rolled back. The local pgvector test installation is disposable and separate from the existing PostgreSQL installation.
