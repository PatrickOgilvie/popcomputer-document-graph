# Changelog

## 0.11.0 - 2026-10-09

Stable Effect v4.

### Breaking changes

- The `effect` peer dependency is now `^4.0.0` instead of `^4.0.0-rc.109`.
  Effect v4 release candidates are no longer supported; upgrade to Effect
  `4.0.0` or later.

### Maintenance

- Developed and tested against `effect@4.0.2` and `@effect/vitest@4.0.2`.
- `Sha256HexSchema`, the identity schemas built on it, and
  `TurbopufferNamespaceSchema` use `Schema.isBetweenLength`, the stable name of
  `Schema.isLengthBetween`. Validation and its messages are unchanged.
- Identifier, digest, and numeric-string patterns use Unicode (`u`) regular
  expressions, so they keep appearing as `pattern` when consumers generate JSON
  Schema from them. Effect 4.0.2 exports a pattern only for Unicode
  expressions; matching is unchanged.

No schema migrations or reindexing are required.

## 0.10.0 - 2026-10-04

Document embeddings from Turbopuffer's managed models.

### Added

- `makeTurbopufferNativeEmbeddingProvider` embeds documents with a Turbopuffer
  managed model, such as `qwen/qwen3-embedding-0p6b`, behind the ordinary
  `EmbeddingProvider` seam. It writes each document to a dedicated namespace
  under its content hash and reads the stored vector back. Content already
  embedded by the same model is read instead of embedded again. Query vectors
  come from a supplied `embedQuery`.
- `InvalidTurbopufferConfiguration` reports `batch_size` and `embedding_model`.

## 0.9.0 - 2026-10-02

Mirror PostgreSQL projection changes into another index.

### Added

- Migration 0007 records every write to `projected_revisions` in a
  `projection_changes` set, by trigger, in the writing transaction. A
  projection keeps one row however often it changes.
- `mirrorPostgresProjectionChanges` drains that set into any
  `ProjectionIndexStore`, such as a Turbopuffer composition. It copies changed
  revisions with their stored vectors, deletes revisions PostgreSQL no longer
  holds, and clears each applied change. Failed writes stay recorded for the
  next drain, and a change renewed during a drain is never cleared by it.

### Changed

- `copyPostgresProjectionIndex` compares revision hashes before reading
  chunks, so revisions the target already holds cost one row read instead of
  their chunks and vectors. A resumed or repeated copy is mostly row reads.

## 0.8.0 - 2026-10-01

Turbopuffer retrieval with PostgreSQL topology and journal.

### Added

- `postgresProjectionPublicationCoordinator` keeps the Turbopuffer publication
  journal in PostgreSQL (migration 0006). Each transition runs in one
  transaction with the head row locked, and the D1 coordinator's behaviour is
  held by a shared conformance suite that runs against both.
- `makeTurbopufferPostgresDocumentGraph` composes PostgreSQL topology and
  journal with Turbopuffer index, semantic, text and hybrid search.
- `copyPostgresProjectionIndex` and `readPostgresProjectionIndexPage` copy a
  graph's PostgreSQL projection index into any `ProjectionIndexStore`, reusing
  every stored vector. Unchanged revisions are skipped, so a copy resumes.
- `vectorElementType: "f16"` on Turbopuffer partitions stores half-precision
  vectors.
- Turbopuffer text search reads web-search syntax: BM25 ranks every positive
  word, and a query of quoted phrases requires one alternative's phrases, as
  PostgreSQL's `websearch_to_tsquery` does.
- Turbopuffer search options:
  - `candidateVerification: "provider"` trusts live rows (each publication
    writes a document's rows atomically), so search never reads the journal.
  - `coalesceSearches` packs searches that arrive together into multi-queries
    of up to 16 subqueries.
  - `chunksPerDocument` caps each document's chunks per channel.
- `OfficialTurbopufferClient.warmCache` and the composition's `warmCache` load
  a namespace into Turbopuffer's cache ahead of the first search.

### Changed

- **Breaking:** Turbopuffer namespace identities include the vector element
  type, so every partition maps to a new namespace and journal generation.
- English Turbopuffer full-text fields are stemmed and drop stopwords, matching
  PostgreSQL's `english` configuration. Existing namespaces rebuild those
  indexes in place.
- The Turbopuffer client asks for gzipped responses (`compressResponses`,
  default on) and sends query vectors as base64, cutting search transfer about
  fourfold.

## 0.7.0 - 2026-09-30

PostgreSQL search coalescing, plus everything in 0.6.0, which was never
published.

### Added

- `coalesceSearches: { windowMilliseconds, maximumBatch }` on
  `postgresDocumentGraph` merges searches that arrive together:
  - semantic searches that share a scope, embedding profile and plan become one
    statement with a branch per query vector, with results identical to
    searching alone;
  - topology reads for the same relation become one read.

  A fan-out of retrievals then holds one pooled connection per statement
  instead of one per query. It is off unless configured.

## 0.6.0 - 2026-09-30

Graph retrieval within a known population of targets.

### Added

- `retrieval.within(targets, { maximumDocuments })` resolves a population of
  targets once for any number of searches, and `search(query, { within })`
  ranks only those targets. Direct routes search the targets' own documents;
  relation routes search only source documents related to a target, read from
  canonical topology when the scope is created. The search provider applies
  the population before candidate limits. Sources related to targets outside
  the scope rank only their in-scope targets. An empty population returns
  nothing without embedding the query.
- A population above `maximumDocuments` (at most
  `MAX_GRAPH_SEARCH_TARGET_DOCUMENT_KEYS`) fails with the new
  `InvalidSearchQuery` reason `scope_too_large` rather than searching an
  arbitrary subset.
- PostgreSQL `approximateAboveDocuments` (default 1,000): document-key scopes
  larger than this use the HNSW index with the key filter applied during the
  scan; smaller ones keep exact exhaustive scoring.
- `retrieval_scope` operation name for tracing and `DocumentGraphUnavailable`.

### Changed

- Large document-key scopes no longer always search exhaustively under
  `vectorSearch: { mode: "approximate" }`; see `approximateAboveDocuments`.

## 0.5.0 - 2026-09-30

Approximate PostgreSQL semantic search through a pgvector HNSW expression
index, and server-side search timeouts.

### Added

- `vectorSearch: { mode: "approximate", index, dimensions }` takes whole-graph
  semantic candidates from a pgvector HNSW index, applying scope during the
  index scan with iterative scanning, then rescores them exactly in float64,
  so scores and ties match `"float64"` search. Only recall is approximate. Document-key scopes, other dimensions, out-of-range query
  vectors, and a missing, building, or invalid index keep exhaustive `"auto"`
  scoring. Index readiness is discovered and cached with pgvector detection.
- `postgresVectorIndexSql()` returns the matching `CREATE INDEX CONCURRENTLY`
  statement. It indexes an expression over the canonical float64 arrays, so no
  column, table rewrite, or backfill is needed. `halfvec` (default) halves the
  index size; `vector` keeps float32.
- `searchTimeoutMilliseconds` sets `statement_timeout` for each semantic and
  text search, so PostgreSQL cancels a search its caller has abandoned.
- Migration `0005_native_halfvec_eligibility.sql` adds the halfvec range guard
  the halfvec index predicate uses.
- Graph retrieval `search(query, { textQuery })` sends a separate query to text
  channels, such as quoted key phrases joined with OR, while semantic channels
  still embed `query`.
- `textSearchTimeoutMilliseconds` makes PostgreSQL text search best effort: a
  text search cancelled at that budget returns no candidates, so hybrid
  retrieval continues on its semantic channel.

### Changed

- PostgreSQL text search ranks all matches on the stored combined vector, then
  applies the exact per-field weights to the best four per requested candidate,
  instead of re-tokenising every match. Common phrases no longer turn one text
  search into thousands of per-row `to_tsvector` calls.

- Searches that need transaction-local settings run in a read-only
  transaction in pool mode, or a savepoint that is rolled back in transaction
  mode, so settings never leak into the caller's transaction. Searches without
  settings issue the same single statement as before.

## 0.4.1 - 2026-09-12

Validation fixes and internal cleanup following a one-off audit with
`@popcomputer/lint`.

### Fixed

- PostgreSQL semantic and text candidate decoding rejects non-finite scores
  as invalid stored state before they can reach ranking.

### Maintenance

- Simplified yieldable errors, direct service provisioning, typed decoding,
  tagged schemas, and nullish defaults while preserving encoded data shapes.
- Replaced ad hoc persisted JSON parsing with typed Effects while preserving
  storage error handling.
- Made the retrieval concurrency regression deterministic and expanded
  malformed-row and non-finite-score coverage.
- Applied the audit's spacing and type-import rules. The one-off recommended
  preset findings fell from 3,881 to 998; the existing lint configuration and
  dependency set are unchanged.

No schema migrations or reindexing are required when upgrading from 0.4.0.

## 0.4.0 - 2026-09-09

Faster graph retrieval, automatic PostgreSQL native vector scoring, and
Cloudflare D1 + Turbopuffer storage. Agency profiles and related CaseStudy
evidence continue to contribute independently to retrieval results.

### Performance

- On the local 10,000-document Agency/CaseStudy fixture, native PostgreSQL
  hybrid retrieval improved from 1,074.9 ms to 106.6 ms (10.1×), and lexical
  retrieval from 24.8 ms to 11.1 ms (2.2×). Hybrid database calls fell from 104
  to five retrieval statements plus an initial capability lookup. These are
  synthetic measurements excluding embedding API latency; see the
  [benchmark report](./docs/experiments/agency-retrieval-production-2026-09-07.md).
- Graph retrieval batches relationship expansion in groups of 100 source
  documents across PostgreSQL, D1 and in-memory adapters. Per-source limits,
  input identity, ordering and evidence provenance are preserved.
- PostgreSQL automatically uses installed pgvector for eligible vectors and
  falls back to extension-free float64 scoring. `vectorSearch: "float64"`
  explicitly retains float64 scoring throughout. Native scores use float32
  precision and can reorder near ties; both paths remain exhaustive.
- PostgreSQL float64 cosine search expands vectors together and computes the
  query norm once. Plain-text ranking reuses stored full-text vectors while
  attributed text retains its separate field weights.
- PostgreSQL document-key search targets use the native indexed key type.
  Topology replacements batch source and target nodes in a consistent lock
  order and restrict orphan cleanup to former targets.
- Turbopuffer replacements that supply every vector skip the previous
  revision's vector download. D1 publication cleanup only scans mutation
  history for the affected document projection.

### Fixed

- Concurrent Turbopuffer retries retain their shared publication lease when
  one request is rejected, allowing another in-flight attempt to finalize.
- Default node listing filters to currently registered document kinds,
  including after retired kinds leave materialized nodes in storage.
- Automatic pgvector selection falls back when the executing database role
  cannot access the extension's schema, types or required functions.
- PostgreSQL orphan cleanup locks candidates without waiting on concurrent
  publishers and rechecks references before deletion, preventing a stale
  snapshot from cascading away newly committed edges. Explicit node deletion
  locks the node before counting its incident edges.
- PostgreSQL reconciliation removes obsolete projection versions, and mixed
  versioned/unversioned catalogs retain each entry's own search constraint.
- D1 committed replay returns the original persisted commit counts.
- Turbopuffer mutation identities distinguish absent metadata from explicit
  JSON null.

### Breaking changes

- Search queries are limited to 8,192 characters across retrieval channels.
- Replaced the edge-only `GraphRelationStore` adapter contract with
  `GraphTopologyStore`. Topology adapters now persist canonical referenced and
  materialized nodes as well as directed relations, support bounded node pages,
  resolve related node populations, hard-delete incident topology, and collect
  orphan referenced nodes during reconciliation. `findRelatedNodes` takes
  `documentKeys` and returns ordered `RelatedGraphNodeSet` groups, including
  empty and duplicate requests, with an independent limit per source.
- Projection replacements now carry their `TextSearchPolicy`, and registered
  projection catalog entries carry the projection version. Custom
  `ProjectionIndexStore` implementations must retain those fields when
  publishing and pruning revisions.
- Relation-constrained `searchWithin` requires an explicit `maximumDocuments`
  bound because topology selects the eligible population before ranking.
- Prepared mutation replay can perform storage-provider network calls. Its
  guarantee is that captured vectors are reused and embeddings are not
  recomputed; it is not an offline replay guarantee.
- `makeTurbopufferWorkspacePartition` now requires a stable, non-secret
  provider deployment ID and an explicit regional or custom endpoint. The
  official client derives placement exclusively from that partition rather
  than accepting independent `region` or `baseURL` settings. Custom endpoints
  must use HTTPS.

### Added

- Cloudflare D1 graph topology adapter and fixed normalized migrations under
  `migrations/d1`.
- D1 projection publication coordinator with durable logical snapshots,
  optimistic compare-and-set, monotonically increasing generation leases,
  slot high-water tracking, mutation inventory, and bounded publication
  journaling.
  Expired diagnostic deadlines do not transfer authority to a competing
  mutation; the exact prepared mutation must reconcile or provider fencing
  must prove its lease can no longer write before it is superseded.
- Turbopuffer adapter foundations built on the official SDK: explicit schema
  generations, stable marker/slot identities, plaintext and attributed chunk
  rows, external-vector reuse, metadata-term filters, cosine ANN, weighted BM25,
  and same-snapshot hybrid multi-query.
- Closed graph search targets: `AllDocuments`, `NoDocuments`, and non-empty
  `DocumentKeys`. Every adapter must apply the target before candidate limits;
  `NoDocuments` short-circuits without a provider request.
- Optional `ProjectionHybridSearchStore` capability. Providers that can execute
  both channels against one snapshot may return separate semantic and lexical
  candidates while the package continues to own validation, signals, and
  reciprocal-rank fusion.
- PostgreSQL migrations `0003_graph_topology.sql` for canonical nodes and
  `0004_native_vector_eligibility.sql` for generated native-scoring eligibility.
  Apply both before using the updated adapter. Existing canonical embeddings
  remain float64 and do not need to be recomputed.
- D1 and Turbopuffer public entry points, focused protocol tests, and
  [ADR 0002](./docs/decisions/0002-workspace-d1-and-turbopuffer-storage.md)
  describing the workspace storage model.
- A complete exported `d1DocumentGraphSchema` under the deliberately
  Drizzle-coupled `@popcomputer/document-graph/d1/schema` entry point for
  composing all package-owned topology and publication tables.
- `makeTurbopufferD1Workspace`, a deep composition facade that derives one
  partition and wires embeddings, D1 topology/publication, and Turbopuffer
  indexing and semantic, lexical, and hybrid retrieval capabilities.
- Provider-aware Turbopuffer retry directives with aggregate write-outcome
  safety, provider dimension/request/document/attribute/filterable/query
  limits, and an opt-in disposable live conformance suite exposed through
  `test:live:turbopuffer`.
- Runtime-decoded query consistency and explicit credential precedence over
  ambient SDK custom Authorization headers.
- A strict version-1 `PreparedGraphMutationArtifact` codec for durable workflow
  checkpoints. It emits canonical JSON, validates projection and topology
  invariants on both encode and decode, and restores the existing deeply frozen
  replay value without recomputing embeddings.

### Changed

- Indexing materializes a source graph node even when it has no outgoing edges.
  Relation targets begin as referenced nodes and are upgraded, never
  downgraded, when their own documents are indexed.
- In-memory and PostgreSQL semantic/text adapters enforce document-key targets
  before result limiting.
- Graph manifests and stale-revision pruning distinguish projection policy
  versions.
- Removal and reconciliation preserve the existing public result shapes while
  topology adapters retain their more detailed node deletion counts.

## 0.3.0 - 2026-08-22

### Breaking changes

- Replaced the projection-index adapter's single-key `loadRevision` operation
  with ordered, non-empty `loadRevisions` batches. The corresponding
  `ProjectionIndexStoreFailed.operation` value is now `load_revisions`.
- Removed the low-level `searchGraph`, `searchGraphText`, `searchGraphHybrid`,
  `semantic`, and `text` orchestration helpers from the adapter entry point.
  Applications should use graph and projection handles; adapter authors retain
  only the storage contracts they implement.
- PostgreSQL mutation serialization now requires
  `migrations/postgres/0002_mutation_locks.sql`. Exact-key lock rows replace
  advisory locks and avoid false contention and proxy incompatibilities.

### Added

- Prepared-mutation capture and replay:
  `prepareGraphMutation`, `replayPreparedGraphMutation`,
  `PreparedGraphMutation`, `PreparedMutationReplayReport`, and
  `DuplicatePreparedMutation`. Capture runs an ordinary indexing program
  without storage writes and with embeddings resolved; replay applies the
  frozen, deterministically ordered operation set to any storage.
- Post-search evidence currency verification: `verifyEvidenceCurrency` and
  `evidenceReferenceFromHit` report `Current`, `Stale`, or `Missing` per
  reference without exposing storage internals.
- Root re-exports of `EmbeddingProviderFailed`, `InvalidEmbeddingOutput`,
  and `EmbeddedContent` so embedding-provider implementers need only the
  root entry point.

### Changed

- Graph definitions now compile into a pure immutable representation before
  cohesive reference, indexing, traversal, maintenance, and retrieval
  workflows are bound. Projection search uses one closed semantic/text/hybrid
  plan interpreter, with named Effect workflow boundaries throughout core and
  PostgreSQL adapter operations.
- Projection revision reads are batch-first through `loadRevisions`. Evidence
  currency verification deduplicates references into one storage call, and the
  PostgreSQL adapter resolves the complete batch with one query.
- Prepared mutation capture is now one composable Effect combinator rather
  than a public capture-layer lifecycle. Replay accepts only the two mutation
  methods it actually uses.
- PostgreSQL mutation serialization now uses exact-key row locks. Unrelated
  documents no longer contend through 64-bit hash collisions, and storage
  works through proxies that do not support advisory locks, such as Cloudflare
  Hyperdrive.
- `postgresDocumentGraph({ transaction })` accepts pg `Client`/`PoolClient`
  instances directly. Other pinned transaction query surfaces opt in through
  `postgresTransactionClient(...)`; pools are rejected because their queries
  are not pinned to one transaction connection.

### Fixed

- `replaceOutgoingRelationsInTransaction` receives the full tables record so
  relation replacements lock through the shared helper.

## 0.2.0-rc.1 - 2026-08-15

### Breaking changes

- Migrated the package peer dependency from Effect v3 to
  `effect@4.0.0-rc.109`.
- Renamed the exported runtime registration types:
  - `ChunkingStrategyShape` to `ChunkingStrategyRuntime`
  - `DocumentDefinitionShape` to `RegisteredDocumentDefinition`
  - `VectorProjectionShape` to `RegisteredVectorProjection`
  - `GraphRelationDefinitionShape` to `RegisteredGraphRelationDefinition`
- Removed the `@popcomputer/web` action example and development dependency.
  Its current published release requires Effect v3 and cannot share this
  package's Effect v4 runtime.

### Changed

- Migrated schemas, services, typed failures, tracing, indexing, retrieval,
  in-memory storage, and PostgreSQL storage to their Effect v4 APIs.
- Updated every TypeScript example and test to compile against Effect v4.
- Added the repository's anti-slop lint rules to release verification.
- Declared Node.js 20.19.0 as the minimum supported Node runtime.
