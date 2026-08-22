# Changelog

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
