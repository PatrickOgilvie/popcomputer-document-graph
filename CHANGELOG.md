# Changelog

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
