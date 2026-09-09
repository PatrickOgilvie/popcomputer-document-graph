# Turbopuffer production-boundary hardening

## Summary

The D1-coordinated publication and committed-read protocol is retained. Public
production integrations support its core choices: deterministic row identity,
explicit schemas, strong reads, provider-neutral capabilities, authoritative
relational hydration, and live contract tests. This change hardens the outer
provider boundary where the current implementation can still address the same
logical partition through a different Turbopuffer deployment.

The recommended implementation makes provider deployment and endpoint part of
the canonical partition, rejects provider-incompatible limits before I/O,
honours Turbopuffer retry directives while keeping Effect as the sole retry
owner, adds one deep D1 + Turbopuffer composition entrypoint, and adds an
explicitly opted-in live provider contract suite.

## Context / Current State

One `TurbopufferWorkspacePartition` currently derives namespace and D1 index
generation from:

```text
(workspace, embedding profile, schema generation)
```

The official client independently accepts optional `region` and `baseURL`.
When either is absent, the SDK may read ambient environment configuration. Two
clients can consequently share the same D1 generation and namespace string
while addressing different regional or organizational data. An empty namespace
in the new deployment looks like an ordinary empty result and cannot be caught
by candidate validation.

The generic embedding model permits more dimensions than Turbopuffer, the
publication-byte override has no provider maximum, retry classification ignores
provider headers, the Cloudflare example manually wires six related Layers,
and deterministic transport tests do not prove the live provider contract.

The existing publication design remains correct and is not replaced:

```text
D1 publication intent
  -> one fenced Turbopuffer marker + live slots + tombstones write
  -> strong marker reconciliation for ambiguous outcomes
  -> D1 finalization
  -> ranked candidates
  -> one batched D1 active-revision verification
```

## Goals

- Make a physical provider deployment impossible to omit from partition
  identity.
- Make region or custom base URL explicit and prevent ambient SDK endpoint
  selection.
- Keep API-key rotation identity-neutral while distinguishing organizations or
  deployments through a non-secret stable identifier.
- Reject unsupported vector and request sizes before provider or D1 mutation.
- Honour explicit provider retry/no-retry and delay guidance without enabling
  SDK retries.
- Give application composition one cohesive D1 + Turbopuffer Layer factory.
- Exercise the official SDK against a real, isolated namespace when explicitly
  enabled, without making normal CI depend on credentials or the network.

## Non-Goals

- Replacing D1 publication authority with provider visibility.
- Adding a distributed transaction.
- Adding ingestion-wide concurrency policy; callers retain workflow and
  backpressure ownership.
- Logging query text, row content, credentials, or arbitrary provider causes.
- Removing advanced low-level exports in this slice.
- Making live provider tests part of the default offline verification command.

## Invariants

1. Namespace, D1 index generation, provider endpoint, and deployment identity
   are derived or validated from one partition value.
2. API keys never participate in identity and remain `Redacted` until the SDK
   adapter unwraps them. The configured key remains authoritative over ambient
   SDK custom Authorization headers.
3. Every SDK request receives an explicit region or HTTPS base URL, clears the
   other SDK endpoint option, and sets `maxRetries: 0`.
4. Effect is the only retry owner; retry count remains bounded.
5. `x-should-retry: false` prevents retry. `x-should-retry: true` permits a
   bounded retry even when status classification alone would not.
6. Provider retry delays use the greater of local backoff and bounded provider
   guidance. If any write attempt has an ambiguous outcome, the aggregate
   terminal outcome remains ambiguous even when a later attempt is rejected.
7. Dense vector dimensions are at most 10,752.
8. One revision uses at most 10,000 stable slots, matching the provider query
   result maximum needed for complete reusable-vector enumeration.
9. One write request is at most 512 MiB; individual provider document,
   attribute, and 4 KiB filterable-scalar limits are checked before D1 begins
   a publication.
10. Live tests use a fresh partition identity, destroy its namespace in cleanup,
   and never print credentials.
11. D1 bindings remain at the composition seam; service/core modules receive
    package capabilities.
12. Strong marker reconciliation terminalizes a D1 lease proven fenced by a
    newer generation or an equal-generation competing publication ID.

## Design Constraints

- Effect 4 Services, Layers, Schema, Redacted, Schedule, and typed errors are
  already the local architecture.
- The official `@turbopuffer/turbopuffer` SDK remains the transport adapter.
- Existing deterministic tests remain the authority for partial writes and
  ambiguous transport outcomes because live provider tests cannot reliably
  induce those cases.
- Configuration failures occur before network I/O and contain only safe field
  names and stable reason tags.

## Alternatives Considered

### Option 1: Keep endpoint outside the partition

```ts
officialTurbopufferClient({ partition, region, apiKey })
```

This minimizes API changes but preserves two independent sources of physical
placement. Runtime equality can catch an explicit mismatch only if both values
are present; it cannot prove account identity or prevent SDK environment
fallback. Rejected.

### Option 2: Hash the API key into the partition

```ts
partitionIdentity = hash(workspace, profile, schema, apiKey)
```

This distinguishes credentials but makes normal key rotation create a new
namespace and risks secret-derived identifiers becoming correlation material.
Rejected.

### Option 3: Canonical non-secret deployment plus explicit endpoint

```ts
type TurbopufferDeploymentCoordinates = {
  readonly deploymentId: TurbopufferDeploymentId
  readonly endpoint: TurbopufferEndpoint
}
```

The control/composition plane supplies a stable deployment identifier that is
unchanged by credential rotation. Region or normalized custom base URL is part
of the same parsed value. The partition hash includes both. Recommended.

### Composition alternatives

- Expose only individual Layers: flexible but repeats ordering and equality
  obligations in every application.
- Hide D1 and Turbopuffer behind a new mega-service: too broad and would erase
  the existing capability seams.
- Add one factory that builds the existing capability Layers: recommended. It
  owns construction policy without changing core service interfaces.

## Recommendation

Adopt Option 3 and make the deployment required. Do not provide an implicit
legacy/default deployment because that would preserve the aliasing failure.
Construct the SDK endpoint only from the validated partition. Add provider
limits at the Turbopuffer adapter boundary, typed retry metadata on transport
failures, and a workspace factory that returns the canonical partition with the
fully provided capability Layer.

## Proposed Design

### Domain Model and Types

```ts
type TurbopufferDeploymentId = string & Brand<"TurbopufferDeploymentId">
type TurbopufferRegion = string & Brand<"TurbopufferRegion">
type TurbopufferBaseURL = string & Brand<"TurbopufferBaseURL">

type TurbopufferEndpoint =
  | { readonly _tag: "Region"; readonly region: TurbopufferRegion }
  | { readonly _tag: "Custom"; readonly baseURL: TurbopufferBaseURL }

interface TurbopufferWorkspacePartition {
  readonly workspace: TurbopufferWorkspaceId
  readonly deploymentId: TurbopufferDeploymentId
  readonly endpoint: TurbopufferEndpoint
  readonly embeddingProfile: EmbeddingProfile
  readonly schemaGeneration: TurbopufferSchemaGeneration
  readonly identity: TurbopufferNamespaceIdentity
  readonly namespace: TurbopufferNamespace
  readonly d1IndexGeneration: TurbopufferD1IndexGeneration
}
```

The namespace identity changes to:

```text
hash(
  protocol-version,
  deploymentId,
  endpoint,
  workspace,
  embedding profile id/version/dimensions,
  schema generation,
)
```

`deploymentId` is a caller-owned, non-secret control-plane identity. It names
the provider organization/account/deployment, not one credential.

### Types, Interfaces, and APIs

```ts
interface MakeTurbopufferWorkspacePartitionInput {
  readonly deploymentId: string
  readonly endpoint:
    | { readonly _tag: "Region"; readonly region: string }
    | { readonly _tag: "Custom"; readonly baseURL: string }
  readonly workspace: string
  readonly embeddingProfile: EmbeddingProfile
  readonly schemaGeneration: number
}

interface TurbopufferClientConfig {
  readonly apiKey: Redacted.Redacted<string>
  readonly partition: TurbopufferWorkspacePartition
  readonly timeoutMilliseconds?: number
  readonly retries?: number
  readonly fetch?: ClientOptions["fetch"]
}

class TurbopufferTransportFailed {
  readonly operation: TurbopufferOperation
  readonly reason: TurbopufferTransportFailureReason
  readonly requestOutcome?: "definitely_not_applied" | "unknown"
  readonly providerRetryDirective?: "retry" | "do_not_retry"
  readonly providerRetryAfterMilliseconds?: number
  readonly providerRetryAtEpochMilliseconds?: number
  readonly cause: unknown
}

interface TurbopufferD1WorkspaceConfig {
  readonly workspace: string
  readonly database: DocumentGraphD1Database
  readonly embeddings: EmbeddingProviderService
  readonly turbopuffer: {
    readonly apiKey: Redacted.Redacted<string>
    readonly deploymentId: string
    readonly endpoint: MakeTurbopufferWorkspacePartitionInput["endpoint"]
    readonly schemaGeneration: number
    readonly timeoutMilliseconds?: number
    readonly retries?: number
    readonly maximumSlotsPerRevision?: number
    readonly maximumPublicationBytes?: number
    readonly consistency?: TurbopufferQueryConsistency
    readonly publicationLeaseMilliseconds?: number
    readonly retainedPublicationHistory?: number
  }
}

interface TurbopufferD1Workspace {
  readonly partition: TurbopufferWorkspacePartition
  readonly layer: Layer.Layer<
    | EmbeddingProvider
    | GraphTopologyStore
    | ProjectionIndexStore
    | ProjectionSearchStore
    | ProjectionTextSearchStore
    | ProjectionHybridSearchStore,
    InvalidTurbopufferConfiguration
  >
}

declare function makeTurbopufferD1Workspace(
  config: TurbopufferD1WorkspaceConfig,
): TurbopufferD1Workspace
```

### Seams, Boundaries, Adapters, and Implementations

- `partition.ts` owns deployment/endpoint parsing and all physical identity.
- `client.ts` translates the endpoint into official SDK options, parses SDK
  error headers, and owns retry scheduling.
- `config.ts` owns provider schema and vector dimension constraints.
- `projection-index.ts` owns provider publication bounds before D1 begins a
  publication.
- The workspace composition module owns D1/TP Layer wiring but no domain
  behavior.
- The live test reads environment variables only in test bootstrap, immediately
  wraps the key in `Redacted`, and calls the public client interface.

## Call Stacks and Data Flow

### Current / Old Flow

```text
workspace + profile + schema
  -> make partition
region/baseURL + API key + partition
  -> make SDK client (ambient endpoint fallback possible)
D1 binding + partition generation
  -> coordinator
caller manually provides client + coordinator to index and search Layers
```

### Proposed / New Flow

```text
raw deployment/workspace/runtime config
  -> deployment/endpoint/partition parsers
  -> canonical TurbopufferWorkspacePartition
  -> explicit SDK region or baseURL + redacted API key
  -> TurbopufferClient service

D1 binding + canonical partition
  -> ProjectionPublicationCoordinator
client + coordinator + partition
  -> ProjectionIndexStore + three search capabilities
embeddings + D1 topology + index/search capabilities
  -> one workspace Layer returned with the partition
```

### Failure Flow

```text
invalid deployment/HTTPS endpoint/dimensions/limits/runtime settings
  -> InvalidTurbopufferConfiguration before I/O

SDK throw/APIError
  -> classify status and safe retry headers
  -> TurbopufferTransportFailed
  -> classify definitive rejection vs ambiguous write outcome
  -> bounded retry decision
  -> caller receives final typed failure
```

Unknown causes remain attached for programmatic inspection but are never
serialized or added to telemetry by this adapter.

### Retry / Cancellation / Idempotency Flow

```text
caller interruption
  -> Effect tryPromise signal
  -> SDK request signal
  -> interruption (not retried)

typed transport failure
  -> x-should-retry=false ? stop
  -> x-should-retry=true ? eligible
  -> otherwise status/reason eligibility
  -> max(local exponential+jitter, bounded provider delay)
  -> retry at most configured count
  -> preserve unknown if any write attempt was ambiguous
```

All publication writes remain retry-safe because stable physical IDs,
generation conditions, marker reconciliation, and D1 intent already own
idempotency.

### Observability Flow

No new logging dependency is introduced because the repository has no common
adapter telemetry service. Safe information remains available through operation,
reason, retry directive, and retry delay fields. API keys, query text, content,
and raw response bodies are excluded from messages and configuration failures.

## Files to Add / Change / Delete

- Change `src/storage/turbopuffer/partition.ts`: deployment and endpoint domain
  values, parsing, identity derivation, validation.
- Change `src/storage/turbopuffer/identity.ts`: include deployment in namespace
  identity.
- Change `src/storage/turbopuffer/client.ts`: remove independent optional
  endpoint settings; parse retry directives and use a provider-aware schedule.
- Change `src/storage/turbopuffer/errors.ts`: typed safe retry metadata and new
  configuration fields where required.
- Change `src/storage/turbopuffer/config.ts`: Turbopuffer dimension maximum.
- Change `src/storage/turbopuffer/projection-index.ts`: request/document/value
  publication bounds.
- Add `src/storage/turbopuffer/workspace.ts`: cohesive composition factory.
- Change `src/turbopuffer.ts`: export the high-level factory and deployment
  types; retain advanced contracts for this slice.
- Change `examples/cloudflare-workspace.ts` and README composition examples to
  use the factory.
- Add `tests/turbopuffer-live.integration.test.ts`: opt-in real provider suite.
- Change focused unit/integration tests for required deployment identity,
  provider limits, endpoint binding, retry directives, and facade composition.
- Change ADR 0002 to state the complete physical partition tuple.
- Delete no files.

## RGR TDD Test Plan

1. Red: equal workspace/profile/schema under different deployment IDs or
   endpoints currently aliases. Green: include deployment in identity. Refactor:
   centralize parsing and derivation.
2. Red: API-key rotation must keep identity equal. Green: credentials remain
   outside the partition. Refactor: explicit test fixture builders.
3. Red: missing, cleartext, or invalid endpoints and dimensions above 10,752
   are accepted. Green: fail with `InvalidTurbopufferConfiguration` before
   fetch. Refactor: reuse provider schemas at every adapter construction path.
4. Red: publication bound above 512 MiB is accepted. Green: reject it and prove
   the 4 MiB default remains valid. Reject slot bounds above 10,000 and add
   attribute and 4 KiB filterable-scalar cases before D1 begins publication.
5. Red: `x-should-retry:false` on retryable status retries. Green: stop after one
   attempt.
6. Red: `x-should-retry:true` and provider retry delays are ignored. Green: use
   typed directive/delay with `TestClock`; prove retry count and no real sleep.
   Prove an ambiguous write followed by a definitive rejection remains
   ambiguous and marker-reconciles instead of superseding D1 state.
7. Red: composition callers must know the six-layer graph. Green: test the new
   factory exposes the intended capabilities with one config and one partition.
8. Red/live: real provider schema/write/query/multi-query/overwrite/delete
   behavior is unproved. Green: opt-in unique namespace test with cleanup in
   `finally`; default invocation reports it skipped without reading a key.
9. Refactor gate: run focused tests after each slice, then type checks, type
   tests, all offline tests, Workers tests, Node ESM smoke, Oxlint including
   anti-slop, and diff whitespace validation.

## Risks and Open Questions

- A caller must supply a stable deployment identifier from its control plane.
  Changing that identifier intentionally creates a new physical partition.
- Custom gateways may use HTTPS base URLs with paths. The parser normalizes
  those paths and rejects credentials, query strings, fragments, and cleartext
  transport before the Bearer credential reaches the SDK.
- `Retry-After` HTTP dates require Effect clock-aware delay calculation; tests
  must not depend on wall-clock time.
- Provider document/attribute/filterable-scalar size accounting is
  JSON-wire-size based and must fail conservatively before D1 publication
  allocation.
- Live tests prove provider compatibility, not deterministic partial conditional
  writes or transport ambiguity; those stay covered by injected-transport tests.
