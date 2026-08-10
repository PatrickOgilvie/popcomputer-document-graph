# ADR 0001: Graph indexing atomicity and storage ownership

- Status: Accepted
- Date: 2026-08-10
- Scope: `GraphDocumentHandle.index`, projection persistence, relation
  persistence, embedding providers, and the PostgreSQL adapter

## Context

One document index operation can update several independently versioned vector
projections and one complete outgoing relation set:

```ts
const indexed = yield* WorkNode.index(work)
```

The application should not have to orchestrate those steps. The package parses
the document once, projects every revision and relation set before writing, and
then performs the required persistence operations.

The storage architecture deliberately exposes independent Effect capabilities:

- `ProjectionIndexStore` owns complete projected revisions and their vectors;
- `ProjectionSearchStore` owns semantic candidate retrieval;
- `ProjectionTextSearchStore` owns lexical candidate retrieval;
- `GraphRelationStore` owns directed edges and neighbour traversal;
- `EmbeddingProvider` remains separate from persistence.

This decomposition allows one cohesive PostgreSQL adapter, an in-memory
adapter, or heterogeneous storage engines to satisfy the same graph. It also
means the core cannot assume that every capability shares one database or one
transaction manager.

There are two distinct atomicity questions:

1. Must one projection replacement or one outgoing-edge replacement be atomic?
2. Must every projection and the relation set for a document commit as one
   graph-wide transaction?

The first is a universal adapter contract. The second is only possible when a
cohesive storage implementation owns every mutation involved.

## Decision

### 1. Keep the end-user operation unchanged

Ordinary application and Honertia action code continues to express one domain
operation:

```ts
export const indexWorkAction = action(
  Effect.gen(function* () {
    const work = yield* validateRequest(WorkSchema, strictBody)
    const indexed = yield* indexForHttp(WorkNode.index(work))

    return yield* json({ indexed })
  }),
)
```

Transport parsing, HTTP errors, and response shaping remain application-owned.
Atomicity policy belongs at storage composition, not in each action.

### 2. Require atomicity inside every mutation capability

Every `ProjectionIndexStore.replaceRevision` must atomically:

- verify its optimistic token;
- write the complete next revision;
- reuse or install all required vectors;
- delete chunk IDs absent from the replacement;
- expose either the complete old revision or the complete new revision.

Every `GraphRelationStore.replaceOutgoing` must atomically:

- validate the complete relation replacement;
- serialize concurrent replacement for the same source where necessary;
- replace every outgoing edge for that source;
- delete stale outgoing edges;
- expose either the complete old relation set or the complete new relation set.

Partial state inside either capability is a contract violation. The package's
conformance harness certifies these laws through production Effect service
seams.

### 3. Use convergent, independently atomic steps by default

With ordinary pooled storage, `WorkNode.index(work)` performs:

```txt
parse document once
  -> project every revision and outgoing relation set
  -> for each projection, in declaration order:
       load snapshot
       request only missing embeddings
       atomically replace that complete revision
  -> atomically replace the complete outgoing relation set
```

If a later step fails, earlier projection commits remain visible. Retrying the
same index operation converges because:

- `documentKey`, `chunkId`, `contentHash`, and `revisionHash` are deterministic;
- projection writes use complete replacement and optimistic tokens;
- relation writes use complete replacement per source;
- unchanged vectors are reusable by `contentHash` and embedding profile;
- stale chunks and edges are deleted by the successful retry.

The default PostgreSQL composition therefore remains simple:

```ts
const DocumentGraphLive = Layer.mergeAll(
  Layer.succeed(EmbeddingProvider, embeddings),
  postgresDocumentGraph({ pool }),
)

const indexWork = (work: Work) =>
  WorkNode.index(work).pipe(Effect.provide(DocumentGraphLive))
```

No transaction object, unit-of-work abstraction, or partial-progress state is
added to application code.

### 4. Retain caller-owned PostgreSQL transactions as an explicit escape hatch

A cohesive PostgreSQL application may provide an already active transaction:

```ts
const client = await pool.connect()

try {
  await client.query("BEGIN")

  const AtomicDocumentGraphLive = Layer.mergeAll(
    Layer.succeed(EmbeddingProvider, embeddings),
    postgresDocumentGraph({ transaction: client }),
  )

  const indexed = await Effect.runPromise(
    WorkNode.index(work).pipe(
      Effect.provide(AtomicDocumentGraphLive),
    ),
  )

  await client.query("COMMIT")
  return indexed
} catch (cause: unknown) {
  try {
    await client.query("ROLLBACK")
  } catch (_rollbackCause: unknown) {
    // Preserve the failure that made the transaction unsuccessful.
  }
  throw cause
} finally {
  client.release()
}
```

The adapter uses savepoints for each operation and never commits the caller's
transaction. The caller owns commit, rollback, cancellation, and connection
lifetime.

This mode is not the recommended default. The outer transaction begins before
`WorkNode.index` discovers and requests missing embeddings, so a remote
embedding call can keep the database transaction open. Applications should use
this escape hatch only when the all-or-nothing requirement outweighs that
resource and contention cost.

### 5. Do not add a universal graph-wide mutation capability yet

The package will not currently add a `DocumentGraphMutationStore`, transaction
service, or mandatory unit of work.

Such an interface would be misleading for heterogeneous adapters, because no
local transaction can atomically commit PostgreSQL, a remote vector engine, and
a separate graph store. Making it optional now would add another orchestration
path, conflict model, adapter contract, and test matrix without a demonstrated
consumer need.

### 6. Keep embedding providers independent from storage

Storage adapters do not call embedding providers. Embedding identity, batching,
response validation, and reuse planning remain package orchestration concerns.
This preserves provider substitution and lets text-only retrieval omit the
embedding capability entirely.

## Failure and retry semantics

| Failure point | Default pooled outcome | Safe next action |
|---|---|---|
| Document or projection parsing | No writes have started | Correct the typed input failure |
| Missing-embedding request | Earlier projections may be committed | Retry after provider recovery |
| Projection optimistic conflict | Earlier projections may be committed; conflicted projection is unchanged | Retry so delta planning uses the latest snapshot |
| Projection replacement failure | That projection remains at its complete previous revision | Retry after storage recovery |
| Relation replacement failure | Projections may be current; the old complete relation set remains | Retry to replace the relation set |
| Process interruption after a commit | A deterministic prefix may be current | Replay the same index operation |
| Caller-owned transaction failure | Caller rolls back every enclosed mutation | Retry the complete transaction when appropriate |

Once competing writes cease, retries converge to the retried document input;
they do not choose between two different concurrent application intents.
PostgreSQL projection tokens prevent lost projection updates. Relation
replacement is serialized per source and is last-successful-writer wins.

Callers must classify failures before applying a retry schedule. Invalid input,
invalid projection output, and invalid chunking output are not transient.

## Ownership

| Concern | Owner |
|---|---|
| Document parsing, projection order, deterministic identity, delta planning | Package core |
| Embedding requests and response validation | `EmbeddingProvider` plus package orchestration |
| One complete revision transaction and optimistic-token enforcement | `ProjectionIndexStore` adapter |
| One complete source-edge transaction and source-level serialization | `GraphRelationStore` adapter |
| SQL transactions, savepoints, advisory locks, and row parsing | PostgreSQL External Adapter Module |
| Pool or active transaction lifecycle | Application composition root |
| HTTP failure mapping and response projection | Honertia application |
| Retry scheduling and durable job policy | Calling application or workflow |

## Consequences

### Positive

- The common action remains one line of domain intent.
- Independent adapters remain genuinely composable.
- Embedding providers do not become coupled to persistence engines.
- Every visible revision and relation set is internally complete.
- Ordinary failures can converge through deterministic replay.
- Cohesive PostgreSQL applications retain an all-or-nothing escape hatch.

### Trade-offs

- Default pooled indexing can expose a temporary mix of old and new projections
  after a partial failure.
- A relation set may temporarily lag successfully committed projections.
- Retrying may repeat a provider request that completed remotely but failed
  before its result was committed.
- Caller-owned transaction mode can hold a database transaction across remote
  embedding work.
- The package does not provide distributed transactions or compensation across
  heterogeneous stores.

These limitations are explicit rather than hidden behind an interface that
cannot deliver its advertised guarantee for every adapter composition.

## Alternatives considered

### Force every adapter into one cohesive storage service

Rejected. This would make the simple PostgreSQL case look uniform by removing
the package's ability to compose separate vector, text, graph, and embedding
implementations.

### Add an optional graph-wide mutation store immediately

Deferred. It is plausible for cohesive storage, but the current workflow has no
prepared-mutation boundary. A naive implementation would either embed inside a
database transaction or duplicate projection, conflict, and failure semantics.

### Put embedding inside storage adapters

Rejected. It couples providers to databases, prevents clean text-only
composition, and makes reusable embedding policy adapter-specific.

### Compensate earlier commits after a later failure

Rejected. Compensation would race concurrent writers, require retaining old
complete payloads and vectors, and could fail itself. Deterministic forward
replay is safer.

### Introduce distributed transactions

Rejected. Remote vector and graph engines generally cannot participate in one
portable transaction protocol, and the operational burden is disproportionate
to current evidence.

## Reconsideration triggers

Revisit this decision when a real consumer demonstrates at least one of:

- a correctness invariant spanning multiple projections or relations that
  cannot tolerate temporary mixed revisions;
- repeated partial failures that convergence does not resolve within the
  application's operational requirements;
- a cohesive storage adapter where graph-wide commit is a frequent requirement,
  rather than an exceptional policy;
- measured contention or provider latency showing the caller-owned transaction
  escape hatch is unsafe in practice;
- a source adapter or ingestion workflow that naturally prepares every vector
  and mutation before persistence.

Any future cohesive mutation design must:

1. project and request missing embeddings before opening the write transaction;
2. carry a complete prepared mutation for every projection and relation set;
3. revalidate every optimistic token inside the transaction;
4. abort the whole commit when any token is stale;
5. perform no network calls while the database transaction is open;
6. remain optional so independently composed adapters keep the convergent path;
7. gain its own conformance laws and representative PostgreSQL transaction
   tests.

## Verification status

The current behavioural suites prove deterministic replay, vector reuse,
metadata-only updates, stale deletion, optimistic conflicts, complete relation
replacement, and adapter conformance through production Effect seams.

The PostgreSQL integration suite verifies transaction/savepoint rollback when
enabled. In-memory tests are not evidence of PostgreSQL transaction behaviour,
and the live PostgreSQL suite remains conditional on a supplied test database.
