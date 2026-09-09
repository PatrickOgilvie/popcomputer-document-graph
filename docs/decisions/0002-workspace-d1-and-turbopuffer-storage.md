# ADR 0002: Workspace-local graph topology with Turbopuffer retrieval

- Status: Accepted
- Date: 2026-08-28
- Scope: Cloudflare workspace storage, graph topology, projection publication,
  semantic and lexical retrieval, and cross-store consistency

## Context

The platform treats a playground as a public workspace. Future customers will
be able to create the same kind of workspace for their own datasets, files,
graphs, and applications. A workspace can contain ordinary application tables,
but its document graph and retrieval corpus have different storage needs:

- graph traversal needs exact joins, bounded adjacency reads, and transactional
  replacement of a document's complete outgoing edge set;
- semantic and lexical retrieval need vectors, BM25, selective attributes, and
  provider-side filters over a potentially much larger text corpus;
- source files and large blobs should not be duplicated into a relational
  database merely to make them searchable;
- the platform control plane must manage workspace identity, ownership,
  placement, quotas, and lifecycle without sharing every workspace's data
  tables;
- a failed or timed-out remote write must never expose a mixture of two
  projected revisions.

Using one PlanetScale PostgreSQL schema for every workspace would either put a
workspace discriminator on every data-plane row or create an unbounded catalog
of workspace-specific schemas and tables. Neither matches the desired isolation
or lifecycle model. Using D1 for vectors and full text would duplicate a search
engine and eventually make its per-database size ceiling the corpus ceiling.
Using Turbopuffer as the only graph store would make multi-edge traversal and
referential topology depend on large metadata filters and client-side joins.

## Decision

### 1. Separate control plane, source plane, topology plane, and retrieval plane

The production workspace layout is:

| Plane | Owner | Canonical data |
|---|---|---|
| Control | PlanetScale PostgreSQL | workspace identity, ownership, placement, quotas, deployment and lifecycle state |
| Source | R2-backed application filesystem | uploaded files, crawled objects, original blobs, and application-owned source records |
| Topology | one D1 database per workspace | graph nodes, typed directed edges, application tables, and the Turbopuffer publication journal |
| Retrieval | Turbopuffer | derived plaintext chunks, attribution, vectors, BM25 fields, and filter attributes |

The package implements the topology and retrieval adapters. R2 hydration and
PlanetScale control-plane records remain application concerns. Search hits keep
the stable document reference needed to hydrate the canonical file or record.

### 2. Keep graph topology canonical in D1

D1 stores normalized `document_graph_nodes` and
`document_graph_relations` tables. A node is either:

- `Materialized`, when its own document has been indexed; or
- `Referenced`, when it exists only because another materialized document has
  an edge to it.

Indexing a document materializes the source even when it has no relations.
Relation targets are inserted as referenced nodes. Later materialization is an
upgrade and no operation silently downgrades a materialized node. Removing a
node is a hard delete whose foreign keys remove incident edges. Reconciliation
can garbage-collect referenced nodes that have no remaining incident edges.

Each complete outgoing-edge replacement runs in one D1 batch. Exact reads use
a `first-primary` session. Page and mutation limits are explicit package
contracts so an unexpectedly large graph operation fails before partial state
is written.

### 3. Resolve graph constraints before asking Turbopuffer to rank

Graph-constrained retrieval has two phases:

```text
D1 topology query
  -> AllDocuments | NoDocuments | DocumentKeys(non-empty)
  -> Turbopuffer filter (graph + projection + generation + document keys)
  -> ANN and/or BM25 candidate limit
  -> one batched D1 active-revision integrity check
  -> package-owned validation and rank fusion
```

`NoDocuments` is distinct from an empty key array and must issue no provider
request. A non-empty key set is included in the Turbopuffer query filter before
`top_k`; filtering candidates after Turbopuffer has applied its limit is a
contract violation. This preserves both graph semantics and recall.

For every non-empty provider result, the search adapter batches the distinct
document/projection keys into one D1 read. A candidate is visible only when its
revision hash, chunk ID, and content hash match the D1 active revision exactly.
Any mismatch invalidates the complete provider response; it is not silently
filtered. This is a logical-publication integrity gate, distinct from graph
population filtering, and prevents pending or partially written provider rows
from escaping as search results.

D1 answers operations such as "all nodes of this kind" and "nodes connected by
this relation." Turbopuffer answers "which text chunks inside that resolved
population best match this query." Neither engine emulates the other.

### 4. Store derived plaintext and retrieval attributes in Turbopuffer

Each live retrieval slot contains:

- stable document, projection, revision, section, and chunk identity;
- focused plaintext plus embedding text;
- the encoded application document ID needed for attribution;
- the embedding vector;
- canonical metadata JSON and type-sensitive hashed filter terms;
- one selected set of pinned full-text fields.

The first adapter uses application-supplied external embeddings. It supports
cosine ANN, weighted BM25, and a same-snapshot multi-query. Semantic and lexical
channels remain separate until the package validates candidates and applies
weighted reciprocal-rank fusion. Provider-side reranking is not part of this
adapter.

The full-text manifest has explicit English and simple-policy attributes for
context, label, and content. Only the projection's selected language fields are
populated. Metadata predicates compile to provider filters with scalar type
separation.

### 5. Partition namespaces by immutable retrieval compatibility

One Turbopuffer namespace represents:

```text
(provider deployment, endpoint placement, workspace,
 embedding/retrieval profile, schema generation)
```

Composition derives one branded `TurbopufferWorkspacePartition` from that
tuple. The value contains the canonical partition hash, provider namespace,
non-secret deployment identity, explicit regional or custom endpoint,
embedding profile, schema generation, and the matching D1 index-generation
identity. The official client, publication adapter, search adapter, and D1
coordinator must consume values derived from that same partition; endpoint,
namespace, and D1 generation are not independent settings. API credentials do
not participate in identity, so rotating a key inside the same provider
deployment does not move data.

It is not partitioned by graph node type. Every physical row carries the
partition hash, and every publication/reuse/search query filters it. Returned
search rows are checked against the expected hash. These checks fail closed if
a client, adapter, or provider namespace is accidentally shared across
workspaces. Document and projection identity remain row attributes and query
filters. A schema or vector-space change creates a new namespace generation;
rollout switches the control-plane pointer after backfill and validation. Old
generations can then be retired explicitly.

Namespace bootstrap, inspection, and destruction are administration
operations, not implicit side effects of a search request.

### 6. Coordinate every remote revision publication through D1

Turbopuffer ranks physical candidates; D1 is the durable authority for their
logical visibility, revision heads, optimistic tokens, generation allocation,
slot high-water marks, mutation payloads, and publication outcomes.

Rows use stable physical IDs derived from
`(partitionHash, documentKey, projection, slot)`.
Every publication writes, atomically:

- one marker row;
- every live slot in the next revision; and
- tombstones for every remaining slot below the historical high-water mark.

Every row carries the publication ID and a monotonically increasing generation.
Every row condition accepts either an older generation or the same generation
and same publication ID. Missing rows may be inserted. Stable slots are never
reused without being overwritten as part of the complete publication, and the
high-water mark never decreases.

Before D1 allocates a generation, the adapter computes embeddings, freezes the
complete row payload and exact slot high-water closure into the mutation
identity, checks the maximum slot closure, and checks the serialized atomic
request size. D1 never raises that frozen closure during `beginPublication`.
If another attempt has already advanced the durable closure, D1 returns a typed
stale-plan result and the adapter reloads the head and rebuilds the mutation
before allocating a generation. Prepared mutation replay therefore never
recomputes embeddings, although replay may still perform D1 and Turbopuffer
network calls.

The expected successful write count is exactly `1 + slotHighWater`. Any partial
count is invalid provider behavior, and the D1 active-revision search gate keeps
any mixed physical state invisible. A zero count is reconciled against the
marker. A transport timeout is ambiguous: the adapter retains the pending D1
journal entry and reads the marker with strong consistency. A matching marker
allows idempotent D1 finalization; a newer marker or an equal-generation marker
with a competing publication ID supersedes the fenced attempt; an unresolved
outcome is reported as `publication_in_doubt` rather than guessed.
D1 finalization may lag a successful Turbopuffer write and can be safely
retried.

Completed journal history is bounded per document projection (32 entries by
default, configurable from 1 to 1,000). Retention never removes active or
pending mutation payloads; once an older journal is pruned, its now-unreferenced
mutation inventory and chunk rows are collected in the same D1 batch.
Cleanup is restricted to the affected document projection and preserves
references from every physical generation. Exact committed replay reads the
original persisted outcome, so retry planning cannot change its commit counts.

An expired diagnostic deadline does not transfer a pending publication to a
different mutation. Only the exact prepared mutation may resume that
publication ID and generation. A competing mutation receives
`publication_in_progress` until the durable workflow reconciles the pending
marker or observes provider fencing that proves the lease can no longer write.
Concurrent attempts of the exact mutation share that lease. A definite request
rejection, including authentication or capacity failure, does not revoke their
shared authority: another attempt may still publish successfully. Repair the
request's failure and retry the exact prepared mutation to finish publication.
This conservative rule avoids a check-then-write race in which an old writer
could publish after D1 had already handed authority to a successor but before
the successor had installed its higher Turbopuffer generation.

### 7. Preserve independent Effect capabilities

The application composes:

- `GraphTopologyStore` from D1;
- `ProjectionPublicationCoordinator` from D1;
- `ProjectionIndexStore`, `ProjectionSearchStore`,
  `ProjectionTextSearchStore`, and optionally `ProjectionHybridSearchStore`
  from Turbopuffer;
- `EmbeddingProvider` from the selected embedding implementation.

The in-memory and PostgreSQL adapters remain supported. The core has no direct
Cloudflare or Turbopuffer dependency; provider details stay behind adapter
capabilities and typed failures.

The Turbopuffer search Layer requires both `TurbopufferClient` and
`ProjectionPublicationCoordinator`: the client supplies ranked candidates and
the coordinator supplies the authoritative active-revision snapshot.

## Consistency and failure semantics

| Event | Visible result |
|---|---|
| D1 topology replacement commits | New canonical graph shape is visible |
| Turbopuffer publication succeeds and D1 finalization succeeds | New logical revision and all physical rows are current |
| Turbopuffer publication succeeds but D1 finalization fails | Physical marker proves success; retry finalization without republishing embeddings |
| Turbopuffer request times out | Read the marker; never infer success or failure from the timeout alone |
| D1 slot closure advances after a mutation is planned | Reject the stale plan, reload the head, and rebuild the exact mutation identity before generation allocation |
| A competing mutation observes an expired pending deadline | Keep the prepared mutation authoritative and return `publication_in_progress`; never transfer authority before provider reconciliation |
| Another generation wins first | Old per-row conditions affect zero rows; return conflict/superseded state |
| Provider reports a partial conditional write | Fail the publication; D1 retains the prior active revision and search rejects every mismatched provider response |
| Replacement has fewer chunks | All historical surplus slots become non-live tombstones in the same write |
| D1 graph scope is empty | Return no results without calling Turbopuffer |
| A source file changes | Reproject from canonical application/R2 content, then publish a complete derived revision |

Cross-plane indexing is convergent rather than a distributed transaction. A
durable workflow retries the deterministic prepared mutation until D1 topology,
D1 publication state, and Turbopuffer agree. Search never joins arbitrary D1
rows to repair an already-limited result population; it performs one batched
identity check solely to prove that every ranked candidate belongs to the D1
active revision.

## Operational limits

- D1 is monitored for graph and application-table growth, but plaintext,
  vectors, and blobs do not consume its database budget.
- Turbopuffer atomic request bytes and stable slots per revision are bounded
  before generation allocation.
- Provider-specific vector dimensions, document size, attribute size,
  filterable scalar size, request size, and query-result limits are enforced at
  the adapter boundary.
- Topology expansions and result populations are bounded and paged.
- Partition identity, namespace generation, and embedding profile are immutable
  query filters derived from one workspace-partition value.
- Credentials and query text are excluded from telemetry causes and span
  attributes.

If one workspace eventually exceeds a D1 limit, its canonical files remain in
R2 and its retrieval corpus remains in Turbopuffer. The control plane can move
or shard that workspace deliberately; the ordinary workspace design does not
require pre-sharding every tenant.

## Consequences

### Positive

- Each storage engine performs the work it is designed for.
- Workspace data has a natural lifecycle and isolation boundary.
- D1 size tracks topology and application records rather than corpus size.
- Plaintext, metadata, BM25, and vectors can scale independently in
  Turbopuffer.
- Graph constraints retain correct pre-limit semantics.
- Ambiguous remote writes are recoverable without exposing partial revisions.
- Source files remain portable and can rebuild derived stores.

### Trade-offs

- Indexing spans two data-plane systems and therefore needs a durable
  coordinator rather than a single database transaction.
- Graph-constrained search requires a D1 read before the provider query.
- Every non-empty provider result requires one batched D1 active-revision read
  before candidates can become visible.
- Large eligible node populations may require a future alternate scope
  materialization strategy instead of one large `In` filter.
- Namespace generation rollout and retirement become explicit operational
  responsibilities.
- D1-per-workspace provisioning, quota monitoring, and migration orchestration
  belong in the platform control plane.

## Alternatives considered

### One shared PostgreSQL schema with a workspace column

Retained as a possible enterprise/control-plane pattern, but rejected as the
default data plane. It couples every workspace table and migration to global
multi-tenancy and creates a larger blast radius.

### PostgreSQL schema or table namespacing per workspace

Rejected for the general platform. Thousands of user-defined workspaces and
application tables create unbounded catalog and migration overhead.

### One D1 database for all workspaces

Rejected. It restores row-level multi-tenancy and couples unrelated workspace
capacity and lifecycle.

### D1 as the vector and plaintext store

Rejected. It spends the relational database budget on derived corpus data and
lacks the provider's ANN and managed lexical retrieval capabilities.

### Turbopuffer as the canonical graph

Rejected. Metadata filters are useful after a graph population is known, but
they are not a replacement for normalized, transactional edge topology and
multi-step adjacency operations.

### R2 as the only database

Rejected. R2 is the right canonical file/blob layer and rebuild source, but it
does not provide transactional graph traversal or low-latency ANN/BM25 search.

### Filter Turbopuffer results after `top_k`

Rejected. It produces incorrect graph semantics and silently destroys recall
when the highest-ranked global candidates are outside the eligible graph set.

### Distributed transactions across D1 and Turbopuffer

Rejected. Neither system offers a portable shared commit protocol. Durable
generation fencing, marker reconciliation, and deterministic replay provide a
smaller and testable consistency model.
