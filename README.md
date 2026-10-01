# @popcomputer/document-graph

Schema-first document graphs for Effect applications.

Define document types, searchable projections, metadata, chunking, and graph
relations once with Effect Schema. The package derives typed indexing, hybrid
search, graph retrieval, traversal, and grounding APIs from that definition.

```ts
// Run after the source Article changes.
yield* Articles.index(article)

// Run later, whenever a user searches.
const hits = yield* ArticleContent.search(
  "How do we handle enterprise authentication?",
  { limit: 10 },
)
```

The package includes PostgreSQL, Cloudflare D1 + Turbopuffer, and in-memory
storage. Applications retain control of embedding providers, canonical source
documents and files, authorization, and public response shapes.

## Features

- Effect Schema as the source of truth for documents and metadata
- Typed document, projection, relation, and retrieval handles
- Semantic, full-text, and reciprocal-rank-fused hybrid search
- Complete-revision delta indexing with embedding reuse
- Prepared-mutation capture and replay for deterministic publication retries
- Post-search evidence currency verification
- Schema-defined metadata filters and graph search scopes
- Target-oriented retrieval across direct and related evidence
- Typed incoming and outgoing neighbour traversal
- Canonical referenced/materialized graph nodes with bounded catalog queries
- D1-first, Turbopuffer-second graph-constrained retrieval
- Per-document custom chunking with section-level attribution
- PostgreSQL storage with no required extensions
- Workspace-local D1 topology and publication coordination
- Turbopuffer plaintext, vector, BM25, and metadata-filter retrieval
- In-memory storage and backend conformance suites
- Typed Effect failures and safe tracing attributes

## Installation

```sh
bun add @popcomputer/document-graph effect@4.0.0-rc.109 pg
```

The package currently targets Effect `4.0.0-rc.109`. The published
`@popcomputer/web@0.3.0-rc.1` package still declares an Effect v3 peer, so its
integration needs a v4-compatible release before the two packages can be used
together. `pg` is needed when composing the included PostgreSQL adapter.

For PostgreSQL, apply
[`migrations/postgres/0001_initial.sql`](./migrations/postgres/0001_initial.sql)
and
[`migrations/postgres/0002_mutation_locks.sql`](./migrations/postgres/0002_mutation_locks.sql),
then
[`migrations/postgres/0003_graph_topology.sql`](./migrations/postgres/0003_graph_topology.sql)
and
[`migrations/postgres/0004_native_vector_eligibility.sql`](./migrations/postgres/0004_native_vector_eligibility.sql)
with the application's migration tool. The runtime never modifies the database
schema.

For a workspace-local Cloudflare deployment, apply the ordered migrations in
[`migrations/d1`](./migrations/d1) to that workspace's D1 database. The
Turbopuffer adapter uses the official SDK and creates the pinned retrieval
schema as part of the inaugural atomic publication.

## Quick start

### 1. Define a document and its searchable projection

```ts
import { Schema } from "effect"
import {
  defineDocument,
  defineDocumentGraph,
} from "@popcomputer/document-graph"

export const ArticleSchema = Schema.Struct({
  id: Schema.String.check(Schema.isUUID()),
  title: Schema.Trimmed.check(Schema.isNonEmpty()),
  body: Schema.Trimmed.check(Schema.isNonEmpty()),
})

const ArticleDocument = defineDocument(ArticleSchema, { id: "id" }).vectorise({
  id: "content",
  version: "v1",
  select: (article) => ({
    context: article.title,
    sections: [
      {
        key: "body",
        content: article.body,
      },
    ],
  }),
})

export const KnowledgeGraph = defineDocumentGraph({
  id: "knowledge-base",
  documents: { Article: ArticleDocument },
})

export const Articles = KnowledgeGraph.document("Article")
export const ArticleContent = Articles.projection("content")
```

This expression defines three separate pieces of identity:

- `defineDocument(ArticleSchema, { id: "id" })` selects the `id` field from
  `ArticleSchema` as the document's graph-node identity. It does not set the ID
  to the string `"id"`. For each parsed Article, the package validates
  `article.id` with that field's schema and combines its encoded value with the
  graph ID and document kind to derive a stable storage key.
- `id: "content"` names one searchable projection of an Article. It is the
  value used by `Articles.projection("content")` and allows the same document
  type to expose other projections such as `summary` or `title`.
- `version: "v1"` is the application-owned compatibility version of the
  `content` projection. It describes the projection policy, not an individual
  Article revision or a package release.

The document identity shorthand is equivalent to the advanced form:

```ts
const ArticleDocument = defineDocument({
  id: ArticleSchema.fields.id,
  value: ArticleSchema,
  identify: (article) => article.id,
})
```

Changing an Article's title or body updates the same graph node. Treat its `id`
as immutable: another value describes another node. If an identity must change,
index the new Article and remove the previous node with
`Articles.remove(previousId)`. Change a projection ID only when introducing or
renaming a logical searchable view; the package treats the new ID as a new
stored projection. Change its version when evolving the meaning of the same
projection. Version changes and their rollout effects are detailed in
[Versioning policy](#versioning-policy).

This is the complete graph definition. It determines the document ID type,
projection name, persistent projection version, indexed text, and search result
reference. There is no separate vector-to-document mapping.

The example also establishes an important ingestion boundary. `ArticleSchema`
should contain application-ready content, not unprocessed HTML, PDF bytes, or
layout chrome. A source adapter should first extract meaningful structure and
normalise text; `select` then turns that structure into attributed sections.
For richer documents, prefer separate sections for headings, CMS blocks, table
rows, or evidence items rather than flattening everything into one body.

`context: article.title` preserves meaning when a body fragment is embedded in
isolation. The default chunker prefixes document context and the section label
to `embeddingContent`, while `hit.content` remains the focused fragment shown
to callers. Retrieval therefore benefits from surrounding meaning without
forcing every result or grounding prompt to repeat it.

The same code is typechecked in
[`examples/quick-start.ts`](./examples/quick-start.ts).

### How one input becomes searchable

An Article does not contain many documents in this model. `Article` is a
document kind, and each parsed Article value is one document instance and one
graph node. A document definition may have several projection recipes, and
every recipe is applied independently to each instance.

A projection is not an embedding. It is a named, versioned recipe that selects
searchable structure from a document. Running that recipe produces sections;
chunking splits each section into retrieval units; the embedding provider then
creates a vector for each chunk's `embeddingContent`.

```mermaid
flowchart TD
  subgraph Application["Application-owned source and schema"]
    Source["Article source of truth<br/>CMS, database, file, or API"]
    Value["Article value<br/>{ id, title, body }"]
    Schema["ArticleSchema"]
    Source --> Value
  end

  subgraph Definition["Schema-time graph definition"]
    Document["Article document definition<br/>identity: article.id"]
    Projections["Projection recipes<br/>content, summary, title, ..."]
    Schema --> Document
    Document --> Projections
  end

  subgraph Indexing["Index this one Article instance"]
    Node["Article graph node<br/>stable documentKey"]
    Revision["One projected revision<br/>per projection"]
    Sections["Attributed sections<br/>stable section keys + metadata"]
    Chunks["Searchable chunks<br/>content + embeddingContent"]
    Vectors["Embedding vectors<br/>one stored vector per chunk"]
    Edges["Typed outgoing relations<br/>to other document nodes"]

    Value -->|"parse + identify"| Node
    Document -->|"identity rule"| Node
    Projections -->|"run each recipe"| Revision
    Node --> Revision
    Revision --> Sections
    Sections -->|"chunk within each section"| Chunks
    Chunks -->|"embed embeddingContent"| Vectors
    Node -->|"run relation selectors"| Edges
  end

  subgraph Retrieval["Search and grounding"]
    Query["Search query"]
    QueryVector["Query vector"]
    Match["Rank matching chunks"]
    Hit["SearchHit<br/>document reference + section key + content"]
    Grounding["Hydrate the section or document<br/>from the source of truth"]

    Query --> QueryVector
    QueryVector --> Match
    Vectors --> Match
    Match --> Hit
    Hit --> Grounding
    Grounding -.-> Source
  end
```

The cardinality is therefore:

```text
one document kind
  -> many document instances / graph nodes
    -> many named projections
      -> one current revision per projection
        -> many attributed sections
          -> one or more chunks
            -> one embedding vector per chunk
```

The package stores the derived retrieval records, vectors, references, and
relations. The application remains the source of truth for the complete
Article, which is why a search hit can later hydrate larger grounding material
without treating the vector index as document storage.

### 2. Compose storage and embeddings

```ts
import { Effect, Layer } from "effect"
import { Pool } from "pg"
import {
  defineEmbeddingProfile,
  EmbeddingProvider,
  type EmbeddingProviderService,
} from "@popcomputer/document-graph"
import { postgresDocumentGraph } from "@popcomputer/document-graph/postgres"

export const documentGraphLive = (
  pool: Pool,
  embeddings: EmbeddingProviderService,
) =>
  Layer.mergeAll(
    Layer.succeed(EmbeddingProvider, embeddings),
    postgresDocumentGraph({ pool }),
  )
```

`postgresDocumentGraph` provides indexing, semantic search, full-text search,
and relations. The application supplies its embedding implementation through a
small Effect service:

```ts
const embeddings: EmbeddingProviderService = {
  profile: defineEmbeddingProfile({
    id: "provider/model-name",
    version: "v1", // Application-owned compatibility revision
    dimensions: 1_536,
  }),
  embedDocuments: (requests) =>
    embeddingAdapter.embedDocuments(requests),
  embedQuery: (query) => embeddingAdapter.embedQuery(query),
}
```

Use `defineEmbeddingProfile` when constructing the service so model identity,
configuration version, and dimensions are validated. The package does not
inspect the embedding adapter, so the application must declare when its output
is safe to reuse.

The complete `{ id, version, dimensions }` tuple identifies one compatible
vector space:

- `id` is a stable, application-chosen name for the model or embedding profile,
  such as `provider/model-name`. Change it when adopting a different model
  family or introducing a separate embedding purpose.
- `version` is the application's compatibility revision for that profile. Start
  with `v1` and increment it when evolving the same logical profile in a way
  that can produce meaningfully different document vectors.
- `dimensions` is the exact number of components returned by both embedding
  operations. Provider output with another size is rejected.

Bump the profile version when changing model revisions, output dimensions,
document preprocessing, task modes, prefixes, pooling, normalisation, or any
other adapter behaviour that makes stored vectors unsafe to reuse. Do not bump
it for API keys, retries, timeouts, batching, or an SDK upgrade that preserves
embedding behaviour. Source content, projection, metadata, and chunking changes
are tracked separately and do not require an embedding profile change.

When the tuple is unchanged, indexing can reuse vectors with matching content
hashes. When any part changes, existing vectors are treated as incompatible and
each document is re-embedded the next time it is indexed. Semantic search only
compares query vectors with stored revisions using the exact current tuple.
Changing a profile therefore requires a coordinated full reindex; semantic
coverage is partial while old and new profiles coexist during the rollout.

See [`examples/postgres-storage.ts`](./examples/postgres-storage.ts) for the
typechecked composition boundary.

### 3. Index when source content changes

```ts
const indexed = yield* Articles.index(article).pipe(
  Effect.provide(documentGraphLive(pool, embeddings)),
)
```

`Articles.index(article)` indexes every Article projection and replaces its
complete outgoing relation set. Call it when an Article is created or updated:
for example from the content write action, a CMS webhook, or a background sync.
Indexing is idempotent and delta-aware, so unchanged revisions do not create
new embeddings.

Indexing is also document-scoped. This call loads and reconciles only the
projection revisions and outgoing relations owned by the supplied Article. It
does not scan or update any other Article. Within each projection, vectors are
reused by `contentHash` whenever the embedding text and embedding profile are
unchanged:

| Change to this Article | Work performed for this Article |
|---|---|
| No effective projection or relation change | Each projection returns `Unchanged` with no embedding or projection replacement; the relation set is reconciled idempotently |
| Metadata, visibility, or citation only | Updates the stored revision and reuses existing vectors |
| One section's embedding text changes | Embeds only its new content hashes and reuses the others |
| A section or chunk is removed | Deletes stale records without embedding unchanged content |
| Document context changes, such as the title with the default chunker | Re-embeds chunks whose `embeddingContent` includes that context |
| Embedding profile changes | Re-embeds every chunk in this Article; the application must index the other Articles separately for a corpus-wide migration |

For focused background work, projection handles expose the lower-level
operations without updating graph relations:

```ts
const revision = yield* ArticleContent.project(article)
const indexed = yield* ArticleContent.index(article)
```

`project(article)` performs only deterministic parsing, projection, chunking,
and identity derivation; it does not call an embedding provider or storage.
`ArticleContent.index(article)` delta-indexes only the `content` projection for
this Article. `Articles.index(article)` is the normal complete operation because
it keeps every Article projection and the Article's outgoing relations in sync.

A multi-document reindex is application-orchestrated and is needed after a
policy change affecting existing stored documents, such as an embedding
profile, projection, chunker, or relation version change. The package still
processes one supplied document at a time, allowing the application to iterate
over only the affected document kinds, queue work, retry failures, and
rate-limit the migration using its existing background-work infrastructure.

### 4. Search the current index

```ts
const hits = yield* ArticleContent.search("authentication", {
  limit: 10,
}).pipe(Effect.provide(documentGraphLive(pool, embeddings)))
```

`ArticleContent.search(...)` reads the indexed projection and uses hybrid
semantic and text retrieval by default. It does not index the Article first.
Once source changes have been indexed, this operation can run for every search
request without re-embedding content.

## Web integrations

Graph operations remain ordinary Effects, so applications can place their HTTP
boundary around indexing and search without a transport-specific wrapper. The
published `@popcomputer/web@0.3.0-rc.1` package still requires Effect v3, so the
former Web action example is intentionally not included in this v4 release. It
can return once `@popcomputer/web` publishes an Effect v4-compatible version.

## Design model

The package is built around five concepts:

| Concept | Responsibility |
|---|---|
| Document | A typed graph node with stable identity |
| Projection | A versioned searchable view of one document type |
| Section | The smallest attribution and metadata boundary |
| Relation | A typed, versioned directed edge between document types |
| Retrieval | An application-named policy for ranking target nodes from evidence |

This separation keeps application intent compact while retaining explicit
persistent identity and provenance.

### Why the schema owns the graph

Document definitions are the only mapping between application values, vector
records, and graph nodes. From the graph definition, TypeScript infers:

- valid document IDs and document kinds;
- valid projection and relation names;
- projection metadata fields and filter values;
- legal retrieval routes and target kinds;
- result reference types;
- Effect service requirements.

For composite identity, use the advanced document form:

```ts
const LocalizedArticleId = Schema.Struct({
  site: Schema.Trimmed.check(Schema.isNonEmpty()),
  slug: Schema.Trimmed.check(Schema.isNonEmpty()),
})

const LocalizedArticle = Schema.Struct({
  site: Schema.Trimmed.check(Schema.isNonEmpty()),
  slug: Schema.Trimmed.check(Schema.isNonEmpty()),
  title: Schema.Trimmed.check(Schema.isNonEmpty()),
})

const LocalizedArticleDocument = defineDocument({
  id: LocalizedArticleId,
  value: LocalizedArticle,
  identify: (article) => ({
    site: article.site,
    slug: article.slug,
  }),
})
```

### Versioning policy

IDs answer “which policy is this?” Versions answer “which semantics does the
current form of that policy implement?” Keep an ID stable while evolving the
same logical policy, and increment its version when stored output produced by
the old implementation should no longer be treated as current. The package
does not infer versions from source code.

| Version supplied by the application | What it versions | When to change it | What happens when it changes |
|---|---|---|---|
| Projection `version` in `vectorise(...)` | The `select` mapping, section and attribution contract, metadata meaning, and semantic purpose of one projection ID | Change when the projection starts selecting or interpreting document material differently. Do not change for ordinary document edits. | Revision hashes change. Reindex every document owning the projection; the same projection storage is replaced and unchanged content embeddings can be reused. Search only accepts the registered projection version, so stale documents must not remain during rollout. |
| Chunker `version` in `defineChunker(...)` | The implementation and semantics of one reusable chunker ID | Change when splitting logic changes for the same configuration. Do not change merely because a schema-defined config value changes; encoded configuration already participates in revision identity. | Every projection using the chunker receives a new revision on reindex. Chunk positions are recomputed, stale chunks are deleted, and embeddings are reused only where embedding content remains identical. |
| Relation `version` in `relation(...)` | The persisted meaning, endpoints, and selector policy of one directed relation ID | Change when the relationship or the code selecting its targets changes meaning. Do not change when only one source document's relation values change. | Reindex all source documents for that relation to replace their complete outgoing edge sets. Traversal only reads the registered relation version, so old edges do not contribute while migration is incomplete. |
| Embedding profile `version` in `defineEmbeddingProfile(...)` | The vector-compatibility revision of one embedding profile ID | Change when the same text can produce vectors that are unsafe to reuse because of model, dimensions, preprocessing, task mode, prefix, pooling, normalisation, or similar changes. | Existing vectors are incompatible and cannot be reused. Re-embed the complete corpus; semantic search only includes stored revisions matching the current `{ id, version, dimensions }` tuple. |

Versions are opaque stable strings; the package does not interpret semantic
versioning. `v1`, `v2`, and so on are sufficient. Change the version owned by
the policy that changed rather than incrementing every version together:

- source content or metadata values require indexing, not a version bump;
- a projection mapping change requires a projection version bump;
- a chunking algorithm change requires a chunker version bump;
- a relationship policy change requires a relation version bump;
- an incompatible vector-space change requires an embedding profile version
  bump, or a new profile ID when adopting a distinct model family or purpose.

Treat version changes as data migrations. The included stores retain one
current revision per document and projection rather than parallel projection
versions. Coordinate each change with the affected reindex. When a projection
or relation ID is removed or renamed, reindex the active definitions and run
`KnowledgeGraph.reconcileIndex()` to prune storage belonging to definitions
that are no longer registered.
Reconciliation also removes stored revisions whose projection version no
longer matches the current definition.

## Search

### Hybrid search

Projection search defaults to hybrid retrieval when text search is enabled:

```ts
const hits = yield* WorkEvidence.search(query, { limit: 10 })
```

The semantic and text branches run concurrently. Their ranked lists are
combined by deterministic weighted reciprocal-rank fusion, so unrelated score
scales are never added together.

Hybrid search is a ranked union, not a requirement that every result match both
channels. A conceptually relevant semantic result can rank without containing
the query terms, and an exact lexical result can rank without a strong vector
score. When a condition is mandatory, express it as schema-validated metadata
or graph scope before candidate limiting, or select text-only search when a
lexical match is itself the requirement.

Use an explicit strategy when the query or available infrastructure requires
one channel:

```ts
const lexical = yield* WorkEvidence.search(query, {
  strategy: "text",
  limit: 10,
})

const semantic = yield* WorkEvidence.search(query, {
  strategy: "semantic",
  limit: 10,
})
```

The Effect requirements narrow with the strategy. Text search does not require
an embedding provider; semantic search does not require a text store.

Hybrid weighting and candidate budgets remain available as server-owned
policy:

```ts
const hits = yield* WorkEvidence.search(query, {
  strategy: {
    mode: "hybrid",
    weights: { semantic: 1, text: 2 },
    rankConstant: 60,
  },
  candidates: { semantic: 60, text: 40 },
  limit: 12,
})
```

Invalid runtime options fail as `InvalidSearchQuery` through the Effect error
channel. Do not pass unconstrained client-authored tuning values directly into
search.

### Metadata filters

Projection metadata is declared with Effect Schema:

```ts
const EvidenceMetadata = Schema.Struct({
  kind: Schema.Literals(["challenge", "approach", "outcome"]),
})
```

Object shorthand creates an implicit `AND`:

```ts
const outcomes = yield* WorkEvidence.search(query, {
  where: { kind: "outcome" },
  limit: 10,
})
```

The inferred builder supports Boolean composition:

```ts
const evidence = yield* WorkEvidence.search(query, {
  where: (filter) =>
    filter.any(
      filter.eq("kind", "challenge"),
      filter.eq("kind", "outcome"),
    ),
  limit: 10,
})
```

Unknown fields and invalid literal values are compile-time errors. Persisted
metadata is parsed again before it reaches application code. Filter expressions
are bounded to 100 leaves and depth 8.

### Graph-wide search

When the relevant document type is unknown, search every registered projection:

```ts
const allHits = yield* SiteGraph.search("national retail launch", {
  limit: 20,
})

const workHits = yield* SiteGraph.search("national retail launch", {
  include: ["Work"],
  includeProjections: ["work-evidence"],
  limit: 20,
})
```

Graph-wide search is currently semantic. Document and projection scopes are
schema-checked and applied before candidate limiting, so excluded records do
not consume candidate slots.

## Graph retrieval

Search returns chunks. Graph retrieval ranks a target document kind using one
or more application-defined evidence routes.

### Why similarity alone is sometimes the wrong query

A graph is unnecessary when the caller only needs the passages most similar to
a query. `WorkEvidence.search(query)` already handles that case.

A graph becomes important when the document containing the best evidence is
not the document the caller needs returned. Consider this query:

> Which agency has proved it can improve customer retention?

An Agency profile may only say “product and service design”. A related Work
document may contain the much stronger evidence: “The redesigned membership
journey increased twelve-month retention by 24%.” Searching only Agency
profiles can miss the Agency; searching every vector can find the Work but
returns the wrong entity. Vector similarity also cannot prove which Agency
delivered that Work.

```ts
const directAgencyHits = yield* AgencyProfile.search(query)
const relevantWorkHits = yield* WorkEvidence.search(query)
const rankedAgencies = yield* FindAgencies.search(query)
```

The first search only sees Agency profile text. The second can find the decisive
case study but returns Work hits. The named graph retrieval searches both
routes, follows `deliveredBy` from matching Work, and returns Agency targets
with the supporting chunks attached.

```mermaid
flowchart LR
  Query["Which agency improves retention?"]
  Evidence["Work evidence<br/>24% retention improvement"]
  Agency["Agency document<br/>the result the caller needs"]
  Query -->|"semantic or text relevance"| Evidence
  Evidence -->|"deliveredBy relation"| Agency
  Agency --> Result["Ranked Agency<br/>with attributed Work evidence"]
```

The two responsibilities stay deliberately separate:

- search ranks text that is relevant to the query;
- the graph follows an application-owned fact from that evidence to its target;
- retrieval fuses contributions from direct and related evidence by target;
- the result retains the source chunks that explain why the target ranked.

The same shape appears in many domains:

| Caller wants | Relevant text may live on | Authoritative relationship |
|---|---|---|
| An Agency | Case studies and Work outcomes | `Work -> deliveredBy -> Agency` |
| An expert | Articles, talks, and project contributions | `Evidence -> authoredBy -> Person` |
| A runbook | Incidents describing matching symptoms | `Incident -> mitigatedBy -> Runbook` |
| A governing policy | A matching clause or procedure | `Section -> belongsTo -> Policy` |
| A compatible product | Requirements and verified integrations | `Integration -> supportedBy -> Product` |

These queries could be implemented with bespoke searches followed by manual
joins. That is still a graph query, but with its route, target type, ranking,
and evidence handling scattered through application code. A named retrieval
policy keeps those decisions typed and reusable.

### Define a target-oriented retrieval policy

The complete example graph defines `Agency`, `Work`, and a `deliveredBy`
relation. It then compiles a reusable Agency retrieval policy:

```ts
export const FindAgencies = SiteGraph.retrieval({
  target: "Agency",
  routes: [
    AgencyProfile,
    WorkEvidence.through("deliveredBy", {
      weight: 2,
      neighboursPerSource: 5,
    }),
  ],
  maximumEvidencePerTarget: 3,
})

const agencies = yield* FindAgencies.search(
  "We need to improve customer retention",
  { limit: 6 },
)
```

This policy means:

- an Agency profile directly supports its own Agency;
- Work evidence supports Agencies reached through `deliveredBy`;
- related Work evidence has twice the route weight;
- every ranked Agency retains bounded evidence explaining its position.

The semantic index is responsible for finding relevant evidence; the graph is
responsible for facts such as which Agency delivered the Work. Keeping those
jobs separate avoids asking vector similarity to infer an exact relationship.
The final Agency result still carries the source evidence that caused it to
rank, so applications can explain and hydrate the result rather than returning
an opaque entity score.

`FindAgencies` belongs to the application, not the package. Other applications
can define `FindProducts`, `FindExperts`, or `FindArticles` from their own graph.
Illegal relation names, source kinds, and target kinds fail during authoring.

Semantic and text channels need different queries to work well: a sentence
embeds well, while full-text search wants a few precise phrases. `textQuery`
sends its own query to text channels while semantic channels still embed the
first argument:

```ts
const agencies = yield* FindAgencies.search(
  "close-up food photography and video with appetite appeal",
  { limit: 6, textQuery: '"food photography" OR "food styling"' },
)
```

When the application already knows which targets are eligible, such as the
agencies based in one country, resolve that population once with `within` and
pass it to any number of searches:

```ts
const inCountry = yield* FindAgencies.within(ukAgencyIds, {
  maximumDocuments: 10_000,
})

const agencies = yield* FindAgencies.search("close-up food photography", {
  limit: 6,
  within: inCountry,
})
```

Direct routes search only the targets' own documents. Relation routes search
only source documents related to a target: `within` reads canonical topology
once per route, so only those agencies' Work evidence competes for the
candidate budget. The adapter applies the population inside its semantic and
text queries, before candidate limits, rather than filtering a globally ranked
list, so a narrow population is not crowded out by better matches outside it.
A source related to several targets ranks only the ones in scope.

`maximumDocuments` bounds every route's population, up to
`MAX_GRAPH_SEARCH_TARGET_DOCUMENT_KEYS` (10,000). A larger population fails
with `InvalidSearchQuery` reason `scope_too_large` instead of searching an
arbitrary subset, so the caller can fall back deliberately, for example to an
unscoped search. An empty population is valid: searches within it return
nothing without embedding the query. A scope belongs to the retrieval that
resolved it, and another retrieval rejects it.

Each route discovers its own candidates before target ranking is fused. Agency
profiles and related Work evidence can therefore each introduce a relevant
Agency. Relationship expansion groups up to 100 distinct source documents into
one adapter read, preserving an independent `neighboursPerSource` bound for
every document. Larger populations use successive bounded batches.

The full graph is typechecked in
[`examples/site-graph.ts`](./examples/site-graph.ts).

### Topology queries and relation-constrained search

Relations also work without search:

```ts
const agencies = yield* WorkNode.relatedNodes(work.id, {
  via: "deliveredBy",
  limit: 25,
})

const work = yield* AgencyNode.relatedNodes(agency.id, {
  via: "deliveredBy",
  direction: "incoming",
  limit: 25,
})
```

Relations are stored in their declared direction and may be read in reverse.
Traversal returns typed document references; loading and authorizing source
documents remains an application responsibility. Limits default to 100 and are
bounded to 1,000.

The graph also exposes a bounded canonical node catalog:

```ts
const page = yield* SiteGraph.nodes({
  include: ["Agency"],
  states: ["Materialized"],
  limit: 100,
})
```

For the exact D1-then-Turbopuffer pattern, resolve a relation and search only
inside the resulting population:

```ts
const matchingAgencies = yield* WorkNode.searchWithin(
  work.id,
  "enterprise authentication specialists",
  {
    via: "deliveredBy",
    maximumDocuments: 250,
    search: { limit: 10 },
  },
)
```

`searchWithin` first asks canonical topology for related document keys. It then
passes a closed `NoDocuments` or non-empty `DocumentKeys` target to the search
adapter. Turbopuffer places that target in both ANN and BM25 filters before
`top_k`; it never searches globally and filters an already-limited result set.
An empty topology result performs no embedding or provider query.
`maximumDocuments` is required because topology applies that deterministic
key-ordered bound before ranking; choose it as an explicit recall and
provider-filter-size tradeoff.

### Evidence currency

Search hits carry the `revisionHash` of the indexed revision they were
produced from. When hydration happens later, ingestion may have replaced or
deleted that revision. Verify references instead of joining storage internals:

```ts
import {
  evidenceReferenceFromHit,
  verifyEvidenceCurrency,
} from "@popcomputer/document-graph"

const references = hits.map(evidenceReferenceFromHit)
const currencies = yield* verifyEvidenceCurrency(references)
// currencies[i] is "Current", "Stale", or "Missing", aligned with hits[i]
```

Currency is decided by hash equality against the currently stored revision:
the hash covers content, metadata, chunking policy, text-search policy, and
projection version. Re-embedding an otherwise identical revision leaves its
textual evidence `Current`; changing the projection version makes it `Stale`.
Absence reports `"Missing"` rather than failing; only storage failures fail
the effect. Drop non-`Current` evidence before presenting or hydrating it.

## Sections, metadata, and chunking

A section is the smallest attribution boundary. Every derived chunk belongs to
exactly one section and receives that section's validated metadata unchanged.
When provenance, visibility, trust, or citation changes, emit another section.

```ts
select: (work) => ({
  context: work.title,
  sections: work.evidence.map((evidence) => ({
    key: evidence.id,
    label: evidence.kind,
    content: evidence.text,
    metadata: {
      kind: evidence.kind,
    },
  })),
})
```

Here each authored evidence item becomes a semantic and attribution boundary.
The default chunker processes those sections independently and adds the Work
title and evidence label to the text used for embedding. The stored retrieval
content remains the precise evidence fragment. This preserves enough context
to disambiguate a small chunk without merging unrelated source material.

This rule also prevents chunks from combining content with different
authorization or citation requirements. Reading and chunking therefore
collaborate through a typed intermediate structure: the source adapter retains
meaningful document boundaries, the projection emits them as sections and
metadata, and the chunker only decides how to split within each section.

Source locations are application-owned metadata because web anchors, PDF
pages, CMS blocks, and timestamps use different addressing models:

```ts
const Citation = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("web"),
    url: Schema.String,
    anchor: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    _tag: Schema.Literal("pdf"),
    assetId: Schema.String,
    page: Schema.Number.pipe(
      Schema.check(Schema.isInt()),
      Schema.check(Schema.isGreaterThan(0)),
    ),
  }),
])
```

The default section-aware chunker uses a maximum of 1,800 characters. Override
it per projection when needed:

```ts
chunking: sectionChunking({ maximumCharacters: 3_000 })
```

Custom chunkers are reusable, schema-configured code values:

```ts
const fixedWindowChunking = defineChunker({
  id: "fixed-window",
  version: "v1",
  config: Schema.Struct({
    maximumCharacters: ChunkMaximumCharactersSchema,
  }),
  maximumCharacters: (config) => config.maximumCharacters,
  chunk: ({ section, config }) => {
    const fragments = []

    for (
      let offset = 0;
      offset < section.content.length;
      offset += config.maximumCharacters
    ) {
      const content = section.content
        .slice(offset, offset + config.maximumCharacters)
        .trim()

      if (content.length > 0) {
        fragments.push({ content })
      }
    }

    return fragments
  },
})
```

The fixed-window implementation demonstrates the smallest custom chunker
contract, not a universally suitable splitting policy. Sentence-, paragraph-,
heading-, or format-aware chunkers can preserve more natural semantic
boundaries when the source structure supports them.

Chunkers run independently for each section. They may repeat document context
in `embeddingContent` but cannot access neighbouring section content. This lets
an application add the context required to understand a fragment without
crossing its attribution boundary. Increment the chunker version when its
semantics change.

## Delta indexing

Indexing uses four deterministic identities:

| Identity | Meaning |
|---|---|
| `documentKey` | Stable graph-node identity |
| `chunkId` | Stable projection, section, and part identity |
| `contentHash` | Exact text sent to the embedding provider |
| `revisionHash` | Complete projection revision, including metadata and policy |

On re-index, the package:

- reuses vectors whose content hash and embedding profile are unchanged;
- updates metadata without requesting another embedding;
- embeds changed or new content;
- deletes stale chunk IDs and outgoing edges;
- reports an unchanged revision only when the stored inventory is complete.

These identities separate expensive semantic work from cheaper record updates.
A visibility, citation, or other metadata-only change updates `revisionHash`
but retains the same `contentHash`, allowing the stored vector to be reused.
With the default chunker, changing a document title, section label, or fragment
changes `embeddingContent` and correctly requests a new vector. Only new content
hashes are embedded rather than rebuilding the complete document
indiscriminately.

`EmbeddingProvider.embedDocuments` receives the unique missing content for one
projection revision as a batch. The provider adapter may split that work into
token-aware sub-batches and own rate limiting, retry, and backpressure policy.
Before storage is replaced, the core verifies that every requested content hash
has exactly one valid vector with the declared dimensions; partial or malformed
provider output fails the replacement. Corpus-level queues and worker
concurrency remain application-owned so ingestion can match the deployment's
actual provider limits.

Each projection replacement and each outgoing relation replacement is atomic
and idempotent. With pooled PostgreSQL, a document containing several
projections commits those steps independently. Retry a failed document index to
convergence.

Applications that require one PostgreSQL transaction across all projections
and relations may provide an active transaction:

```ts
const AtomicGraphLive = Layer.mergeAll(
  Layer.succeed(EmbeddingProvider, embeddings),
  postgresDocumentGraph({ transaction: client }),
)
```

The adapter uses savepoints and never commits the caller's transaction. This
mode can hold the transaction open while missing embeddings are requested, so
pooled operation remains the normal choice. See
[`ADR 0001`](./docs/decisions/0001-graph-index-atomicity-and-storage-ownership.md)
for the complete contract.

pg `Client` and `PoolClient` values are accepted directly. A proxy-specific
pinned transaction surface can opt in explicitly:

```ts
import {
  postgresDocumentGraph,
  postgresTransactionClient,
} from "@popcomputer/document-graph/postgres"

const transaction = postgresTransactionClient(proxyTransaction)
const StorageLive = postgresDocumentGraph({ transaction })
```

Do not wrap or pass a pool as `transaction`; pool queries are not guaranteed to
run on one connection. Use `postgresDocumentGraph({ pool })` instead.

### Atomic publication with prepared mutations

When publication must be all-or-nothing across application tables and graph
tables, resolve embeddings outside the write transaction by capturing a
complete prepared mutation first. Wrap your normal indexing Effect with
`prepareGraphMutation`: chunking, embedding calls, and planning execute
normally, but no storage is written. Preparation produces an immutable, deterministically
ordered operation set that replays into any storage - pooled for convergent
publication, or transaction-scoped when the replay must commit together with
application rows:

```ts
import {
  prepareGraphMutation,
  replayPreparedGraphMutation,
} from "@popcomputer/document-graph"

const { result, mutation } = yield* prepareGraphMutation(indexingProgram)

// Inside one short transaction: lock application rows, then
yield* replayPreparedGraphMutation(mutation, transactionScopedStorage)
// then mark application state indexed, then COMMIT.
```

Preparation returns the indexing result alongside the mutation and rejects two
mutations claiming the same identity with
`DuplicatePreparedMutation`, orders projections before topology replacements,
and performs no storage writes during capture. Replay performs no embedding
calls because every vector was resolved before capture. A remote adapter may
still call its storage provider while replaying the frozen payload. Its target
needs only `replaceRevision` and `replaceDocumentTopology`, rather than the
complete storage API. See
[`examples/atomic-publication.ts`](./examples/atomic-publication.ts) for a
compile-checked walkthrough.

Prepared mutation capture supports projection and outgoing-relation
replacements. Delete and reconciliation operations fail without touching live
storage; run those operations through their ordinary storage layer.

## Storage and Effect composition

The graph definition is independent of infrastructure. A storage Layer provides
only the services required by each operation.

For tests and local tools:

```ts
import { Layer } from "effect"
import { inMemoryDocumentGraph } from "@popcomputer/document-graph/in-memory"

const TestGraphLive = Layer.mergeAll(
  Layer.succeed(EmbeddingProvider, embeddings),
  inMemoryDocumentGraph(),
)
```

Every in-memory Layer owns isolated state. It implements the same replacement,
filtering, vector-reuse, relation, and optimistic-concurrency contracts as a
production adapter, but it is neither durable nor a performance simulator.

Embedding providers remain separate from storage. A model change does not
require a PostgreSQL adapter change, and a storage change does not alter source
projection code.

### Workspace D1 + Turbopuffer composition

The Cloudflare adapter deliberately composes two data-plane stores:

```text
R2-backed application filesystem  canonical files and blobs
                 |
                 v
document projection + external embeddings
        |                         |
        v                         v
workspace D1                 Turbopuffer
nodes + edges                text + vectors + BM25
publication CAS/journal      metadata filters + ranked chunks
```

D1 answers exact topology questions. Turbopuffer ranks only the population D1
resolved. PlanetScale can remain the platform control plane for workspaces,
ownership, deployment state, and quotas; it is not required in the package's
data-plane Layer.

```ts
import { Redacted } from "effect"
import { makeTurbopufferD1Workspace } from
  "@popcomputer/document-graph/turbopuffer"

const WorkspaceDataPlane = makeTurbopufferD1Workspace({
  workspace: workspace.id,
  database: env.WORKSPACE_DB,
  embeddings,
  turbopuffer: {
    apiKey: Redacted.make(env.TURBOPUFFER_API_KEY),
    deploymentId: workspace.turbopufferDeploymentId,
    endpoint: {
      _tag: "Region",
      region: workspace.turbopufferRegion,
    },
    schemaGeneration: 1,
  },
})

const WorkspaceGraphLive = WorkspaceDataPlane.layer
```

The facade derives one physical partition and uses it for the SDK endpoint,
namespace, D1 generation, publication rows, and retrieval filters. It exposes
the partition separately for safe administration and diagnostics, while the
application Layer contains only embeddings, topology, index, and search
capabilities. The provider client and publication coordinator stay internal.

Apply both D1 migrations before building the Layer. One D1 database may retain
side-by-side publication heads for different `indexGeneration` values during a
namespace or embedding-profile rollout. The active platform pointer chooses
which composed Layer serves traffic.

Applications using Drizzle can import `d1DocumentGraphSchema` from the
deliberately Drizzle-coupled `@popcomputer/document-graph/d1/schema` entry
point. It contains the topology, mutation inventory, publication-head, and
journal tables created by those migrations.

The adapter uses stable physical slots for each document projection. Every
publication writes a marker, every live chunk, and tombstones through the
historical slot high-water mark under one conditional Turbopuffer write. D1
allocates monotonically increasing generations and keeps the pending journal.
A timeout is reconciled by a strong marker read; it is never treated as proof
of either success or failure. See
[`ADR 0002`](./docs/decisions/0002-workspace-d1-and-turbopuffer-storage.md)
for the complete protocol and failure table.
The same composition is typechecked in
[`examples/cloudflare-workspace.ts`](./examples/cloudflare-workspace.ts).

### PostgreSQL + Turbopuffer composition

When topology already lives in PostgreSQL, keep it there and move only chunks,
vectors and full-text indexes to Turbopuffer. PostgreSQL also holds the
publication journal (migration 0006), so one database answers topology and
verifies every Turbopuffer candidate:

```ts
import { makeTurbopufferPostgresDocumentGraph } from
  "@popcomputer/document-graph/turbopuffer"

const Catalogue = makeTurbopufferPostgresDocumentGraph({
  workspace: "catalogue",
  embeddingProfile,
  postgres: { pool, schema: "honertia_document_graph" },
  turbopuffer: {
    apiKey: Redacted.make(env.TURBOPUFFER_API_KEY),
    deploymentId: "production",
    endpoint: { _tag: "Region", region: "aws-eu-west-2" },
    schemaGeneration: 1,
    vectorElementType: "f16",
  },
})
```

An existing PostgreSQL index moves without re-embedding.
`copyPostgresProjectionIndex` reads each stored revision with its vectors and
publishes it through the provided `ProjectionIndexStore`. It skips revisions
already present, so a copy can stop and resume. For a bulk copy, set
`coalesceWrites` so concurrent publications share Turbopuffer writes, and
`compression: true` when the copy is limited by upload bandwidth.

An opt-in live provider contract covers schema creation, upsert, strong ANN and
BM25 multi-query reads, overwrite, deletion, and disposable-namespace cleanup:

```sh
bun run test:live:turbopuffer
```

The command requires `TURBOPUFFER_API_KEY`,
`TURBOPUFFER_DEPLOYMENT_ID`, and exactly one of `TURBOPUFFER_REGION` or
`TURBOPUFFER_BASE_URL`. The default test and verification commands do not make
network requests, and credentials remain redacted at the test boundary.

## Grounding

Search hits carry compact, attributable retrieval content:

```ts
hit.reference
hit.projection
hit.sectionKey
hit.content
hit.metadata
```

An application-owned `GroundingHydrator` can load a complete section or source
document after ranking:

```ts
const GroundingLive = Layer.succeed(GroundingHydrator, {
  hydrate: ({ hit, level }) =>
    SourceContent.load(hit.reference, { level }).pipe(
      Effect.map((source) => ({
        content: source.content,
        metadata: source.metadata,
      })),
    ),
})

const grounding = yield* Effect.forEach(hits, (hit) =>
  hydrateGrounding(hit, { level: "section" }),
)
```

The hydrator owns source access, authorization, and format-specific locations.
The package retains the verified document reference and section key so the
hydrator cannot reattribute returned material.

Retrieval and grounding have different size requirements. A compact chunk is
useful for locating and ranking one precise piece of evidence; the model may
need the complete section or document to answer reliably. Hydrating only after
ranking preserves precise search while allowing the application to assemble
larger, authorized grounding material without making every indexed vector or
candidate unnecessarily broad.

### Evaluate policy changes in the application

No chunk size, embedding model, hybrid weight, or candidate budget is optimal
for every corpus. Keep a representative query set with expected document and
section matches, and evaluate retrieval separately from the quality of any
generated answer. This distinguishes a search failure from a grounding or
generation failure.

The graph manifest and explicit policy versions make experiments reproducible.
Change one policy at a time, record the manifest used for an evaluation run,
and test both conceptual queries and exact terminology. The package provides
deterministic retrieval mechanics; corpus-specific relevance remains an
application-owned measurement problem.

## Failures and observability

Search exposes a small public error union:

- `InvalidSearchQuery` for invalid caller input or runtime options;
- `DocumentGraphUnavailable` for unavailable or invalid external capabilities.

Indexing retains actionable domain failures such as invalid projection output
and `ProjectionIndexConflict`. Expected failures remain in the Effect error
channel.

Public I/O operations create package-owned `document_graph` spans containing
safe policy attributes such as graph, document kind, projection, strategy, and
limits. Queries, document IDs, content, metadata, vectors, and provider causes
are never attached.

Use `toDocumentGraphErrorTelemetry(error)` before recording a
`DocumentGraphUnavailable`; its internal cause is retained for error handling
but excluded from the returned telemetry record.

## PostgreSQL

```ts
import { Pool } from "pg"
import { postgresDocumentGraph } from "@popcomputer/document-graph/postgres"

const pool = new Pool({ connectionString: process.env.DATABASE_URL })
const StorageLive = postgresDocumentGraph({ pool })
```

The adapter uses the SQL namespace created by the included migration. Supply a
`schema` option when the application owns a different namespace.

The included PostgreSQL implementation:

- stores canonical embeddings as `double precision[]`;
- automatically uses installed pgvector for eligible vectors, with an
  extension-free float64 cosine fallback;
- scores every chunk eligible under graph, projection, metadata and embedding
  profile filters, then applies deterministic candidate limits;
- reuses stored full-text vectors for plain text and preserves separate field
  weights for attributed text;
- applies scope before candidate limits;
- protects complete revision replacement with transactions, exact-key row
  locks on a dedicated `mutation_locks` table, and optimistic tokens;
- stores directed relations in the same schema;
- requires no PostgreSQL extension.

Mutation serialization takes transaction-scoped row locks keyed by mutation
kind and exact document identity. Topology writes also lock shared source and
target nodes in document-key order. These locks work through Cloudflare
Hyperdrive and other proxies that do not support advisory lock functions.
Orphan cleanup skips nodes held by concurrent writers; a later
`reconcileIndex()` collects any remaining orphan references.

`vectorSearch` defaults to `"auto"`. The adapter discovers pgvector in its
installed schema, verifies the executing role's access to the schema, vector
type, distance function and array cast, and shares the lookup across concurrent
searches. An inaccessible extension uses the float64 fallback. Successful
lookups, including absence, are cached for five minutes per built storage
layer. Failed discovery is returned as a typed storage failure and is not
cached. The runtime never installs an extension.

Migration `0004_native_vector_eligibility.sql` adds a generated eligibility
flag computed when an embedding is written. Canonical vectors retain their
float64 precision. Native scoring uses pgvector's float32 representation, so
scores and the order of near ties can differ. Zero vectors, vectors above
16,000 dimensions, and extreme magnitudes use float64 scoring. For callers
requiring float64 scores throughout, set:

```ts
const StorageLive = postgresDocumentGraph({ pool, vectorSearch: "float64" })
```

Both paths perform exhaustive cosine search. The float64 path calculates the
query norm once per search. Measure representative latency before selecting
an approximate vector adapter for larger datasets.

### Approximate search with an HNSW index

Exhaustive search reads every embedding in scope, so its latency grows with the
corpus. With pgvector 0.8 or later, an HNSW index serves whole-graph searches
from a small part of the corpus instead:

```ts
const approximate = { mode: "approximate", index: "projected_chunks_embedding_hnsw", dimensions: 1024 } as const

// Once, outside a transaction, after migration 0005:
await pool.query(postgresVectorIndexSql({ index: approximate.index, dimensions: approximate.dimensions }))

const StorageLive = postgresDocumentGraph({ pool, vectorSearch: approximate, searchTimeoutMilliseconds: 3_000 })
```

`postgresVectorIndexSql` indexes an expression over the canonical float64
arrays, so it adds no column and needs no backfill. It builds with
`CREATE INDEX CONCURRENTLY`, which does not block reads or writes. Pass the
same `index`, `dimensions` and `representation` to both calls; the query must
repeat the indexed expression exactly for PostgreSQL to use it. `halfvec`, the
default, stores two bytes per dimension. Its predicate skips vectors outside
halfvec's range, so those rows stay writable and exhaustive search still finds
them for small document-key scopes. Use `representation: "vector"` for float32
index entries.

Searches take the nearest `candidates × overfetch` chunks (default 4×) from
the index with `hnsw.iterative_scan = relaxed_order`, applying graph,
projection, metadata and profile filters during the scan, then rescore them
exactly in float64 and return the requested count, so scores and ties match
`vectorSearch: "float64"`. Only recall is approximate. The
adapter checks the index is valid and maintained before using it, so a missing
index, a build in progress, or a failed build falls back to exhaustive search.
Other dimensions and query vectors outside the index type's range also search
exhaustively.

Document-key scopes, such as a retrieval `within` population, use the index
when they name more than `approximateAboveDocuments` documents (default
1,000), with the key filter applied during the scan. Smaller scopes are scored
exhaustively: exhaustive scoring reads every embedding in scope, which suits a
few hundred documents, and a very selective filter can exhaust
`maxScanTuples` before the index finds enough scoped chunks. Set it to 0 to
send every document-key scope to the index.

A failed concurrent build leaves an invalid index that writes still maintain.
Drop it with `DROP INDEX CONCURRENTLY` before retrying. Building needs roughly
the index size in `maintenance_work_mem` to stay in memory; below that pgvector
builds on disk, much more slowly.

`searchTimeoutMilliseconds` applies to every semantic and text search, with or
without an index. PostgreSQL cancels the statement itself, so a search its
caller abandoned does not keep running.

Full-text search ranks every match on the stored combined vector, then applies
the exact per-field weights to the best four matches per requested candidate.
Common phrases can still match much of a large corpus. Set
`textSearchTimeoutMilliseconds` to make text search best effort under its own
budget: a text search PostgreSQL cancels at that timeout returns no candidates,
so hybrid retrieval continues on its semantic channel. Other failures still
fail.

An application that fans out, such as one retrieval per key phrase, sends many
small searches at once, and each holds a pooled connection. Set
`coalesceSearches` to merge searches that arrive together:

```ts
const StorageLive = postgresDocumentGraph({
  pool,
  vectorSearch: approximate,
  coalesceSearches: { windowMilliseconds: 2, maximumBatch: 16 },
})
```

Semantic searches that share a scope, embedding profile and plan become one
statement with a branch per query vector, and topology reads for the same
relation become one read. Each branch has the single-search shape, so results
are identical to searching alone. An exhaustive plan reads the scope once for
every vector in the batch. The first search waits up to `windowMilliseconds`
for others; a full batch flushes at once. A batch runs under one statement
timeout and fails together.

The coalescer belongs to the storage Layer. Build that Layer per request where
the runtime ties I/O to a request, as Cloudflare Workers does with Hyperdrive.

The repository includes a reproducible comparison of the previous cosine
query and the optimized query, plus document-key filtering:

```sh
bun run build
TEST_DATABASE_URL=postgresql://localhost/document_graph_test \
  node benchmarks/postgres-semantic.mjs
```

The benchmark uses temporary synthetic data and rolls it back. It measures
database execution time; production latency also includes embedding and
network time. See the [0.4.0 review](./docs/reviews/0.4.0.md) for results and
verification limits.

For the complete Agency-profile plus Work-evidence retrieval workflow:

```sh
bun run build
TEST_DATABASE_URL=postgresql://localhost/document_graph_test \
  node benchmarks/postgres-agency-production.mjs
```

This compares `auto` and `float64` through the public graph API. On a disposable
server with pgvector's extension files available, `BENCHMARK_ENABLE_PGVECTOR=true`
creates the extension inside the benchmark's rolled-back transaction. An
optional `BENCHMARK_BASELINE_PACKAGE` points to an unpacked earlier package for
before/after measurement. See the [production results](./docs/experiments/agency-retrieval-production-2026-09-07.md).

## Adapter authors

`GraphTopologyStore.findRelatedNodes` accepts `documentKeys` and returns one
`RelatedGraphNodeSet` for each input position. Preserve input order, duplicate
keys and empty neighbour sets. Within each group, return unique nodes ordered
by document key, with `limit` applied independently to that source. Empty input
returns an empty array. Core workflows validate these guarantees before using
relations as retrieval evidence.

Low-level contracts are isolated behind the adapter entry point:

```ts
import {
  makeDocumentGraphStorage,
  planOutgoingGraphRelationReplacement,
  planProjectedRevisionReplacement,
  type ProjectionIndexStoreService,
  type ProjectionSearchStoreService,
} from "@popcomputer/document-graph/adapter"
```

Applications normally import only from `@popcomputer/document-graph`. Adapter
authors provide one storage object and expose its capabilities through
`makeDocumentGraphStorage`.

Package-owned planners validate complete revision and relation replacements,
resolve reusable embeddings, and derive exact upsert and deletion identities.
Adapters remain responsible for persistence mechanics, atomicity, row parsing,
scope-before-limit, descending scores, and deterministic ordering.

Run the conformance suite through the same services used in production:

```ts
import { Effect } from "effect"
import { verifyDocumentGraphStorageConformance } from
  "@popcomputer/document-graph/testing"

const report = await Effect.runPromise(
  verifyDocumentGraphStorageConformance().pipe(
    Effect.provide(MyDocumentGraphStorage),
  ),
)
```

The suite covers complete replacement, snapshot inventories, vector reuse,
optimistic conflicts, invalid-write atomicity, stale deletion, referenced and
materialized node state, graph pruning, bidirectional traversal, bounded
ordering, scope-before-limit, score ordering, candidate uniqueness, stable
ties, and repeatability. Run it against an isolated database or disposable
schema.

## Current boundaries

- Graph-wide search is semantic; projection search supports semantic, text,
  and hybrid strategies.
- PostgreSQL vector search is exact and does not include an ANN index.
- Graph retrieval composes direct and one-relation routes. Neighbour traversal
  and relation-constrained `searchWithin` are available, but there is no
  universal multi-hop path-search API.
- Explicit Turbopuffer document-key targets are bounded to 10,000 keys. Very
  large graph populations will need a future materialized-scope strategy.
- The Turbopuffer adapter uses external embeddings for its first release;
  provider-managed embedding remains a future optional adapter mode.
- D1 and Turbopuffer converge through a durable publication protocol rather
  than a distributed transaction. Durable indexing jobs must retry pending
  prepared mutations.
- Fuzzy typo-tolerant retrieval is not currently included.
- Sections, rather than universal source spans, are the attribution boundary.
- Source hydration, application authorization, and public response shaping
  remain application responsibilities.
- Projection, relation, and chunker policy versions are explicit rather than
  inferred from code changes.
- The in-memory adapter is a behavioural implementation, not a load-testing
  substitute.

These boundaries keep the package API stable across providers and storage
engines without obscuring application-owned policy.

## Package entry points

| Entry point | Intended use |
|---|---|
| `@popcomputer/document-graph` | Document schemas and application operations |
| `@popcomputer/document-graph/d1` | Workspace topology and TP publication coordination |
| `@popcomputer/document-graph/d1/schema` | Optional Drizzle declarations for package-owned D1 tables |
| `@popcomputer/document-graph/turbopuffer` | TP client, indexing, search, schema, and administration |
| `@popcomputer/document-graph/postgres` | PostgreSQL composition roots |
| `@popcomputer/document-graph/in-memory` | Tests and local tools |
| `@popcomputer/document-graph/adapter` | Storage adapter implementations |
| `@popcomputer/document-graph/testing` | Adapter conformance tests |

## Development

```sh
bun install
bun run verify
```

`bun run verify` runs strict TypeScript checks, type-level API tests,
behavioural tests, a production build, and Node ESM entry-point checks. The
PostgreSQL integration suite runs when both
`RUN_DOCUMENT_GRAPH_POSTGRES_TESTS=true` and `TEST_DATABASE_URL` are set.
`RUN_DOCUMENT_GRAPH_PGVECTOR_TESTS=true` additionally runs the native-scoring
suite against a disposable database whose server has pgvector extension files.
That suite creates its extension and fixtures transactionally and rolls them
back, including a namespace containing punctuation and a quote.

## License

MIT
