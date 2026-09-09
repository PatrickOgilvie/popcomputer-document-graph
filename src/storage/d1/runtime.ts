import { Effect, Layer, Option, Result, Schema } from "effect"
import {
  DocumentKeySchema,
  makeDocumentKey,
  type DocumentKey,
} from "../../document/document-identity.js"
import { JsonValueSchema, type JsonValue } from "../../document/json-value.js"
import {
  countGraphRelationReplacement,
  makeGraphRelationEdgeIdentity,
  planOutgoingGraphRelationReplacement,
  type GraphRelationCommit,
  type ReplaceOutgoingGraphRelations,
} from "../../graph/graph-relation.js"
import {
  GraphNodeStateSchema,
  GraphTopologyStore,
  GraphTopologyStoreFailed,
  type FindRelatedGraphNodes,
  type RelatedGraphNodeSet,
  type GraphNodePage,
  type GraphTopologyDeletion,
  type GraphTopologyPrune,
  type GraphTopologyStoreService,
  type ListGraphNodes,
  type PruneGraphTopology,
  type StoredGraphNode,
} from "../../graph/graph-topology.js"
import type {
  DocumentGraphD1Database,
  DocumentGraphD1PreparedStatement,
  DocumentGraphD1Result,
} from "./contract.js"

/** Maximum number of outgoing edges accepted by one atomic D1 replacement. */
export const D1_GRAPH_TOPOLOGY_MAX_OUTGOING_EDGES = 1_000

/** Maximum number of active relation declarations accepted by one D1 prune. */
export const D1_GRAPH_TOPOLOGY_MAX_REGISTERED_RELATIONS = 1_000

/** Maximum UTF-8 payload accepted by one JSON-driven D1 topology operation. */
export const D1_GRAPH_TOPOLOGY_MAX_MUTATION_BYTES = 1_000_000

/** Maximum number of document-kind filters accepted by one D1 catalog read. */
export const D1_GRAPH_TOPOLOGY_MAX_DOCUMENT_KIND_FILTERS = 1_000

/** Cloudflare binding required by the workspace-local D1 topology adapter. */
export interface D1GraphTopologyConfig {
  readonly database: DocumentGraphD1Database
}

// Fixed by the ordered D1 migration and mirrored in the optional Drizzle
// schema entry point. Keeping runtime SQL independent avoids loading an ORM
// merely to compose the structural D1 adapter.
const NodesTable = "\"document_graph_nodes\""
const RelationsTable = "\"document_graph_relations\""
const Utf8 = new TextEncoder()

const NonEmptyTextSchema = Schema.Trimmed.check(Schema.isNonEmpty())

const GraphEdgeIdentityRowSchema = Schema.Struct({
  relation_id: NonEmptyTextSchema,
  target_document_key: DocumentKeySchema,
})

const GraphNodeRowSchema = Schema.Struct({
  document_key: DocumentKeySchema,
  graph_id: NonEmptyTextSchema,
  document_kind: NonEmptyTextSchema,
  encoded_document_id: Schema.String,
  node_state: GraphNodeStateSchema,
})

const CountRowSchema = Schema.Struct({
  item_count: Schema.Number.pipe(
    Schema.check(Schema.isInt()),
    Schema.check(Schema.isGreaterThanOrEqualTo(0)),
  ),
})

type GraphEdgeIdentityRow = typeof GraphEdgeIdentityRowSchema.Type
type GraphNodeRow = typeof GraphNodeRowSchema.Type

interface TargetPayload {
  readonly documentKey: DocumentKey
  readonly documentKind: string
  readonly encodedDocumentId: string
}

interface EdgePayload {
  readonly relation: string
  readonly relationVersion: string
  readonly sourceDocumentKind: string
  readonly targetDocumentKey: DocumentKey
  readonly targetDocumentKind: string
}

interface RegisteredRelationPayload {
  readonly id: string
  readonly version: string
  readonly sourceDocumentKind: string
  readonly targetDocumentKind: string
}

class InvalidD1StoredState extends Error {
  override readonly name = "InvalidD1StoredState"
}

class D1TopologyCapacityExceeded extends Error {
  override readonly name = "D1TopologyCapacityExceeded"

  constructor(
    readonly capacity: string,
    readonly actual: number,
    readonly limit: number,
  ) {
    super(`D1 graph topology ${capacity} capacity exceeded`)
  }
}

const topologyFailure = (
  operation: GraphTopologyStoreFailed["operation"],
  cause: unknown,
): GraphTopologyStoreFailed =>
  new GraphTopologyStoreFailed({
    operation,
    reason: cause instanceof D1TopologyCapacityExceeded
      ? "capacity_exceeded"
      : cause instanceof InvalidD1StoredState
        ? "invalid_stored_state"
        : "unavailable",
    cause,
  })

const invalidStoredState = (message: string): InvalidD1StoredState =>
  new InvalidD1StoredState(message)

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This is the D1 row boundary and immediately decodes the value with GraphEdgeIdentityRowSchema.
const parseEdgeIdentityRow = (input: unknown): GraphEdgeIdentityRow => {
  try {
    return Schema.decodeUnknownSync(GraphEdgeIdentityRowSchema)(input, {
      onExcessProperty: "error",
    })
  } catch {
    throw invalidStoredState("D1 returned an invalid graph edge identity row")
  }
}

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This is the D1 row boundary and immediately decodes the value with CountRowSchema.
const parseCountRow = (input: unknown, rowKind: string): number => {
  try {
    return Schema.decodeUnknownSync(CountRowSchema)(input, {
      onExcessProperty: "error",
    }).item_count
  } catch {
    throw invalidStoredState(`D1 returned an invalid ${rowKind} count row`)
  }
}

const parseJson = (input: string): JsonValue => {
  let decoded: unknown
  try {
    decoded = JSON.parse(input)
  } catch {
    throw invalidStoredState("D1 returned malformed encoded document JSON")
  }

  try {
    return Schema.decodeUnknownSync(JsonValueSchema)(decoded, {
      onExcessProperty: "error",
    })
  } catch {
    throw invalidStoredState("D1 returned a non-JSON document identity")
  }
}

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This is the D1 row boundary and immediately decodes the value with GraphNodeRowSchema.
const parseGraphNodeRow = (input: unknown): StoredGraphNode => {
  let row: GraphNodeRow
  try {
    row = Schema.decodeUnknownSync(GraphNodeRowSchema)(input, {
      onExcessProperty: "error",
    })
  } catch {
    throw invalidStoredState("D1 returned an invalid graph node row")
  }

  const encodedId = parseJson(row.encoded_document_id)
  const derivedKey = makeDocumentKey({
    graph: row.graph_id,
    documentKind: row.document_kind,
    encodedId,
  })
  if (derivedKey !== row.document_key) {
    throw invalidStoredState("D1 returned a graph node with mismatched identity")
  }

  return {
    documentKey: row.document_key,
    reference: {
      graph: row.graph_id,
      kind: row.document_kind,
      id: encodedId,
    },
    state: row.node_state,
  }
}

const encodeJson = (input: JsonValue): string => {
  const encoded = JSON.stringify(input)
  if (encoded === undefined) {
    throw invalidStoredState("A graph document identity was not JSON encodable")
  }
  return encoded
}

const encodePayload = <Input>(input: Input): string => {
  const encoded = JSON.stringify(input)
  if (encoded === undefined) {
    throw invalidStoredState("A D1 graph topology payload was not JSON encodable")
  }
  return encoded
}

const assertPayloadCapacity = (
  payloads: ReadonlyArray<string>,
): void => {
  let bytes = 0
  for (const payload of payloads) {
    bytes += Utf8.encode(payload).byteLength
  }
  if (bytes > D1_GRAPH_TOPOLOGY_MAX_MUTATION_BYTES) {
    throw new D1TopologyCapacityExceeded(
      "mutation_bytes",
      bytes,
      D1_GRAPH_TOPOLOGY_MAX_MUTATION_BYTES,
    )
  }
}

const resultAt = (
  results: ReadonlyArray<DocumentGraphD1Result<unknown>>,
  index: number,
  operation: string,
): DocumentGraphD1Result<unknown> => {
  const result = results[index]
  if (result === undefined) {
    throw invalidStoredState(`D1 omitted the ${operation} batch result`)
  }
  return result
}

const replaceDocumentTopology = async (
  database: DocumentGraphD1Database,
  replacement: ReplaceOutgoingGraphRelations,
): Promise<GraphRelationCommit> => {
  const planned = planOutgoingGraphRelationReplacement(replacement)
  if (Result.isFailure(planned)) {
    throw invalidStoredState(
      `Invalid outgoing graph relation replacement: ${planned.failure}`,
    )
  }
  if (planned.success.edges.length > D1_GRAPH_TOPOLOGY_MAX_OUTGOING_EDGES) {
    throw new D1TopologyCapacityExceeded(
      "outgoing_edges",
      planned.success.edges.length,
      D1_GRAPH_TOPOLOGY_MAX_OUTGOING_EDGES,
    )
  }

  const targetsByKey = new Map<DocumentKey, TargetPayload>()
  const edges: Array<EdgePayload> = []
  for (const edge of planned.success.edges) {
    targetsByKey.set(edge.target.documentKey, {
      documentKey: edge.target.documentKey,
      documentKind: edge.target.reference.kind,
      encodedDocumentId: encodeJson(edge.target.reference.id),
    })
    edges.push({
      relation: edge.relation,
      relationVersion: edge.version,
      sourceDocumentKind: replacement.source.kind,
      targetDocumentKey: edge.target.documentKey,
      targetDocumentKind: edge.targetDocumentKind,
    })
  }

  const encodedSourceId = encodeJson(replacement.source.id)
  const encodedTargets = encodePayload(Array.from(targetsByKey.values()))
  const encodedEdges = encodePayload(edges)
  assertPayloadCapacity([encodedSourceId, encodedTargets, encodedEdges])

  const statements: Array<DocumentGraphD1PreparedStatement> = [
    database.prepare(
      `SELECT relation_id, target_document_key
       FROM ${RelationsTable}
       WHERE graph_id = ?1 AND source_document_key = ?2
       ORDER BY relation_id, target_document_key`,
    ).bind(replacement.graph, replacement.sourceDocumentKey),
    database.prepare(
      `INSERT INTO ${NodesTable}
         (graph_id, document_key, document_kind, encoded_document_id, node_state)
       VALUES (?1, ?2, ?3, ?4, 'Materialized')
       ON CONFLICT (graph_id, document_key) DO UPDATE SET
         document_kind = excluded.document_kind,
         encoded_document_id = excluded.encoded_document_id,
         node_state = 'Materialized'`,
    ).bind(
      replacement.graph,
      replacement.sourceDocumentKey,
      replacement.source.kind,
      encodedSourceId,
    ),
    database.prepare(
      `INSERT INTO ${NodesTable}
         (graph_id, document_key, document_kind, encoded_document_id, node_state)
       SELECT ?1,
              json_extract(target.value, '$.documentKey'),
              json_extract(target.value, '$.documentKind'),
              json_extract(target.value, '$.encodedDocumentId'),
              'Referenced'
       FROM json_each(?2) AS target
       WHERE true
       ON CONFLICT (graph_id, document_key) DO UPDATE SET
         document_kind = excluded.document_kind,
         encoded_document_id = excluded.encoded_document_id,
         node_state = CASE
           WHEN ${NodesTable}.node_state = 'Materialized' THEN 'Materialized'
           ELSE 'Referenced'
         END`,
    ).bind(replacement.graph, encodedTargets),
    database.prepare(
      `DELETE FROM ${RelationsTable}
       WHERE graph_id = ?1 AND source_document_key = ?2`,
    ).bind(replacement.graph, replacement.sourceDocumentKey),
    database.prepare(
      `INSERT INTO ${RelationsTable}
         (graph_id, relation_id, relation_version,
          source_document_key, source_document_kind,
          target_document_key, target_document_kind)
       SELECT ?1,
              json_extract(edge.value, '$.relation'),
              json_extract(edge.value, '$.relationVersion'),
              ?2,
              json_extract(edge.value, '$.sourceDocumentKind'),
              json_extract(edge.value, '$.targetDocumentKey'),
              json_extract(edge.value, '$.targetDocumentKind')
       FROM json_each(?3) AS edge`,
    ).bind(
      replacement.graph,
      replacement.sourceDocumentKey,
      encodedEdges,
    ),
    database.prepare(
      `DELETE FROM ${NodesTable} AS node
       WHERE node.graph_id = ?1
         AND node.node_state = 'Referenced'
         AND NOT EXISTS (
           SELECT 1 FROM ${RelationsTable} AS edge
           WHERE edge.graph_id = node.graph_id
             AND (edge.source_document_key = node.document_key
               OR edge.target_document_key = node.document_key)
         )`,
    ).bind(replacement.graph),
  ]

  const results = await database.batch<unknown>(statements)
  if (results.length !== statements.length) {
    throw invalidStoredState("D1 returned an incomplete replacement batch")
  }
  const previousRows = resultAt(results, 0, "previous-edge read").results
  const previous = new Set(
    previousRows.map((row) => {
      const parsed = parseEdgeIdentityRow(row)
      return makeGraphRelationEdgeIdentity({
        relation: parsed.relation_id,
        targetDocumentKey: parsed.target_document_key,
      })
    }),
  )
  return countGraphRelationReplacement(previous, planned.success.identities)
}

const OrphanReferencedNodePredicate = `
  node.graph_id = ?1
  AND node.node_state = 'Referenced'
  AND NOT EXISTS (
    SELECT 1 FROM ${RelationsTable} AS edge
    WHERE edge.graph_id = node.graph_id
      AND (edge.source_document_key = node.document_key
        OR edge.target_document_key = node.document_key)
  )`

const deleteNode = async (
  database: DocumentGraphD1Database,
  input: { readonly graph: string; readonly documentKey: DocumentKey },
): Promise<GraphTopologyDeletion> => {
  const statements: Array<DocumentGraphD1PreparedStatement> = [
    database.prepare(
      `SELECT count(*) AS item_count
       FROM ${RelationsTable}
       WHERE graph_id = ?1
         AND (source_document_key = ?2 OR target_document_key = ?2)`,
    ).bind(input.graph, input.documentKey),
    database.prepare(
      `SELECT count(*) AS item_count
       FROM ${NodesTable}
       WHERE graph_id = ?1 AND document_key = ?2`,
    ).bind(input.graph, input.documentKey),
    database.prepare(
      `DELETE FROM ${RelationsTable}
       WHERE graph_id = ?1
         AND (source_document_key = ?2 OR target_document_key = ?2)`,
    ).bind(input.graph, input.documentKey),
    database.prepare(
      `DELETE FROM ${NodesTable}
       WHERE graph_id = ?1 AND document_key = ?2`,
    ).bind(input.graph, input.documentKey),
    database.prepare(
      `SELECT count(*) AS item_count
       FROM ${NodesTable} AS node
       WHERE ${OrphanReferencedNodePredicate}`,
    ).bind(input.graph),
    database.prepare(
      `DELETE FROM ${NodesTable} AS node
       WHERE ${OrphanReferencedNodePredicate}`,
    ).bind(input.graph),
  ]
  const results = await database.batch<unknown>(statements)
  if (results.length !== statements.length) {
    throw invalidStoredState("D1 returned an incomplete node-deletion batch")
  }

  const relationCount = resultAt(results, 0, "edge-count read").results[0]
  const nodeCount = resultAt(results, 1, "node-count read").results[0]
  const referencedNodeCount = resultAt(
    results,
    4,
    "orphaned referenced-node count read",
  ).results[0]
  return {
    deletedRelations: parseCountRow(relationCount, "deleted edge"),
    deletedNodes: parseCountRow(nodeCount, "deleted node"),
    deletedReferencedNodes: parseCountRow(
      referencedNodeCount,
      "deleted referenced node",
    ),
  }
}

const registeredRelationPayload = (
  input: PruneGraphTopology,
): ReadonlyArray<RegisteredRelationPayload> =>
  input.registered.map((relation) => ({
    id: relation.id,
    version: relation.version,
    sourceDocumentKind: relation.sourceDocumentKind,
    targetDocumentKind: relation.targetDocumentKind,
  }))

const StaleRelationPredicate = `
  edge.graph_id = ?1
  AND NOT EXISTS (
    SELECT 1 FROM json_each(?2) AS active
    WHERE json_extract(active.value, '$.id') = edge.relation_id
      AND json_extract(active.value, '$.version') = edge.relation_version
      AND json_extract(active.value, '$.sourceDocumentKind') = edge.source_document_kind
      AND json_extract(active.value, '$.targetDocumentKind') = edge.target_document_kind
  )`

const pruneTopology = async (
  database: DocumentGraphD1Database,
  input: PruneGraphTopology,
): Promise<GraphTopologyPrune> => {
  if (
    input.registered.length > D1_GRAPH_TOPOLOGY_MAX_REGISTERED_RELATIONS
  ) {
    throw new D1TopologyCapacityExceeded(
      "registered_relations",
      input.registered.length,
      D1_GRAPH_TOPOLOGY_MAX_REGISTERED_RELATIONS,
    )
  }
  const encodedRegistered = encodePayload(registeredRelationPayload(input))
  assertPayloadCapacity([encodedRegistered])

  const statements: Array<DocumentGraphD1PreparedStatement> = [
    database.prepare(
      `SELECT count(*) AS item_count
       FROM ${RelationsTable} AS edge
       WHERE ${StaleRelationPredicate}`,
    ).bind(input.graph, encodedRegistered),
    database.prepare(
      `DELETE FROM ${RelationsTable} AS edge
       WHERE ${StaleRelationPredicate}`,
    ).bind(input.graph, encodedRegistered),
    database.prepare(
      `SELECT count(*) AS item_count
       FROM ${NodesTable} AS node
       WHERE ${OrphanReferencedNodePredicate}`,
    ).bind(input.graph),
    database.prepare(
      `DELETE FROM ${NodesTable} AS node
       WHERE ${OrphanReferencedNodePredicate}`,
    ).bind(input.graph),
  ]
  const results = await database.batch<unknown>(statements)
  if (results.length !== statements.length) {
    throw invalidStoredState("D1 returned an incomplete topology-prune batch")
  }

  const relationCount = resultAt(results, 0, "stale-edge count").results[0]
  const nodeCount = resultAt(results, 2, "orphan-node count").results[0]
  return {
    deletedRelations: parseCountRow(relationCount, "stale edge"),
    deletedReferencedNodes: parseCountRow(nodeCount, "orphan node"),
  }
}

const strongRead = (
  database: DocumentGraphD1Database,
  query: string,
  values: ReadonlyArray<unknown>,
): Promise<DocumentGraphD1Result<unknown>> =>
  database.withSession("first-primary").prepare(query).bind(...values).all()

const listNodes = async (
  database: DocumentGraphD1Database,
  input: ListGraphNodes,
): Promise<GraphNodePage> => {
  if (
    input.documentKinds.length > D1_GRAPH_TOPOLOGY_MAX_DOCUMENT_KIND_FILTERS
  ) {
    throw new D1TopologyCapacityExceeded(
      "document_kind_filters",
      input.documentKinds.length,
      D1_GRAPH_TOPOLOGY_MAX_DOCUMENT_KIND_FILTERS,
    )
  }

  const encodedDocumentKinds = encodePayload(input.documentKinds)
  const encodedStates = encodePayload(input.states)
  assertPayloadCapacity([encodedDocumentKinds, encodedStates])
  const after = Option.match(input.after, {
    onNone: () => null,
    onSome: (documentKey) => documentKey,
  })
  const result = await strongRead(
    database,
    `SELECT document_key, graph_id, document_kind,
            encoded_document_id, node_state
     FROM ${NodesTable}
     WHERE graph_id = ?1
       AND (
         json_array_length(?2) = 0 OR
         document_kind IN (SELECT value FROM json_each(?2))
       )
       AND (
         json_array_length(?3) = 0 OR
         node_state IN (SELECT value FROM json_each(?3))
       )
       AND (?4 IS NULL OR document_key > ?4)
     ORDER BY document_key
     LIMIT ?5`,
    [
      input.graph,
      encodedDocumentKinds,
      encodedStates,
      after,
      input.limit + 1,
    ],
  )
  const parsed = result.results.map(parseGraphNodeRow)
  if (parsed.length > input.limit + 1) {
    throw invalidStoredState("D1 returned too many graph catalog rows")
  }
  const hasMore = parsed.length > input.limit
  const nodes = hasMore ? parsed.slice(0, input.limit) : parsed
  const last = nodes.at(-1)
  return {
    nodes,
    next: hasMore && last !== undefined
      ? Option.some(last.documentKey)
      : Option.none(),
  }
}

const RelatedGraphNodeRowSchema = Schema.Struct({
  ...GraphNodeRowSchema.fields,
  request_ordinal: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
})

const findRelatedNodes = async (
  database: DocumentGraphD1Database,
  input: FindRelatedGraphNodes,
): Promise<ReadonlyArray<RelatedGraphNodeSet>> => {
  if (input.documentKeys.length === 0) return []
  const current = input.direction === "outgoing" ? "source" : "target"
  const related = input.direction === "outgoing" ? "target" : "source"
  const result = await strongRead(
    database,
    `SELECT requested.key AS request_ordinal,
            node.document_key, node.graph_id, node.document_kind,
            node.encoded_document_id, node.node_state
     FROM json_each(?2) AS requested
     CROSS JOIN ${NodesTable} AS node
     WHERE node.graph_id = ?1
       AND node.document_key IN (
         SELECT edge.${related}_document_key
         FROM ${RelationsTable} AS edge
         WHERE edge.graph_id = ?1
           AND edge.${current}_document_key = requested.value
           AND edge.${current}_document_kind = ?3
           AND edge.relation_id = ?4
           AND edge.relation_version = ?5
           AND edge.${related}_document_kind = ?6
         ORDER BY edge.${related}_document_key
         LIMIT ?7
       )
     ORDER BY requested.key, node.document_key`,
    [input.graph, JSON.stringify(input.documentKeys), input.documentKind, input.relation,
      input.relationVersion, input.relatedDocumentKind, input.limit],
  )
  const groups = input.documentKeys.map((documentKey) => ({
    documentKey,
    nodes: new Array<StoredGraphNode>(),
  }))
  for (const unknownRow of result.results) {
    let parsed: typeof RelatedGraphNodeRowSchema.Type
    try {
      parsed = Schema.decodeUnknownSync(RelatedGraphNodeRowSchema)(unknownRow, { onExcessProperty: "error" })
    } catch {
      throw invalidStoredState("D1 returned an invalid related graph node row")
    }
    const { request_ordinal, ...node } = parsed
    const group = groups[request_ordinal]
    if (group === undefined || group.nodes.length >= input.limit) {
      throw invalidStoredState("Invalid related graph node batch")
    }
    group.nodes.push(parseGraphNodeRow(node))
  }
  return groups
}

const makeD1GraphTopologyStore = (
  config: D1GraphTopologyConfig,
): GraphTopologyStoreService => ({
  replaceDocumentTopology: Effect.fn(
    "D1GraphTopology.replaceDocumentTopology",
  )((replacement) =>
    Effect.tryPromise({
      try: () => replaceDocumentTopology(config.database, replacement),
      catch: (cause) => topologyFailure("replace_document", cause),
    })
  ),
  deleteNode: Effect.fn("D1GraphTopology.deleteNode")((input) =>
    Effect.tryPromise({
      try: () => deleteNode(config.database, input),
      catch: (cause) => topologyFailure("delete_node", cause),
    })
  ),
  pruneTopology: Effect.fn("D1GraphTopology.pruneTopology")((input) =>
    Effect.tryPromise({
      try: () => pruneTopology(config.database, input),
      catch: (cause) => topologyFailure("prune_graph", cause),
    })
  ),
  listNodes: Effect.fn("D1GraphTopology.listNodes")((input) =>
    Effect.tryPromise({
      try: () => listNodes(config.database, input),
      catch: (cause) => topologyFailure("list_nodes", cause),
    })
  ),
  findRelatedNodes: Effect.fn("D1GraphTopology.findRelatedNodes")((input) =>
    Effect.tryPromise({
      try: () => findRelatedNodes(config.database, input),
      catch: (cause) => topologyFailure("find_related", cause),
    })
  ),
})

/**
 * Provide canonical graph topology through one workspace-local D1 database.
 *
 * Apply `migrations/d1/0001_graph_topology.sql` before using the Layer. Writes
 * use D1 transactional batches; exact reads use a fresh `first-primary`
 * session so graph-constrained retrieval does not use stale topology.
 */
export const d1GraphTopology = (
  config: D1GraphTopologyConfig,
): Layer.Layer<GraphTopologyStore> =>
  Layer.succeed(GraphTopologyStore, makeD1GraphTopologyStore(config))
