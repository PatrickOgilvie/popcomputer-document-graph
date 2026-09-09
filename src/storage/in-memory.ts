import {
  Array as EffectArray,
  Effect,
  Layer,
  Option,
  Result,
  Schema,
} from "effect"
import type { JsonValue } from "../document/json-value.js"
import {
  makeDocumentGraphStorage,
  type DocumentGraphStorageService,
} from "./document-graph-storage.js"
import type { EncodedDocumentReference } from "../document/document-instance.js"
import type { ContentHash, DocumentKey } from "../document/document-identity.js"
import {
  type CandidateFields,
  documentKeyMatchesGraphSearchTarget,
  type GraphSearchScope,
  projectionMatchesGraphSearchScope,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
} from "../retrieval/graph-retrieval.js"
import { evaluateMetadataFilter } from "../retrieval/metadata-filter.js"
import {
  countProjectedRevisionReplacement,
  embeddingProfilesEqual,
  IndexRevisionTokenSchema,
  planProjectedRevisionReplacement,
  ProjectionIndexConflict,
  ProjectionIndexStore,
  ProjectionIndexStoreFailed,
  type IndexedRevisionSnapshot,
  type ProjectedChunkRecord,
  type ReplaceProjectedRevision,
} from "../indexing/projection-index.js"
import {
  countGraphRelationReplacement,
  planOutgoingGraphRelationReplacement,
  type OutgoingGraphRelationTarget,
} from "../graph/graph-relation.js"
import {
  GraphTopologyStore,
  GraphTopologyStoreFailed,
  type StoredGraphNode,
} from "../graph/graph-topology.js"
import type {
  ProjectedText,
  TextSearchPolicy,
  TextSearchWeight,
} from "../document/text-search-policy.js"

interface StoredChunk {
  readonly record: ProjectedChunkRecord
  readonly vector: ReadonlyArray<number>
}

interface StoredRevision {
  readonly snapshot: IndexedRevisionSnapshot
  readonly documentKey: DocumentKey
  readonly encodedTarget: EncodedDocumentReference
  readonly projectionId: string
  readonly projectionVersion: string
  readonly chunks: ReadonlyArray<StoredChunk>
}

interface StoredGraphEdge {
  readonly graph: string
  readonly relation: string
  readonly relationVersion: string
  readonly sourceDocumentKey: DocumentKey
  readonly sourceDocumentKind: string
  readonly source: EncodedDocumentReference
  readonly target: OutgoingGraphRelationTarget
  readonly targetDocumentKind: string
}

interface InMemoryGraphNode extends StoredGraphNode {
  readonly graph: string
}

const storageKey = (input: {
  readonly documentKey: string
  readonly projection: string
}): string => `${input.documentKey}:${input.projection}`

const edgeKey = (edge: StoredGraphEdge): string =>
  `${edge.graph}:${edge.sourceDocumentKey}:${edge.relation}:${edge.target.documentKey}`

const nodeKey = (input: {
  readonly graph: string
  readonly documentKey: DocumentKey
}): string => `${input.graph}:${input.documentKey}`

const cloneJson = <Value extends JsonValue | undefined>(
  value: Value,
): Value => structuredClone(value)

const cloneReference = (
  reference: EncodedDocumentReference,
): EncodedDocumentReference => ({
  graph: reference.graph,
  kind: reference.kind,
  id: cloneJson(reference.id),
})

const cloneChunkRecord = (
  chunk: ProjectedChunkRecord,
): ProjectedChunkRecord => ({
  chunkId: chunk.chunkId,
  contentHash: chunk.contentHash,
  ordinal: chunk.ordinal,
  sectionKey: chunk.sectionKey,
  sectionIndex: chunk.sectionIndex,
  sectionPart: chunk.sectionPart,
  content: chunk.content,
  embeddingContent: chunk.embeddingContent,
  text: { ...chunk.text },
  metadata: cloneJson(chunk.metadata),
})

const cloneSnapshot = (
  snapshot: IndexedRevisionSnapshot,
): IndexedRevisionSnapshot => ({
  token: snapshot.token,
  revisionHash: snapshot.revisionHash,
  embeddingProfile: {
    id: snapshot.embeddingProfile.id,
    version: snapshot.embeddingProfile.version,
    dimensions: snapshot.embeddingProfile.dimensions,
  },
  chunks: EffectArray.map(snapshot.chunks, (chunk) => ({
    chunkId: chunk.chunkId,
    contentHash: chunk.contentHash,
  })),
})

const invalidReplacement = (cause: string): ProjectionIndexStoreFailed =>
  new ProjectionIndexStoreFailed({
    operation: "replace_revision",
    reason: "invalid_stored_state",
    cause,
  })

const invalidTopologyStorage = (
  operation: GraphTopologyStoreFailed["operation"],
  cause: string,
): GraphTopologyStoreFailed =>
  new GraphTopologyStoreFailed({
    operation,
    reason: "invalid_stored_state",
    cause,
  })

const expectedTokenMatches = (
  current: StoredRevision | undefined,
  replacement: ReplaceProjectedRevision,
): boolean =>
  Option.match(replacement.expectedToken, {
    onNone: () => current === undefined,
    onSome: (expected) => current?.snapshot.token === expected,
  })

const cosineSimilarity = (
  left: ReadonlyArray<number>,
  right: ReadonlyArray<number>,
): number => {
  let dotProduct = 0
  let leftMagnitude = 0
  let rightMagnitude = 0
  for (let index = 0; index < left.length; index += 1) {
    const leftValue = left[index]
    const rightValue = right[index]
    if (leftValue === undefined || rightValue === undefined) {
      return 0
    }

    dotProduct += leftValue * rightValue
    leftMagnitude += leftValue * leftValue
    rightMagnitude += rightValue * rightValue
  }

  if (leftMagnitude === 0 || rightMagnitude === 0) {
    return 0
  }

  return dotProduct / Math.sqrt(leftMagnitude * rightMagnitude)
}

const tokenize = (value: string): ReadonlyArray<string> =>
  value.toLocaleLowerCase("en").match(/[\p{L}\p{N}]+/gu) ?? []

const fieldScore = (
  queryTokens: ReadonlyArray<string>,
  value: string | undefined,
  weight: TextSearchWeight,
): number => {
  if (value === undefined || weight === 0) {
    return 0
  }

  const fieldTokens = tokenize(value)
  let matches = 0
  for (const queryToken of queryTokens) {
    for (const fieldToken of fieldTokens) {
      if (queryToken === fieldToken) {
        matches += 1
      }
    }
  }

  return matches * weight
}

const textScore = (
  queryTokens: ReadonlyArray<string>,
  text: ProjectedText,
  policy: Exclude<TextSearchPolicy, "disabled">,
): number =>
  fieldScore(queryTokens, text.context, policy.weights.context) +
  fieldScore(queryTokens, text.label, policy.weights.label) +
  fieldScore(queryTokens, text.content, policy.weights.content)

interface StoredCandidate {
  readonly score: number
  readonly revision: StoredRevision
  readonly chunk: StoredChunk
}

const projectStoredCandidate = (
  candidate: StoredCandidate,
): CandidateFields => ({
  score: candidate.score,
  chunkId: candidate.chunk.record.chunkId,
  documentKey: candidate.revision.documentKey,
  reference: cloneReference(candidate.revision.encodedTarget),
  projection: {
    id: candidate.revision.projectionId,
    version: candidate.revision.projectionVersion,
  },
  revisionHash: candidate.revision.snapshot.revisionHash,
  sectionKey: candidate.chunk.record.sectionKey,
  sectionPart: candidate.chunk.record.sectionPart,
  content: candidate.chunk.record.content,
  metadata: cloneJson(candidate.chunk.record.metadata),
})

const collectStoredCandidates = (input: {
  readonly revisions: Iterable<StoredRevision>
  readonly scope: GraphSearchScope
  readonly candidates: number
  readonly acceptRevision?: (revision: StoredRevision) => boolean
  readonly score: (
    revision: StoredRevision,
    chunk: StoredChunk,
  ) => number | undefined
}): ReadonlyArray<CandidateFields> => {
  const ranked: Array<StoredCandidate> = []
  for (const revision of input.revisions) {
    if (
      input.acceptRevision?.(revision) === false ||
      !projectionMatchesGraphSearchScope(input.scope, {
        graph: revision.encodedTarget.graph,
        documentKind: revision.encodedTarget.kind,
        projection: revision.projectionId,
        projectionVersion: revision.projectionVersion,
      }) ||
      !documentKeyMatchesGraphSearchTarget(
        input.scope.target,
        revision.documentKey,
      )
    ) {
      continue
    }

    for (const chunk of revision.chunks) {
      if (
        !input.scope.where.every((filter) =>
          evaluateMetadataFilter(chunk.record.metadata, filter),
        )
      ) {
        continue
      }

      const score = input.score(revision, chunk)
      if (score !== undefined) {
        ranked.push({ score, revision, chunk })
      }
    }
  }

  return ranked
    .sort(
      (left, right) =>
        right.score - left.score ||
        String(left.chunk.record.chunkId).localeCompare(
          String(right.chunk.record.chunkId),
        ),
    )
    .slice(0, input.candidates)
    .map(projectStoredCandidate)
}

const makeInMemoryStorage = (): DocumentGraphStorageService => {
  const revisions = new Map<string, StoredRevision>()
  const nodes = new Map<string, InMemoryGraphNode>()
  const edges = new Map<string, StoredGraphEdge>()
  let tokenSequence = 0

  const collectOrphanedReferencedNodes = (graph: string): number => {
    const connectedDocumentKeys = new Set<DocumentKey>()
    for (const edge of edges.values()) {
      if (edge.graph !== graph) continue
      connectedDocumentKeys.add(edge.sourceDocumentKey)
      connectedDocumentKeys.add(edge.target.documentKey)
    }

    let deletedReferencedNodes = 0
    for (const [key, node] of nodes) {
      if (
        node.graph !== graph ||
        node.state === "Materialized" ||
        connectedDocumentKeys.has(node.documentKey)
      ) {
        continue
      }
      nodes.delete(key)
      deletedReferencedNodes += 1
    }
    return deletedReferencedNodes
  }

  return {
    loadRevisions: (keys) =>
      Effect.sync(() =>
        keys.map((key) => {
          const revision = revisions.get(storageKey(key))
          return {
            key,
            revision: revision === undefined
              ? Option.none()
              : Option.some(cloneSnapshot(revision.snapshot)),
          }
        })
      ),

    replaceRevision: (replacement) =>
      Effect.gen(function*() {
        const key = storageKey(replacement.key)
        const current = revisions.get(key)
        if (!expectedTokenMatches(current, replacement)) {
          return yield* Effect.fail(
            new ProjectionIndexConflict({
              documentKey: replacement.key.documentKey,
              projection: replacement.key.projection,
            }),
          )
        }

        const reusableVectors = new Map<
          ContentHash,
          ReadonlyArray<number>
        >()
        if (
          current !== undefined &&
          embeddingProfilesEqual(
            current.snapshot.embeddingProfile,
            replacement.embeddingProfile,
          )
        ) {
          for (const chunk of current.chunks) {
            reusableVectors.set(chunk.record.contentHash, chunk.vector)
          }
        }

        const plan = planProjectedRevisionReplacement(
          replacement,
          reusableVectors,
        )
        if (Result.isFailure(plan)) {
          return yield* Effect.fail(invalidReplacement(plan.failure))
        }

        const nextChunks: Array<StoredChunk> = []
        for (const chunk of replacement.chunks) {
          const vector = plan.success.vectors.get(chunk.contentHash)
          if (vector === undefined) {
            return yield* Effect.die(
              new Error(
                "A planned projection replacement lost a required vector",
              ),
            )
          }

          nextChunks.push({
            record: cloneChunkRecord(chunk),
            vector: [...vector],
          })
        }

        const previousIds = new Set(
          current?.chunks.map((chunk) => chunk.record.chunkId) ?? [],
        )
        const counts = countProjectedRevisionReplacement(
          previousIds,
          plan.success.chunkIds,
        )

        tokenSequence += 1
        const token = Schema.decodeSync(IndexRevisionTokenSchema)(
          `memory-revision-${tokenSequence}`,
        )
        const snapshot: IndexedRevisionSnapshot = {
          token,
          revisionHash: replacement.revisionHash,
          embeddingProfile: {
            id: replacement.embeddingProfile.id,
            version: replacement.embeddingProfile.version,
            dimensions: replacement.embeddingProfile.dimensions,
          },
          chunks: EffectArray.map(replacement.chunks, (chunk) => ({
            chunkId: chunk.chunkId,
            contentHash: chunk.contentHash,
          })),
        }

        revisions.set(key, {
          snapshot,
          documentKey: replacement.key.documentKey,
          encodedTarget: cloneReference(replacement.encodedTarget),
          projectionId: replacement.key.projection,
          projectionVersion: replacement.projectionVersion,
          chunks: nextChunks,
        })

        return {
          token,
          ...counts,
        }
      }),

    deleteRevision: (key) =>
      Effect.sync(() => {
        const stored = revisions.get(storageKey(key))
        if (stored === undefined) {
          return { deletedRevisions: 0, deletedChunks: 0 }
        }

        revisions.delete(storageKey(key))
        return {
          deletedRevisions: 1,
          deletedChunks: stored.chunks.length,
        }
      }),

    pruneGraph: (input) =>
      Effect.sync(() => {
        let deletedRevisions = 0
        let deletedChunks = 0
        for (const [key, revision] of revisions) {
          if (revision.encodedTarget.graph !== input.graph) {
            continue
          }

          const registered = input.registered.some(
            (target) =>
              target.documentKind === revision.encodedTarget.kind &&
              target.projection === revision.projectionId &&
              (target.projectionVersion === undefined ||
                target.projectionVersion === revision.projectionVersion),
          )
          if (registered) {
            continue
          }

          revisions.delete(key)
          deletedRevisions += 1
          deletedChunks += revision.chunks.length
        }

        return { deletedRevisions, deletedChunks }
      }),

    searchCandidates: (request) =>
      Effect.sync(() =>
        collectStoredCandidates({
          revisions: revisions.values(),
          scope: request.scope,
          candidates: request.candidates,
          acceptRevision: (revision) =>
            embeddingProfilesEqual(
              revision.snapshot.embeddingProfile,
              request.embeddingProfile,
            ),
          score: (_revision, chunk) =>
            cosineSimilarity(request.vector, chunk.vector),
        }),
      ),

    searchTextCandidates: (request) =>
      Effect.sync(() => {
        const queryTokens = tokenize(request.query)
        return collectStoredCandidates({
          revisions: revisions.values(),
          scope: request.scope,
          candidates: request.candidates,
          score: (_revision, chunk) => {
            const score = textScore(
              queryTokens,
              chunk.record.text,
              request.policy,
            )
            return score > 0 ? score : undefined
          },
        })
      }),

    replaceDocumentTopology: (replacement) =>
      Effect.gen(function*() {
        const plan = planOutgoingGraphRelationReplacement(replacement)
        if (Result.isFailure(plan)) {
          return yield* Effect.fail(
            invalidTopologyStorage("replace_document", plan.failure),
          )
        }

        nodes.set(
          nodeKey({
            graph: replacement.graph,
            documentKey: replacement.sourceDocumentKey,
          }),
          {
            graph: replacement.graph,
            documentKey: replacement.sourceDocumentKey,
            reference: cloneReference(replacement.source),
            state: "Materialized",
          },
        )

        const next = new Map<string, StoredGraphEdge>()
        for (const planned of plan.success.edges) {
          const targetNodeKey = nodeKey({
            graph: replacement.graph,
            documentKey: planned.target.documentKey,
          })
          const existingTarget = nodes.get(targetNodeKey)
          nodes.set(targetNodeKey, {
            graph: replacement.graph,
            documentKey: planned.target.documentKey,
            reference: cloneReference(planned.target.reference),
            state: existingTarget?.state === "Materialized"
              ? "Materialized"
              : "Referenced",
          })
          const edge: StoredGraphEdge = {
            graph: replacement.graph,
            relation: planned.relation,
            relationVersion: planned.version,
            sourceDocumentKey: replacement.sourceDocumentKey,
            sourceDocumentKind: replacement.source.kind,
            source: cloneReference(replacement.source),
            target: {
              documentKey: planned.target.documentKey,
              reference: cloneReference(planned.target.reference),
            },
            targetDocumentKind: planned.targetDocumentKind,
          }
          next.set(edgeKey(edge), edge)
        }

        const previousKeys = new Set(
          Array.from(edges.entries())
            .filter(
              ([, edge]) =>
                edge.graph === replacement.graph &&
                edge.sourceDocumentKey === replacement.sourceDocumentKey,
            )
            .map(([key]) => key),
        )
        for (const key of previousKeys) {
          edges.delete(key)
        }
        for (const [key, edge] of next) {
          edges.set(key, edge)
        }

        collectOrphanedReferencedNodes(replacement.graph)

        return countGraphRelationReplacement(
          previousKeys,
          new Set(next.keys()),
        )
      }),

    deleteNode: (input) =>
      Effect.sync(() => {
        const deletedNodes = nodes.delete(nodeKey(input)) ? 1 : 0
        let deletedRelations = 0
        for (const [key, edge] of edges) {
          if (
            edge.graph === input.graph &&
            (edge.sourceDocumentKey === input.documentKey ||
              edge.target.documentKey === input.documentKey)
          ) {
            edges.delete(key)
            deletedRelations += 1
          }
        }
        const deletedReferencedNodes = collectOrphanedReferencedNodes(
          input.graph,
        )
        return { deletedNodes, deletedRelations, deletedReferencedNodes }
      }),

    pruneTopology: (input) =>
      Effect.sync(() => {
        let deletedRelations = 0
        for (const [key, edge] of edges) {
          if (edge.graph !== input.graph) {
            continue
          }

          const active = input.registered.some(
            (relation) =>
              relation.id === edge.relation &&
              relation.version === edge.relationVersion &&
              relation.sourceDocumentKind === edge.sourceDocumentKind &&
              relation.targetDocumentKind === edge.targetDocumentKind,
          )
          if (!active) {
            edges.delete(key)
            deletedRelations += 1
          }
        }
        const deletedReferencedNodes = collectOrphanedReferencedNodes(
          input.graph,
        )
        return { deletedRelations, deletedReferencedNodes }
      }),

    listNodes: (input) =>
      Effect.sync(() => {
        const after = Option.getOrUndefined(input.after)
        const page = Array.from(nodes.values())
          .filter(
            (node) =>
              node.graph === input.graph &&
              (input.documentKinds.length === 0 ||
                input.documentKinds.includes(node.reference.kind)) &&
              (input.states.length === 0 || input.states.includes(node.state)) &&
              (after === undefined ||
                String(node.documentKey).localeCompare(String(after)) > 0),
          )
          .sort((left, right) =>
            String(left.documentKey).localeCompare(String(right.documentKey)),
          )
          .slice(0, input.limit + 1)
        const hasMore = page.length > input.limit
        const visible = hasMore ? page.slice(0, input.limit) : page
        const last = visible.at(-1)
        return {
          nodes: visible.map((node) => ({
            documentKey: node.documentKey,
            reference: cloneReference(node.reference),
            state: node.state,
          })),
          next: hasMore && last !== undefined
            ? Option.some(last.documentKey)
            : Option.none(),
        }
      }),

    findRelatedNodes: (input) =>
      Effect.sync(() => {
        const relatedBySource = new Map<DocumentKey, Array<DocumentKey>>(
          input.documentKeys.map((documentKey) => [documentKey, []]),
        )
        for (const edge of edges.values()) {
          const outgoing = input.direction === "outgoing"
          const currentKey = outgoing ? edge.sourceDocumentKey : edge.target.documentKey
          const relatedKeys = relatedBySource.get(currentKey)
          if (relatedKeys === undefined || edge.graph !== input.graph ||
            edge.relation !== input.relation || edge.relationVersion !== input.relationVersion ||
            (outgoing ? edge.sourceDocumentKind : edge.targetDocumentKind) !== input.documentKind ||
            (outgoing ? edge.targetDocumentKind : edge.sourceDocumentKind) !== input.relatedDocumentKind) continue
          relatedKeys.push(outgoing ? edge.target.documentKey : edge.sourceDocumentKey)
        }
        return input.documentKeys.map((documentKey) => ({
          documentKey,
          nodes: [...(relatedBySource.get(documentKey) ?? [])]
            .sort((left, right) => String(left).localeCompare(String(right)))
            .slice(0, input.limit)
            .flatMap((relatedKey) => {
              const node = nodes.get(nodeKey({ graph: input.graph, documentKey: relatedKey }))
              return node === undefined ? [] : [{
                documentKey: node.documentKey,
                reference: cloneReference(node.reference),
                state: node.state,
              }]
            }),
        }))
      }),
  }
}

/**
 * Create an isolated in-memory index, semantic search, and text search Layer.
 *
 * State is private to the returned Layer instance and lasts for its provision
 * lifetime. Retrieval uses deterministic cosine/token scoring and applies graph
 * scopes before candidate limiting.
 */
export const inMemoryDocumentGraph = (): Layer.Layer<
  | ProjectionIndexStore
  | ProjectionSearchStore
  | ProjectionTextSearchStore
  | GraphTopologyStore
> => {
  const storage = makeInMemoryStorage()
  return makeDocumentGraphStorage(storage)
}
