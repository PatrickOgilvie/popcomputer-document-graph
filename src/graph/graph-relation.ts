import { Context, Effect, Either, Schema } from "effect"
import {
  type DocumentDefinitions,
  type DocumentId,
  type DocumentKind,
  type DocumentValue,
} from "../document/document-definition.js"
import {
  InvalidDocumentValue,
  parseDocumentInstance,
  type EncodedDocumentReference,
  type ParsedDocumentInstance,
} from "../document/document-instance.js"
import {
  encodeDocumentId,
  makeDocumentKey,
  type DocumentKey,
  type InvalidDocumentIdentity,
} from "../document/document-identity.js"

/** One schema-bound, directed relationship between two document types. */
export interface GraphRelationDefinition<
  From extends string,
  To extends string,
  Version extends string,
  Source,
  TargetId,
> {
  readonly from: From
  readonly to: To
  readonly version: Version
  readonly select: (source: Source) => ReadonlyArray<TargetId>
}

/** Minimum runtime shape retained for every graph relation declaration. */
export interface GraphRelationDefinitionShape {
  readonly from: string
  readonly to: string
  readonly version: string
  readonly select: (source: never) => ReadonlyArray<unknown>
}

/** Runtime relation registry compiled from one graph schema. */
export type GraphRelationDefinitions = Readonly<
  Record<string, GraphRelationDefinitionShape>
>

/** Contextual builder used while declaring relations for one document graph. */
export type DefineGraphRelation<Documents extends DocumentDefinitions> = <
  const From extends DocumentKind<Documents>,
  const To extends DocumentKind<Documents>,
  const Version extends string,
>(input: {
  readonly from: From
  readonly to: To
  readonly version: Version
  readonly select: (
    source: DocumentValue<Documents, From>,
  ) => ReadonlyArray<DocumentId<Documents, To>>
}) => GraphRelationDefinition<
  From,
  To,
  Version,
  DocumentValue<Documents, From>,
  DocumentId<Documents, To>
>

/** A relation selector returned targets outside its declared target schema. */
export class InvalidGraphRelationOutput extends Schema.TaggedError<
  InvalidGraphRelationOutput
>()("InvalidGraphRelationOutput", {
  graph: Schema.String,
  documentKind: Schema.String,
  relation: Schema.String,
  reason: Schema.Literal("invalid_targets", "duplicate_target"),
}) {}

/** Stable identifier for one directed relation declared by a graph schema. */
export const GraphRelationIdSchema = Schema.NonEmptyTrimmedString.pipe(
  Schema.maxLength(100),
  Schema.pattern(/^[A-Za-z0-9]+([._-][A-Za-z0-9]+)*$/),
)

/** Version of the persisted semantics for one directed relation. */
export const GraphRelationVersionSchema = Schema.NonEmptyTrimmedString.pipe(
  Schema.maxLength(100),
  Schema.pattern(/^[a-z0-9]+([._-][a-z0-9]+)*$/),
)

/** Maximum number of neighbours returned by one bounded traversal. */
export const GraphNeighbourLimitSchema = Schema.Number.pipe(
  Schema.int(),
  Schema.between(1, 1_000),
  Schema.brand("GraphNeighbourLimit"),
)

/** Maximum number of neighbours returned by one bounded traversal. */
export type GraphNeighbourLimit = typeof GraphNeighbourLimitSchema.Type

/** One target in a complete outgoing relation replacement. */
export interface OutgoingGraphRelationTarget {
  readonly documentKey: DocumentKey
  readonly reference: EncodedDocumentReference
}

/** Complete targets selected for one outgoing relation. */
export interface OutgoingGraphRelationSet {
  readonly id: string
  readonly version: string
  readonly targetDocumentKind: string
  readonly targets: ReadonlyArray<OutgoingGraphRelationTarget>
}

/** Atomic replacement of every declared outgoing relation for one source. */
export interface ReplaceOutgoingGraphRelations {
  readonly graph: string
  readonly sourceDocumentKey: DocumentKey
  readonly source: EncodedDocumentReference
  readonly relations: ReadonlyArray<OutgoingGraphRelationSet>
}

/** Structural reason a complete outgoing-relation replacement is invalid. */
export type OutgoingGraphRelationReplacementIssue =
  | "source_graph_mismatch"
  | "duplicate_relation"
  | "target_graph_mismatch"
  | "target_kind_mismatch"
  | "duplicate_edge"

/** One validated edge flattened from an outgoing relation replacement. */
export interface PlannedOutgoingGraphRelation {
  readonly relation: string
  readonly version: string
  readonly targetDocumentKind: string
  readonly target: OutgoingGraphRelationTarget
}

/** Validated structural plan shared by graph relation storage adapters. */
export interface OutgoingGraphRelationReplacementPlan {
  readonly identities: ReadonlySet<string>
  readonly edges: ReadonlyArray<PlannedOutgoingGraphRelation>
}

/** Derive the replacement-local identity used for relation commit counts. */
export const makeGraphRelationEdgeIdentity = (input: {
  readonly relation: string
  readonly targetDocumentKey: string
}): string => `${input.relation}\u0000${input.targetDocumentKey}`

/** Validate and flatten one complete outgoing-relation replacement. */
export const planOutgoingGraphRelationReplacement = (
  replacement: ReplaceOutgoingGraphRelations,
): Either.Either<
  OutgoingGraphRelationReplacementPlan,
  OutgoingGraphRelationReplacementIssue
> => {
  if (replacement.source.graph !== replacement.graph) {
    return Either.left("source_graph_mismatch")
  }

  const relationIds = new Set<string>()
  const identities = new Set<string>()
  const edges: Array<PlannedOutgoingGraphRelation> = []
  for (const relation of replacement.relations) {
    if (relationIds.has(relation.id)) {
      return Either.left("duplicate_relation")
    }
    relationIds.add(relation.id)

    for (const target of relation.targets) {
      if (target.reference.graph !== replacement.graph) {
        return Either.left("target_graph_mismatch")
      }
      if (target.reference.kind !== relation.targetDocumentKind) {
        return Either.left("target_kind_mismatch")
      }

      const identity = makeGraphRelationEdgeIdentity({
        relation: relation.id,
        targetDocumentKey: target.documentKey,
      })
      if (identities.has(identity)) {
        return Either.left("duplicate_edge")
      }
      identities.add(identity)
      edges.push({
        relation: relation.id,
        version: relation.version,
        targetDocumentKind: relation.targetDocumentKind,
        target,
      })
    }
  }

  return Either.right({ identities, edges })
}

/** Input for projecting every outgoing relation from one document value. */
export interface ProjectOutgoingGraphRelationsInput {
  readonly graph: string
  readonly documentKind: string
  readonly documents: DocumentDefinitions
  readonly relations: GraphRelationDefinitions
  readonly value: unknown
}

/** Input for projecting relations from one already parsed document instance. */
export interface ProjectOutgoingGraphRelationsForInstanceInput {
  readonly graph: string
  readonly documentKind: string
  readonly documents: DocumentDefinitions
  readonly relations: GraphRelationDefinitions
  readonly document: ParsedDocumentInstance
}

/** Project every outgoing relation from one already parsed document. */
export const projectOutgoingGraphRelationsForInstance = (
  input: ProjectOutgoingGraphRelationsForInstanceInput,
): Effect.Effect<
  ReplaceOutgoingGraphRelations,
  InvalidDocumentIdentity | InvalidGraphRelationOutput
> =>
  Effect.gen(function*() {
    const sourceValue = input.document.value
    const sourceDocumentKey = input.document.documentKey
    const source = input.document.encodedReference

    const outgoing = Object.entries(input.relations).filter(
      ([, relation]) => relation.from === input.documentKind,
    )
    const projected = yield* Effect.forEach(
      outgoing,
      ([relationId, relation]) => {
        const targetDefinition = input.documents[relation.to]
        if (targetDefinition === undefined) {
          return Effect.dieMessage(
            `Unknown relation target ${relation.to} for graph ${input.graph}`,
          )
        }

        // SAFETY: The graph compiler contextually typed this selector from the
        // parsed source schema and checked both document kinds are registered.
        const select = relation.select as (
          source: typeof sourceValue,
        ) => ReadonlyArray<unknown>
        return Effect.sync(() => select(sourceValue)).pipe(
          Effect.flatMap((targets) =>
            Schema.validate(Schema.Array(targetDefinition.id))(targets, {
              onExcessProperty: "error",
            }),
          ),
          Effect.mapError(
            () =>
              new InvalidGraphRelationOutput({
                graph: input.graph,
                documentKind: input.documentKind,
                relation: relationId,
                reason: "invalid_targets",
              }),
          ),
          Effect.flatMap((targetIds) =>
            Effect.forEach(targetIds, (targetId) =>
              encodeDocumentId(
                input.graph,
                relation.to,
                targetDefinition.id,
                targetId,
              ).pipe(
                Effect.map((encodedTargetId) => {
                  const documentKey = makeDocumentKey({
                    graph: input.graph,
                    documentKind: relation.to,
                    encodedId: encodedTargetId,
                  })
                  return {
                    documentKey,
                    reference: {
                      graph: input.graph,
                      kind: relation.to,
                      id: encodedTargetId,
                    },
                  }
                }),
              ),
            ),
          ),
          Effect.flatMap((targets) => {
            const keys = new Set(
              targets.map((target) => target.documentKey),
            )
            if (keys.size !== targets.length) {
              return Effect.fail(
                new InvalidGraphRelationOutput({
                  graph: input.graph,
                  documentKind: input.documentKind,
                  relation: relationId,
                  reason: "duplicate_target",
                }),
              )
            }

            return Effect.succeed({
              id: relationId,
              version: relation.version,
              targetDocumentKind: relation.to,
              targets,
            })
          }),
        )
      },
    )

    return {
      graph: input.graph,
      sourceDocumentKey,
      source,
      relations: projected,
    }
  })

/** Validate one document and project its complete outgoing relation set. */
export const projectOutgoingGraphRelations = (
  input: ProjectOutgoingGraphRelationsInput,
): Effect.Effect<
  ReplaceOutgoingGraphRelations,
  | InvalidDocumentValue
  | InvalidDocumentIdentity
  | InvalidGraphRelationOutput
> =>
  Effect.gen(function*() {
    const definition = input.documents[input.documentKind]
    if (definition === undefined) {
      return yield* Effect.dieMessage(
        `Unknown document kind ${input.documentKind} for graph ${input.graph}`,
      )
    }

    const document = yield* parseDocumentInstance({
      graph: input.graph,
      documentKind: input.documentKind,
      definition,
      value: input.value,
    })
    return yield* projectOutgoingGraphRelationsForInstance({
      graph: input.graph,
      documentKind: input.documentKind,
      documents: input.documents,
      relations: input.relations,
      document,
    })
  })

/** Observable changes from replacing one source document's outgoing edges. */
export interface GraphRelationCommit {
  readonly inserted: number
  readonly retained: number
  readonly deleted: number
}

/** Derive stable commit counts from complete previous and next edge sets. */
export const countGraphRelationReplacement = (
  previous: ReadonlySet<string>,
  next: ReadonlySet<string>,
): GraphRelationCommit => {
  let retained = 0
  for (const identity of next) {
    if (previous.has(identity)) retained += 1
  }

  let deleted = 0
  for (const identity of previous) {
    if (!next.has(identity)) deleted += 1
  }

  return {
    inserted: next.size - retained,
    retained,
    deleted,
  }
}

/** Idempotent result of deleting every edge touching one graph node. */
export interface GraphRelationDeletion {
  readonly deleted: number
}

/** One active relation retained while reconciling persisted graph edges. */
export interface RegisteredGraphRelation {
  readonly id: string
  readonly version: string
  readonly sourceDocumentKind: string
  readonly targetDocumentKind: string
}

/** Command for pruning edges whose relation declaration is no longer active. */
export interface PruneGraphRelations {
  readonly graph: string
  readonly registered: ReadonlyArray<RegisteredGraphRelation>
}

/** Result of pruning edges whose relation policy left the graph manifest. */
export interface GraphRelationPrune {
  readonly deleted: number
}

/** Bounded outgoing-neighbour request sent to a graph storage adapter. */
export interface FindOutgoingGraphNeighbours {
  readonly graph: string
  readonly sourceDocumentKey: DocumentKey
  readonly sourceDocumentKind: string
  readonly relation: string
  readonly relationVersion: string
  readonly targetDocumentKind: string
  readonly limit: GraphNeighbourLimit
}

/** Bounded incoming-neighbour request sent to a graph storage adapter. */
export interface FindIncomingGraphNeighbours {
  readonly graph: string
  readonly targetDocumentKey: DocumentKey
  readonly targetDocumentKind: string
  readonly relation: string
  readonly relationVersion: string
  readonly sourceDocumentKind: string
  readonly limit: GraphNeighbourLimit
}

/** Persisted neighbour reference returned by a graph storage adapter. */
export interface StoredGraphNeighbour {
  readonly documentKey: DocumentKey
  readonly reference: EncodedDocumentReference
}

/** Graph relation storage could not complete an operation. */
export class GraphRelationStoreFailed extends Schema.TaggedError<
  GraphRelationStoreFailed
>()("GraphRelationStoreFailed", {
  operation: Schema.Literal(
    "replace_outgoing",
    "delete_node",
    "prune_graph",
    "find_outgoing",
    "find_incoming",
  ),
  reason: Schema.Literal("unavailable", "invalid_stored_state"),
  cause: Schema.Unknown,
}) {}

/** A relation adapter returned neighbours outside its declared contract. */
export class InvalidGraphNeighbourOutput extends Schema.TaggedError<
  InvalidGraphNeighbourOutput
>()("InvalidGraphNeighbourOutput", {
  reason: Schema.Literal("too_many", "duplicate", "not_ordered"),
}) {}

/** Persistence capability consumed by graph relation workflows. */
export interface GraphRelationStoreService {
  /** Atomically replace every outgoing relation declared for one source. */
  readonly replaceOutgoing: (
    replacement: ReplaceOutgoingGraphRelations,
  ) => Effect.Effect<GraphRelationCommit, GraphRelationStoreFailed>

  /** Idempotently delete every incoming and outgoing edge for one node. */
  readonly deleteNode: (input: {
    readonly graph: string
    readonly documentKey: DocumentKey
  }) => Effect.Effect<GraphRelationDeletion, GraphRelationStoreFailed>

  /** Delete edges whose relation declaration is no longer registered. */
  readonly pruneRelations: (
    input: PruneGraphRelations,
  ) => Effect.Effect<GraphRelationPrune, GraphRelationStoreFailed>

  /** Find bounded, deterministically ordered outgoing neighbours. */
  readonly findOutgoing: (
    input: FindOutgoingGraphNeighbours,
  ) => Effect.Effect<ReadonlyArray<StoredGraphNeighbour>, GraphRelationStoreFailed>

  /** Find bounded, deterministically ordered incoming neighbours. */
  readonly findIncoming: (
    input: FindIncomingGraphNeighbours,
  ) => Effect.Effect<ReadonlyArray<StoredGraphNeighbour>, GraphRelationStoreFailed>
}

/** Effect service tag for graph relation persistence and traversal. */
export class GraphRelationStore extends Context.Tag(
  "@popcomputer/document-graph/GraphRelationStore",
)<GraphRelationStore, GraphRelationStoreService>() {}
