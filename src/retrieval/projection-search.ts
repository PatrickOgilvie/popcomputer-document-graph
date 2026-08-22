import { Effect } from "effect"
import type { TextSearchPolicy } from "../document/text-search-policy.js"
import { EmbeddingProvider } from "../indexing/embedding-provider.js"
import type { RetrievalStrategy } from "./retrieval-strategy.js"
import type { ReciprocalRankConstant } from "./rank-fusion.js"
import {
  InvalidSearchQuery,
  ProjectionSearchStore,
  ProjectionTextSearchStore,
  searchGraph,
  searchGraphHybrid,
  searchGraphHybridWithSemanticQuery,
  searchGraphText,
  searchGraphWithPreparedSemanticQuery,
  semantic,
  text,
  type GraphSearchScope,
  type ProjectionRetrievalRouteKey,
  type SearchGraphError,
  type SearchHit,
  type SearchResultCount,
  type SemanticQueryPreparation,
  type SemanticSearchStrategy,
  type TextSearchStrategy,
} from "./graph-retrieval.js"

/** Closed execution plan for every projection-search mode. */
export type ProjectionSearchPlan =
  | {
      readonly _tag: "Semantic"
      readonly scope: GraphSearchScope
      readonly strategy: SemanticSearchStrategy
    }
  | {
      readonly _tag: "Text"
      readonly scope: GraphSearchScope
      readonly strategy: TextSearchStrategy
    }
  | {
      readonly _tag: "Hybrid"
      readonly scope: GraphSearchScope
      readonly route: ProjectionRetrievalRouteKey
      readonly semantic: SemanticSearchStrategy
      readonly text: TextSearchStrategy
      readonly results: SearchResultCount
      readonly rankConstant: ReciprocalRankConstant
    }

/** Effect services required by one compiled plan variant. */
export type ProjectionSearchRequirements<Plan extends ProjectionSearchPlan> =
  Plan extends { readonly _tag: "Semantic" }
    ? EmbeddingProvider | ProjectionSearchStore
    : Plan extends { readonly _tag: "Text" }
      ? ProjectionTextSearchStore
      : EmbeddingProvider | ProjectionSearchStore | ProjectionTextSearchStore

export interface CompileProjectionSearchPlanInput {
  readonly scope: GraphSearchScope
  readonly route: ProjectionRetrievalRouteKey
  readonly strategy: RetrievalStrategy
  readonly textPolicy: TextSearchPolicy
  readonly results: SearchResultCount
  readonly semanticCandidates: SearchResultCount
  readonly textCandidates: SearchResultCount
}

/** Turn parsed public search choices into one exhaustive runtime plan. */
export const compileProjectionSearchPlan = (
  input: CompileProjectionSearchPlanInput,
): Effect.Effect<ProjectionSearchPlan, InvalidSearchQuery> => {
  if (input.strategy._tag === "Semantic") {
    return Effect.succeed({
        _tag: "Semantic",
        scope: input.scope,
        strategy: semantic({
          candidates: input.semanticCandidates,
          results: input.results,
          weight: input.strategy.weight,
          rankConstant: input.strategy.rankConstant,
        }),
      })
  }
  if (input.textPolicy === "disabled") {
    return Effect.fail(new InvalidSearchQuery({ reason: "text_disabled" }))
  }
  if (input.strategy._tag === "Text") {
    return Effect.succeed({
        _tag: "Text",
        scope: input.scope,
        strategy: text({
          policy: input.textPolicy,
          candidates: input.textCandidates,
          results: input.results,
          weight: input.strategy.weight,
          rankConstant: input.strategy.rankConstant,
        }),
      })
  }
  return Effect.succeed({
        _tag: "Hybrid",
        scope: input.scope,
        route: input.route,
        semantic: semantic({
          candidates: input.semanticCandidates,
          results: input.semanticCandidates,
          weight: input.strategy.weights.semantic,
          rankConstant: input.strategy.rankConstant,
        }),
        text: text({
          policy: input.textPolicy,
          candidates: input.textCandidates,
          results: input.textCandidates,
          weight: input.strategy.weights.text,
          rankConstant: input.strategy.rankConstant,
        }),
        results: input.results,
        rankConstant: input.strategy.rankConstant,
      })
}

export interface ExecuteProjectionSearchInput<Plan extends ProjectionSearchPlan> {
  readonly query: string
  readonly plan: Plan
  readonly semanticQuery?: SemanticQueryPreparation | undefined
}

const executeProjectionSearchPlan = Effect.fn(
  "ProjectionSearch.execute",
)(function*(input: ExecuteProjectionSearchInput<ProjectionSearchPlan>) {
  const plan = input.plan
  switch (plan._tag) {
    case "Semantic":
      return input.semanticQuery === undefined
        ? yield* searchGraph({
            query: input.query,
            scope: plan.scope,
            strategy: plan.strategy,
          })
        : yield* input.semanticQuery.pipe(
            Effect.flatMap((query) =>
              searchGraphWithPreparedSemanticQuery({
                query,
                scope: plan.scope,
                strategy: plan.strategy,
              }),
            ),
          )
    case "Text":
      return yield* searchGraphText({
        query: input.query,
        scope: plan.scope,
        strategy: plan.strategy,
      })
    case "Hybrid": {
      const hybrid = {
        query: input.query,
        scope: plan.scope,
        route: plan.route,
        semantic: plan.semantic,
        text: plan.text,
        results: plan.results,
        rankConstant: plan.rankConstant,
      }
      return input.semanticQuery === undefined
        ? yield* searchGraphHybrid(hybrid)
        : yield* searchGraphHybridWithSemanticQuery({
            ...hybrid,
            semanticQuery: input.semanticQuery,
          })
    }
  }
})

/** Interpret one compiled plan while preserving channel-specific requirements. */
export const executeProjectionSearch = <Plan extends ProjectionSearchPlan>(
  input: ExecuteProjectionSearchInput<Plan>,
): Effect.Effect<
  ReadonlyArray<SearchHit>,
  SearchGraphError,
  ProjectionSearchRequirements<Plan>
> => {
  // SAFETY: The exhaustive plan tag determines exactly which services the
  // interpreter acquires; TypeScript widens the shared implementation body.
  return executeProjectionSearchPlan(input) as Effect.Effect<
    ReadonlyArray<SearchHit>,
    SearchGraphError,
    ProjectionSearchRequirements<Plan>
  >
}
