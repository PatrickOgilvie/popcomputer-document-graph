import type { JsonValue } from "../document/json-value.js"

/** Scalar metadata value supported by storage-neutral search filters. */
export type MetadataSearchValue = null | boolean | number | string

/** One leaf metadata comparison. */
export type MetadataSearchPredicate =
  | {
      readonly _tag: "Equals"
      readonly key: string
      readonly value: MetadataSearchValue
    }
  | {
      readonly _tag: "OneOf"
      readonly key: string
      readonly values: readonly [
        MetadataSearchValue,
        ...ReadonlyArray<MetadataSearchValue>,
      ]
    }

/** Bounded storage-neutral Boolean expression over projected metadata. */
export type MetadataFilter =
  | MetadataSearchPredicate
  | {
      readonly _tag: "All"
      readonly filters: readonly [
        MetadataFilter,
        ...ReadonlyArray<MetadataFilter>,
      ]
    }
  | {
      readonly _tag: "Any"
      readonly filters: readonly [
        MetadataFilter,
        ...ReadonlyArray<MetadataFilter>,
      ]
    }
  | { readonly _tag: "Not"; readonly filter: MetadataFilter }

type SearchableMetadataValue<Value> = Extract<
  Value,
  MetadataSearchValue
>

/** Metadata keys whose decoded values can be compared by search adapters. */
export type SearchableMetadataKey<Metadata> = {
  [Key in keyof Metadata & string]: [
    SearchableMetadataValue<Metadata[Key]>,
  ] extends [never]
    ? never
    : Key
}[keyof Metadata & string]

/** Schema-inferred constructor surface for composable metadata filters. */
export interface MetadataFilterBuilder<Metadata> {
  readonly eq: <Key extends SearchableMetadataKey<Metadata>>(
    key: Key,
    value: SearchableMetadataValue<Metadata[Key]>,
  ) => MetadataFilter
  readonly oneOf: <Key extends SearchableMetadataKey<Metadata>>(
    key: Key,
    values: readonly [
      SearchableMetadataValue<Metadata[Key]>,
      ...ReadonlyArray<SearchableMetadataValue<Metadata[Key]>>,
    ],
  ) => MetadataFilter
  readonly all: (
    first: MetadataFilter,
    ...rest: ReadonlyArray<MetadataFilter>
  ) => MetadataFilter
  readonly any: (
    first: MetadataFilter,
    ...rest: ReadonlyArray<MetadataFilter>
  ) => MetadataFilter
  readonly not: (filter: MetadataFilter) => MetadataFilter
}

const MaximumFilterLeaves = 100
const MaximumFilterDepth = 8

const parseKey = (key: string): string => {
  const parsed = key.trim()
  if (parsed.length === 0 || parsed.length > 200) {
    throw new Error("Metadata filter keys must contain 1 to 200 characters")
  }
  return parsed
}

const parseValue = (value: MetadataSearchValue): MetadataSearchValue => {
  if (typeof value === "number" && !Number.isFinite(value)) {
    throw new Error("Metadata filter numbers must be finite")
  }
  return value
}

const inspectFilter = (
  filter: MetadataFilter,
  depth: number,
): { readonly filter: MetadataFilter; readonly leaves: number } => {
  if (depth > MaximumFilterDepth) {
    throw new Error(
      `Metadata filters support at most depth ${MaximumFilterDepth}`,
    )
  }

  switch (filter._tag) {
    case "Equals":
      return {
        filter: {
          _tag: "Equals",
          key: parseKey(filter.key),
          value: parseValue(filter.value),
        },
        leaves: 1,
      }
    case "OneOf": {
      if (filter.values.length === 0 || filter.values.length > 100) {
        throw new Error(
          "Metadata set-membership searches support at least one and at most 100 values",
        )
      }
      const [first, ...rest] = filter.values
      return {
        filter: {
          _tag: "OneOf",
          key: parseKey(filter.key),
          values: [parseValue(first), ...rest.map(parseValue)],
        },
        leaves: 1,
      }
    }
    case "Not": {
      const child = inspectFilter(filter.filter, depth + 1)
      return {
        filter: { _tag: "Not", filter: child.filter },
        leaves: child.leaves,
      }
    }
    case "All":
    case "Any": {
      if (filter.filters.length === 0) {
        throw new Error("Boolean metadata filters cannot be empty")
      }
      const [firstFilter, ...restFilters] = filter.filters
      const firstChild = inspectFilter(firstFilter, depth + 1)
      const restChildren = restFilters.map((child) =>
        inspectFilter(child, depth + 1),
      )
      const children = [firstChild, ...restChildren]
      const leaves = children.reduce(
        (total, child) => total + child.leaves,
        0,
      )
      if (leaves > MaximumFilterLeaves) {
        throw new Error(
          `Metadata filters support at most ${MaximumFilterLeaves} leaves`,
        )
      }
      return {
        filter: {
          _tag: filter._tag,
          filters: [
            firstChild.filter,
            ...restChildren.map((child) => child.filter),
          ],
        },
        leaves,
      }
    }
  }
}

/** Validate and defensively copy one metadata filter expression. */
export const normalizeMetadataFilter = (
  filter: MetadataFilter,
): MetadataFilter => inspectFilter(filter, 1).filter

/** Validate a scope's implicit-AND filter list and its aggregate leaf bound. */
export const normalizeMetadataFilters = (
  filters: ReadonlyArray<MetadataFilter>,
): ReadonlyArray<MetadataFilter> => {
  const inspected = filters.map((filter) => inspectFilter(filter, 1))
  const leaves = inspected.reduce(
    (total, filter) => total + filter.leaves,
    0,
  )
  if (leaves > MaximumFilterLeaves) {
    throw new Error(
      `Metadata filters support at most ${MaximumFilterLeaves} leaves`,
    )
  }
  return inspected.map((filter) => filter.filter)
}

/** Define an equality condition over one top-level metadata field. */
export const metadataEquals = (
  key: string,
  value: MetadataSearchValue,
): MetadataSearchPredicate => ({
  _tag: "Equals",
  key: parseKey(key),
  value: parseValue(value),
})

/** Define a set-membership condition over one top-level metadata field. */
export const metadataOneOf = (
  key: string,
  values: readonly [
    MetadataSearchValue,
    ...ReadonlyArray<MetadataSearchValue>,
  ],
): MetadataSearchPredicate => {
  if (values.length > 100) {
    throw new Error(
      "Metadata set-membership searches support at most 100 values",
    )
  }
  const [first, ...rest] = values
  return {
    _tag: "OneOf",
    key: parseKey(key),
    values: [parseValue(first), ...rest.map(parseValue)],
  }
}

/** Define a conjunction of metadata filters. */
export const metadataAll = (
  first: MetadataFilter,
  ...rest: ReadonlyArray<MetadataFilter>
): MetadataFilter =>
  normalizeMetadataFilter({ _tag: "All", filters: [first, ...rest] })

/** Define a disjunction of metadata filters. */
export const metadataAny = (
  first: MetadataFilter,
  ...rest: ReadonlyArray<MetadataFilter>
): MetadataFilter =>
  normalizeMetadataFilter({ _tag: "Any", filters: [first, ...rest] })

/** Negate one metadata filter. */
export const metadataNot = (filter: MetadataFilter): MetadataFilter =>
  normalizeMetadataFilter({ _tag: "Not", filter })

/** Create a schema-parameterized Boolean metadata filter builder. */
export const makeMetadataFilterBuilder = <
  Metadata,
>(): MetadataFilterBuilder<Metadata> => ({
  eq: (key, value) => metadataEquals(key, value),
  oneOf: (key, values) => metadataOneOf(key, values),
  all: metadataAll,
  any: metadataAny,
  not: metadataNot,
})

/** Evaluate a normalized metadata filter with storage-neutral semantics. */
export const evaluateMetadataFilter = (
  metadata: JsonValue | undefined,
  filter: MetadataFilter,
): boolean => {
  switch (filter._tag) {
    case "All":
      return filter.filters.every((child) =>
        evaluateMetadataFilter(metadata, child),
      )
    case "Any":
      return filter.filters.some((child) =>
        evaluateMetadataFilter(metadata, child),
      )
    case "Not":
      return !evaluateMetadataFilter(metadata, filter.filter)
    case "Equals":
    case "OneOf": {
      if (
        metadata === undefined ||
        metadata === null ||
        Array.isArray(metadata) ||
        typeof metadata !== "object"
      ) {
        return false
      }
      // SAFETY: Primitive, null, and array JsonValue branches were eliminated.
      const record = metadata as Readonly<Record<string, JsonValue>>
      const value = record[filter.key]
      if (
        value === undefined ||
        (typeof value === "object" && value !== null)
      ) {
        return false
      }
      return filter._tag === "Equals"
        ? value === filter.value
        : filter.values.some((candidate) => candidate === value)
    }
  }
}
