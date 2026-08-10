import { Schema } from "effect"
import type {
  ChunkingStrategyShape,
  SectionChunkingStrategy,
} from "./chunking-strategy.js"
import {
  makeVectorProjection,
  type DefineVectorProjectionInput,
  type VectorProjection,
  type VectorProjectionShape,
} from "./vector-projection.js"

type RegisteredProjectionId<
  Projections extends ReadonlyArray<VectorProjectionShape>,
> = Projections[number]["id"]

/** Minimum runtime shape retained for every graph document definition. */
export interface DocumentDefinitionShape<
  IdSchema extends Schema.Schema.AnyNoContext = Schema.Schema.AnyNoContext,
  ValueSchema extends Schema.Schema.AnyNoContext = Schema.Schema.AnyNoContext,
  Projections extends ReadonlyArray<VectorProjectionShape> = ReadonlyArray<
    VectorProjectionShape
  >,
> {
  readonly id: IdSchema
  readonly value: ValueSchema
  readonly identify: (
    value: Schema.Schema.Type<ValueSchema>,
  ) => Schema.Schema.Type<IdSchema>
  readonly projections: Projections
}

/** Runtime registry shape accepted by one compiled document graph. */
export type DocumentDefinitions = Readonly<
  Record<string, DocumentDefinitionShape>
>

/** Registered document-kind union inferred from a document registry. */
export type DocumentKind<Documents extends DocumentDefinitions> =
  keyof Documents & string

/** Parsed document ID selected by one registered document kind. */
export type DocumentId<
  Documents extends DocumentDefinitions,
  Kind extends DocumentKind<Documents>,
> = Schema.Schema.Type<Documents[Kind]["id"]>

/** Parsed document value selected by one registered document kind. */
export type DocumentValue<
  Documents extends DocumentDefinitions,
  Kind extends DocumentKind<Documents>,
> = Schema.Schema.Type<Documents[Kind]["value"]>

/** Schema-defined document type and its registered semantic projections. */
export interface DocumentDefinition<
  IdSchema extends Schema.Schema.AnyNoContext,
  ValueSchema extends Schema.Schema.AnyNoContext,
  Projections extends ReadonlyArray<VectorProjectionShape>,
> extends DocumentDefinitionShape<IdSchema, ValueSchema, Projections> {
  /** Return a new definition with one additional typed vector projection. */
  readonly vectorise: <
    const ProjectionId extends string,
    const ProjectionVersion extends string,
    MetadataSchema extends Schema.Schema.AnyNoContext | undefined = undefined,
    Chunking extends ChunkingStrategyShape = SectionChunkingStrategy,
  >(
    input: DefineVectorProjectionInput<
      Schema.Schema.Type<ValueSchema>,
      ProjectionId,
      ProjectionVersion,
      MetadataSchema,
      Chunking
    > & {
      readonly id: ProjectionId extends RegisteredProjectionId<Projections>
        ? never
        : ProjectionId
    },
  ) => DocumentDefinition<
    IdSchema,
    ValueSchema,
    readonly [
      ...Projections,
      VectorProjection<
        Schema.Schema.Type<ValueSchema>,
        MetadataSchema extends Schema.Schema.AnyNoContext
          ? Schema.Schema.Type<MetadataSchema>
          : never,
        ProjectionId,
        ProjectionVersion,
        Chunking
      >,
    ]
  >
}

/** Schemas and identity projection for one graph document definition. */
export interface DefineDocumentInput<
  IdSchema extends Schema.Schema.AnyNoContext,
  ValueSchema extends Schema.Schema.AnyNoContext,
> {
  readonly id: IdSchema
  readonly value: ValueSchema
  readonly identify: (
    value: Schema.Schema.Type<ValueSchema>,
  ) => Schema.Schema.Type<IdSchema>
}

const makeDocumentDefinition = <
  IdSchema extends Schema.Schema.AnyNoContext,
  ValueSchema extends Schema.Schema.AnyNoContext,
  Projections extends ReadonlyArray<VectorProjectionShape>,
>(
  input: DefineDocumentInput<IdSchema, ValueSchema>,
  projections: Projections,
): DocumentDefinition<IdSchema, ValueSchema, Projections> => ({
  id: input.id,
  value: input.value,
  identify: input.identify,
  projections,
  vectorise: (projection) =>
    makeDocumentDefinition(input, [
      ...projections,
      makeVectorProjection(projection),
    ]),
})

/** Define one graph document and how its parsed value yields node identity. */
const defineDocumentAdvanced = <
  IdSchema extends Schema.Schema.AnyNoContext,
  ValueSchema extends Schema.Schema.AnyNoContext,
>(
  input: DefineDocumentInput<IdSchema, ValueSchema>,
): DocumentDefinition<IdSchema, ValueSchema, readonly []> =>
  makeDocumentDefinition(input, [] as const)

type DirectSchemaFieldKey<Fields extends Schema.Struct.Fields> = {
  [Key in keyof Fields]: Fields[Key] extends Schema.Schema.AnyNoContext
    ? Key
    : never
}[keyof Fields] & string

/** Define a document whose identity is stored in one of its schema fields. */
export function defineDocument<
  Fields extends Schema.Struct.Fields,
  const IdKey extends DirectSchemaFieldKey<Fields>,
>(
  value: Schema.Struct<Fields> extends Schema.Schema.AnyNoContext
    ? Schema.Struct<Fields>
    : never,
  input: { readonly id: IdKey },
): DocumentDefinition<
  Extract<Fields[IdKey], Schema.Schema.AnyNoContext>,
  Extract<Schema.Struct<Fields>, Schema.Schema.AnyNoContext>,
  readonly []
>

/** Define a document with an advanced or composite identity projection. */
export function defineDocument<
  IdSchema extends Schema.Schema.AnyNoContext,
  ValueSchema extends Schema.Schema.AnyNoContext,
>(
  input: DefineDocumentInput<IdSchema, ValueSchema>,
): DocumentDefinition<IdSchema, ValueSchema, readonly []>

export function defineDocument(
  valueOrInput:
    | Schema.Struct<Schema.Struct.Fields>
    | DefineDocumentInput<
        Schema.Schema.AnyNoContext,
        Schema.Schema.AnyNoContext
      >,
  shorthand?: { readonly id: string },
) {
  if (shorthand === undefined) {
    return defineDocumentAdvanced(valueOrInput as DefineDocumentInput<
      Schema.Schema.AnyNoContext,
      Schema.Schema.AnyNoContext
    >)
  }

  const value = valueOrInput as Schema.Struct<Schema.Struct.Fields>
  const id = value.fields[shorthand.id]
  if (id === undefined) {
    throw new Error(`Unknown document identity field: ${shorthand.id}`)
  }

  // SAFETY: The shorthand overload only permits direct Schema fields, and
  // identify reads the same field selected from the parsed Struct value.
  return defineDocumentAdvanced({
    id: id as Schema.Schema.AnyNoContext,
    value: value as unknown as Schema.Schema.AnyNoContext,
    identify: (document) =>
      (document as Readonly<Record<string, unknown>>)[shorthand.id],
  })
}
