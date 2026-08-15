import { Schema } from "effect"
import type {
  ChunkingStrategyRuntime,
  SectionChunkingStrategy,
} from "./chunking-strategy.js"
import {
  makeVectorProjection,
  type DefineVectorProjectionInput,
  type VectorProjection,
  type RegisteredVectorProjection,
} from "./vector-projection.js"

type RegisteredProjectionId<
  Projections extends ReadonlyArray<RegisteredVectorProjection>,
> = Projections[number]["id"]

/** Minimum runtime shape retained for every graph document definition. */
export interface RegisteredDocumentDefinition<
  IdSchema extends Schema.Codec<unknown, unknown> = Schema.Codec<
    unknown,
    unknown
  >,
  ValueSchema extends Schema.Codec<unknown, unknown> = Schema.Codec<
    unknown,
    unknown
  >,
  Projections extends ReadonlyArray<RegisteredVectorProjection> = ReadonlyArray<
    RegisteredVectorProjection
  >,
> {
  readonly id: IdSchema
  readonly value: ValueSchema
  /** Derive the matching identity from a value parsed by `value`. */
  identify(
    value: Schema.Schema.Type<ValueSchema>,
  ): Schema.Schema.Type<IdSchema>
  readonly projections: Projections
}

/** Runtime registry shape accepted by one compiled document graph. */
export type DocumentDefinitions = Readonly<
  Record<string, RegisteredDocumentDefinition>
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
  IdSchema extends Schema.Codec<unknown, unknown>,
  ValueSchema extends Schema.Codec<unknown, unknown>,
  Projections extends ReadonlyArray<RegisteredVectorProjection>,
> extends RegisteredDocumentDefinition<IdSchema, ValueSchema, Projections> {
  /** Return a new definition with one additional typed vector projection. */
  readonly vectorise: <
    const ProjectionId extends string,
    const ProjectionVersion extends string,
    MetadataSchema extends Schema.Codec<unknown, unknown> | undefined = undefined,
    Chunking extends ChunkingStrategyRuntime = SectionChunkingStrategy,
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
        MetadataSchema extends Schema.Codec<unknown, unknown>
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
  IdSchema extends Schema.Codec<unknown, unknown>,
  ValueSchema extends Schema.Codec<unknown, unknown>,
> {
  readonly id: IdSchema
  readonly value: ValueSchema
  readonly identify: (
    value: Schema.Schema.Type<ValueSchema>,
  ) => Schema.Schema.Type<IdSchema>
}

const makeDocumentDefinition = <
  IdSchema extends Schema.Codec<unknown, unknown>,
  ValueSchema extends Schema.Codec<unknown, unknown>,
  Projections extends ReadonlyArray<RegisteredVectorProjection>,
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
  IdSchema extends Schema.Codec<unknown, unknown>,
  ValueSchema extends Schema.Codec<unknown, unknown>,
>(
  input: DefineDocumentInput<IdSchema, ValueSchema>,
): DocumentDefinition<IdSchema, ValueSchema, readonly []> =>
  makeDocumentDefinition(input, [] as const)

type DirectSchemaFieldKey<Fields extends Schema.Struct.Fields> = {
  [Key in keyof Fields]: Fields[Key] extends Schema.Codec<unknown, unknown>
    ? Key
    : never
}[keyof Fields] & string

/** Define a document whose identity is stored in one of its schema fields. */
export function defineDocument<
  Fields extends Schema.Struct.Fields,
  const IdKey extends DirectSchemaFieldKey<Fields>,
>(
  value: Schema.Struct<Fields> extends Schema.Codec<unknown, unknown>
    ? Schema.Struct<Fields>
    : never,
  input: { readonly id: IdKey },
): DocumentDefinition<
  Extract<Fields[IdKey], Schema.Codec<unknown, unknown>>,
  Extract<Schema.Struct<Fields>, Schema.Codec<unknown, unknown>>,
  readonly []
>

/** Define a document with an advanced or composite identity projection. */
export function defineDocument<
  IdSchema extends Schema.Codec<unknown, unknown>,
  ValueSchema extends Schema.Codec<unknown, unknown>,
>(
  input: DefineDocumentInput<IdSchema, ValueSchema>,
): DocumentDefinition<IdSchema, ValueSchema, readonly []>

export function defineDocument(
  valueOrInput:
    | Schema.Struct<Schema.Struct.Fields>
    | DefineDocumentInput<
        Schema.Codec<unknown, unknown>,
        Schema.Codec<unknown, unknown>
      >,
  shorthand?: { readonly id: string },
) {
  if (shorthand === undefined) {
    // SAFETY: The advanced overload is the only call form without shorthand.
    return defineDocumentAdvanced(valueOrInput as DefineDocumentInput<
      Schema.Codec<unknown, unknown>,
      Schema.Codec<unknown, unknown>
    >)
  }

  // SAFETY: The shorthand overload requires a Struct as its first argument.
  const value = valueOrInput as Schema.Struct<Schema.Struct.Fields>
  const id = value.fields[shorthand.id]
  if (id === undefined || !Schema.isSchema(id)) {
    throw new Error(`Unknown document identity field: ${shorthand.id}`)
  }

  const idSchema = Schema.make<Schema.Codec<unknown, unknown>>(id.ast)
  const valueSchema = Schema.make<Schema.Codec<unknown, unknown>>(
    value.ast,
  )

  return defineDocumentAdvanced({
    id: idSchema,
    value: valueSchema,
    identify: (document) =>
      Object.getOwnPropertyDescriptor(Object(document), shorthand.id)?.value,
  })
}
