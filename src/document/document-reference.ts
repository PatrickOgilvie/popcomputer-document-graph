import type {
  DocumentDefinitions,
  DocumentId,
  DocumentKind,
} from "./document-definition.js"

/** Typed reference to one document instance in a graph definition. */
export type DocumentReference<
  GraphId extends string,
  Documents extends DocumentDefinitions,
  Kind extends DocumentKind<Documents> = DocumentKind<Documents>,
> = Kind extends DocumentKind<Documents>
  ? {
      readonly graph: GraphId
      readonly kind: Kind
      readonly id: DocumentId<Documents, Kind>
    }
  : never
