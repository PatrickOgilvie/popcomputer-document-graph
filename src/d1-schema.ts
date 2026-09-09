/**
 * Drizzle declarations for the fixed workspace-local D1 tables.
 *
 * This deliberately Drizzle-coupled entry point is separate from `./d1` so
 * applications using only the structural D1 adapter do not inherit Drizzle's
 * optional dialect declarations.
 */
export {
  d1DocumentGraphSchema,
  d1GraphTopologySchema,
  documentGraphNodes,
  documentGraphProjectionHeads,
  documentGraphProjectionMutationChunks,
  documentGraphProjectionMutations,
  documentGraphProjectionPublications,
  documentGraphRelations,
} from "./storage/d1/schema.js"
