/**
 * In-memory document-graph storage for local development and behavioural tests.
 *
 * This public subpath stays intentionally small; implementation details live
 * with the storage adapters.
 */
export { inMemoryDocumentGraph } from "./storage/in-memory.js"
