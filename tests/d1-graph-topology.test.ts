/// <reference types="@cloudflare/workers-types" />

/* oxlint-disable anti-slop/no-unknown-parameters, anti-slop/no-runtime-typeof, anti-slop/no-unsafe-dictionary-type -- This faithful test binding implements Cloudflare D1's deliberately unknown-valued boundary and narrows every SQLite binding before execution. */

import { describe, expect, test } from "bun:test"
import {
  Database,
  type SQLQueryBindings,
} from "bun:sqlite"
import { Effect, Option, Result, Schema } from "effect"
import {
  D1_GRAPH_TOPOLOGY_MAX_OUTGOING_EDGES,
  d1GraphTopology,
} from "../src/d1.js"
import {
  GraphNeighbourLimitSchema,
} from "../src/graph/graph-relation.js"
import {
  GraphNodePageLimitSchema,
  GraphTopologyStore,
} from "../src/graph/graph-topology.js"
import {
  makeDocumentKey,
  type DocumentKey,
} from "../src/document/document-identity.js"
import type { EncodedDocumentReference } from "../src/document/document-instance.js"
import { verifyGraphTopologyStoreConformance } from "../src/conformance/graph-topology-conformance.js"

interface KeyedReference {
  readonly documentKey: DocumentKey
  readonly reference: EncodedDocumentReference
}

type SqliteBinding = Exclude<SQLQueryBindings, Record<string, unknown>>

const sqliteBinding = (input: unknown): SqliteBinding => {
  if (
    input === null ||
    typeof input === "string" ||
    typeof input === "number" ||
    typeof input === "boolean" ||
    typeof input === "bigint" ||
    input instanceof Uint8Array
  ) {
    return input
  }
  throw new TypeError("The test D1 binding received an unsupported value")
}

const resultMeta = (
  changes: number,
): D1Meta & Record<string, unknown> => ({
  duration: 0,
  size_after: 0,
  rows_read: 0,
  rows_written: changes,
  last_row_id: 0,
  changed_db: changes > 0,
  changes,
})

class SqliteD1PreparedStatement implements D1PreparedStatement {
  constructor(
    private readonly database: Database,
    private readonly query: string,
    private readonly values: ReadonlyArray<unknown> = [],
  ) {
  }

  bind(...values: unknown[]): D1PreparedStatement {
    return new SqliteD1PreparedStatement(this.database, this.query, values)
  }

  executeSync<T>(): D1Result<T> {
    const bindings = this.values.map(sqliteBinding)
    const statement = this.database.query<unknown, Array<SqliteBinding>>(
      this.query,
    )
    const rows = statement.all(...bindings)
    const changes = this.database
      .query<{ readonly changes: number }, []>(
        "SELECT changes() AS changes",
      )
      .get()?.changes ?? 0
    return {
      success: true,
      // SAFETY: This fake implements D1's caller-selected row generic. The
      // production adapter still decodes every returned value from unknown.
      results: rows as Array<T>,
      meta: resultMeta(changes),
    }
  }

  async first<T = Record<string, unknown>>(): Promise<T | null>
  async first<T = unknown>(columnName: string): Promise<T | null>
  async first<T = Record<string, unknown>>(
    columnName?: string,
  ): Promise<T | null> {
    const first = this.executeSync<Record<string, unknown>>().results[0]
    if (first === undefined) return null
    if (columnName === undefined) {
      // SAFETY: The D1 first<T>() API delegates selection of T to its caller.
      return first as T
    }
    const value = first[columnName]
    // SAFETY: The D1 first<T>(column) API delegates the column type to caller.
    return value === undefined ? null : value as T
  }

  async run<T = Record<string, unknown>>(): Promise<D1Result<T>> {
    return this.executeSync<T>()
  }

  async all<T = Record<string, unknown>>(): Promise<D1Result<T>> {
    return this.executeSync<T>()
  }

  async raw<T = unknown[]>(options: {
    readonly columnNames: true
  }): Promise<[string[], ...T[]]>
  async raw<T = unknown[]>(options?: {
    readonly columnNames?: false
  }): Promise<T[]>
  async raw<T = unknown[]>(
    _options?: { readonly columnNames?: boolean },
  ): Promise<T[] | [string[], ...T[]]> {
    throw new Error("Raw D1 rows are outside this adapter's test seam")
  }
}

class SqliteD1Session implements D1DatabaseSession {
  constructor(private readonly owner: SqliteD1Database) {
  }

  prepare(query: string): D1PreparedStatement {
    return this.owner.prepare(query)
  }

  batch<T = unknown>(
    statements: D1PreparedStatement[],
  ): Promise<D1Result<T>[]> {
    return this.owner.batch(statements)
  }

  getBookmark(): string | null {
    return null
  }
}

class SqliteD1Database implements D1Database {
  readonly sessionConstraints: Array<D1SessionConstraint | string | undefined> = []

  constructor(private readonly sqlite: Database) {
  }

  prepare(query: string): D1PreparedStatement {
    return new SqliteD1PreparedStatement(this.sqlite, query)
  }

  async batch<T = unknown>(
    statements: D1PreparedStatement[],
  ): Promise<D1Result<T>[]> {
    this.sqlite.exec("BEGIN IMMEDIATE")
    try {
      const results: Array<D1Result<T>> = []
      for (const statement of statements) {
        if (!(statement instanceof SqliteD1PreparedStatement)) {
          throw new TypeError("The test D1 batch received a foreign statement")
        }
        results.push(statement.executeSync<T>())
      }
      this.sqlite.exec("COMMIT")
      return results
    } catch (cause: unknown) {
      this.sqlite.exec("ROLLBACK")
      throw cause
    }
  }

  async exec(query: string): Promise<D1ExecResult> {
    this.sqlite.exec(query)
    return { count: 0, duration: 0 }
  }

  withSession(
    constraintOrBookmark?: D1SessionBookmark | D1SessionConstraint,
  ): D1DatabaseSession {
    this.sessionConstraints.push(constraintOrBookmark)
    return new SqliteD1Session(this)
  }

  async dump(): Promise<ArrayBuffer> {
    throw new Error("Database dumps are outside this adapter's test seam")
  }

  execute(query: string, values: ReadonlyArray<unknown> = []): void {
    this.sqlite.query<unknown, Array<SqliteBinding>>(query).run(
      ...values.map(sqliteBinding),
    )
  }

  close(): void {
    this.sqlite.close()
  }
}

const makeDatabase = async (): Promise<SqliteD1Database> => {
  const sqlite = new Database(":memory:", { strict: true })
  const database = new SqliteD1Database(sqlite)
  const migration = await Bun.file(
    new URL(
      "../migrations/d1/0001_graph_topology.sql",
      import.meta.url,
    ),
  ).text()
  await database.exec(migration)
  return database
}

const reference = (
  graph: string,
  kind: string,
  id: string,
): KeyedReference => {
  const encoded: EncodedDocumentReference = { graph, kind, id }
  return {
    documentKey: makeDocumentKey({
      graph,
      documentKind: kind,
      encodedId: id,
    }),
    reference: encoded,
  }
}

describe("d1GraphTopology", () => {
  test("satisfies the canonical topology conformance laws on SQLite", async () => {
    const database = await makeDatabase()
    try {
      const report = await Effect.runPromise(
        verifyGraphTopologyStoreConformance().pipe(
          Effect.provide(d1GraphTopology({ database })),
        ),
      )
      expect(report.capability).toBe("graph_topology")
      expect(report.verified).toHaveLength(12)
      expect(database.sessionConstraints.length).toBeGreaterThan(0)
      expect(
        database.sessionConstraints.every(
          (constraint) => constraint === "first-primary",
        ),
      ).toBe(true)
    } finally {
      database.close()
    }
  })

  test("collects a referenced target only after hard deletion removes its last edge", async () => {
    const database = await makeDatabase()
    const graph = "d1-hard-deletion-orphans"
    const firstSource = reference(graph, "Source", "first-source")
    const secondSource = reference(graph, "Source", "second-source")
    const target = reference(graph, "Target", "shared-target")
    const replacement = (source: KeyedReference) => ({
      graph,
      sourceDocumentKey: source.documentKey,
      source: source.reference,
      relations: [{
        id: "related",
        version: "v1",
        targetDocumentKind: target.reference.kind,
        targets: [target],
      }],
    })
    try {
      const result = await Effect.runPromise(
        Effect.gen(function*() {
          const store = yield* GraphTopologyStore
          yield* store.replaceDocumentTopology(replacement(firstSource))
          yield* store.replaceDocumentTopology(replacement(secondSource))
          const firstDeletion = yield* store.deleteNode({
            graph,
            documentKey: firstSource.documentKey,
          })
          const afterFirst = yield* store.listNodes({
            graph,
            documentKinds: [],
            states: [],
            after: Option.none(),
            limit: Schema.decodeSync(GraphNodePageLimitSchema)(10),
          })
          const secondDeletion = yield* store.deleteNode({
            graph,
            documentKey: secondSource.documentKey,
          })
          const afterSecond = yield* store.listNodes({
            graph,
            documentKinds: [],
            states: [],
            after: Option.none(),
            limit: Schema.decodeSync(GraphNodePageLimitSchema)(10),
          })
          return { firstDeletion, afterFirst, secondDeletion, afterSecond }
        }).pipe(Effect.provide(d1GraphTopology({ database }))),
      )

      expect(result.firstDeletion).toEqual({
        deletedNodes: 1,
        deletedRelations: 1,
        deletedReferencedNodes: 0,
      })
      expect(result.afterFirst.nodes.map((node) => node.documentKey).sort())
        .toEqual([secondSource.documentKey, target.documentKey].sort())
      expect(result.secondDeletion).toEqual({
        deletedNodes: 1,
        deletedRelations: 1,
        deletedReferencedNodes: 1,
      })
      expect(result.afterSecond.nodes).toEqual([])
    } finally {
      database.close()
    }
  })

  test("rolls back hard deletion when orphan collection fails", async () => {
    const database = await makeDatabase()
    const graph = "d1-hard-deletion-atomicity"
    const source = reference(graph, "Source", "source")
    const target = reference(graph, "Target", "target")
    try {
      const result = await Effect.runPromise(
        Effect.gen(function*() {
          const store = yield* GraphTopologyStore
          yield* store.replaceDocumentTopology({
            graph,
            sourceDocumentKey: source.documentKey,
            source: source.reference,
            relations: [{
              id: "related",
              version: "v1",
              targetDocumentKind: target.reference.kind,
              targets: [target],
            }],
          })
          database.execute(
            `CREATE TRIGGER reject_orphan_collection
             BEFORE DELETE ON document_graph_nodes
             WHEN OLD.node_state = 'Referenced'
             BEGIN
               SELECT RAISE(ABORT, 'rejected orphan collection');
             END`,
          )
          const deletion = yield* store.deleteNode({
            graph,
            documentKey: source.documentKey,
          }).pipe(Effect.result)
          const nodes = yield* store.listNodes({
            graph,
            documentKinds: [],
            states: [],
            after: Option.none(),
            limit: Schema.decodeSync(GraphNodePageLimitSchema)(10),
          })
          const related = yield* store.findRelatedNodes({
            graph,
            documentKeys: [source.documentKey],
            documentKind: source.reference.kind,
            direction: "outgoing",
            relation: "related",
            relationVersion: "v1",
            relatedDocumentKind: target.reference.kind,
            limit: Schema.decodeSync(GraphNeighbourLimitSchema)(10),
          }).pipe(Effect.map((groups) => groups.flatMap((group) => group.nodes)))
          return { deletion, nodes, related }
        }).pipe(Effect.provide(d1GraphTopology({ database }))),
      )

      expect(Result.isFailure(result.deletion)).toBe(true)
      expect(result.nodes.nodes.map((node) => node.documentKey).sort()).toEqual(
        [source.documentKey, target.documentKey].sort(),
      )
      expect(result.related.map((node) => node.documentKey)).toEqual([
        target.documentKey,
      ])
    } finally {
      database.close()
    }
  })

  test("rejects oversized replacements before changing canonical state", async () => {
    const database = await makeDatabase()
    const graph = "d1-capacity"
    const source = reference(graph, "Source", "source")
    const targets = Array.from(
      { length: D1_GRAPH_TOPOLOGY_MAX_OUTGOING_EDGES + 1 },
      (_, index) => reference(graph, "Target", `target-${index}`),
    )
    try {
      const result = await Effect.runPromise(
        Effect.gen(function*() {
          const store = yield* GraphTopologyStore
          const replacement = yield* store.replaceDocumentTopology({
            graph,
            sourceDocumentKey: source.documentKey,
            source: source.reference,
            relations: [{
              id: "related",
              version: "v1",
              targetDocumentKind: "Target",
              targets,
            }],
          }).pipe(Effect.result)
          const page = yield* store.listNodes({
            graph,
            documentKinds: [],
            states: [],
            after: Option.none(),
            limit: Schema.decodeSync(GraphNodePageLimitSchema)(10),
          })
          return { replacement, page }
        }).pipe(Effect.provide(d1GraphTopology({ database }))),
      )

      expect(Result.isFailure(result.replacement)).toBe(true)
      if (Result.isFailure(result.replacement)) {
        expect(result.replacement.failure.reason).toBe("capacity_exceeded")
      }
      expect(result.page.nodes).toEqual([])
    } finally {
      database.close()
    }
  })

  test("rejects a persisted node whose key no longer matches its reference", async () => {
    const database = await makeDatabase()
    const graph = "d1-runtime-decode"
    const source = reference(graph, "Source", "source")
    try {
      const result = await Effect.runPromise(
        Effect.gen(function*() {
          const store = yield* GraphTopologyStore
          yield* store.replaceDocumentTopology({
            graph,
            sourceDocumentKey: source.documentKey,
            source: source.reference,
            relations: [],
          })
          database.execute(
            `UPDATE document_graph_nodes
             SET encoded_document_id = ?1
             WHERE graph_id = ?2 AND document_key = ?3`,
            [JSON.stringify("corrupt"), graph, source.documentKey],
          )
          return yield* store.listNodes({
            graph,
            documentKinds: [],
            states: [],
            after: Option.none(),
            limit: Schema.decodeSync(GraphNodePageLimitSchema)(10),
          }).pipe(Effect.result)
        }).pipe(Effect.provide(d1GraphTopology({ database }))),
      )

      expect(Result.isFailure(result)).toBe(true)
      if (Result.isFailure(result)) {
        expect(result.failure.reason).toBe("invalid_stored_state")
      }
      expect(database.sessionConstraints).toEqual(["first-primary"])
    } finally {
      database.close()
    }
  })

  test("rolls back source materialization when a later batch statement fails", async () => {
    const database = await makeDatabase()
    const graph = "d1-atomic-batch"
    const source = reference(graph, "Source", "source")
    const target = reference(graph, "Rejected", "target")
    database.execute(
      `CREATE TRIGGER reject_test_target
       BEFORE INSERT ON document_graph_nodes
       WHEN NEW.document_kind = 'Rejected'
       BEGIN
         SELECT RAISE(ABORT, 'rejected by test trigger');
       END`,
    )
    try {
      const result = await Effect.runPromise(
        Effect.gen(function*() {
          const store = yield* GraphTopologyStore
          const replacement = yield* store.replaceDocumentTopology({
            graph,
            sourceDocumentKey: source.documentKey,
            source: source.reference,
            relations: [{
              id: "related",
              version: "v1",
              targetDocumentKind: "Rejected",
              targets: [target],
            }],
          }).pipe(Effect.result)
          const page = yield* store.listNodes({
            graph,
            documentKinds: [],
            states: [],
            after: Option.none(),
            limit: Schema.decodeSync(GraphNodePageLimitSchema)(10),
          })
          return { replacement, page }
        }).pipe(Effect.provide(d1GraphTopology({ database }))),
      )

      expect(Result.isFailure(result.replacement)).toBe(true)
      expect(result.page.nodes).toEqual([])
    } finally {
      database.close()
    }
  })

  test("pages filtered node catalogs in deterministic key order", async () => {
    const database = await makeDatabase()
    const graph = "d1-catalog"
    const first = reference(graph, "Included", "first")
    const second = reference(graph, "Included", "second")
    const excluded = reference(graph, "Excluded", "third")
    const pageLimit = Schema.decodeSync(GraphNodePageLimitSchema)(1)
    try {
      const pages = await Effect.runPromise(
        Effect.gen(function*() {
          const store = yield* GraphTopologyStore
          for (const node of [second, excluded, first]) {
            yield* store.replaceDocumentTopology({
              graph,
              sourceDocumentKey: node.documentKey,
              source: node.reference,
              relations: [],
            })
          }
          const initial = yield* store.listNodes({
            graph,
            documentKinds: ["Included"],
            states: ["Materialized"],
            after: Option.none(),
            limit: pageLimit,
          })
          const continuation = yield* store.listNodes({
            graph,
            documentKinds: ["Included"],
            states: ["Materialized"],
            after: initial.next,
            limit: pageLimit,
          })
          return { initial, continuation }
        }).pipe(Effect.provide(d1GraphTopology({ database }))),
      )

      const keys = [
        ...pages.initial.nodes,
        ...pages.continuation.nodes,
      ].map((node) => node.documentKey)
      expect(keys).toEqual([first.documentKey, second.documentKey].sort())
      expect(Option.isSome(pages.initial.next)).toBe(true)
      expect(Option.isNone(pages.continuation.next)).toBe(true)
      expect(
        [...pages.initial.nodes, ...pages.continuation.nodes].every(
          (node) => node.reference.kind === "Included",
        ),
      ).toBe(true)
    } finally {
      database.close()
    }
  })
})
