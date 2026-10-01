/// <reference types="@cloudflare/workers-types" />

/* oxlint-disable anti-slop/no-unknown-parameters, anti-slop/no-runtime-typeof, anti-slop/no-unsafe-dictionary-type -- This faithful test binding implements Cloudflare D1's deliberately unknown-valued boundary and narrows every SQLite binding before execution. */

import {
  Database,
  type SQLQueryBindings,
} from "bun:sqlite"

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
  ) {}

  bind(...values: unknown[]): D1PreparedStatement {
    return new SqliteD1PreparedStatement(this.database, this.query, values)
  }

  executeSync<T>(): D1Result<T> {
    const statement = this.database.query<unknown, Array<SqliteBinding>>(
      this.query,
    )

    const rows = statement.all(...this.values.map(sqliteBinding))

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
      // SAFETY: D1's first<T>() API delegates selection of the row type to its
      // caller, matching this faithful boundary implementation.
      return first as T
    }

    const value = first[columnName]

    // SAFETY: D1's first<T>(column) API delegates selection of the column type
    // to its caller, matching this faithful boundary implementation.
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
  constructor(private readonly owner: SqliteD1Database) {}

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

export class SqliteD1Database implements D1Database {
  readonly sessionConstraints: Array<
    string | undefined
  > = []

  constructor(private readonly sqlite: Database) {}

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
    constraintOrBookmark?: D1SessionBookmark,
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

  row<T>(query: string, values: ReadonlyArray<unknown> = []): T | null {
    return this.sqlite.query<T, Array<SqliteBinding>>(query).get(
      ...values.map(sqliteBinding),
    )
  }

  close(): void {
    this.sqlite.close()
  }
}

/** In-memory SQLite database behind the D1 binding, with the publication journal migrated. */
export const makeDatabase = async (): Promise<SqliteD1Database> => {
  const sqlite = new Database(":memory:", { strict: true })
  sqlite.exec("PRAGMA foreign_keys = ON")
  const database = new SqliteD1Database(sqlite)

  const migration = await Bun.file(
    new URL(
      "../../migrations/d1/0002_projection_publications.sql",
      import.meta.url,
    ),
  ).text()

  await database.exec(migration)

  return database
}
