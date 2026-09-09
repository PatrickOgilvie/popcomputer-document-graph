/* oxlint-disable anti-slop/no-unsafe-dictionary-type -- This package-owned structural contract faithfully models D1's unknown persisted-row boundary; every adapter decodes returned rows before use. */

/** Minimal result surface consumed from a Cloudflare D1 statement. */
export interface DocumentGraphD1Result<Row = unknown> {
  readonly results: ReadonlyArray<Row>
}

/** Structural prepared-statement surface consumed by the D1 adapters. */
export interface DocumentGraphD1PreparedStatement {
  bind(...values: Array<unknown>): DocumentGraphD1PreparedStatement
  first<Value = unknown>(columnName: string): Promise<Value | null>
  first<Row = Record<string, unknown>>(): Promise<Row | null>
  run<Row = Record<string, unknown>>(): Promise<DocumentGraphD1Result<Row>>
  all<Row = Record<string, unknown>>(): Promise<DocumentGraphD1Result<Row>>
  raw<Row = unknown[]>(options: {
    readonly columnNames: true
  }): Promise<[string[], ...Row[]]>
  raw<Row = unknown[]>(options?: {
    readonly columnNames?: false
  }): Promise<Row[]>
}

/** Strongly consistent session surface consumed by exact topology reads. */
export interface DocumentGraphD1Session {
  prepare(query: string): DocumentGraphD1PreparedStatement
}

/**
 * Package-owned structural contract satisfied by a Cloudflare `D1Database`.
 *
 * Keeping this boundary local prevents consumers from needing Cloudflare or
 * Miniflare ambient declarations merely to typecheck another package entry
 * point.
 */
export interface DocumentGraphD1Database {
  prepare(query: string): DocumentGraphD1PreparedStatement
  batch<Row = unknown>(
    statements: Array<DocumentGraphD1PreparedStatement>,
  ): Promise<Array<DocumentGraphD1Result<Row>>>
  withSession(
    constraint: "first-primary",
  ): DocumentGraphD1Session
}
