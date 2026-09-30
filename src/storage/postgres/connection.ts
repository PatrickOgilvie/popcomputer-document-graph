import { Effect } from "effect"
import type {
  Client,
  Pool,
  PoolClient,
  QueryResultRow,
} from "pg"

interface PostgresQueryExecutor {
  readonly query: (
    text: string,
    values?: ReadonlyArray<unknown>,
  ) => Promise<PostgresQueryOutcome>
}

const PostgresQueryClientTypeId = Symbol(
  "@popcomputer/document-graph/PostgresQueryClient",
)

/** Explicitly transaction-scoped query surface consumed by storage writes. */
export interface PostgresQueryClient extends PostgresQueryExecutor {
  readonly [PostgresQueryClientTypeId]: true
}

/** Unvalidated row payload produced by one PostgreSQL query execution. */
export interface PostgresQueryOutcome {
  readonly rows: ReadonlyArray<unknown>
}

type StructuralTransactionClient = PostgresQueryExecutor & {
  readonly connect?: never
}

/**
 * Opt a pinned, caller-owned transaction query surface into PostgreSQL storage.
 *
 * Pools are rejected because their queries are not guaranteed to use one
 * connection. The caller still owns beginning, committing, rolling back, and
 * releasing the transaction represented by `client`.
 */
export const postgresTransactionClient = (
  client: StructuralTransactionClient,
): PostgresQueryClient => {
  const transactionClient: PostgresQueryClient = {
    [PostgresQueryClientTypeId]: true,
    query: (text, values) => client.query(text, values),
  }

  return Object.freeze(transactionClient)
}

/**
 * Approximate semantic candidates from a pgvector HNSW expression index.
 *
 * Build the index with {@link postgresVectorIndexSql} using the same `index`,
 * `dimensions` and `representation`. Searches use it only while it is valid,
 * for whole-graph scopes at exactly these dimensions; every other search keeps
 * exhaustive `"auto"` scoring. Index candidates are rescored exactly in float64,
 * so only recall is approximate.
 */
export interface PostgresApproximateVectorSearch {
  readonly mode: "approximate"
  /** Unqualified index name on `projected_chunks`. */
  readonly index: string
  /** Embedding dimensions covered by the partial index. */
  readonly dimensions: number
  /** Index element type. `halfvec` (default) halves index size; `vector` keeps float32. */
  readonly representation?: "halfvec" | "vector"
  /** HNSW candidate list size per scan. Defaults to 100, raised to the fetch size. */
  readonly efSearch?: number
  /** Index candidates fetched per requested result before exact rescoring. Defaults to 4. */
  readonly overfetch?: number
  /** Index tuples an iterative scan may visit when scope filters discard candidates. Defaults to 20,000. */
  readonly maxScanTuples?: number
}

interface PostgresDocumentGraphOptions {
  readonly schema?: string
  /** Auto uses installed pgvector for eligible vectors; float64 keeps array scoring. */
  readonly vectorSearch?: "auto" | "float64" | PostgresApproximateVectorSearch
  /**
   * Server-side limit for each semantic and text search statement. PostgreSQL
   * cancels the statement itself, so an abandoned search cannot keep running.
   */
  readonly searchTimeoutMilliseconds?: number
}

/**
 * Use a shared Pool or an explicitly caller-owned active transaction.
 *
 * Transaction mode lets a caller make a multi-projection document index
 * all-or-nothing by committing or rolling back after the complete Effect.
 * pg `Client` and `PoolClient` are accepted directly. Other pinned query
 * surfaces must opt in through {@link postgresTransactionClient}; a Pool is
 * not a transaction client because its queries may use different connections.
 */
export type PostgresDocumentGraphConfig =
  | PostgresDocumentGraphOptions & {
      readonly pool: Pool
      readonly transaction?: never
    }
  | PostgresDocumentGraphOptions & {
      readonly transaction: Client | PoolClient | PostgresQueryClient
      readonly pool?: never
    }

export type PostgresQueryable =
  | Client
  | Pool
  | PoolClient
  | PostgresQueryClient

export type PostgresTransactionClient =
  | Client
  | PoolClient
  | PostgresQueryClient

/** Execute a query and expose only its row payload to adapter operations. */
export const queryRows = async <Row extends QueryResultRow>(
  connection: PostgresQueryable,
  text: string,
  values: ReadonlyArray<unknown> = [],
): Promise<ReadonlyArray<Row>> => {
  const client: PostgresQueryExecutor = connection
  const result = await client.query(text, [...values])

  // SAFETY: every PostgresQueryable member resolves to a pg QueryResult
  // payload whose rows carry the caller-declared encoded shape; parseRow
  // revalidates each row before any stored state is trusted.
  return result.rows as Array<Row>
}

const withTransaction = async <A>(
  config: PostgresDocumentGraphConfig,
  operation: (client: PostgresTransactionClient) => Promise<A>,
): Promise<A> => {
  if (config.pool !== undefined) {
    const client = await config.pool.connect()

    try {
      await client.query("BEGIN")

      try {
        const value = await operation(client)
        await client.query("COMMIT")

        return value
      } catch (cause) {
        try {
          await client.query("ROLLBACK")
        } catch {
          // Preserve the failure that caused the transaction to roll back.
        }

        throw cause
      }
    } finally {
      client.release()
    }
  }

  const client = config.transaction
  const savepoint = "honertia_document_graph_operation"
  await client.query(`SAVEPOINT ${savepoint}`)

  try {
    const value = await operation(client)
    await client.query(`RELEASE SAVEPOINT ${savepoint}`)

    return value
  } catch (cause) {
    try {
      await client.query(`ROLLBACK TO SAVEPOINT ${savepoint}`)
      await client.query(`RELEASE SAVEPOINT ${savepoint}`)
    } catch {
      // Preserve the failure that caused the savepoint rollback.
    }

    throw cause
  }
}

/** One transaction-local setting; names and values are adapter-built, never caller text. */
export type PostgresReadSetting = readonly [name: string, value: string]

/**
 * Run one read with transaction-local settings.
 *
 * Pool mode owns a read-only transaction. Transaction mode rolls back to a
 * savepoint afterwards, which also reverts the settings, so they never leak
 * into the caller's transaction. Settings travel with BEGIN or SAVEPOINT in
 * one simple-protocol round trip.
 */
export const withReadSettings = async <A>(
  config: PostgresDocumentGraphConfig,
  settings: ReadonlyArray<PostgresReadSetting>,
  read: (client: PostgresQueryable) => Promise<A>,
): Promise<A> => {
  if (settings.length === 0) return read(connectionFor(config))
  const assignments = settings.map(([name, value]) => `SET LOCAL ${name} = ${value}`)

  if (config.pool !== undefined) {
    const client = await config.pool.connect()

    try {
      await client.query(["BEGIN READ ONLY", ...assignments].join("; "))

      try {
        const value = await read(client)
        await client.query("COMMIT")

        return value
      } catch (cause) {
        try {
          await client.query("ROLLBACK")
        } catch {
          // Preserve the read failure.
        }

        throw cause
      }
    } finally {
      client.release()
    }
  }

  const client = config.transaction
  const savepoint = "honertia_document_graph_read"
  await client.query([`SAVEPOINT ${savepoint}`, ...assignments].join("; "))

  try {
    return await read(client)
  } finally {
    try {
      await client.query(`ROLLBACK TO SAVEPOINT ${savepoint}; RELEASE SAVEPOINT ${savepoint}`)
    } catch {
      // Preserve the read outcome; the caller owns the enclosing transaction.
    }
  }
}

/** Select the caller-owned connection used for non-transactional reads. */
export const connectionFor = (
  config: PostgresDocumentGraphConfig,
): PostgresQueryable =>
  "pool" in config ? config.pool : config.transaction

/** Run one adapter write in a pool transaction or caller savepoint. */
export const transactionEffect = <A, E>(
  config: PostgresDocumentGraphConfig,
  execute: (client: PostgresTransactionClient) => Promise<A>,
  classifyFailure: (cause: unknown) => E,
): Effect.Effect<A, E> =>
  Effect.tryPromise({
    try: () => withTransaction(config, execute),
    catch: classifyFailure,
  })
