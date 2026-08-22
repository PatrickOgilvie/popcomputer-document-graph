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
 * Use a shared Pool or an explicitly caller-owned active transaction.
 *
 * Transaction mode lets a caller make a multi-projection document index
 * all-or-nothing by committing or rolling back after the complete Effect.
 * pg `Client` and `PoolClient` are accepted directly. Other pinned query
 * surfaces must opt in through {@link postgresTransactionClient}; a Pool is
 * not a transaction client because its queries may use different connections.
 */
export type PostgresDocumentGraphConfig =
  | {
      readonly pool: Pool
      readonly transaction?: never
      readonly schema?: string
    }
  | {
      readonly transaction: Client | PoolClient | PostgresQueryClient
      readonly pool?: never
      readonly schema?: string
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
