import { Effect } from "effect"
import type {
  Client,
  Pool,
  PoolClient,
  QueryResultRow,
} from "pg"

/**
 * Use a shared Pool or an explicitly caller-owned active transaction.
 *
 * Transaction mode lets a caller make a multi-projection document index
 * all-or-nothing by committing or rolling back after the complete Effect.
 */
export type PostgresDocumentGraphConfig =
  | {
      readonly pool: Pool
      readonly transaction?: never
      readonly schema?: string
    }
  | {
      readonly transaction: Client | PoolClient
      readonly pool?: never
      readonly schema?: string
    }

export type PostgresQueryable = Client | Pool | PoolClient
export type PostgresTransactionClient = Client | PoolClient

/** Execute a query and expose only its row payload to adapter operations. */
export const queryRows = async <Row extends QueryResultRow>(
  connection: PostgresQueryable,
  text: string,
  values: ReadonlyArray<unknown> = [],
): Promise<ReadonlyArray<Row>> => {
  const result = await connection.query<Row>(text, [...values])
  return result.rows
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
