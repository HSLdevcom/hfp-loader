import Knex from 'knex'
import { PG_CONNECTION } from '../constants'

let knexInstance: Knex | null = null

export function getKnex(): Knex {
  if (knexInstance) {
    return knexInstance
  }

  knexInstance = Knex({
    dialect: 'postgres',
    client: 'pg',
    connection: PG_CONNECTION,
    pool: {
      min: 0,
      max: 500,
      idleTimeoutMillis: 30000,
      acquireTimeoutMillis: 30000,
    },
  })

  return knexInstance
}
