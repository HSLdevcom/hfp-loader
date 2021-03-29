import { Pool } from 'pg'
import { PG_CONNECTION, PG_POOL_SIZE } from '../constants'

let pgPool: Pool | null = null

export function getPool() {
  if (pgPool) {
    return pgPool
  }

  pgPool = new Pool({
    ...PG_CONNECTION,
    min: 0,
    max: PG_POOL_SIZE,
    idleTimeoutMillis: 100000,
    connectionTimeoutMillis: 100000,
  })

  return pgPool
}
