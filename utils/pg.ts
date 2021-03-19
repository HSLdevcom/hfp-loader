import { Pool } from 'pg'
import { PG_CONNECTION } from '../constants'

let pgPool: Pool | null = null

export function getPool() {
  if (pgPool) {
    return pgPool
  }

  pgPool = new Pool({
    ...PG_CONNECTION,
    min: 0,
    max: 500,
    idleTimeoutMillis: 30000,
  })

  return pgPool
}
