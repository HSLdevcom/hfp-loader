import * as fs from 'fs'
import { mapValues, orderBy } from 'lodash'

const SECRETS_PATH = '/run/secrets/'

// For any env variable with the value of "secret", resolve the actual value from the
// associated secrets file. Using sync fs methods for the sake of simplicity,
// since this will only run once when staring the app, sync is OK.
const secrets = (fs.existsSync(SECRETS_PATH) && fs.readdirSync(SECRETS_PATH)) || []

const secretsEnv = mapValues(process.env, (value, key) => {
  const matchingSecrets: string[] = secrets.filter((secretFile) => secretFile.startsWith(key))

  const currentSecret: string | null =
    orderBy(
      matchingSecrets,
      (secret) => {
        const secretVersion = parseInt(secret[secret.length - 1], 10)
        return isNaN(secretVersion) ? 0 : secretVersion
      },
      'desc'
    )[0] || null

  const filepath = SECRETS_PATH + currentSecret

  if (fs.existsSync(filepath)) {
    return (fs.readFileSync(filepath, { encoding: 'utf8' }) || '').trim()
  }

  return value
})

const numval = (val: string | number): number => (typeof val === 'string' ? parseInt(val, 10) : val)

export const PG_CONNECTION = {
  host: secretsEnv.PGHOST,
  port: numval(secretsEnv.PGPORT || '5432'),
  username: secretsEnv.PGUSER,
  password: secretsEnv.PGPASSWORD,
  database: secretsEnv.PGDATABASE,
  ssl: secretsEnv.PG_SSL === 'true',
  extra: {
    ssl: secretsEnv.PG_SSL === 'true',
  },
}

export const PG_POOL_SIZE = parseInt(secretsEnv.PG_POOL_SIZE || '50', 10)

export const HFP_STORAGE_CONNECTION_STRING = secretsEnv.HFP_STORAGE_CONNECTION_STRING || ''
export const HFP_STORAGE_CONTAINER = secretsEnv.HFP_STORAGE_CONTAINER || 'hfp-v2'
export const INSERT_CONCURRENCY = parseInt(secretsEnv.INSERT_CONCURRENCY || '100', 10)
export const EVENT_BATCH_SIZE = parseInt(secretsEnv.EVENT_BATCH_SIZE || '1000', 10)
