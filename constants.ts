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

export const APP_PORT = numval(secretsEnv.PORT || '4100')
export const DEBUG = secretsEnv.DEBUG === 'true' || false

export const PG_CONNECTION = {
  host: secretsEnv.PGHOST,
  port: numval(secretsEnv.PGPORT || '5432'),
  username: secretsEnv.PGUSER,
  password: secretsEnv.PGPASSWORD,
  database: secretsEnv.PGDATABASE,
  ssl: secretsEnv.PG_SSL === 'true',
}

export const HFP_STORAGE_CONNECTION_STRING = secretsEnv.HFP_STORAGE_CONNECTION_STRING || ''

// URLs
export const PATH_PREFIX = secretsEnv.PATH_PREFIX || '/'

// HSL ID authentication
export const CLIENT_ID = secretsEnv.CLIENT_ID
export const CLIENT_SECRET = secretsEnv.CLIENT_SECRET
export const REDIRECT_URI = secretsEnv.REDIRECT_URI
export const LOGIN_PROVIDER_URI = secretsEnv.LOGIN_PROVIDER_URI
export const AUTH_URI = secretsEnv.AUTH_URI
export const AUTH_SCOPE = secretsEnv.AUTH_SCOPE

// Cache
export const REDIS_HOST = secretsEnv.REDIS_HOST || '0.0.0.0'
export const REDIS_PORT: string = secretsEnv.REDIS_PORT || '6379'
export const REDIS_PASSWORD: string | undefined = secretsEnv.REDIS_PASSWORD || undefined
export const REDIS_SSL: boolean = secretsEnv.REDIS_SSL === 'true'
export const DISABLE_CACHE = secretsEnv.DISABLE_CACHE === 'true'
export const HFP_SCHEMA = secretsEnv.HFP_SCHEMAHFP_SCHEMA || 'hfp'
export const INSERT_CONCURRENCY = parseInt(secretsEnv.INSERT_CONCURRENCY || '100', 10)
export const BLOB_CONCURRENCY = parseInt(secretsEnv.BLOB_CONCURRENCY || '1', 10)
