import { format } from "date-fns"

import { getPool } from './pg'

export async function getEvents(minTst: Date, maxTst: Date, table: string): Promise<{ uuid?: string | null }[]> {
  let pool = getPool()
  // The HFP tables do not have primary keys, so we must filter out events that already
  // exist in the table. Fetch the existing events for the date to facilitate this.

  // language=PostgreSQL
  let existingEvents = await pool.query(
    `
      SELECT t.uuid
      FROM public."${table}" t
      WHERE $1 <= t.tst AND $2 <= t.tst
    `,
    //TODO: might need to consider timezones here
    [format(minTst, "yyyy-MM-dd HH:mm:ss"), format(maxTst, "yyyy-MM-dd HH:mm:ss")]
  )

  return existingEvents?.rows || []
}
