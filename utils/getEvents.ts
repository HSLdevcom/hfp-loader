import { getPool } from './pg'

export async function getEvents(date: string, table: string): Promise<{ uuid?: string | null }[]> {
  let pool = getPool()
  // The HFP tables do not have primary keys, so we must filter out events that already
  // exist in the table. Fetch the existing events for the date to facilitate this.

  // language=PostgreSQL
  let existingEvents = await pool.query(
    `
      SELECT t.uuid
      FROM public."${table}" t
      WHERE t.oday = $1
    `,
    [date]
  )

  console.log(existingEvents)

  return existingEvents?.rows || []
}
