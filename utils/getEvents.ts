import { format, utcToZonedTime } from 'date-fns-tz'
import { getPool } from './pg'

const timeZone = "Europe/Helsinki"

//Timestamp should be formatted in local time for Postgres query
function formatTimestamp(date: Date): string {
  return format(utcToZonedTime(date, timeZone), "yyyy-MM-dd HH:mm:ss", { timeZone })
}

export async function getEvents(minTst: Date, maxTst: Date, table: string): Promise<{ unique_vehicle_id: string | null, tsi: string | null, event_type: string | null }[]> {
  let pool = getPool()
  // The HFP tables do not have primary keys, so we must filter out events that already
  // exist in the table. Fetch the existing events for the date to facilitate this.

  //We need to fetch unique vehicle ID, timestamp and event type and calculate a hash code from them to avoid inserting duplicate events
  //UUIDs are randomly generated so they are not usable for deduplication

  // language=PostgreSQL
  let existingEvents = await pool.query(
    `
      SELECT t.unique_vehicle_id, t.tsi, t.event_type
      FROM public."${table}" t
      WHERE to_timestamp($1, 'YYYY-MM-DD HH:MI:SS') <= t.tst AND to_timestamp($2, 'YYYY-MM-DD HH:MI:SS') <= t.tst
    `,
    [formatTimestamp(minTst), formatTimestamp(maxTst)]
  )

  return existingEvents?.rows || []
}
