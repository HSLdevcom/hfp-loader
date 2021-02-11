import { EventGroup, eventGroupTables } from './hfp'
import { getKnex } from './knex'

export async function getEvents(date: string, eventGroup: EventGroup) {
  let knex = await getKnex()
  // The HFP tables do not have primary keys, so we must filter out events that already
  // exist in the table. Fetch the existing events for the date to facilitate this.
  let table = eventGroupTables[eventGroup]

  // language=PostgreSQL
  let existingEvents = await knex.raw(
    `
      SELECT *
      FROM :table: t
      WHERE t.oday = :date
    `,
    { table, date }
  )

  return existingEvents?.rows || []
}
