import { HfpRow } from './hfp'
import { getKnex } from './knex'

// "Upsert" function for PostgreSQL. Inserts or updates lines in bulk. Insert if
// the primary key for the line is available, update otherwise.
export function upsert(tableName: string, items: HfpRow[]): Promise<void> {
  let itemsLength = items.length

  if (itemsLength === 0) {
    return Promise.resolve()
  }

  let knex = getKnex()

  let tableId = `public.${tableName}`

  // Get the set of keys for all items from the first item.
  // All items should have the same keys.
  const itemKeys = Object.keys(items[0])
  let placeholderRow = `(${itemKeys.map(() => '?').join(',')})`

  // Create a string of placeholder values (?,?,?) for each item we want to insert
  let valuesPlaceholders: string[] = []

  // Collect all values to insert from all objects in a one-dimensional array.
  let insertValues: any[] = []

  let valueIdx = 0

  for (let item of items) {
    if (item) {
      valuesPlaceholders.push(placeholderRow)

      for (let key of itemKeys) {
        insertValues[valueIdx] = item[key]
        valueIdx++
      }
    }
  }

  const upsertQuery = `
INSERT INTO ?? (${itemKeys.map(() => '??').join(',')})
VALUES ${valuesPlaceholders.join(',')}
ON CONFLICT DO NOTHING;
`

  const upsertBindings = [tableId, ...itemKeys, ...insertValues]
  return knex.raw(upsertQuery, upsertBindings).then(() => {})
}
