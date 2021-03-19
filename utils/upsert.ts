import { HfpRow } from './hfp'
import { getPool } from './pg'

// "Upsert" function for PostgreSQL. Inserts or updates lines in bulk. Insert if
// the primary key for the line is available, update otherwise.
export function upsert(tableName: string, items: HfpRow[]): Promise<void> {
  let itemsLength = items.length

  if (itemsLength === 0) {
    return Promise.resolve()
  }

  let pool = getPool()

  let tableId = `public.${tableName}`

  // Get the set of keys for all items from the first item.
  // All items should have the same keys.
  const itemKeys = Object.keys(items[0])

  // Create a string of placeholder values ($1,$2,$3) for each item we want to insert
  let valuePlaceholders: string[] = []

  // Collect all values to insert from all objects in a one-dimensional array.
  let insertValues: any[] = []

  // pg params are 1-indexed
  let valueIdx = 1

  for (let item of items) {
    if (item) {
      let placeholders: string[] = []

      for (let key of itemKeys) {
        placeholders.push(`$${valueIdx}`)
        insertValues.push(item[key])
        valueIdx++
      }

      let placeholderRow = `(${placeholders.join(',')})`
      valuePlaceholders.push(placeholderRow)
    }
  }

  const upsertQuery = `
INSERT INTO ${tableId} (${itemKeys.join(',')})
VALUES ${valuePlaceholders.join(',')}
ON CONFLICT DO NOTHING;
`

  return pool.query(upsertQuery, insertValues).then(() => {})
}
