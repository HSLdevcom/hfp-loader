import { chunk } from 'lodash'
import { HfpRow } from './hfp'
import { getKnex } from './knex'

// "Upsert" function for PostgreSQL. Inserts or updates lines in bulk. Insert if
// the primary key for the line is available, update otherwise.
export function upsert(tableName: string, items: HfpRow[]): Promise<number> {
  if (items.length === 0) {
    return Promise.resolve(0)
  }

  let knex = getKnex()
  let tableId = `public.${tableName}`

  // Get the set of keys for all items from the first item.
  // All items should have the same keys.
  const itemKeys = Object.keys(items[0])
  const keysLength = itemKeys.length
  let placeholderRow = `(${itemKeys.map(() => '?').join(',')})`

  let itemsPerQuery = Math.ceil(10000 / Math.max(1, keysLength))
  // Split the items up into chunks
  let queryChunks = chunk(items, itemsPerQuery)

  let prevInsertPromise = Promise.resolve(items.length)

  // Create upsert queries for each chunk of items.
  for (let itemsChunk of queryChunks) {
    let chunkLength = itemsChunk.length
    // Create a string of placeholder values (?,?,?) for each item we want to insert
    let valuesPlaceholders: string[] = []

    // Collect all values to insert from all objects in a one-dimensional array.
    let insertValues: any[] = []

    let itemIdx = 0
    let valueIdx = 0

    while (itemIdx < chunkLength) {
      let insertItem = itemsChunk[itemIdx]

      if (insertItem) {
        valuesPlaceholders.push(placeholderRow)

        for (let k = 0; k < keysLength; k++) {
          insertValues[valueIdx] = insertItem[itemKeys[k]]
          valueIdx++
        }
      }

      itemIdx++
    }

    const upsertQuery = `
INSERT INTO ?? (${itemKeys.map(() => '??').join(',')})
VALUES ${valuesPlaceholders.join(',')}
ON CONFLICT DO NOTHING;
`

    const upsertBindings = [tableId, ...itemKeys, ...insertValues]

    prevInsertPromise = prevInsertPromise.then(() =>
      knex.raw(upsertQuery, upsertBindings).then(() => items.length)
    )
  }

  return prevInsertPromise
}
