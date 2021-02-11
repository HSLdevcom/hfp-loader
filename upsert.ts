import { getKnex } from './knex'
import { chunk } from 'lodash'
import { HfpRow } from './hfp'
import { Transaction } from 'knex'

// "Upsert" function for PostgreSQL. Inserts or updates lines in bulk. Insert if
// the primary key for the line is available, update otherwise.
export async function upsert(trx: Transaction, tableName: string, items: HfpRow[]) {
  if (items.length === 0) {
    return Promise.resolve()
  }

  let tableId = `public.${tableName}`

  // Get the set of keys for all items from the first item.
  // All items should have the same keys.
  const itemKeys = Object.keys(items[0])
  const keysLength = itemKeys.length
  let placeholderRow = `(${itemKeys.map(() => '?').join(',')})`

  // 30k bindings is a conservative estimate of what the node-pg library can handle per query.
  let itemsPerQuery = Math.ceil(30000 / Math.max(1, keysLength))
  // Split the items up into chunks
  let queryChunks = chunk(items, itemsPerQuery)

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

    try {
      await trx.raw(upsertQuery, upsertBindings)
    } catch (err) {
      console.error(err)
    }
  }
}
