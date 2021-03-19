import { logTime } from '../utils/logTime'
import { EventGroup, HfpRow } from '../utils/hfp'
import { pipeline, Transform } from 'stream'
import { transformHfpItem } from '../utils/transformHfpItem'
import { createSpecificEventKey } from './hfpStorage'
import parse from 'csv-parse'
import { getCsvParseOptions } from '../utils/parseCsv'
import { hfpColumns } from '../utils/hfpColumns'

const BATCH_SIZE = 1000

export function insertHfpFromBlobStream({
  blobName,
  table,
  eventGroup,
  existingKeys,
  eventStream,
  onBatch,
  onDone,
  onError,
}: {
  blobName: string
  table: string
  eventGroup: EventGroup
  existingKeys: Set<string>
  eventStream: NodeJS.ReadableStream
  onBatch: (events: HfpRow[], tableName: string) => Promise<unknown>
  onDone: (string) => unknown
  onError: (err: Error) => unknown
}) {
  let blobTime = process.hrtime()
  let insertsQueued = 0
  let insertsCompleted = 0

  let statusInterval = setInterval(() => {
    console.log(
      `[${blobName}] Inserts queued: ${insertsQueued} | Inserts completed: ${insertsCompleted}`
    )
  }, 10000)

  let eventsByTable: { [tableName: string]: HfpRow[] } = { [table]: [] }

  function onBlobError(err) {
    logTime(`Event stream ERROR for blob ${blobName}`, blobTime)
    onError(err)
  }

  // Sends the batch to the insert queue with the onBatch callback.
  // flush = send the batch if i has any length, else wait for the length to become
  function sendBatchIfFull(flush: boolean = false) {
    let insertPromise = Promise.resolve()

    for (let tableName in eventsByTable) {
      let events = eventsByTable[tableName]
      let eventsLength = events.length
      let shouldInsertBatch = flush ? eventsLength !== 0 : eventsLength >= BATCH_SIZE

      if (shouldInsertBatch) {
        insertPromise = insertPromise.then(() => {
          insertsQueued++

          return onBatch(events, tableName).then(() => {
            insertsCompleted++
          })
        })
        eventsByTable[tableName] = []
      }
    }

    return insertPromise
  }

  let insertStream = new Transform({
    objectMode: true,
    flush: (callback) => {
      console.log(`Flushing insert stream for ${blobName}`)
      sendBatchIfFull(true).then(() => callback(null))
    },
    transform: (data: HfpRow, encoding: BufferEncoding, callback) => {
      // Unsigned and vehicle position events come from the same storage group,
      // but are inserted in different tables. Skip if the journey_type is
      // wrong for either case.
      let tableName = table

      if (eventGroup === EventGroup.VehiclePosition && data.journey_type !== 'journey') {
        tableName = 'unsignedevent'
      }

      let dataItem = transformHfpItem(data)
      let eventKey = createSpecificEventKey(dataItem)

      if (eventKey && !existingKeys.has(eventKey)) {
        eventsByTable[tableName].push(dataItem)
        sendBatchIfFull(false).then(() => callback(null))
      } else {
        callback(null)
      }
    },
  })

  pipeline(eventStream, parse(getCsvParseOptions(hfpColumns)), insertStream, (err) => {
    clearInterval(statusInterval)

    if (err) {
      onBlobError(err)
    } else {
      logTime(`Event stream completed for blob ${blobName}`, blobTime)
      onDone(blobName)
    }
  })
}
