import { logTime } from '../utils/logTime'
import PQueue from 'p-queue'
import { upsert } from '../utils/upsert'
import { EventGroup, HfpRow } from '../utils/hfp'
import { INSERT_CONCURRENCY } from '../constants'
import { finished, Transform } from 'stream'
import { transformHfpItem } from '../utils/transformHfpItem'
import { createSpecificEventKey } from './hfpStorage'
import parse from 'csv-parse'
import { getCsvParseOptions } from '../utils/parseCsv'
import { hfpColumns } from '../utils/hfpColumns'

const BATCH_SIZE = 5000

export function insertHfpFromBlobStream({
  blobName,
  table,
  eventGroup,
  existingKeys,
  eventStream,
  onDone,
  onError,
}: {
  blobName: string
  table: string
  eventGroup: EventGroup
  existingKeys: string[]
  eventStream: NodeJS.ReadableStream
  onDone: (string) => unknown
  onError: (err: Error) => unknown
}) {
  let blobTime = process.hrtime()
  let eventsByTable: { [tableName: string]: HfpRow[] } = { [table]: [] }

  let insertsQueued = 0
  let insertsCompleted = 0

  let insertQueue = new PQueue({
    concurrency: INSERT_CONCURRENCY,
  })

  function onBlobError(err) {
    logTime(`Event stream ERROR for blob ${blobName}`, blobTime)
    insertQueue.clear()
    onError(err)
  }

  function insertEvents(dataToInsert: HfpRow[], tableName: string) {
    let whenQueueAcceptsTasks = Promise.resolve()

    // Wait for the queue to finish work if it gets too large
    if (insertQueue.size > INSERT_CONCURRENCY) {
      whenQueueAcceptsTasks = insertQueue.onEmpty()
    }

    return whenQueueAcceptsTasks.then(() => {
      insertsQueued++

      insertQueue // Do not return insert promise! It would hold up the whole stream.
        .add(() => upsert(tableName, dataToInsert))
        .then(() => {
          insertsCompleted++
          console.log(`Inserted ${dataToInsert.length} events for ${blobName}`)
        })
        .catch(onBlobError)

      return Promise.resolve()
    })
  }

  function insertEventsIfBatchIsFull(flush: boolean = false) {
    let insertPromise = Promise.resolve()

    for (let tableName in eventsByTable) {
      let events = eventsByTable[tableName]
      let eventsLength = events.length
      let shouldInsertBatch = flush ? eventsLength !== 0 : eventsLength >= BATCH_SIZE

      if (shouldInsertBatch) {
        insertPromise = insertPromise.then(() => insertEvents(events, tableName))
        eventsByTable[tableName] = []
      }
    }

    return insertPromise
  }

  let streamPromise = new Promise<void>((resolve, reject) => {
    let insertStream = new Transform({
      objectMode: true,
      flush: (callback) => {
        console.log(`Flushing insert stream for ${blobName}`)
        insertEventsIfBatchIsFull(true).then(() => callback(null))
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

        if (!eventKey || !existingKeys.includes(eventKey)) {
          eventsByTable[tableName].push(dataItem)
        }

        insertEventsIfBatchIsFull(false).then(() => callback(null))
      },
    })

    let blobStream = eventStream.pipe(parse(getCsvParseOptions(hfpColumns))).pipe(insertStream)

    eventStream.on('end', () => {
      console.log(`------------ Blob stream ${blobName} ended. ----------------------`)
    })

    finished(blobStream, (err) => {
      if (err) {
        reject(err)
      } else {
        logTime(`Event stream completed for blob ${blobName}`, blobTime)
        resolve()
      }
    })
  })

  return streamPromise
    .then(() => insertQueue.add(() => Promise.resolve()))
    .then(() => insertQueue.onIdle())
    .then(() => {
      console.log(
        `${blobName} | Inserts queued: ${insertsQueued} | Inserts completed: ${insertsCompleted}`
      )

      onDone(blobName)
    })
    .catch(onBlobError)
}
