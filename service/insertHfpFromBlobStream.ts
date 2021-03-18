import { logTime } from '../utils/logTime'
import PQueue from 'p-queue'
import { upsert } from '../utils/upsert'
import { EventGroup, HfpRow } from '../utils/hfp'
import { INSERT_CONCURRENCY } from '../constants'
import { pipeline } from 'stream'
import { transformHfpItem } from '../utils/transformHfpItem'
import { createSpecificEventKey } from './hfpStorage'
import parse from 'csv-parse'
import { getCsvParseOptions } from '../utils/parseCsv'
import { hfpColumns } from '../utils/hfpColumns'
import transform from 'stream-transform'

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

    console.log('Queue size', insertQueue.size)

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
    let insertTableData = Object.entries(eventsByTable).filter(([_, events]) => {
      let eventsLength = events.length
      return flush ? eventsLength !== 0 : eventsLength >= BATCH_SIZE
    })

    if (insertTableData.length !== 0) {
      eventStream.pause()

      return Promise.resolve()
        .then(() => {
          let insertPromise = Promise.resolve()

          for (let [tableName, events] of insertTableData) {
            insertPromise = insertPromise.then(() => insertEvents(events, tableName))
            eventsByTable[tableName] = []
          }

          return insertPromise
        })
        .then(() => {
          eventStream?.resume()
        })
    }

    return Promise.resolve()
  }

  let streamPromise = new Promise<void>((resolve, reject) => {
    let rawDataTransformer = transform((record, callback) => {
      let dataItem = transformHfpItem(record)
      let eventKey = createSpecificEventKey(dataItem)

      if (!eventKey || !existingKeys.includes(eventKey)) {
        callback(null, dataItem)
      } else {
        callback()
      }
    })

    let batchTransformer = transform((dataItem, callback) => {
      if (!dataItem) {
        return callback()
      }
      // Unsigned and vehicle position events come from the same storage group,
      // but are inserted in different tables. Skip if the journey_type is
      // wrong for either case.
      let tableName = table

      if (eventGroup === EventGroup.VehiclePosition && dataItem.journey_type !== 'journey') {
        tableName = 'unsignedevent'
      }

      eventsByTable[tableName].push(dataItem)
      insertEventsIfBatchIsFull(false).then(() => callback())
    })

    pipeline(
      eventStream,
      parse(getCsvParseOptions(hfpColumns)),
      rawDataTransformer,
      batchTransformer,
      (err) => {
        if (err) {
          reject(err)
        } else {
          logTime(`Event stream completed for blob ${blobName}`, blobTime)
          resolve()
        }
      }
    )
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
