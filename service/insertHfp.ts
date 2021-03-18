import { logTime } from '../utils/logTime'
import PQueue from 'p-queue'
import { upsert } from '../utils/upsert'
import { EventGroup, HfpRow } from '../utils/hfp'
import { INSERT_CONCURRENCY } from '../constants'
import { pipeline, Transform } from 'stream'
import { transformHfpItem } from '../utils/transformHfpItem'
import { createSpecificEventKey } from './hfpStorage'
import parse from 'csv-parse'
import { getCsvParseOptions } from '../utils/parseCsv'
import { hfpColumns } from '../utils/hfpColumns'
import { logMaxTimes } from '../utils/logMaxTimes'

const BATCH_SIZE = 2500

export function insertHfpBlobData({
  blobName,
  table,
  eventGroup,
  existingKeys,
  eventStream,
  onDone,
  onError,
}) {
  let blobTime = process.hrtime()
  let eventsByTable: { [tableName: string]: HfpRow[] } = { [table]: [] }

  let insertQueue = new PQueue({
    concurrency: INSERT_CONCURRENCY,
  })

  // Call when the blob is done. Finishes the queued inserts and closes the transaction.
  async function onBlobDone(err?: any) {
    eventStream.destroy()

    if (err) {
      logTime(`Event stream ERROR for blob ${blobName}`, blobTime)
      insertQueue.clear()
      return onError(err)
    }

    await insertQueue.onIdle()

    logTime(`Event stream completed for blob ${blobName}`, blobTime)
    return onDone(blobName)
  }

  function insertEvents(dataToInsert: HfpRow[], tableName: string) {
    let whenQueueAcceptsTasks = Promise.resolve()

    // Wait for the queue to finish work if it gets too large
    if (insertQueue.size > INSERT_CONCURRENCY) {
      whenQueueAcceptsTasks = insertQueue.onEmpty()
    }

    return whenQueueAcceptsTasks.then(() => {
      insertQueue
        .add(() => upsert(tableName, dataToInsert))
        .then(() => console.log(`Inserted ${dataToInsert.length} events for ${blobName}`))
        .catch(onBlobDone)
    })
  }

  function insertEventsIfBatchIsFull(flush: boolean = false) {
    let insertPromises: Promise<unknown>[] = []

    for (let [tableName, events] of Object.entries(eventsByTable)) {
      let eventsLength = events.length
      let shouldInsertBatch = flush ? eventsLength !== 0 : eventsLength >= BATCH_SIZE

      if (shouldInsertBatch) {
        let insertPromise = insertEvents(events, tableName)
        eventsByTable[tableName] = []
        insertPromises.push(insertPromise)
      }
    }

    return Promise.all(insertPromises)
  }

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

      logMaxTimes(`Event received for table ${tableName}`, [data], 10)

      let dataItem = transformHfpItem(data)
      let eventKey = createSpecificEventKey(dataItem)

      if (!existingKeys.includes(eventKey)) {
        eventsByTable[tableName].push(dataItem)
      }

      insertEventsIfBatchIsFull(false).then(() => callback(null))
    },
  })

  pipeline(eventStream, parse(getCsvParseOptions(hfpColumns)), insertStream, (err) => {
    console.log(`Stream done for blob ${blobName}.`)
    onBlobDone(err)
  })
}
