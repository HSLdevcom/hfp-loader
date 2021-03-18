import { logTime } from './utils/logTime'
import PQueue from 'p-queue'
import { upsert } from './upsert'
import { EventGroup, HfpRow } from './hfp'
import { INSERT_CONCURRENCY } from './constants'
import { pipeline, Transform } from 'stream'
import { transformHfpItem } from './transformHfpItem'
import { createSpecificEventKey } from './hfpStorage'
import parse from 'csv-parse'
import { getCsvParseOptions } from './parseCsv'
import { hfpColumns } from './hfpColumns'

const BATCH_SIZE = 2500

export function insertHfpBlobData({
  blobName,
  table,
  eventGroup,
  existingKeys,
  eventStream,
  trxPool,
  onDone,
  onError,
}) {
  let blobTime = process.hrtime()
  let events: HfpRow[] = []

  let insertQueue = new PQueue({
    concurrency: INSERT_CONCURRENCY,
  })

  // Call when the blob is done. Finishes the queued inserts and closes the transaction.
  async function onBlobDone(err?: any) {
    if (err) {
      logTime(`Event stream ERROR for blob ${blobName}`, blobTime)
      insertQueue.clear()
      return onError(err)
    }

    await insertQueue.onIdle()
    await trxPool.closePool()

    logTime(`Event stream completed for blob ${blobName}`, blobTime)
    return onDone(blobName)
  }

  function insertEvents(dataToInsert) {
    let whenQueueAcceptsTasks = Promise.resolve()

    // Wait for the queue to finish work if it gets too large
    if (insertQueue.size > INSERT_CONCURRENCY) {
      whenQueueAcceptsTasks = insertQueue.onEmpty()
    }

    return whenQueueAcceptsTasks.then(() => {
      return insertQueue
        .add(() => trxPool.runWithTransaction((trx) => upsert(trx, table, events)))
        .then(() => console.log(`Inserted ${dataToInsert.length} events for ${blobName}`))
    })
  }

  function insertEventsIfBatchIsFull(flush: boolean = false) {
    let eventsLength = events.length
    let shouldInsertBatch = flush ? eventsLength !== 0 : eventsLength >= BATCH_SIZE

    let insertPromise = Promise.resolve()

    if (shouldInsertBatch) {
      insertPromise = insertEvents(events)
      events = []
    }

    return insertPromise
  }

  let insertStream = new Transform({
    objectMode: true,
    flush: (callback) => {
      console.log(`Flushing insert stream for ${blobName}`)
      insertEventsIfBatchIsFull(true)
        .then(() => callback(null))
        .catch((err) => callback(err))
    },
    transform: (data: HfpRow, encoding: BufferEncoding, callback) => {
      // Unsigned and vehicle position events come from the same storage group,
      // but are inserted in different tables. Skip if the journey_type is
      // wrong for either case.
      if (
        (eventGroup === EventGroup.UnsignedEvent && data.journey_type !== 'unsigned') ||
        (eventGroup === EventGroup.VehiclePosition && data.journey_type !== 'journey')
      ) {
        return callback(null)
      }

      let dataItem = transformHfpItem(data)
      let eventKey = createSpecificEventKey(dataItem)

      if (!existingKeys.includes(eventKey)) {
        events.push(dataItem)
      }

      insertEventsIfBatchIsFull(false)
        .then(() => callback(null))
        .catch((err) => callback(err))
    },
  })

  pipeline(eventStream, parse(getCsvParseOptions(hfpColumns)), insertStream, (err) => {
    console.log(`Stream done for blob ${blobName}.`)
    onBlobDone(err)
  })
}
