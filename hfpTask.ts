import { pipeline, Transform } from 'stream'
import { logTime } from './utils/logTime'
import { EventGroup, eventGroupTables, HfpRow } from './hfp'
import { createJourneyBlobStreamer, createSpecificEventKey, getHfpBlobs } from './hfpStorage'
import { upsert } from './upsert'
import { getEvents } from './getEvents'
import parse from 'csv-parse'
import { getCsvParseOptions } from './parseCsv'
import { hfpColumns } from './hfpColumns'
import { createTransactionPool } from './knex'
import { transformHfpItem } from './transformHfpItem'
import PQueue from 'p-queue'

const BATCH_SIZE = 1000

export async function hfpTask(date: string) {
  let time = process.hrtime()
  let getJourneyBlobStream = await createJourneyBlobStreamer()

  let eventGroups = [
    EventGroup.StopEvent,
    EventGroup.UnsignedEvent,
    EventGroup.VehiclePosition,
    EventGroup.OtherEvent,
  ]

  for (let eventGroup of eventGroups) {
    let eventGroupTime = process.hrtime()
    let groupBlobs = await getHfpBlobs(date, eventGroup)
    console.log(eventGroup)

    if (groupBlobs.length === 0) {
      continue
    }

    let table = eventGroupTables[eventGroup]

    console.log(`Loading existing events for ${eventGroup}`)

    let existingEvents = await getEvents(date, table)
    let existingKeys = existingEvents.map(createSpecificEventKey)

    console.log(existingKeys.slice(0, 10))

    logTime(`Existing events loaded for ${eventGroup}`, eventGroupTime)

    let blobQueue = new PQueue({
      concurrency: 10,
    })

    for (let blobName of groupBlobs) {
      let blobTask = () =>
        new Promise<string>(async (resolve, reject) => {
          let blobTime = process.hrtime()
          console.log(`Processing blob ${blobName}`)

          const INSERT_CONCURRENCY = 10

          let insertQueue = new PQueue({
            concurrency: INSERT_CONCURRENCY,
          })

          let blobTrxPool = await createTransactionPool(INSERT_CONCURRENCY)

          // Call when the blob is done. Finishes the queued inserts and closes the transaction.
          async function onBlobDone(err?: any) {
            if (err) {
              logTime(`Event stream ERROR for blob ${blobName}`, blobTime)
              insertQueue.clear()
              return reject(err)
            }

            await insertQueue.onIdle()
            await blobTrxPool.closePool()

            logTime(`Event stream completed for blob ${blobName}`, blobTime)
            return resolve(blobName)
          }

          let eventStream = await getJourneyBlobStream(blobName)

          if (!eventStream) {
            console.log(`No data found for blob ${blobName}`)
            return onBlobDone()
          }

          let events: HfpRow[] = []

          function insertEventsIfBatchIsFull(flush: boolean = false) {
            let eventsLength = events.length
            let shouldInsertBatch = flush ? eventsLength !== 0 : eventsLength >= BATCH_SIZE

            if (shouldInsertBatch) {
              insertQueue
                .add(() => blobTrxPool.runWithTransaction((trx) => upsert(trx, table, events)))
                .then(() => console.log(`Inserted ${eventsLength} events for ${blobName}`))

              events = []
            }
          }

          let insertStream = new Transform({
            objectMode: true,
            flush: (callback) => {
              console.log(`Flushing insert stream for ${blobName}`)
              insertEventsIfBatchIsFull(true)
              callback(null)
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
                insertEventsIfBatchIsFull(false)
              }

              callback(null)
            },
          })

          pipeline(eventStream, parse(getCsvParseOptions(hfpColumns)), insertStream, (err) => {
            console.log(`Stream done for blob ${blobName}.`)
            onBlobDone(err)
          })
        })

      blobQueue.add(blobTask).catch((err) => {
        console.error(err)
      })
    }

    await blobQueue.onIdle()
  }

  logTime(`HFP loading task completed`, time)
}
