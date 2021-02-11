import { pipeline, Transform } from 'stream'
import { logTime } from './utils/logTime'
import { EventGroup, eventGroupTables, HfpRow } from './hfp'
import { createJourneyBlobStreamer, createSpecificEventKey, getHfpBlobs } from './hfpStorage'
import PQueue from 'p-queue'
import { upsert } from './upsert'
import { logMaxTimes } from './utils/logMaxTimes'
import { getEvents } from './getEvents'
import parse from 'csv-parse'
import { getCsvParseOptions } from './parseCsv'
import { hfpColumns } from './hfpColumns'
import { getKnex } from './knex'

const BATCH_SIZE = 5000

export async function hfpTask(date: string) {
  let time = process.hrtime()
  let knex = getKnex()

  const BLOB_CONCURRENCY = 10

  let blobQueue = new PQueue({
    concurrency: BLOB_CONCURRENCY,
    timeout: 100 * 1000,
  })

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

    logTime(`Existing events loaded for ${eventGroup}`, eventGroupTime)

    for (let blobName of groupBlobs) {
      let blobTask = () =>
        new Promise<string>(async (resolve, reject) => {
          let blobTime = process.hrtime()
          console.log(`Processing blob ${blobName}`)

          let trx = await knex.transaction()

          let insertQueue = new PQueue({
            concurrency: 10,
          })

          // Call when the blob is done. Finishes the queued inserts and closes the transaction.
          async function onBlobDone(err?: any) {
            if (err) {
              console.error(err)
              insertQueue.clear()
              return reject(trx.rollback(err))
            }

            await insertQueue.onIdle()

            if (!trx.isCompleted()) {
              await trx.commit()
            }

            return resolve(blobName)
          }

          let eventStream = await getJourneyBlobStream(blobName)

          if (!eventStream) {
            console.log(`No data found for blob ${blobName}`)
            return onBlobDone()
          }

          let events: HfpRow[] = []

          async function insertEventsIfBatchIsFull(flush: boolean = false) {
            let eventsLength = events.length
            let shouldInsertBatch = flush ? eventsLength !== 0 : eventsLength >= BATCH_SIZE

            if (shouldInsertBatch) {
              insertQueue
                .add(() => upsert(trx, table, events))
                .then(() => console.log(`Inserted ${eventsLength} events for ${blobName}`))
                .catch(onBlobDone) // Rollback if error

              events = []
            }
          }

          let insertStream = new Transform({
            objectMode: true,
            flush(callback) {
              insertEventsIfBatchIsFull(true)
              callback(null)
            },
            transform(data: HfpRow, encoding: BufferEncoding, callback) {
              // Unsigned and vehicle position events come from the same storage group,
              // but are inserted in different tables. Skip if the journey_type is
              // wrong for either case.
              if (
                (eventGroup === EventGroup.UnsignedEvent && data.journey_type !== 'unsigned') ||
                (eventGroup === EventGroup.VehiclePosition && data.journey_type !== 'journey')
              ) {
                logMaxTimes(
                  'Invalid VP event skipped',
                  `Invalid ${data.journey_type} event for ${eventGroups} skipped.`,
                  10
                )
                return callback(null)
              }

              let eventKey = createSpecificEventKey(data)

              if (!existingKeys.includes(eventKey)) {
                events.push(data)
                insertEventsIfBatchIsFull(false)
              }

              callback(null)
            },
          })

          pipeline(eventStream, parse(getCsvParseOptions(hfpColumns)), insertStream, (err) => {
            onBlobDone(err).then(() => {
              logTime(`Event stream ended for blob ${blobName}`, blobTime)
            })
          })
        })

      blobQueue.add(blobTask).catch((err) => {
        console.log(err)
      })
    }
  }

  await blobQueue.onIdle()

  logTime(`HFP loading task completed`, time)
}
