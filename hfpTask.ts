import { finished, Transform } from 'stream'
import { logTime } from './utils/logTime'
import { EventGroup, HfpRow } from './hfp'
import { createJourneyBlobStreamer, getHfpBlobs } from './hfpStorage'
import PQueue from 'p-queue'
import { upsert } from './upsert'

let eventGroupTables = {
  [EventGroup.StopEvent]: 'stopevent',
  [EventGroup.OtherEvent]: 'otherevent',
  [EventGroup.VehiclePosition]: 'vehicleposition',
  [EventGroup.UnsignedEvent]: 'unsignedevent',
}

export async function hfpTask(date: string) {
  let time = process.hrtime()

  async function insertEvents(events: HfpRow[], eventGroup: EventGroup) {
    let table = eventGroupTables[eventGroup]
    await upsert(table, events)
  }

  let insertQueue = new PQueue({
    concurrency: 500,
  })

  const BLOB_CONCURRENCY = 100

  let blobQueue = new PQueue({
    concurrency: BLOB_CONCURRENCY,
    timeout: 100 * 1000,
  })

  let getJourneyBlobStream = await createJourneyBlobStreamer()
  let eventGroups = Object.keys(eventGroupTables)

  for (let eventGroupName of eventGroups) {
    let eventGroup = EventGroup[eventGroupName]
    let groupBlobs = await getHfpBlobs(date, eventGroup)

    if (groupBlobs.length === 0) {
      continue
    }

    for (let blobName of groupBlobs) {
      let blobTask = () =>
        new Promise<string>(async (resolve, reject) => {
          let blobTime = process.hrtime()
          console.log(`Processing blob ${blobName}`)

          let eventStream = await getJourneyBlobStream(blobName)

          if (!eventStream) {
            console.log(`No data found for blob ${blobName}`)
            return resolve(blobName)
          }

          let events: HfpRow[] = []

          let insertStream = new Transform({
            objectMode: true,
            flush(callback) {
              let eventsLength = events.length

              if (eventsLength !== 0) {
                insertQueue
                  .add(async () => insertEvents(events, eventGroup))
                  .then(() => console.log(`Inserted ${eventsLength} events for ${blobName}`))
                events = []
              }

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
                return callback(null)
              }

              events.push(data)

              let eventsLength = events.length

              if (eventsLength >= 1000) {
                insertQueue
                  .add(() => insertEvents(events, eventGroup))
                  .then(() => console.log(`Inserted ${eventsLength} events for ${blobName}`))

                events = []
              }

              callback(null)
            },
          })

          let blobStream = eventStream.pipe(insertStream)

          blobStream.on('end', () => {
            console.log(`------------ Blob stream ${blobName} ended. ----------------------`)
          })

          finished(blobStream, (err) => {
            if (err) {
              reject(err)
            } else {
              eventStream!.destroy()

              logTime(`Events fetched and inserted ${blobName}`, blobTime)
              resolve(blobName)
            }
          })
        })

      blobQueue.add(blobTask).catch((err) => {
        console.log(err)
      })
    }
  }

  await blobQueue.onIdle()
  await insertQueue.onIdle()

  logTime(`HFP loading task completed`, time)
}
