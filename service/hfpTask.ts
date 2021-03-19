import { logTime } from '../utils/logTime'
import { EventGroup, eventGroupTables, HfpRow } from '../utils/hfp'
import { createJourneyBlobStreamer, createSpecificEventKey, getHfpBlobs } from './hfpStorage'
import { getEvents } from '../utils/getEvents'
import PQueue from 'p-queue'
import { insertHfpFromBlobStream } from './insertHfpFromBlobStream'
import { BLOB_CONCURRENCY, INSERT_CONCURRENCY } from '../constants'
import { compact } from 'lodash'
import { upsert } from '../utils/upsert'
import prexit from 'prexit'
import { getKnex } from '../utils/knex'

export async function hfpTask(date: string, onDone: () => unknown) {
  let time = process.hrtime()
  let getJourneyBlobStream = await createJourneyBlobStreamer()

  let insertQueue = new PQueue({
    concurrency: INSERT_CONCURRENCY,
  })

  let blobQueue = new PQueue({
    concurrency: BLOB_CONCURRENCY,
  })

  async function onExit() {
    insertQueue.clear()
    blobQueue.clear()
    await blobQueue.onIdle()
    await insertQueue.onIdle()
    await getKnex().destroy()
  }

  prexit(onExit)

  async function onError(err) {
    console.log('Loader task error', err)
    process.exit(1)
  }

  let waitingForQueue = false
  let whenQueueAcceptsTasks = Promise.resolve()

  function insertEvents(dataToInsert: HfpRow[], tableName: string) {
    if (dataToInsert.length === 0) {
      return whenQueueAcceptsTasks
    }

    // Wait for the queue to finish work if it gets too large
    if (insertQueue.size > INSERT_CONCURRENCY && !waitingForQueue) {
      waitingForQueue = true
      whenQueueAcceptsTasks = insertQueue.onEmpty()
    }

    whenQueueAcceptsTasks = whenQueueAcceptsTasks.then(() => {
      waitingForQueue = false

      insertQueue // Do not return insert promise! It would hold up the whole stream.
        .add(() => upsert(tableName, dataToInsert))
        .catch(onError)

      return Promise.resolve()
    })

    return whenQueueAcceptsTasks
  }

  let eventGroups = [EventGroup.StopEvent, EventGroup.OtherEvent, EventGroup.VehiclePosition]

  for (let eventGroup of eventGroups) {
    let groupBlobs = await getHfpBlobs(date, eventGroup)

    if (groupBlobs.length === 0) {
      continue
    }

    let table = eventGroupTables[eventGroup]

    console.log(`Loading existing events for ${eventGroup}`)

    let existingEvents = await getEvents(date, table)

    // Also check existing VP events from the unsigned events table.
    if (eventGroup === EventGroup.VehiclePosition) {
      let existingUnsignedEvents = await getEvents(date, 'unsignedevent')
      existingEvents = [...existingEvents, ...existingUnsignedEvents]
    }

    let existingKeys = new Set<string>(compact(existingEvents.map(createSpecificEventKey)))

    for (let blobName of groupBlobs) {
      let blobTask = () =>
        new Promise<string>(async (resolve, reject) => {
          console.log(`Processing blob ${blobName}`)

          await getJourneyBlobStream(blobName)
            .then((eventStream) => {
              if (!eventStream) {
                console.log(`No data found for blob ${blobName}`)
                return resolve(blobName)
              }

              return eventStream
            })
            .then((eventStream) => {
              if (eventStream) {
                return insertHfpFromBlobStream({
                  blobName,
                  table,
                  existingKeys,
                  eventGroup,
                  eventStream,
                  onBatch: insertEvents,
                  onDone: resolve,
                  onError: reject,
                })
              }

              return Promise.resolve()
            })
            .catch(reject)
        })

      blobQueue.add(blobTask).catch(onError)
    }
  }

  await blobQueue.onIdle()
  await insertQueue.onIdle()

  logTime(`HFP loading task completed`, time)
  onDone()
}
