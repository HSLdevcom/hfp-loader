import { logTime } from '../utils/logTime'
import { EventGroup, eventGroupTables, HfpRow } from '../utils/hfp'
import { createEventsBlobStreamer, createSpecificEventKey, getHfpBlobs } from './hfpStorage'
import { getEvents } from '../utils/getEvents'
import PQueue from 'p-queue'
import { insertHfpFromBlobStream } from './insertHfpFromBlobStream'
import { INSERT_CONCURRENCY } from '../constants'
import { chunk, compact } from 'lodash'
import { upsert } from '../utils/upsert'
import prexit from 'prexit'
import { getPool } from '../utils/pg'

export async function hfpTask(date: string, onDone: () => unknown) {
  let time = process.hrtime() // Track execution time
  // Create the blob streamer here. Call the returned function with a blobName to get the stream.
  let getJourneyBlobStream = await createEventsBlobStreamer()

  let insertsQueued = 0
  let insertsCompleted = 0
  let currentBlob = ''

  let insertQueue = new PQueue({
    concurrency: INSERT_CONCURRENCY,
  })

  // Log the status every 10 seconds.
  let statusInterval = setInterval(() => {
    console.log(
      `[${date}] Inserts queued: ${insertsQueued} | Inserts completed: ${insertsCompleted} | Current blob: ${currentBlob} | Queue size: ${insertQueue.size} | Queue pending: ${insertQueue.pending}`
    )
  }, 10000)

  // Call this function when exiting the process doe to an error to clear the queue and shut down the pool.
  async function onExit() {
    console.log('HFP loader exiting.')

    insertQueue.clear()
    await insertQueue.onIdle()

    await getPool().end()
    console.log('HFP loader exited.')
  }

  prexit(onExit)

  async function onError(err) {
    console.log('Loader task error', err)
    process.exit(1)
  }

  // Insert the HFP rows in dataToInsert into tableName.
  // Wait for the queue to calm down if it has gotten too full.
  function insertEvents(dataToInsert: HfpRow[], tableName: string) {
    let whenQueueAcceptsTasks = Promise.resolve()

    if (dataToInsert.length === 0) {
      return whenQueueAcceptsTasks
    }

    // Wait for the queue to finish work if it gets too large
    if (insertQueue.size > INSERT_CONCURRENCY * 2) {
      whenQueueAcceptsTasks = insertQueue.onEmpty()
    }

    whenQueueAcceptsTasks = whenQueueAcceptsTasks.then(() => {
      insertsQueued++

      // Add the insert query to the insertQueue.
      insertQueue // Do not return insert promise! It would hold up the whole stream.
        .add(() => upsert(tableName, dataToInsert))
        .then(() => {
          insertsCompleted++
        })
        .catch(onError)

      return Promise.resolve()
    })

    return whenQueueAcceptsTasks
  }

  // The event groups will be inserted in this order.
  let eventGroups = [EventGroup.StopEvent, EventGroup.OtherEvent, EventGroup.VehiclePosition]

  // Loop through the event groups. They will not run concurrentöy.
  for (let eventGroup of eventGroups) {
    let groupBlobs = await getHfpBlobs(date, eventGroup)

    if (groupBlobs.length === 0) {
      continue
    }

    let table = eventGroupTables[eventGroup]

    console.log(`Loading existing events for ${eventGroup}`)

    let existingEvents = (await getEvents(date, table).catch(onError)) || []

    // Also check existing VP events from the unsigned events table.
    if (eventGroup === EventGroup.VehiclePosition) {
      let existingUnsignedEvents = await getEvents(date, 'unsignedevent')
      existingEvents = [...existingEvents, ...existingUnsignedEvents]
    }

    let existingUuidChunks = chunk(compact(existingEvents.map(createSpecificEventKey)), 1000000)
    let existingEventUuids: Set<string>[] = []

    for (let uuidChunk of existingUuidChunks) {
      let set = new Set(uuidChunk)
      existingEventUuids.push(set)
    }

    function eventExists(eventId: string) {
      return existingEventUuids.some((set) => set.has(eventId))
    }

    for (currentBlob of groupBlobs) {
      console.log(`Processing blob ${currentBlob}`)

      await getJourneyBlobStream(currentBlob)
        .then((eventStream) => {
          if (!eventStream) {
            console.log(`No data found for blob ${currentBlob}`)
            return currentBlob
          }

          return insertHfpFromBlobStream({
            blobName: currentBlob,
            table,
            eventExists,
            eventGroup,
            eventStream,
            onBatch: insertEvents,
          })
        })
        .catch(onError)

      console.log(`${currentBlob} Processed`)
    }
  }

  console.log(`[${date}] Blobs done.`)

  insertQueue.add(() => Promise.resolve())
  await insertQueue.onIdle()

  clearInterval(statusInterval)

  logTime(`HFP loading task completed`, time)
  onDone()
}
