import { logTime } from '../utils/logTime'
import { EventGroup, eventGroupTables } from '../utils/hfp'
import { createJourneyBlobStreamer, createSpecificEventKey, getHfpBlobs } from './hfpStorage'
import { getEvents } from '../utils/getEvents'
import PQueue from 'p-queue'
import { insertHfpFromBlobStream } from './insertHfpFromBlobStream'
import { BLOB_CONCURRENCY } from '../constants'

export async function hfpTask(date: string, onDone: () => unknown) {
  let time = process.hrtime()
  let getJourneyBlobStream = await createJourneyBlobStreamer()

  let eventGroups = [EventGroup.VehiclePosition, EventGroup.StopEvent, EventGroup.OtherEvent]

  for (let eventGroup of eventGroups) {
    let eventGroupTime = process.hrtime()
    let groupBlobs = await getHfpBlobs(date, eventGroup)

    if (groupBlobs.length === 0) {
      continue
    }

    let table = eventGroupTables[eventGroup]

    console.log(`Loading existing events for ${eventGroup}`)

    let existingEvents = await getEvents(date, table)

    if (eventGroup === EventGroup.VehiclePosition) {
      let existingUnsignedEvents = await getEvents(date, 'unsignedevent')
      existingEvents = [...existingEvents, ...existingUnsignedEvents]
    }

    let existingKeys = existingEvents.map(createSpecificEventKey)

    logTime(`Existing events loaded for ${eventGroup}`, eventGroupTime)

    let blobQueue = new PQueue({
      concurrency: BLOB_CONCURRENCY,
      throwOnTimeout: true,
    })

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
                  onDone: resolve,
                  onError: reject,
                })
              }

              return Promise.resolve()
            })
            .catch(reject)
        })

      blobQueue.add(blobTask).catch((err) => {
        console.error(err)
      })
    }

    await blobQueue.onIdle()
    logTime(`HFP events inserted for ${eventGroup}`, time)
  }

  logTime(`HFP loading task completed`, time)
  onDone()
}
