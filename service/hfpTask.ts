import { logTime } from '../utils/logTime'
import { EventGroup, eventGroupTables } from '../utils/hfp'
import { createJourneyBlobStreamer, createSpecificEventKey, getHfpBlobs } from './hfpStorage'
import { getEvents } from '../utils/getEvents'
import PQueue from 'p-queue'
import { insertHfpBlobData } from './insertHfp'

export async function hfpTask(date: string) {
  let time = process.hrtime()
  let getJourneyBlobStream = await createJourneyBlobStreamer()

  let eventGroups = [EventGroup.StopEvent, EventGroup.VehiclePosition, EventGroup.OtherEvent]

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
    console.log(existingKeys.slice(0, 10))

    logTime(`Existing events loaded for ${eventGroup}`, eventGroupTime)

    let blobQueue = new PQueue({
      concurrency: 3,
    })

    for (let blobName of groupBlobs) {
      let blobTask = () =>
        new Promise<string>((resolve, reject) => {
          console.log(`Processing blob ${blobName}`)

          getJourneyBlobStream(blobName)
            .then((eventStream) => {
              if (!eventStream) {
                console.log(`No data found for blob ${blobName}`)
                return resolve(blobName)
              }

              return eventStream
            })
            .then((eventStream) => {
              insertHfpBlobData({
                blobName,
                table,
                existingKeys,
                eventGroup,
                eventStream,
                onDone: resolve,
                onError: reject,
              })
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
