import { logTime } from './utils/logTime'
import { EventGroup, eventGroupTables } from './hfp'
import { createJourneyBlobStreamer, createSpecificEventKey, getHfpBlobs } from './hfpStorage'
import { getEvents } from './getEvents'
import { createTransactionPool } from './knex'
import PQueue from 'p-queue'
import { INSERT_CONCURRENCY } from './constants'
import { insertHfpBlobData } from './insertHfp'

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
            .then((eventStream) =>
              createTransactionPool(INSERT_CONCURRENCY).then((trxPool) => ({
                eventStream,
                trxPool,
              }))
            )
            .then(({ eventStream, trxPool }) => {
              insertHfpBlobData({
                blobName,
                table,
                existingKeys,
                eventGroup,
                trxPool,
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
