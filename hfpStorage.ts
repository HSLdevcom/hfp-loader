import { getBlobDownloadStream, getContainer, listBlobs } from './azureStorage'
import { getCsvParseOptions } from './parseCsv'
import { hfpColumns } from './hfpColumns'
import parse from 'csv-parse'
import { pipeline, Transform } from 'stream'
import { EventGroup, HfpRow } from './hfp'
import { format } from 'date-fns'

let hfpContainerName = 'hfp-v2'
let blobsPrefix = 'csv/'

let eventTypePrefixes = {
  [EventGroup.StopEvent]: blobsPrefix + 'StopEvent/',
  [EventGroup.OtherEvent]: blobsPrefix + 'OtherEvent/',
  [EventGroup.VehiclePosition]: blobsPrefix + 'VehiclePosition/',
  [EventGroup.UnsignedEvent]: blobsPrefix + 'VehiclePosition/',
}

export function createSpecificEventKey(item: HfpRow) {
  let oday: string | null

  if (item.oday instanceof Date) {
    try {
      oday = format(item.oday, 'yyyy-MM-dd')
    } catch (err) {
      oday = null
    }
  } else {
    oday = item.oday || null
  }

  return `${item.event_type}_${item.journey_type}_${item.route_id}_${item.direction_id}_${item.journey_start_time}_${oday}_${item.unique_vehicle_id}_${item.tst}_${item.lat}_${item.long}`
}

export async function getHfpBlobs(date: string, eventGroup: EventGroup) {
  let container = await getContainer(hfpContainerName)
  let prefix = eventTypePrefixes[eventGroup] + date
  let hfpBlobs = await listBlobs(container, prefix)

  if (hfpBlobs.length === 0) {
    return []
  }

  return hfpBlobs
}

export async function createJourneyBlobStreamer(): Promise<
  (blobName: string) => Promise<Transform | undefined>
> {
  let container = await getContainer(hfpContainerName)

  return async (blobName) => {
    let blobStream = await getBlobDownloadStream(container, blobName)

    if (!blobStream) {
      return
    }

    return pipeline<Transform>(blobStream, parse(getCsvParseOptions(hfpColumns)), (err) => {
      if (err) {
        console.log(`Stream ${blobName} error`, err)
      }

      console.log(`Stream ${blobName} finished.`)
    })
  }
}
