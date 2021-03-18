import { getBlobDownloadStream, getContainer, listBlobs } from '../utils/azureStorage'
import { EventGroup, HfpRow } from '../utils/hfp'
import { format, formatISO, parseISO } from 'date-fns'

let hfpContainerName = 'hfp-v2'
let blobsPrefix = 'csv/'

let eventTypePrefixes = {
  [EventGroup.StopEvent]: blobsPrefix + 'StopEvent/',
  [EventGroup.OtherEvent]: blobsPrefix + 'OtherEvent/',
  [EventGroup.VehiclePosition]: blobsPrefix + 'VehiclePosition/',
}

export function createSpecificEventKey(item: HfpRow) {
  let oday: any = item.oday
  let tst: any = item.tst

  if (oday instanceof Date) {
    try {
      oday = format(item.oday!, 'yyyy-MM-dd')
    } catch (err) {
      oday = null
    }
  } else if (typeof oday === 'string') {
    let odayDate: Date

    if (oday.indexOf('-') !== -1) {
      odayDate = parseISO(oday)
    } else {
      odayDate = new Date(parseInt(oday, 10))
    }

    oday = format(odayDate, 'yyyy-MM-dd')
  }

  if (tst instanceof Date) {
    try {
      tst = formatISO(item.tst!)
    } catch (err) {
      tst = null
    }
  }

  return [
    item.event_type,
    item.journey_type,
    item.route_id,
    item.direction_id,
    item.journey_start_time,
    oday,
    item.unique_vehicle_id,
    tst,
    item.lat,
    item.long,
  ].join('_')
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
  (blobName: string) => Promise<NodeJS.ReadableStream | undefined>
> {
  let container = await getContainer(hfpContainerName)

  return async (blobName) => {
    let blobStream = await getBlobDownloadStream(container, blobName)

    if (!blobStream) {
      return
    }

    return blobStream
  }
}
