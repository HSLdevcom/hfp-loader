import { getBlobDownloadStream, getContainer, listBlobs } from '../utils/azureStorage'
import { EventGroup } from '../utils/hfp'

let hfpContainerName = 'hfp-v2'
let blobsPrefix = 'csv/'

let eventTypePrefixes = {
  [EventGroup.StopEvent]: blobsPrefix + 'StopEvent/',
  [EventGroup.OtherEvent]: blobsPrefix + 'OtherEvent/',
  [EventGroup.VehiclePosition]: blobsPrefix + 'VehiclePosition/',
}

export function createSpecificEventKey(item: { uuid?: string | null }) {
  return item.uuid
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

export async function createEventsBlobStreamer(): Promise<
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
