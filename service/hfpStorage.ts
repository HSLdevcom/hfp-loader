import { getBlobDownloadStream, getContainer, listBlobs } from '../utils/azureStorage'
import { EventGroup } from '../utils/hfp'
import { HFP_STORAGE_CONTAINER } from '../constants'

let hfpContainerName = HFP_STORAGE_CONTAINER
let blobsPrefix = 'csv/'

// The event groups to stoeage container path mapping. This is where the blobs for each event group can be found.
let eventTypePrefixes = {
  [EventGroup.StopEvent]: blobsPrefix + 'StopEvent/',
  [EventGroup.OtherEvent]: blobsPrefix + 'OtherEvent/',
  [EventGroup.VehiclePosition]: blobsPrefix + 'VehiclePosition/',
}

// Events are identified by the UUID. This is used for deduplication, so the HFP loader
// can be run on top of a day that already has events and it won't insert duplicates.
export function createSpecificEventKey(item: { uuid?: string | null }) {
  return item.uuid || ''
}

// Fetch a list of all blobs for the date and event group. The list will be used to
// fetch blobs one by one later.
export async function getHfpBlobs(date: string, eventGroup: EventGroup) {
  let container = await getContainer(hfpContainerName)
  // Build the path (called prefix in Azure blob storage parlance)
  let prefix = eventTypePrefixes[eventGroup] + date
  let hfpBlobs = await listBlobs(container, prefix)

  if (hfpBlobs.length === 0) {
    return []
  }

  return hfpBlobs
}

// Get a stream of an individual blob by name.
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
