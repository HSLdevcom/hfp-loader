import { ZSTDDecompress } from "simple-zstd"

import { getBlobDownloadStream, getContainer, queryBlobs } from '../utils/azureStorage'
import { HFP_STORAGE_CONTAINER } from '../constants'
import { formatUTC } from "../utils/formatUTC"
import { hash } from "../utils/murmur"

let hfpContainerName = HFP_STORAGE_CONTAINER

// Calculate hash code from unique vehicle ID, timestamp and event type to avoid inserting duplicates to the database.
// UUID cannot be used for this because they are randomly generated.
export function createSpecificEventKey(item: { unique_vehicle_id: string | null, tsi: string | null, event_type: string | null }) {
  return hash(item.unique_vehicle_id + "_" + item.tsi + "_" + item.event_type)
}

export async function getHfpBlobsByTstAndEventType(minTst: Date, maxTst: Date, eventType: string) {
  const minTstFormatted = formatUTC(minTst, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  const maxTstFormatted = formatUTC(maxTst, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  //Azure API does not allow using OR so we need to do multiple queries and combine their results
  return Promise.all([
    queryBlobs(`@container='${hfpContainerName}' AND eventType = '${eventType}' AND max_tst <= '${maxTstFormatted}' AND min_tst >= '${minTstFormatted}'`),
    queryBlobs(`@container='${hfpContainerName}' AND eventType = '${eventType}' AND min_tst <= '${minTstFormatted}' AND max_tst >= '${minTstFormatted}'`),
    queryBlobs(`@container='${hfpContainerName}' AND eventType = '${eventType}' AND min_tst <= '${maxTstFormatted}' AND max_tst >= '${maxTstFormatted}'`),
  ]).then(blobLists => {
    const uniqueBlobs = new Set<string>()
    for (const blobList of blobLists) {
      for (const blob of blobList) {
        uniqueBlobs.add(blob)
      }
    }
    return Array.from(uniqueBlobs)
  })
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

    return blobStream.pipe(ZSTDDecompress())
  }
}
