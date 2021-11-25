import { ZSTDDecompress } from "simple-zstd"

import { getBlobDownloadStream, getContainer, queryBlobs } from '../utils/azureStorage'
import { HFP_STORAGE_CONTAINER } from '../constants'
import { format } from "date-fns"

let hfpContainerName = HFP_STORAGE_CONTAINER

// Events are identified by the UUID. This is used for deduplication, so the HFP loader
// can be run on top of a day that already has events and it won't insert duplicates.
export function createSpecificEventKey(item: { uuid?: string | null }) {
  return item.uuid || ''
}

export async function getHfpBlobsByTstAndEventType(minTst: Date, maxTst: Date, eventType: string) {
  const minTstFormatted = format(minTst, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  const maxTstFormatted = format(maxTst, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  //Azure API does not allow using OR so we need to do multiple queries and combine their results
  return Promise.all([
    queryBlobs(`@container='${hfpContainerName}' AND eventType = '${eventType}' AND max_tst <= '${maxTstFormatted}' AND min_tst >= '${minTstFormatted}'`),
    queryBlobs(`@container='${hfpContainerName}' AND eventType = '${eventType}' AND min_tst <= '${minTstFormatted}' AND max_tst >= '${minTstFormatted}'`),
    queryBlobs(`@container='${hfpContainerName}' AND eventType = '${eventType}' AND max_tst <= '${maxTstFormatted}' AND min_tst >= '${maxTstFormatted}'`),
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
