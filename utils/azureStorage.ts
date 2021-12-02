import { BlobServiceClient, ContainerClient } from '@azure/storage-blob'
import { HFP_STORAGE_CONNECTION_STRING } from '../constants'

let storageClient: BlobServiceClient | null = null

export function createStorageClient() {
  if (storageClient) {
    return storageClient
  }

  if (!HFP_STORAGE_CONNECTION_STRING) {
    throw new Error('HFP connection string not defined.')
  }

  storageClient = BlobServiceClient.fromConnectionString(HFP_STORAGE_CONNECTION_STRING)
  return storageClient
}

export async function getContainer(containerName: string) {
  let storageClient = createStorageClient()
  return storageClient.getContainerClient(containerName)
}

export function getBlockBlobClient(containerClient: ContainerClient, blobName: string) {
  return containerClient.getBlockBlobClient(blobName)
}

export async function listBlobs(containerClient: ContainerClient, prefix: string) {
  let blobs: string[] = []

  for await (const blob of containerClient.listBlobsFlat({ prefix })) {
    blobs.push(blob.name)
  }

  return blobs
}

export async function queryBlobs(query: string) {
  const storageClient = createStorageClient()
  
  let blobs: string[] = []
  for await (const blob of storageClient.findBlobsByTags(query)) {
    blobs.push(blob.name)
  }

  return blobs
}

export async function getBlobDownloadStream(containerClient, blobName) {
  let blockBlobClient = getBlockBlobClient(containerClient, blobName)
  const snapshotResponse = await blockBlobClient.createSnapshot()
  const blobSnapshotClient = blockBlobClient.withSnapshot(snapshotResponse.snapshot!)
  const response = await blobSnapshotClient.download()

  return response.readableStreamBody
}
