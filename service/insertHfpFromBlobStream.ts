import { logTime } from '../utils/logTime'
import { EventGroup, HfpRow } from '../utils/hfp'
import { pipeline, Transform } from 'stream'
import { transformHfpItem } from '../utils/transformHfpItem'
import { createSpecificEventKey } from './hfpStorage'
import parse from 'csv-parse'
import { getCsvParseOptions } from '../utils/parseCsv'
import { hfpColumns } from '../utils/hfpColumns'
import { EVENT_BATCH_SIZE } from '../constants'
import { parseISO } from 'date-fns'

export function insertHfpFromBlobStream({
  blobName,
  table,
  minTst,
  maxTst,
  eventGroup,
  eventExists,
  eventStream,
  onBatch,
}: {
  blobName: string
  table: string
  minTst: Date
  maxTst: Date
  eventGroup: EventGroup
  eventExists: (eventKey: string) => boolean
  eventStream: NodeJS.ReadableStream
  onBatch: (events: HfpRow[], tableName: string) => Promise<void>
}): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    let blobTime = process.hrtime()

    let eventsByTable: { [tableName: string]: HfpRow[] } = { [table]: [] }

    function onBlobError(err) {
      logTime(`Event stream ERROR for blob ${blobName}`, blobTime)
      reject(err)
    }

    // Sends the batch to the insert queue with the onBatch callback.
    // flush = send the batch if it has any length, else wait for the batch to become full.
    function sendBatchIfFull(flush: boolean = false) {
      let insertPromise = Promise.resolve()

      for (let tableName in eventsByTable) {
        let events = eventsByTable[tableName]
        let eventsLength = events.length
        let shouldInsertBatch = flush ? eventsLength !== 0 : eventsLength >= EVENT_BATCH_SIZE

        if (shouldInsertBatch) {
          insertPromise = insertPromise.then(() => onBatch(events, tableName))
          eventsByTable[tableName] = []
        }
      }

      return insertPromise
    }

    let insertStream = new Transform({
      objectMode: true,
      flush: (callback) => {
        console.log(`Flushing insert stream for ${blobName}`)
        sendBatchIfFull(true)
        callback(null)
      },
      transform: (data: HfpRow, encoding: BufferEncoding, callback) => {
        // Unsigned and vehicle position events come from the same storage group,
        // but are inserted in different tables. Skip if the journey_type is
        // wrong for either case.
        let tableName = table

        if (eventGroup === EventGroup.VehiclePosition && data.journey_type !== 'journey') {
          tableName = 'unsignedevent'
        }

        let dataItem = transformHfpItem(data)
        let eventKey = createSpecificEventKey(dataItem)

        //For some reason tst type is defined as Date when it actually is string -> ignore type checking
        // @ts-ignore
        const tstAsDate = dataItem.tst === null ? null : parseISO(dataItem.tst)

        if (eventKey && !eventExists(eventKey) && tstAsDate != null && minTst.getTime() <= tstAsDate.getTime() && tstAsDate.getTime() <= maxTst.getTime()) {
          eventsByTable[tableName].push(dataItem)
          sendBatchIfFull(false)
        }

        callback(null)
      },
    })

    pipeline(eventStream, parse(getCsvParseOptions(hfpColumns)), insertStream, (err) => {
      if (err) {
        onBlobError(err)
      } else {
        logTime(`Event stream completed for blob ${blobName}`, blobTime)
        resolve(blobName)
      }
    })
  })
}
