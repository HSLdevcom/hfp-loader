import { Client, Connection } from 'pg'
import { PG_CONNECTION } from '../constants'
import {
  BackendMessage,
  MessageName,
  ParameterDescriptionMessage,
  RowDescriptionMessage,
} from 'pg-protocol/src/messages'

/**
 * THIS DOES NOT WORK CURRENTLY! Needs a new node-postgres version. But it might be useful so I don't want to delete it.
 */

function isParseCompleteMessage(msg: BackendMessage) {
  return msg.name === MessageName.parseComplete
}
function isParameterDescriptionMessage(msg: BackendMessage): msg is ParameterDescriptionMessage {
  return msg.name === MessageName.parameterDescription
}
function isRowDescriptionMessage(msg: BackendMessage): msg is RowDescriptionMessage {
  return msg.name === MessageName.rowDescription
}

let count = 0

let _connection: Connection | undefined
let _client: Client | undefined

async function getClient(): Promise<{ connection: Connection; client: Client }> {
  if (_connection && _client) {
    return { connection: _connection, client: _client }
  }

  _client = new Client({ ...PG_CONNECTION })
  await _client.connect()

  _connection = (_client as any).connection

  return { connection: _connection!, client: _client! }
}

export async function createMessageListener() {
  let { connection, client } = await getClient()
  // Check client is working
  const result = await client.query('SELECT 123 AS x;')

  console.assert(result.rows.length === 1)
  console.assert(result.rows[0]['x'] === 123)

  // Divert messages. First, add a `message` listener once, so that `message` events even get emitted
  connection.on('message', () => {})
  // Now, remove all existing listeners to this connection. The `client` won't hear from this connection anymore.
  connection.removeAllListeners()

  // Install my own listener for messages
  const receivedMessages: BackendMessage[] = []
  const waitingCallbacks: Array<(msg: BackendMessage) => void> = []

  connection.on('message', (msg: BackendMessage) => {
    if (waitingCallbacks.length) {
      const callback = waitingCallbacks.shift()!
      callback(msg)
    } else {
      receivedMessages.push(msg)
    }
  })

  // Get the next message. If a message has been received already, immediately returns it.
  // If not, it returns a promise that will be resolved when the next message arrives.
  return function getMessage(): Promise<BackendMessage> {
    if (receivedMessages.length) {
      return Promise.resolve(receivedMessages.shift()!)
    } else {
      return new Promise<BackendMessage>((resolve) => {
        waitingCallbacks.push(resolve)
      })
    }
  }
}

export async function getQueryInformation(query: string) {
  let { connection, client } = await getClient()
  let getMessage = await createMessageListener()

  const serial = `s${count++}`

  connection.parse({ name: serial, text: query, types: [] }, true)
  const prom1 = getMessage()
  connection.describe({ type: 'S', name: serial }, true)
  const prom2 = getMessage()
  connection.flush()
  const prom3 = getMessage()

  const parseCompleteMsg = await prom1
  if (!isParseCompleteMessage(parseCompleteMsg)) {
    throw new Error('Expected ParseComplete Message')
  }

  const parameterDescriptionMsg = await prom2
  if (!isParameterDescriptionMessage(parameterDescriptionMsg)) {
    throw new Error('Expected ParameterDescription Message')
  }

  const rowDescriptionMsg = await prom3
  if (!isRowDescriptionMessage(rowDescriptionMsg)) {
    throw new Error('Expected RowDescription Message')
  }

  return {
    parameters: parameterDescriptionMsg.dataTypeIDs,
    columns: rowDescriptionMsg.fields.map(({ name, dataTypeID, tableID, columnID }) => ({
      name,
      dataTypeID,
      tableID,
      columnID,
    })),
  }
}
