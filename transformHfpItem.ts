import { HfpRow } from './hfp'
import { mapValues, toString } from 'lodash'
import { format, formatISO, parseISO } from 'date-fns'

const schema = {
  id: 'float',
  topic_prefix: 'string',
  topic_version: 'string',
  journey_type: 'string',
  is_ongoing: 'boolean',
  event_type: 'string',
  mode: 'string',
  owner_operator_id: 'int',
  vehicle_int: 'int',
  unique_vehicle_id: 'string',
  route_id: 'string',
  direction_id: 'int',
  headsign: 'string',
  journey_start_time: 'string',
  next_stop_id: 'string',
  geohash_level: 'int',
  topic_latitude: 'float',
  topic_longitude: 'float',
  desi: 'string',
  dir: 'int',
  oper: 'int',
  veh: 'int',
  tst: 'isodate',
  tsi: 'int',
  spd: 'float',
  hdg: 'int',
  lat: 'float',
  long: 'float',
  acc: 'float',
  dl: 'int',
  odo: 'float',
  drst: 'boolean',
  oday: 'date',
  jrn: 'int',
  line: 'int',
  start: 'string',
  loc: 'string',
  stop: 'int',
  route: 'string',
  occu: 'int',
  received_at: 'isodate',
  uuid: 'string',
  seq: 'int',
  dr_type: 'int',
  version: 'int',
}

// Ensure props are of the correct type before DB insert.
export function transformHfpItem(item: HfpRow): HfpRow {
  return mapValues(item, (value: string, key) => {
    let valueType = schema[key] || 'string'

    if (valueType === 'string') {
      if (!value) {
        return null
      }

      return toString(value)
    }

    if (valueType === 'int' || valueType === 'float') {
      let numVal: number
      let parseFn = (val) => (valueType === 'float' ? parseFloat(val) : parseInt(val, 10))

      if (!value) {
        return 0
      }

      numVal = parseFn(value)
      return isNaN(numVal) ? 0 : numVal
    }

    if (valueType === 'boolean') {
      return !!value
    }

    if (valueType === 'date' || valueType === 'isodate') {
      if (!value) {
        return null
      }

      let formatFn = (val) => (valueType === 'isodate' ? formatISO(val) : format(val, 'yyyy-MM-dd'))
      let dateObj: Date

      if (value.indexOf('-') !== -1) {
        // Value is an ISO date string
        dateObj = parseISO(value)
      } else {
        // Value is a milliseconds epoch string
        dateObj = new Date(parseInt(value, 10))
      }

      try {
        return formatFn(dateObj)
      } catch {
        return null
      }
    }

    return null
  }) as HfpRow
}
