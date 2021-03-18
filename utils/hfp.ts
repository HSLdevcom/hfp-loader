export interface HfpRow {
  acc: string | null
  desi: string | null
  dir: string | null
  direction_id: string | null
  dl: string | null
  dr_type: string | null
  drst: string | null
  event_type: string | null
  geohash_level: string | null
  hdg: string | null
  headsign: string | null
  is_ongoing: string | null
  journey_start_time: string | null
  journey_type: string | null
  jrn: string | null
  lat: string | null
  line: string | null
  loc: string | null
  long: string | null
  mode: string | null
  next_stop_id: string | null
  occu: string | null
  oday: Date | null
  odo: string | null
  oper: string | null
  owner_operator_id: string | null
  received_at: Date | null
  route_id: string | null
  route: string | null
  seq: string | null
  spd: string | null
  start: string | null
  stop: string | null
  topic_latitude: string | null
  topic_longitude: string | null
  topic_prefix: string | null
  topic_version: string | null
  tsi: string | null
  tst: Date | null
  unique_vehicle_id: string | null
  uuid: string | null
  veh: string | null
  vehicle_number: string | null
  version: string | null
}

export enum EventGroup {
  StopEvent = 'stopEvent',
  OtherEvent = 'otherEvent',
  VehiclePosition = 'vehiclePosition',
}

export const eventGroupTables = {
  [EventGroup.StopEvent]: 'stopevent',
  [EventGroup.OtherEvent]: 'otherevent',
  [EventGroup.VehiclePosition]: 'vehicleposition',
}
