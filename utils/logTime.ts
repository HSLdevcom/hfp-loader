import { logMaxTimes } from './logMaxTimes'
import { round } from './round'

const NS_PER_SEC = 1e9

export function currentSeconds(time) {
  let [execS, execNs] = process.hrtime(time)
  let ms = (execS * NS_PER_SEC + execNs) / 1000000
  return round(ms / 1000, 6)
}

export function logTime(message: string, time: any, prevSeconds: number = 0, maxTimes?: number) {
  let seconds = currentSeconds(time) - prevSeconds

  if (typeof maxTimes === 'number' && maxTimes > 0) {
    logMaxTimes(message, `[${seconds} s]`, maxTimes)
  } else if (!maxTimes && maxTimes !== 0) {
    console.log(`${message} [${seconds} s]`)
  }

  return seconds + prevSeconds
}
