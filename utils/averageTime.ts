import { round } from 'lodash'

export function averageTime(name = '', chunk = 50) {
  let durations: number[] = []

  return (duration: number) => {
    durations.push(duration)

    if (durations.length >= chunk) {
      let sum = durations.reduce((total, dur) => total + dur, 0)
      let length = Math.max(1, durations.length)
      let avg = round(sum / length, 4)

      console.log(
        `--- [Average Time]   ${
          name ? `${name}:` : ''
        } average duration over ${chunk} items: ${avg} s`
      )

      durations = []
    }
  }
}
