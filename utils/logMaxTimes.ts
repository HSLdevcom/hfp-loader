let counts = new Map<string, number>()

export function logMaxTimes(message: string, values: any | any[], times = 1) {
  let currentCount = counts.get(message) || 0

  if (currentCount >= times) {
    return
  }

  currentCount++
  counts.set(message, currentCount)

  let valuesArray = Array.isArray(values) ? values : [values]
  console.log(`[${currentCount}x]  ${message}`, ...valuesArray)
}
