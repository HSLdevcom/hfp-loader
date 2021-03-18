let counts = new Map<string, number>()

export function logMaxTimes(
  message: string,
  values: any | any[],
  times = 1,
  clearAfterTimes?: number
) {
  let currentCount = counts.get(message) || 0

  if (clearAfterTimes && currentCount >= clearAfterTimes) {
    counts.set(message, 0)
  }

  if (currentCount >= times) {
    return
  }

  currentCount++
  counts.set(message, currentCount)

  let valuesArray = Array.isArray(values) ? values : [values]
  console.log(`[${currentCount}x]  ${message}`, ...valuesArray)
}
