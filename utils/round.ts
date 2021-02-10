function getMultiplier(decimals = 3) {
  return parseInt('1'.padEnd(decimals + 1, '0'), 10)
}

// Round down
export function floor(number: number, decimals = 3): number {
  let decimalsMultiplier = getMultiplier(decimals)
  return Math.floor(number * decimalsMultiplier) / decimalsMultiplier
}

// Round up
export function ceil(number: number, decimals = 3): number {
  let decimalsMultiplier = getMultiplier(decimals)
  return Math.ceil(number * decimalsMultiplier) / decimalsMultiplier
}

// Round to three decimals
export function round(number: number, decimals: number = 3): number {
  let decimalsMultiplier = getMultiplier(decimals)
  return Math.round(number * decimalsMultiplier) / decimalsMultiplier
}
