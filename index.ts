import { isValid, parse } from 'date-fns'
import { hfpTask } from './service/hfpTask'

if (process.argv.length === 3) {
  console.error('Expected at least two argument!')
  process.exit(1)
}

let minTst
let minTstValid

try {
  minTst = parse(process.argv[2], "yyyy-MM-dd'T'HH:mm:ss", new Date(0))
  minTstValid = isValid(minTst)
} catch (err) {
  minTstValid = false
}

let maxTst
let maxTstValid

try {
  maxTst = parse(process.argv[3], "yyyy-MM-dd'T'HH:mm:ss", new Date(0))
  maxTstValid = isValid(maxTst)
} catch (err) {
  maxTstValid = false
}

if (!minTstValid || !maxTstValid) {
  console.error('Invalid timestamps')
  process.exit(1)
}

let isDone = false

console.log(`Loading events for ${minTst} - ${maxTst}`)
hfpTask(minTst, maxTst, () => (isDone = true)).then(() => console.log(`All events loaded for ${minTst} - ${maxTst}`))

// Keep the process going until done. Otherwise it will quit if there are no events to insert for a blob.
// This is a Node limitation.
;(function wait() {
  if (!isDone) {
    setTimeout(wait, 1000)
  }
})()
