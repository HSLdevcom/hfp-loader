import { isValid, parseISO } from 'date-fns'
import { hfpTask } from './service/hfpTask'

if (process.argv.length === 2) {
  console.error('Expected at least one argument!')
  process.exit(1)
}

let date = process.argv[2]
let dateValid

try {
  dateValid = isValid(parseISO(date))
} catch (err) {
  dateValid = false
}

if (!dateValid) {
  console.error('Invalid date.')
  process.exit(1)
}

let isDone = false

console.log(`Loading events for ${date}`)
hfpTask(date, () => (isDone = true)).then(() => console.log(`All events loaded for ${date}`))

// Keep the process going until done. Otherwise it will quit if there are no events to insert for a blob.
// This is a Node limitation.
;(function wait() {
  if (!isDone) {
    setTimeout(wait, 1000)
  }
})()
