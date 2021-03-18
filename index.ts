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

;(async (loadDate) => {
  console.log(`Loading events for ${loadDate}`)
  await hfpTask(loadDate)
})(date)
