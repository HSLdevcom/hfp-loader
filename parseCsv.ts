import parse from 'csv-parse'

export function getCsvParseOptions(columns, delimiter = ',', startFromLine = 1) {
  return {
    delimiter,
    from_line: startFromLine,
    columns: columns || false,
    quote: '"',
    skip_empty_lines: true,
    skip_lines_with_empty_values: true,
    trim: true,
  }
}

export async function parseCsvStream<TData>(
  stream,
  columns,
  delimiter = ',',
  startFromLine = 1
): Promise<TData[]> {
  return new Promise((resolve, reject) => {
    let csvRows: TData[] = []
    const parser = parse(getCsvParseOptions(columns, delimiter, startFromLine))

    parser.on('readable', () => {
      let record

      // tslint:disable-next-line:no-conditional-assignment
      while ((record = parser.read())) {
        csvRows.push(record)
      }
    })

    parser.on('error', err => {
      reject(err)
    })

    parser.on('end', () => {
      stream.destroy()
      resolve(csvRows)
    })

    stream.pipe(parser)
  })
}

export function parseCsvStreamIntoStream(
  stream,
  columns,
  delimiter = ',',
  startFromLine = 1
) {
  const parser = parse(getCsvParseOptions(columns, delimiter, startFromLine))
  return stream.pipe(parser)
}

export async function parseCsvAsync<TData>(
  csvString,
  columns,
  delimiter = ',',
  startFromLine = 1
): Promise<TData[]> {
  return new Promise((resolve, reject) => {
    parse(csvString, getCsvParseOptions(columns, delimiter, startFromLine), (err, result) => {
      if (err) {
        reject(err)
      } else {
        resolve(result)
      }
    })
  })
}
