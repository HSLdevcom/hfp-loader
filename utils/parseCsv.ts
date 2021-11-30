export function getCsvParseOptions(columns, delimiter = ',', startFromLine = 2) {
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
