import { format, utcToZonedTime } from "date-fns-tz";

export function formatUTC(date: Date, fmt: string): string { 
    return format(utcToZonedTime(date, "UTC"), fmt, { timeZone: "UTC" })
}