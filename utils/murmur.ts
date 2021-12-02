import murmurhash from "murmurhash"

//Static seed to make sure that we always get the same hash code
const seed = 7625

export function hash(content: string): number {
    return murmurhash.v3(content, seed)
}