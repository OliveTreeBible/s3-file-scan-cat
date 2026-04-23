/** Parse `YYYY-MM-DD` as UTC midnight. */
export function utcDayStartMs(ymd: string): number {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(ymd.trim())
    if (!m) {
        throw new RangeError(`Invalid date (expected YYYY-MM-DD): ${ymd}`)
    }
    return Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]))
}

export function formatUtcYmdParts(t: number): { year: string; month: string; day: string } {
    const d = new Date(t)
    const y = d.getUTCFullYear()
    const mo = String(d.getUTCMonth() + 1).padStart(2, '0')
    const da = String(d.getUTCDate()).padStart(2, '0')
    return { year: String(y), month: mo, day: da }
}
