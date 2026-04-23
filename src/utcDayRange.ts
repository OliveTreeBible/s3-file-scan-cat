/** Parse `YYYY-MM-DD` as UTC midnight. */
export function utcDayStartMs(ymd: string): number {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(ymd.trim())
    if (!m) {
        throw new RangeError(`Invalid date (expected YYYY-MM-DD): ${ymd}`)
    }

    const year = Number(m[1])
    const month = Number(m[2])
    const day = Number(m[3])
    const t = Date.UTC(year, month - 1, day)
    const d = new Date(t)

    if (
        d.getUTCFullYear() !== year ||
        d.getUTCMonth() !== month - 1 ||
        d.getUTCDate() !== day
    ) {
        throw new RangeError(`Invalid date (expected YYYY-MM-DD): ${ymd}`)
    }

    return t
}

export function formatUtcYmdParts(t: number): { year: string; month: string; day: string } {
    const d = new Date(t)
    const y = d.getUTCFullYear()
    const mo = String(d.getUTCMonth() + 1).padStart(2, '0')
    const da = String(d.getUTCDate()).padStart(2, '0')
    return { year: String(y), month: mo, day: da }
}
