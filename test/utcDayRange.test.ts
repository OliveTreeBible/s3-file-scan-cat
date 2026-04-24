import { describe, expect, it } from 'vitest'

import { formatUtcYmdParts, utcDayStartMs } from '../src/utcDayRange.js'

describe('utcDayStartMs', () => {
    it('parses YYYY-MM-DD as UTC midnight', () => {
        expect(utcDayStartMs('2020-01-01')).toBe(Date.UTC(2020, 0, 1))
        expect(utcDayStartMs(' 2020-12-31 ')).toBe(Date.UTC(2020, 11, 31))
    })

    it('throws RangeError for invalid input', () => {
        expect(() => utcDayStartMs('20-01-01')).toThrow(RangeError)
        expect(() => utcDayStartMs('2020-1-01')).toThrow(RangeError)
        expect(() => utcDayStartMs('')).toThrow(RangeError)
    })
})

describe('formatUtcYmdParts', () => {
    it('formats UTC calendar parts with zero padding', () => {
        expect(formatUtcYmdParts(Date.UTC(2020, 0, 5))).toEqual({
            year: '2020',
            month: '01',
            day: '05',
        })
    })

    it('round-trips with utcDayStartMs for a valid day', () => {
        const ymd = '2019-06-15'
        const { year, month, day } = formatUtcYmdParts(utcDayStartMs(ymd))
        expect(`${year}-${month}-${day}`).toBe(ymd)
    })
})
