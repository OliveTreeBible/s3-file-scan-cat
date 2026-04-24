import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { waitUntil } from '../src/waitUntil.js'

describe('waitUntil', () => {
    beforeEach(() => {
        vi.useFakeTimers()
    })

    afterEach(() => {
        vi.useRealTimers()
    })

    it('returns immediately when predicate is true', async () => {
        await expect(waitUntil(() => true)).resolves.toBeUndefined()
    })

    it('polls until predicate becomes true', async () => {
        let n = 0
        const p = waitUntil(() => ++n >= 3, { pollMs: 5 })
        await vi.advanceTimersByTimeAsync(20)
        await p
        expect(n).toBe(3)
    })

    it('throws when numeric timeout elapses', async () => {
        const p = waitUntil(() => false, 25)
        const assertion = expect(p).rejects.toThrow('waitUntil timed out')
        await vi.advanceTimersByTimeAsync(30)
        await assertion
    })

    it('throws when options.timeout elapses', async () => {
        const p = waitUntil(() => false, { timeout: 40, pollMs: 15 })
        const assertion = expect(p).rejects.toThrow('waitUntil timed out')
        await vi.advanceTimersByTimeAsync(45)
        await assertion
    })
})
