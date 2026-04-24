import { describe, expect, it } from 'vitest'

import { EmptyPrefixError } from '../src/errors/EmptyPrefixError.js'

describe('EmptyPrefixError', () => {
    it('has expected name and message', () => {
        const err = new EmptyPrefixError()
        expect(err).toBeInstanceOf(Error)
        expect(err).toBeInstanceOf(EmptyPrefixError)
        expect(err.name).toBe('EmptyPrefixError')
        expect(err.message).toBe('Failed to get common prefixes for directory listing in S3.')
    })
})
