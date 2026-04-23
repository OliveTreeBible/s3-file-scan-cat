import { describe, expect, it } from 'vitest'

import { EmptyPrefixError } from '../src/errors/EmptyPrefixError.js'
import { S3FileScanCat } from '../src/S3FileScanCat.js'
import { scannerOptions, testAwsSecrets } from './testDefaults.js'

describe('S3FileScanCat.scanAndProcessFiles (empty prefixes)', () => {
    it('throws EmptyPrefixError when bounds end is before start (no S3 calls)', async () => {
        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                bounds: { startDate: '2020-01-02', endDate: '2020-01-01' },
            }),
            testAwsSecrets
        )

        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow(EmptyPrefixError)
    })
})
