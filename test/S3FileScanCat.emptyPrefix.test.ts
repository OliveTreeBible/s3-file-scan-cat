import { describe, expect, it } from 'vitest'

import { S3FileScanCat } from '../src/S3FileScanCat.js'
import { scannerOptions, testAwsSecrets } from './testDefaults.js'

describe('S3FileScanCat.scanAndProcessFiles (empty prefixes)', () => {
    it('throws RangeError when bounds end is before start (no S3 calls)', async () => {
        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                partitionStack: ['year', 'month', 'day'],
                bounds: { startDate: '2020-01-02', endDate: '2020-01-01' },
            }),
            testAwsSecrets
        )

        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toSatisfy(
            (err: unknown) =>
                err instanceof RangeError &&
                /bounds\.startDate must be on or before bounds\.endDate/.test((err as Error).message)
        )
    })

    it('rejects bounds with a partitionStack that does not start with year/month/day', async () => {
        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                partitionStack: ['year', 'month'],
                bounds: { startDate: '2020-01-01', endDate: '2020-01-01' },
            }),
            testAwsSecrets
        )

        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow(
            /partitionStack must begin with/
        )
    })
})
