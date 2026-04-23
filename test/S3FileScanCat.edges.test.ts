import { Readable } from 'node:stream'
import { gunzipSync } from 'node:zlib'

import { GetObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { sdkStreamMixin } from '@smithy/util-stream'
import { mockClient } from 'aws-sdk-client-mock'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { S3FileScanCat } from '../src/S3FileScanCat.js'
import { scannerOptions, testAwsSecrets } from './testDefaults.js'

const s3Mock = mockClient(S3Client)

function stubPartitionAndConcatList(
    concatHandler: (input: { Prefix?: string; ContinuationToken?: string; Delimiter?: string }) => {
        Contents?: { Key?: string; Size?: number }[]
        IsTruncated?: boolean
        NextContinuationToken?: string
    }
) {
    s3Mock.on(ListObjectsV2Command).callsFake((input) => {
        if (input.Delimiter === '/') {
            return Promise.resolve({
                CommonPrefixes: [{ Prefix: 'data/src/year=2020/' }],
                IsTruncated: false,
            })
        }
        return Promise.resolve(concatHandler(input))
    })
}

describe('S3FileScanCat edges (mocked S3)', () => {
    beforeEach(() => {
        s3Mock.reset()
    })

    it('paginates concat ListObjectsV2 using ContinuationToken', async () => {
        let concatCalls = 0
        stubPartitionAndConcatList((input) => {
            concatCalls++
            if (!input.ContinuationToken) {
                return {
                    Contents: [{ Key: 'data/src/year=2020/p1.json', Size: 4 }],
                    IsTruncated: true,
                    NextContinuationToken: 'tok-1',
                }
            }
            return {
                Contents: [{ Key: 'data/src/year=2020/p2.json', Size: 4 }],
                IsTruncated: false,
            }
        })

        s3Mock.on(GetObjectCommand).callsFake((input) => {
            const one = input.Key?.includes('p1') ? '{"a":1}' : '{"b":2}'
            return Promise.resolve({
                Body: sdkStreamMixin(Readable.from([one])),
            })
        })
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        expect(concatCalls).toBe(2)
        const listCalls = s3Mock
            .commandCalls(ListObjectsV2Command)
            .filter((c) => c.args[0].input.Delimiter === undefined)
        expect(listCalls.length).toBe(2)
        expect(listCalls[1].args[0].input.ContinuationToken).toBe('tok-1')
    })

    it('flushes multiple gzip parts when maxFileSizeBytes is exceeded', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [
                { Key: 'data/src/year=2020/1.json', Size: 10 },
                { Key: 'data/src/year=2020/2.json', Size: 10 },
            ],
            IsTruncated: false,
        }))

        s3Mock.on(GetObjectCommand).callsFake((input) => {
            const chunk = input.Key?.includes('1.json') ? '{"a":1}' : '{"b":2}'
            return Promise.resolve({
                Body: sdkStreamMixin(Readable.from([chunk])),
            })
        })
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                partitionStack: ['year'],
                limits: { maxFileSizeBytes: 5 },
            }),
            testAwsSecrets
        )

        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        expect(cat.s3ObjectsPutTotal).toBe(2)
        const keys = s3Mock.commandCalls(PutObjectCommand).map((c) => c.args[0].input.Key)
        expect(keys.sort()).toEqual(['data/dst/year=2020/0.json.gz', 'data/dst/year=2020/1.json.gz'])

        const bodies = s3Mock
            .commandCalls(PutObjectCommand)
            .map((c) => gunzipSync(c.args[0].input.Body as Uint8Array).toString('utf8'))
            .sort()
        expect(bodies).toEqual(['{"a":1}', '{"b":2}'])
    })

    it('retries GetObject with backoff until success', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [{ Key: 'data/src/year=2020/x.json', Size: 12 }],
            IsTruncated: false,
        }))

        let attempts = 0
        s3Mock.on(GetObjectCommand).callsFake(() => {
            attempts++
            if (attempts < 3) {
                return Promise.reject(new Error('ThrottlingException'))
            }
            return Promise.resolve({
                Body: sdkStreamMixin(Readable.from(['{"ok":true}'])),
            })
        })
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        const sleepSpy = vi.spyOn(cat, '_sleep').mockResolvedValue(undefined)

        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        expect(attempts).toBe(3)
        expect(sleepSpy).toHaveBeenCalled()
        sleepSpy.mockRestore()
    })

    it('propagates ListObjectsV2 errors during concat', async () => {
        stubPartitionAndConcatList(() => {
            throw new Error('list failed')
        })

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow('list failed')
    })

    it('throws after GetObject retries are exhausted (_getAndProcessObjectBody)', async () => {
        s3Mock.on(GetObjectCommand).rejects(new Error('permanent failure'))

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        vi.spyOn(cat, '_sleep').mockResolvedValue(undefined)

        const concatState = { buffer: undefined as string | undefined, continuationToken: undefined, fileNumber: 0 }
        await expect(
            cat._getAndProcessObjectBody(
                'bucket',
                'data/src/year=2020/bad.json',
                concatState,
                'data/src/year=2020',
                'data/src',
                'data/dst'
            )
        ).rejects.toThrow(/Unexpected S3 getObject error/)
    })

    it('throws when GetObject returns no Body (_getAndProcessObjectBody)', async () => {
        s3Mock.on(GetObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        const concatState = { buffer: undefined as string | undefined, continuationToken: undefined, fileNumber: 0 }
        await expect(
            cat._getAndProcessObjectBody(
                'bucket',
                'data/src/year=2020/empty.json',
                concatState,
                'data/src/year=2020',
                'data/src',
                'data/dst'
            )
        ).rejects.toThrow('Missing response data for object')
    })

    it('throws on invalid list content entry without Key', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [{ Size: 5 }],
            IsTruncated: false,
        }))

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow(
            'Invalid S3 Object encountered.'
        )
    })
})
