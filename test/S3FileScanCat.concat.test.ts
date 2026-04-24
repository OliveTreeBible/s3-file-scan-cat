import { Readable } from 'node:stream'
import { gunzipSync } from 'node:zlib'

import { GetObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { sdkStreamMixin } from '@smithy/util-stream'
import { mockClient } from 'aws-sdk-client-mock'
import { beforeEach, describe, expect, it } from 'vitest'

import { S3FileScanCat } from '../src/S3FileScanCat.js'
import { scannerOptions, testAwsSecrets } from './testDefaults.js'

const s3Mock = mockClient(S3Client)

describe('S3FileScanCat.concat (mocked S3)', () => {
    beforeEach(() => {
        s3Mock.reset()
    })

    it('concatenates JSON lines, gzips output, and writes expected destination key', async () => {
        s3Mock.on(ListObjectsV2Command).callsFake((input) => {
            if (input.Delimiter === '/') {
                return Promise.resolve({
                    CommonPrefixes: [{ Prefix: 'data/src/year=2020/' }],
                    IsTruncated: false,
                })
            }
            return Promise.resolve({
                Contents: [
                    { Key: 'data/src/year=2020/a.json', Size: 8 },
                    { Key: 'data/src/year=2020/b.json', Size: 8 },
                ],
                IsTruncated: false,
            })
        })

        s3Mock.on(GetObjectCommand).callsFake((input) => {
            const payload = input.Key?.endsWith('a.json') ? JSON.stringify({ x: 1 }) : JSON.stringify({ y: 2 })
            return Promise.resolve({
                Body: sdkStreamMixin(Readable.from([payload])),
            })
        })

        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)

        await cat.scanAndProcessFiles('my-bucket', 'data/src', 'data/dst')

        expect(cat.isDone).toBe(true)
        expect(cat.s3ObjectsPutTotal).toBe(1)
        expect(cat.prefixesProcessedTotal).toBe(1)
        expect(cat.prefixesWithEmittedOutputTotal).toBe(1)

        const putCalls = s3Mock.commandCalls(PutObjectCommand)
        expect(putCalls.length).toBe(1)
        const putInput = putCalls[0].args[0].input
        expect(putInput.Bucket).toBe('my-bucket')
        expect(putInput.Key).toBe('data/dst/year=2020/0.json.gz')

        const body = putInput.Body
        expect(body).toBeInstanceOf(Uint8Array)
        const plain = gunzipSync(body as Uint8Array).toString('utf8')
        // Each source body is normalized to end with '\n', so the concatenated output ends with
        // a trailing newline. This makes gzipped parts safely concatenable as NDJSON streams.
        expect(plain).toBe('{"x":1}\n{"y":2}\n')
    })
})
