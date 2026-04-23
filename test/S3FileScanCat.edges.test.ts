import { Readable } from 'node:stream'
import { gunzipSync } from 'node:zlib'

import { GetObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { sdkStreamMixin } from '@smithy/util-stream'
import { mockClient } from 'aws-sdk-client-mock'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { ConcatState } from '../src/interfaces/scanner.interface.js'
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

    it('continues paginating when a truncated concat page returns no Contents', async () => {
        let concatCalls = 0
        stubPartitionAndConcatList((input) => {
            concatCalls++
            if (!input.ContinuationToken) {
                // First page: truncated but empty. Before the fix, the token update was nested
                // inside `if (response.Contents)`, so we never advanced and dropped page 2.
                return {
                    IsTruncated: true,
                    NextContinuationToken: 'tok-empty',
                }
            }
            return {
                Contents: [{ Key: 'data/src/year=2020/late.json', Size: 8 }],
                IsTruncated: false,
            }
        })

        s3Mock.on(GetObjectCommand).callsFake(() =>
            Promise.resolve({
                Body: sdkStreamMixin(Readable.from(['{"late":true}'])),
            })
        )
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        // We should have issued both the empty first page and the populated second page.
        expect(concatCalls).toBe(2)
        const concatListCalls = s3Mock
            .commandCalls(ListObjectsV2Command)
            .filter((c) => c.args[0].input.Delimiter === undefined)
        expect(concatListCalls.length).toBe(2)
        expect(concatListCalls[1].args[0].input.ContinuationToken).toBe('tok-empty')

        // The object on the second page must have been fetched and written out.
        expect(cat.s3ObjectsFetchedTotal).toBe(1)
        expect(cat.s3ObjectsPutTotal).toBe(1)
        const put = s3Mock.commandCalls(PutObjectCommand)[0].args[0].input
        expect(put.Key).toBe('data/dst/year=2020/0.json.gz')
        expect(gunzipSync(put.Body as Uint8Array).toString('utf8')).toBe('{"late":true}\n')
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
        expect(bodies).toEqual(['{"a":1}\n', '{"b":2}\n'])
    })

    it("defaults the S3 client region to 'us-east-1' for backward compatibility", async () => {
        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        const client = (cat as unknown as { _s3Client: S3Client })._s3Client
        // AWS SDK v3 exposes region as a provider function returning a Promise<string>.
        const resolvedRegion = await client.config.region()
        expect(resolvedRegion).toBe('us-east-1')
    })

    it('uses the region argument when one is provided to the constructor', async () => {
        const cat = new S3FileScanCat(
            false,
            scannerOptions({ partitionStack: ['year'] }),
            testAwsSecrets,
            'eu-west-2'
        )
        const client = (cat as unknown as { _s3Client: S3Client })._s3Client
        const resolvedRegion = await client.config.region()
        expect(resolvedRegion).toBe('eu-west-2')
    })

    it('fails fast without retrying on a non-retryable 4xx S3 error (_isRetryableError)', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [{ Key: 'data/src/year=2020/forbidden.json', Size: 8 }],
            IsTruncated: false,
        }))

        const accessDenied = Object.assign(new Error('Access Denied'), {
            name: 'AccessDenied',
            $metadata: { httpStatusCode: 403 },
        })
        let getAttempts = 0
        s3Mock.on(GetObjectCommand).callsFake(() => {
            getAttempts++
            return Promise.reject(accessDenied)
        })

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        const sleepSpy = vi.spyOn(cat, '_sleep').mockResolvedValue(undefined)

        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow(
            /Unexpected S3 getObject error/
        )

        // One attempt, no retries, no sleeps.
        expect(getAttempts).toBe(1)
        expect(sleepSpy).not.toHaveBeenCalled()
    })

    it('retries on 429 TooManyRequests (retryable 4xx)', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [{ Key: 'data/src/year=2020/throttled.json', Size: 8 }],
            IsTruncated: false,
        }))

        const throttled = Object.assign(new Error('Too Many Requests'), {
            name: 'TooManyRequestsException',
            $metadata: { httpStatusCode: 429 },
        })
        let attempts = 0
        s3Mock.on(GetObjectCommand).callsFake(() => {
            attempts++
            if (attempts < 3) {
                return Promise.reject(throttled)
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
        expect(sleepSpy).toHaveBeenCalledTimes(2)
    })

    it('_getWaitTimeForRetry applies exponential backoff with equal jitter (no zero-ms first retry)', () => {
        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)

        // Equal jitter: result ∈ [cap/2, cap], where cap = 2^retryCount * 100.
        // retryCount=0 -> [50, 100]; retryCount=1 -> [100, 200]; retryCount=5 -> [1600, 3200].
        for (const retry of [0, 1, 2, 5]) {
            const cap = Math.pow(2, retry) * 100
            const half = cap / 2
            for (let i = 0; i < 100; i++) {
                const wait = cat._getWaitTimeForRetry(retry)
                expect(wait).toBeGreaterThanOrEqual(half)
                expect(wait).toBeLessThanOrEqual(cap)
            }
        }
        // Specifically, retry 0 must never be zero anymore.
        for (let i = 0; i < 200; i++) {
            expect(cat._getWaitTimeForRetry(0)).toBeGreaterThan(0)
        }
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

        const concatState: ConcatState = {
            buffer: undefined,
            continuationToken: undefined,
            fileNumber: 0,
            nextSeqToAppend: 0,
            pendingBodies: new Map(),
        }
        await expect(
            cat._getAndProcessObjectBody(
                'bucket',
                'data/src/year=2020/bad.json',
                concatState,
                'data/src/year=2020',
                'data/src',
                'data/dst',
                0
            )
        ).rejects.toThrow(/Unexpected S3 getObject error/)
    })

    it('throws when GetObject returns no Body (_getAndProcessObjectBody)', async () => {
        s3Mock.on(GetObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        const concatState: ConcatState = {
            buffer: undefined,
            continuationToken: undefined,
            fileNumber: 0,
            nextSeqToAppend: 0,
            pendingBodies: new Map(),
        }
        await expect(
            cat._getAndProcessObjectBody(
                'bucket',
                'data/src/year=2020/empty.json',
                concatState,
                'data/src/year=2020',
                'data/src',
                'data/dst',
                0
            )
        ).rejects.toThrow('Missing response data for object')
    })

    it('with bounds and partitionStack=[year,month,day], skips partition scan and concats directly under the day prefix', async () => {
        const concatListInputs: Array<{ Prefix?: string; ContinuationToken?: string }> = []
        s3Mock.on(ListObjectsV2Command).callsFake((input) => {
            if (input.Delimiter === '/') {
                throw new Error(
                    `Unexpected partition-scan list call when day is the leaf (Prefix=${input.Prefix})`
                )
            }
            concatListInputs.push({ Prefix: input.Prefix, ContinuationToken: input.ContinuationToken })
            return Promise.resolve({
                Contents: [{ Key: `${input.Prefix}a.json`, Size: 7 }],
                IsTruncated: false,
            })
        })

        s3Mock.on(GetObjectCommand).callsFake(() =>
            Promise.resolve({
                Body: sdkStreamMixin(Readable.from(['{"v":1}'])),
            })
        )
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                partitionStack: ['year', 'month', 'day'],
                bounds: { startDate: '2020-01-01', endDate: '2020-01-02' },
            }),
            testAwsSecrets
        )

        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        expect(cat.isDone).toBe(true)
        expect(cat.prefixesProcessedTotal).toBe(2)
        expect(concatListInputs.map((c) => c.Prefix).sort()).toEqual([
            'data/src/year=2020/month=01/day=01/',
            'data/src/year=2020/month=01/day=02/',
        ])

        const putKeys = s3Mock.commandCalls(PutObjectCommand).map((c) => c.args[0].input.Key).sort()
        expect(putKeys).toEqual([
            'data/dst/year=2020/month=01/day=01/0.json.gz',
            'data/dst/year=2020/month=01/day=02/0.json.gz',
        ])
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

    it('drains in-flight partition scans before rejecting so background work cannot mutate state', async () => {
        let slowPartitionResolve: (() => void) | undefined
        s3Mock.on(ListObjectsV2Command).callsFake((input) => {
            if (input.Delimiter !== '/') {
                return Promise.resolve({ Contents: [], IsTruncated: false })
            }
            if (input.Prefix === 'data/src/') {
                return Promise.resolve({
                    CommonPrefixes: [
                        { Prefix: 'data/src/year=2020/' },
                        { Prefix: 'data/src/year=2021/' },
                    ],
                    IsTruncated: false,
                })
            }
            if (input.Prefix === 'data/src/year=2020/') {
                return Promise.reject(new Error('boom-2020'))
            }
            if (input.Prefix === 'data/src/year=2021/') {
                // Resolve only after the test releases us, so the fatal flag
                // from the 2020 rejection has already fired.
                return new Promise((resolve) => {
                    slowPartitionResolve = () =>
                        resolve({
                            CommonPrefixes: [{ Prefix: 'data/src/year=2021/month=01/' }],
                            IsTruncated: false,
                        })
                })
            }
            return Promise.resolve({ CommonPrefixes: [], IsTruncated: false })
        })

        const cat = new S3FileScanCat(
            false,
            scannerOptions({ partitionStack: ['year', 'month'] }),
            testAwsSecrets
        )

        const scanPromise = cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        // Give the root list + both child scans a chance to start, then release the slow one.
        await new Promise((resolve) => setTimeout(resolve, 20))
        expect(typeof slowPartitionResolve).toBe('function')
        slowPartitionResolve?.()

        await expect(scanPromise).rejects.toThrow('boom-2020')

        // Cast past `private` so we can verify there is nothing still in flight.
        const inFlight = (cat as unknown as { _inFlightPartitionScans: Set<Promise<void>> })
            ._inFlightPartitionScans
        expect(inFlight.size).toBe(0)

        // The slow scan observed the fatal flag and must not have leaked a push.
        const allPrefixes = (cat as unknown as { _allPrefixes: string[] })._allPrefixes
        expect(allPrefixes).toEqual([])

        // Give any stray microtasks/timers a chance to fire. State must stay stable.
        const keyParamsBefore = [
            ...(cat as unknown as { _keyParams: unknown[] })._keyParams,
        ]
        await new Promise((resolve) => setTimeout(resolve, 50))
        expect((cat as unknown as { _allPrefixes: string[] })._allPrefixes).toEqual([])
        expect((cat as unknown as { _keyParams: unknown[] })._keyParams).toEqual(keyParamsBefore)
    })

    it('drains in-flight body processors before rejecting on fatal body failure', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [
                { Key: 'data/src/year=2020/fail.json', Size: 8 },
                { Key: 'data/src/year=2020/slow.json', Size: 8 },
            ],
            IsTruncated: false,
        }))

        let slowGetResolve: (() => void) | undefined
        s3Mock.on(GetObjectCommand).callsFake((input) => {
            if (input.Key?.endsWith('fail.json')) {
                return Promise.reject(new Error('permanent failure'))
            }
            return new Promise((resolve) => {
                slowGetResolve = () =>
                    resolve({
                        Body: sdkStreamMixin(Readable.from(['{"slow":true}'])),
                    })
            })
        })
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        // Avoid real 13-minute backoff on the failing key.
        vi.spyOn(cat, '_sleep').mockResolvedValue(undefined)

        const runPromise = cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        // Let the concat list fire and both body processors start, then release the slow one
        // after the failing one has exhausted its retries and set the fatal flag.
        await new Promise((resolve) => setTimeout(resolve, 30))
        expect(typeof slowGetResolve).toBe('function')
        slowGetResolve?.()

        await expect(runPromise).rejects.toThrow('permanent failure')

        // The slow body must have observed the fatal flag and returned without appending
        // to the buffer or triggering a put.
        expect(cat.s3ObjectsPutTotal).toBe(0)
        expect(s3Mock.commandCalls(PutObjectCommand).length).toBe(0)

        // No background body workers should still be running at this point.
        expect(cat.s3ObjectBodyProcessInProgress).toBe(0)

        // State must stay stable even as event-loop ticks go by.
        await new Promise((resolve) => setTimeout(resolve, 50))
        expect(cat.s3ObjectsPutTotal).toBe(0)
        expect(cat.s3ObjectBodyProcessInProgress).toBe(0)
    })

    it('resets per-run state so the same instance can be reused across successful runs', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [{ Key: 'data/src/year=2020/a.json', Size: 8 }],
            IsTruncated: false,
        }))
        s3Mock.on(GetObjectCommand).callsFake(() =>
            Promise.resolve({
                Body: sdkStreamMixin(Readable.from(['{"x":1}'])),
            })
        )
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)

        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')
        expect(cat.isDone).toBe(true)
        expect(cat.s3ObjectsPutTotal).toBe(1)
        expect(cat.s3ObjectsFetchedTotal).toBe(1)
        expect(cat.prefixesProcessedTotal).toBe(1)
        expect(cat.totalPrefixesToProcess).toBe(1)

        // Second run against the same instance should zero the per-run counters before starting,
        // not double them.
        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')
        expect(cat.isDone).toBe(true)
        expect(cat.s3ObjectsPutTotal).toBe(1)
        expect(cat.s3ObjectsFetchedTotal).toBe(1)
        expect(cat.prefixesProcessedTotal).toBe(1)
        expect(cat.totalPrefixesToProcess).toBe(1)
    })

    it('allows a retry on the same instance after a failed run', async () => {
        let callCount = 0
        s3Mock.on(ListObjectsV2Command).callsFake((input) => {
            if (input.Delimiter === '/') {
                callCount++
                if (callCount === 1) {
                    return Promise.reject(new Error('transient list failure'))
                }
                return Promise.resolve({
                    CommonPrefixes: [{ Prefix: 'data/src/year=2020/' }],
                    IsTruncated: false,
                })
            }
            return Promise.resolve({
                Contents: [{ Key: 'data/src/year=2020/a.json', Size: 8 }],
                IsTruncated: false,
            })
        })
        s3Mock.on(GetObjectCommand).callsFake(() =>
            Promise.resolve({
                Body: sdkStreamMixin(Readable.from(['{"x":1}'])),
            })
        )
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)

        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow(
            'transient list failure'
        )

        // After the failure, state is stale. Without the reset, `isDone` would still be false
        // *and* the next run would inherit leftover `_keyParams`/`_allPrefixes` pushes.
        expect(cat.isDone).toBe(false)

        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        expect(cat.isDone).toBe(true)
        expect(cat.prefixesProcessedTotal).toBe(1)
        expect(cat.totalPrefixesToProcess).toBe(1)
        expect(cat.s3ObjectsPutTotal).toBe(1)
    })

    it('does not emit double newlines when source bodies already end with a newline', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [
                { Key: 'data/src/year=2020/a.json', Size: 8 },
                { Key: 'data/src/year=2020/b.json', Size: 8 },
            ],
            IsTruncated: false,
        }))
        // Both source bodies already end with '\n'. The join must not add a second newline
        // between them (which would produce an empty NDJSON record).
        s3Mock.on(GetObjectCommand).callsFake((input) => {
            const payload = input.Key?.endsWith('a.json') ? '{"a":1}\n' : '{"b":2}\n'
            return Promise.resolve({ Body: sdkStreamMixin(Readable.from([payload])) })
        })
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        const put = s3Mock.commandCalls(PutObjectCommand)[0].args[0].input
        const plain = gunzipSync(put.Body as Uint8Array).toString('utf8')
        expect(plain).toBe('{"a":1}\n{"b":2}\n')
    })

    it('warns and still emits the part when a single source body exceeds maxFileSizeBytes', async () => {
        const bigBody = 'x'.repeat(128)
        stubPartitionAndConcatList(() => ({
            Contents: [{ Key: 'data/src/year=2020/big.json', Size: bigBody.length }],
            IsTruncated: false,
        }))
        s3Mock.on(GetObjectCommand).callsFake(() =>
            Promise.resolve({ Body: sdkStreamMixin(Readable.from([bigBody])) })
        )
        s3Mock.on(PutObjectCommand).resolves({})

        const warnSpy = vi.fn()
        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                partitionStack: ['year'],
                limits: { maxFileSizeBytes: 16 },
                loggerOptions: { level: 'warn' },
            }),
            testAwsSecrets
        )
        // The warn channel goes through ot-logger-deluxe; stub the created logger's warn.
        const logger = (cat as unknown as { _logger?: { warn: (...args: unknown[]) => unknown } })._logger
        if (logger) {
            logger.warn = warnSpy
        }

        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        const putCalls = s3Mock.commandCalls(PutObjectCommand)
        expect(putCalls.length).toBe(1)
        const plain = gunzipSync(putCalls[0].args[0].input.Body as Uint8Array).toString('utf8')
        expect(plain).toBe(`${bigBody}\n`)
        // The oversized warning should have fired.
        expect(
            warnSpy.mock.calls.some((args) =>
                String(args[0]).includes('exceeds maxFileSizeBytes')
            )
        ).toBe(true)
    })

    it('flushes before adding a body that would push the buffer over maxFileSizeBytes (including the newline)', async () => {
        // Two 10-byte bodies with a 20-byte cap. Without counting the added newline, the code
        // would decide 10+10 == 20 fits and emit a single 21-byte (incl. separator) part. With
        // the fix the cap is honored: the second body triggers a flush first.
        stubPartitionAndConcatList(() => ({
            Contents: [
                { Key: 'data/src/year=2020/a.json', Size: 10 },
                { Key: 'data/src/year=2020/b.json', Size: 10 },
            ],
            IsTruncated: false,
        }))
        s3Mock.on(GetObjectCommand).callsFake((input) => {
            const payload = input.Key?.endsWith('a.json') ? 'AAAAAAAAAA' : 'BBBBBBBBBB'
            return Promise.resolve({ Body: sdkStreamMixin(Readable.from([payload])) })
        })
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                partitionStack: ['year'],
                limits: { maxFileSizeBytes: 20 },
            }),
            testAwsSecrets
        )
        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        const puts = s3Mock.commandCalls(PutObjectCommand)
        expect(puts.length).toBe(2)
        // Each output part is one body + terminating newline (11 bytes) - well under the cap.
        const bodies = puts
            .map((c) => gunzipSync(c.args[0].input.Body as Uint8Array).toString('utf8'))
            .sort()
        expect(bodies).toEqual(['AAAAAAAAAA\n', 'BBBBBBBBBB\n'])
    })

    it('concatenates bodies in S3 listing order even when GetObject completes out of order', async () => {
        stubPartitionAndConcatList(() => ({
            // S3 returns keys in lexicographic order. Our output must follow this order.
            Contents: [
                { Key: 'data/src/year=2020/a.json', Size: 8 },
                { Key: 'data/src/year=2020/b.json', Size: 8 },
                { Key: 'data/src/year=2020/c.json', Size: 8 },
            ],
            IsTruncated: false,
        }))

        // Invert completion order: 'c' resolves first, then 'b', then 'a'.
        // Before the fix this would have produced '{"c":3}\n{"b":2}\n{"a":1}'.
        const resolvers = new Map<string, (v: unknown) => void>()
        s3Mock.on(GetObjectCommand).callsFake(
            (input) =>
                new Promise((resolve) => {
                    resolvers.set(input.Key!, resolve as (v: unknown) => void)
                })
        )
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        const run = cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        // Wait for all three GetObject calls to be registered, then resolve in reverse order.
        const waitForKey = async (key: string) => {
            for (let i = 0; i < 50 && !resolvers.has(key); i++) {
                await new Promise((r) => setTimeout(r, 5))
            }
        }
        await waitForKey('data/src/year=2020/a.json')
        await waitForKey('data/src/year=2020/b.json')
        await waitForKey('data/src/year=2020/c.json')

        resolvers.get('data/src/year=2020/c.json')!({
            Body: sdkStreamMixin(Readable.from(['{"c":3}'])),
        })
        resolvers.get('data/src/year=2020/b.json')!({
            Body: sdkStreamMixin(Readable.from(['{"b":2}'])),
        })
        resolvers.get('data/src/year=2020/a.json')!({
            Body: sdkStreamMixin(Readable.from(['{"a":1}'])),
        })

        await run

        expect(cat.s3ObjectsPutTotal).toBe(1)
        const put = s3Mock.commandCalls(PutObjectCommand)[0].args[0].input
        expect(put.Key).toBe('data/dst/year=2020/0.json.gz')
        const plain = gunzipSync(put.Body as Uint8Array).toString('utf8')
        expect(plain).toBe('{"a":1}\n{"b":2}\n{"c":3}\n')
    })

    it('does not leak _s3PrefixListObjectsProcessCount when concat ListObjectsV2 throws', async () => {
        s3Mock.on(ListObjectsV2Command).callsFake((input) => {
            if (input.Delimiter === '/') {
                return Promise.resolve({
                    CommonPrefixes: [{ Prefix: 'data/src/year=2020/' }],
                    IsTruncated: false,
                })
            }
            return Promise.reject(new Error('list failed for concat'))
        })

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)

        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow(
            'list failed for concat'
        )

        // Without the fix, the pre-await `++` would never be matched by a `--`.
        expect(cat.s3PrefixListObjectsProcessCount).toBe(0)
    })

    it('does not leak _s3ObjectPutProcessCount when PutObject throws', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [{ Key: 'data/src/year=2020/a.json', Size: 8 }],
            IsTruncated: false,
        }))
        s3Mock.on(GetObjectCommand).callsFake(() =>
            Promise.resolve({
                Body: sdkStreamMixin(Readable.from(['{"x":1}'])),
            })
        )
        s3Mock.on(PutObjectCommand).rejects(new Error('put failed'))

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)

        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow('put failed')

        // Without the fix, the `++` happened before the failing send() and the matching `--` was
        // unreachable past `throw e`.
        expect(cat.s3ObjectPutProcessCount).toBe(0)
        // A failed put should not count as a successful one.
        expect(cat.s3ObjectsPutTotal).toBe(0)
    })

    it('rejects concurrent scanAndProcessFiles calls on the same instance', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [{ Key: 'data/src/year=2020/a.json', Size: 8 }],
            IsTruncated: false,
        }))
        let releaseGet: (() => void) | undefined
        s3Mock.on(GetObjectCommand).callsFake(
            () =>
                new Promise((resolve) => {
                    releaseGet = () =>
                        resolve({ Body: sdkStreamMixin(Readable.from(['{"x":1}'])) })
                })
        )
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)

        const first = cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')
        // Give the first call a chance to set `_isRunning` and reach the in-flight body stage.
        await new Promise((resolve) => setTimeout(resolve, 20))

        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow(
            /already running/
        )

        // Let the first run finish and verify it still succeeds.
        releaseGet?.()
        await first
        expect(cat.isDone).toBe(true)
    })
})
