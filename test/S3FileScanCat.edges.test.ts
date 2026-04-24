import { Readable } from 'node:stream'
import { gunzipSync } from 'node:zlib'

import { GetObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { sdkStreamMixin } from '@smithy/util-stream'
import { mockClient } from 'aws-sdk-client-mock'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { EmptyPrefixError } from '../src/errors/EmptyPrefixError.js'
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

    it('paginates partition ListObjectsV2 when a truncated page returns no CommonPrefixes', async () => {
        let partitionCalls = 0
        s3Mock.on(ListObjectsV2Command).callsFake((input) => {
            if (input.Delimiter === '/') {
                partitionCalls++
                if (!input.ContinuationToken) {
                    return Promise.resolve({
                        IsTruncated: true,
                        NextContinuationToken: 'tok-partition-empty',
                        Contents: [{ Key: 'data/src/_SUCCESS', Size: 1 }],
                    })
                }
                return Promise.resolve({
                    CommonPrefixes: [{ Prefix: 'data/src/year=2020/' }],
                    IsTruncated: false,
                })
            }
            return Promise.resolve({
                Contents: [{ Key: 'data/src/year=2020/one.json', Size: 8 }],
                IsTruncated: false,
            })
        })
        s3Mock.on(GetObjectCommand).callsFake(() =>
            Promise.resolve({
                Body: sdkStreamMixin(Readable.from(['{"k":1}'])),
            })
        )
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        expect(partitionCalls).toBe(2)
        const partitionInputs = s3Mock
            .commandCalls(ListObjectsV2Command)
            .filter((c) => c.args[0].input.Delimiter === '/')
            .map((c) => c.args[0].input)
        expect(partitionInputs[1].ContinuationToken).toBe('tok-partition-empty')
        expect(cat.prefixesProcessedTotal).toBe(1)
        expect(cat.prefixesWithEmittedOutputTotal).toBe(1)
        expect(cat.s3ObjectsPutTotal).toBe(1)
    })

    it('does not throw on partition scan when only Contents exist under the prefix (skips objects)', async () => {
        s3Mock.on(ListObjectsV2Command).callsFake((input) => {
            if (input.Delimiter === '/') {
                return Promise.resolve({
                    Contents: [{ Key: 'data/src/orphan.json', Size: 5 }],
                    IsTruncated: false,
                })
            }
            return Promise.resolve({ Contents: [], IsTruncated: false })
        })

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow(
            EmptyPrefixError
        )
    })

    it('throws when inner waitUntil exceeds limits.waitUntilTimeoutMs (stuck body backpressure)', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [{ Key: 'data/src/year=2020/a.json', Size: 8 }],
            IsTruncated: false,
        }))
        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                partitionStack: ['year'],
                limits: {
                    s3ObjectBodyProcessInProgressLimit: 0,
                    waitUntilTimeoutMs: 120,
                },
            }),
            testAwsSecrets
        )
        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow(
            'waitUntil timed out'
        )
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
                limits: {
                    maxFileSizeBytes: 5,
                    // Listing Size is 10 per key; default maxSource would equal maxFile (5) and skip fetch.
                    maxSourceObjectSizeBytes: 32,
                },
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

    it('runs mid-stream flushes concurrently without holding the buffer mutex across gzip+put', async () => {
        // Three 10-byte bodies under a 15-byte cap -> two mid-stream flushes (buffers 0 and 1)
        // followed by a final flush (buffer 2). Under the old behavior the buffer mutex was
        // held across each flush, so only ONE put could be pending at a time. With the fix the
        // mid-stream flushes are detached, so puts 0 and 1 overlap in flight.
        stubPartitionAndConcatList(() => ({
            Contents: [
                { Key: 'data/src/year=2020/a.json', Size: 10 },
                { Key: 'data/src/year=2020/b.json', Size: 10 },
                { Key: 'data/src/year=2020/c.json', Size: 10 },
            ],
            IsTruncated: false,
        }))
        s3Mock.on(GetObjectCommand).callsFake((input) => {
            const payload = input.Key?.endsWith('a.json')
                ? 'AAAAAAAAAA'
                : input.Key?.endsWith('b.json')
                  ? 'BBBBBBBBBB'
                  : 'CCCCCCCCCC'
            return Promise.resolve({ Body: sdkStreamMixin(Readable.from([payload])) })
        })

        const putResolvers: Array<() => void> = []
        let maxConcurrentPuts = 0
        let currentConcurrentPuts = 0
        s3Mock.on(PutObjectCommand).callsFake(
            () =>
                new Promise<object>((resolve) => {
                    currentConcurrentPuts++
                    maxConcurrentPuts = Math.max(maxConcurrentPuts, currentConcurrentPuts)
                    putResolvers.push(() => {
                        currentConcurrentPuts--
                        resolve({})
                    })
                })
        )

        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                partitionStack: ['year'],
                limits: { maxFileSizeBytes: 15 },
            }),
            testAwsSecrets
        )

        const runPromise = cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        // Wait for both mid-stream flushes to be in flight simultaneously. Under the fix the
        // append-phase mutex does not block on the flush, so put 0 can still be pending when
        // put 1 is initiated. Under the old code only one put is ever pending at a time.
        for (let i = 0; i < 50 && maxConcurrentPuts < 2; i++) {
            await new Promise((r) => setTimeout(r, 5))
        }
        expect(maxConcurrentPuts).toBeGreaterThanOrEqual(2)

        // Now let the scan finish by draining any put (including the final flush) as it arrives.
        const drain = setInterval(() => {
            while (putResolvers.length > 0) {
                putResolvers.shift()!()
            }
        }, 5)
        try {
            await runPromise
        } finally {
            clearInterval(drain)
        }
        // Three buffers total: two mid-stream flushes + one final flush.
        expect(cat.s3ObjectsPutTotal).toBe(3)
    })

    it('drains thousands of leaf prefixes without quadratic shift() behavior', async () => {
        // Arrange 5000 year partitions. Under the pre-fix Array.shift() pattern, draining
        // 5000 items is O(n^2) (~12.5M memmove ops in the queue) plus the same for _keyParams.
        // With the index-pointer fix it's strictly O(n).
        const NUM_PREFIXES = 5000
        const commonPrefixes = Array.from({ length: NUM_PREFIXES }, (_, i) => ({
            Prefix: `data/src/year=${String(i).padStart(4, '0')}/`,
        }))

        s3Mock.on(ListObjectsV2Command).callsFake((input) => {
            if (input.Delimiter === '/') {
                // Return all 5000 leaf prefixes in one shot.
                return Promise.resolve({ CommonPrefixes: commonPrefixes, IsTruncated: false })
            }
            // Concat phase list: each leaf contains one tiny object.
            return Promise.resolve({
                Contents: [{ Key: `${input.Prefix}one.json`, Size: 8 }],
                IsTruncated: false,
            })
        })
        s3Mock.on(GetObjectCommand).callsFake(() =>
            Promise.resolve({ Body: sdkStreamMixin(Readable.from(['{"k":1}'])) })
        )
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)

        const t0 = Date.now()
        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')
        const elapsed = Date.now() - t0

        // Correctness: every prefix was processed exactly once.
        expect(cat.prefixesProcessedTotal).toBe(NUM_PREFIXES)
        expect(cat.prefixesWithEmittedOutputTotal).toBe(NUM_PREFIXES)
        expect(cat.totalPrefixesToProcess).toBe(NUM_PREFIXES)
        expect(cat.s3ObjectsPutTotal).toBe(NUM_PREFIXES)

        // Perf guard: not trying to assert a specific wall-clock number (which varies by
        // machine), just that draining 5000 items stayed well under the 30s vitest default.
        // The real signal that the fix is working is the `prefixCountForProcessing` == 0
        // sanity below -- and the whole thing completing at all, because Array.shift on a
        // 5000-length array per iteration would normally work fine for this size (shift is
        // still fast for small N in V8), so this test's primary value is as a regression
        // guard that `prefixCountForProcessing` / the stats accurately report "remaining".
        expect(elapsed).toBeLessThan(20_000)
        expect(cat.prefixCountForProcessing).toBe(0)
        expect(cat.getStats().prefixesRemainingInQueue).toBe(0)
    })

    it('getStats() returns a frozen snapshot, and replacement getters mirror their deprecated counterparts', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [{ Key: 'data/src/year=2020/a.json', Size: 8 }],
            IsTruncated: false,
        }))
        s3Mock.on(GetObjectCommand).callsFake(() =>
            Promise.resolve({ Body: sdkStreamMixin(Readable.from(['{"x":1}'])) })
        )
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)

        // Before any scan runs, every counter is zero and the lifecycle flags are clean.
        const before = cat.getStats()
        expect(before.partitionScansInProgress).toBe(0)
        expect(before.concatListObjectsInProgress).toBe(0)
        expect(before.totalPrefixesToProcess).toBe(0)
        expect(before.prefixesProcessedTotal).toBe(0)
        expect(before.prefixesWithEmittedOutputTotal).toBe(0)
        expect(before.prefixesRemainingInQueue).toBe(0)
        expect(before.s3ObjectBodyWorkersInProgress).toBe(0)
        expect(before.s3ObjectPutWorkersInProgress).toBe(0)
        expect(before.s3ObjectsFetchedTotal).toBe(0)
        expect(before.s3ObjectsPutTotal).toBe(0)
        expect(before.isRunning).toBe(false)
        expect(before.isDone).toBe(false)
        expect(before.isClosed).toBe(false)

        // The snapshot must be frozen; re-reading after mutation attempts should not change it.
        expect(Object.isFrozen(before)).toBe(true)
        expect(() => {
            ;(before as { s3ObjectsPutTotal: number }).s3ObjectsPutTotal = 999
        }).toThrow(TypeError)
        expect(before.s3ObjectsPutTotal).toBe(0)

        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        const after = cat.getStats()
        expect(after.totalPrefixesToProcess).toBe(1)
        expect(after.prefixesProcessedTotal).toBe(1)
        expect(after.prefixesWithEmittedOutputTotal).toBe(1)
        expect(after.prefixesRemainingInQueue).toBe(0)
        expect(after.s3ObjectsFetchedTotal).toBe(1)
        expect(after.s3ObjectsPutTotal).toBe(1)
        expect(after.isRunning).toBe(false)
        expect(after.isDone).toBe(true)
        expect(after.isClosed).toBe(false)

        // Replacement getters must mirror the deprecated ones, so the old surface keeps working
        // for any consumer that hasn't migrated yet.
        expect(cat.partitionScansInProgress).toBe(cat.s3BuildPrefixListObjectsProcessCount)
        expect(cat.concatListObjectsInProgress).toBe(cat.s3PrefixListObjectsProcessCount)

        // After close() the lifecycle flag flips.
        cat.close()
        expect(cat.getStats().isClosed).toBe(true)
    })

    it('_pathSegmentMatchesPartitionCurPart uses Hive name=value boundaries (no substring false positives)', () => {
        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)

        expect(cat._pathSegmentMatchesPartitionCurPart('year=2020', 'year')).toBe(true)
        expect(cat._pathSegmentMatchesPartitionCurPart('year=2020', 'year=')).toBe(true)
        expect(cat._pathSegmentMatchesPartitionCurPart('year', 'year')).toBe(true)
        expect(cat._pathSegmentMatchesPartitionCurPart('yearly=1', 'year')).toBe(false)
        expect(cat._pathSegmentMatchesPartitionCurPart('year_backup=1', 'year')).toBe(false)
        expect(cat._pathSegmentMatchesPartitionCurPart('part-04=00', 'part-04')).toBe(true)
        expect(cat._pathSegmentMatchesPartitionCurPart('part-040=00', 'part-04')).toBe(false)
    })

    it('_lastSegmentOfListPrefix returns the segment before the trailing slash', () => {
        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        expect(cat._lastSegmentOfListPrefix('data/src/year=2020/')).toBe('year=2020')
        expect(cat._lastSegmentOfListPrefix('year=2020')).toBeUndefined()
    })

    it('skips CommonPrefixes that only falsely matched via substring (e.g. yearly= when curPart is year)', async () => {
        s3Mock.on(ListObjectsV2Command).callsFake((input) => {
            if (input.Delimiter === '/') {
                if (input.Prefix === 'data/src/' || input.Prefix === 'data/src') {
                    return Promise.resolve({
                        CommonPrefixes: [
                            { Prefix: 'data/src/year=2020/' },
                            { Prefix: 'data/src/yearly=1/' },
                        ],
                        IsTruncated: false,
                    })
                }
                throw new Error(`Unexpected partition list Prefix=${input.Prefix}`)
            }
            return Promise.resolve({
                Contents: [{ Key: `${input.Prefix}one.json`, Size: 8 }],
                IsTruncated: false,
            })
        })
        s3Mock.on(GetObjectCommand).callsFake(() =>
            Promise.resolve({
                Body: sdkStreamMixin(Readable.from(['{"k":1}'])),
            })
        )
        s3Mock.on(PutObjectCommand).resolves({})

        const warnSpy = vi.fn()
        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                partitionStack: ['year'],
                loggerOptions: { level: 'warn' },
            }),
            testAwsSecrets
        )
        const logger = (cat as unknown as { _logger?: { warn: (...args: unknown[]) => unknown } })._logger
        if (logger) {
            logger.warn = warnSpy
        }

        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        expect(cat.prefixesProcessedTotal).toBe(1)
        expect(cat.prefixesWithEmittedOutputTotal).toBe(1)
        expect(
            warnSpy.mock.calls.some((args) =>
                String(args[0]).includes('Skipping CommonPrefix') && String(args[0]).includes('yearly=1')
            )
        ).toBe(true)
        const putKeys = s3Mock.commandCalls(PutObjectCommand).map((c) => c.args[0].input.Key)
        expect(putKeys).toEqual(['data/dst/year=2020/0.json.gz'])
    })

    it('_buildDestinationKey handles trailing-slash variants, mismatched paths, and deep leaves', () => {
        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)

        // Canonical happy path.
        expect(cat._buildDestinationKey('data/src/year=2020', 'data/src', 'data/dst', 0)).toBe(
            'data/dst/year=2020/0.json.gz'
        )

        // Trailing slashes on any/all of the inputs should normalize cleanly.
        expect(cat._buildDestinationKey('data/src/year=2020/', 'data/src/', 'data/dst/', 1)).toBe(
            'data/dst/year=2020/1.json.gz'
        )
        expect(cat._buildDestinationKey('data/src/year=2020', 'data/src/', 'data/dst', 2)).toBe(
            'data/dst/year=2020/2.json.gz'
        )

        // Deep leaves work end-to-end.
        expect(
            cat._buildDestinationKey(
                'data/src/year=2020/month=01/day=01',
                'data/src',
                'data/dst',
                7
            )
        ).toBe('data/dst/year=2020/month=01/day=01/7.json.gz')

        // Leaf equal to srcPrefix (edge case) -> no empty segment.
        expect(cat._buildDestinationKey('data/src', 'data/src', 'data/dst', 0)).toBe(
            'data/dst/0.json.gz'
        )

        // srcPrefix substring trap: 'data/src' must NOT silently match 'data/source/...'.
        expect(() =>
            cat._buildDestinationKey('data/source/year=2020', 'data/src', 'data/dst', 0)
        ).toThrow(/not a path-descendant/)

        // Completely unrelated prefix throws instead of writing to the wrong key.
        expect(() =>
            cat._buildDestinationKey('other/location/year=2020', 'data/src', 'data/dst', 0)
        ).toThrow(/not a path-descendant/)
    })

    it('backpressure is per-concatFilesAtPrefix call, not shared across parallel calls', async () => {
        // Both prefixes list the same number of keys; their GetObject calls block until we say
        // so. With a per-call limit of 1, sharing the counter (the pre-fix behavior) would mean
        // the second concatFilesAtPrefix could not spawn ANY body worker until the first one
        // released its single slot. With the fix, each call gets its own budget of 1.
        const concatListed: Record<string, number> = {}
        s3Mock.on(ListObjectsV2Command).callsFake((input) => {
            if (input.Delimiter !== undefined) {
                throw new Error(`Unexpected partition-scan list: ${input.Prefix}`)
            }
            const prefix = input.Prefix ?? ''
            concatListed[prefix] = (concatListed[prefix] ?? 0) + 1
            return Promise.resolve({
                Contents: [
                    { Key: `${prefix}a.json`, Size: 8 },
                    { Key: `${prefix}b.json`, Size: 8 },
                ],
                IsTruncated: false,
            })
        })

        const pendingResolvers: Array<(v: unknown) => void> = []
        s3Mock.on(GetObjectCommand).callsFake(
            () =>
                new Promise((resolve) => {
                    pendingResolvers.push(resolve as (v: unknown) => void)
                })
        )
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                partitionStack: ['year'],
                limits: { s3ObjectBodyProcessInProgressLimit: 1 },
            }),
            testAwsSecrets
        )

        const run1 = cat.concatFilesAtPrefix('bucket', 'data/src/year=2020', 'data/src', 'data/dst')
        const run2 = cat.concatFilesAtPrefix('bucket', 'data/src/year=2021', 'data/src', 'data/dst')

        // Each call should spawn exactly one body worker up to the limit. Across the two calls
        // that is 2 pending GetObject calls. Poll briefly for them to be registered.
        for (let i = 0; i < 50 && pendingResolvers.length < 2; i++) {
            await new Promise((r) => setTimeout(r, 5))
        }
        expect(pendingResolvers.length).toBe(2)

        // Sanity: the class-level rollup still reflects total in-flight bodies across calls.
        expect(cat.s3ObjectBodyProcessInProgress).toBe(2)

        // Release the first worker of each call; each call can then spawn the second.
        pendingResolvers[0]({ Body: sdkStreamMixin(Readable.from(['{"x":1}'])) })
        pendingResolvers[1]({ Body: sdkStreamMixin(Readable.from(['{"y":2}'])) })
        for (let i = 0; i < 50 && pendingResolvers.length < 4; i++) {
            await new Promise((r) => setTimeout(r, 5))
        }
        expect(pendingResolvers.length).toBe(4)
        pendingResolvers[2]({ Body: sdkStreamMixin(Readable.from(['{"x":2}'])) })
        pendingResolvers[3]({ Body: sdkStreamMixin(Readable.from(['{"y":3}'])) })

        await Promise.all([run1, run2])

        // Each prefix produced one put (2-body concat fits in the default max).
        const putKeys = s3Mock.commandCalls(PutObjectCommand).map((c) => c.args[0].input.Key).sort()
        expect(putKeys).toEqual(['data/dst/year=2020/0.json.gz', 'data/dst/year=2021/0.json.gz'])

        // After everything settles, the rollup is back to zero.
        expect(cat.s3ObjectBodyProcessInProgress).toBe(0)
    })

    it('close() destroys the S3 client and keep-alive agents, and rejects further scans', async () => {
        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)

        const internals = cat as unknown as {
            _s3Client: { destroy: () => void }
            _httpsAgent: { destroy: () => void }
            _httpAgent: { destroy: () => void }
        }
        const clientDestroy = vi.spyOn(internals._s3Client, 'destroy')
        const httpsDestroy = vi.spyOn(internals._httpsAgent, 'destroy')
        const httpDestroy = vi.spyOn(internals._httpAgent, 'destroy')

        expect(cat.isClosed).toBe(false)
        cat.close()
        expect(cat.isClosed).toBe(true)
        expect(clientDestroy).toHaveBeenCalledTimes(1)
        expect(httpsDestroy).toHaveBeenCalledTimes(1)
        expect(httpDestroy).toHaveBeenCalledTimes(1)

        // Calling close() again is a no-op (idempotent), no extra destroys.
        cat.close()
        expect(clientDestroy).toHaveBeenCalledTimes(1)
        expect(httpsDestroy).toHaveBeenCalledTimes(1)
        expect(httpDestroy).toHaveBeenCalledTimes(1)

        // Further scans must reject loudly rather than silently issuing S3 traffic against a
        // torn-down client.
        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow(
            /called after close\(\)/
        )
    })

    it('close() throws while scanAndProcessFiles is running and does not mark the instance closed', async () => {
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
        const scan = cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')
        await new Promise((r) => setTimeout(r, 25))

        expect(() => cat.close()).toThrow(/await the scan promise first/)
        expect(cat.isClosed).toBe(false)

        releaseGet?.()
        await scan
        expect(cat.isClosed).toBe(false)

        cat.close()
        expect(cat.isClosed).toBe(true)
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

    it('_isRetryableError classifies errors without httpStatusCode via name, $fault, and message', () => {
        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)

        expect(cat._isRetryableError(Object.assign(new Error('x'), { name: 'AccessDenied' }))).toBe(false)
        expect(cat._isRetryableError({ name: 'NoSuchKey', message: '' })).toBe(false)
        expect(cat._isRetryableError(new Error('ThrottlingException'))).toBe(true)
        expect(cat._isRetryableError(new Error('ECONNRESET on read'))).toBe(true)
        expect(cat._isRetryableError(new Error('access denied by policy'))).toBe(false)

        expect(cat._isRetryableError({ $fault: 'client', name: 'Custom', message: '' })).toBe(false)
        expect(cat._isRetryableError({ $fault: 'server', name: 'Custom', message: '' })).toBe(true)

        expect(cat._isRetryableError({ code: 'ENOENT' })).toBe(false)
        expect(cat._isRetryableError(new Error('permanent failure'))).toBe(true)
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
            bodiesInFlight: 0,
            inFlightFlushes: new Set(),
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
            bodiesInFlight: 0,
            inFlightFlushes: new Set(),
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
        expect(cat.prefixesWithEmittedOutputTotal).toBe(2)
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

    it('throws on invalid list content entry without Key, regardless of Size', async () => {
        // Size=5 but no Key -> this is the unambiguous data-integrity case.
        stubPartitionAndConcatList(() => ({
            Contents: [{ Size: 5 }],
            IsTruncated: false,
        }))

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow(
            /Invalid S3 Object encountered \(missing Key\)/
        )
    })

    it('throws on invalid list content entry with zero Size AND missing Key (the misclassification case)', async () => {
        // Size=0 AND no Key -> previously this silently hit the zero-size warn branch (logging
        // `undefined`). It is a malformed entry and must error.
        stubPartitionAndConcatList(() => ({
            Contents: [{ Size: 0 }],
            IsTruncated: false,
        }))

        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        await expect(cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')).rejects.toThrow(
            /Invalid S3 Object encountered \(missing Key\)/
        )
    })

    it('warns when GetObject returns an empty body despite a positive listing Size, and preserves ordering', async () => {
        // Two keys with positive listing Size. The first one returns an empty body at GetObject
        // time (a list/get inconsistency -- produces a listing Size > 0 but an empty payload).
        // We expect: (1) a warn log identifying the key, (2) the empty slot doesn't stall the
        // in-order drain from fix #8, and (3) the second key's body still lands in the output.
        stubPartitionAndConcatList(() => ({
            Contents: [
                { Key: 'data/src/year=2020/ghost.json', Size: 8 },
                { Key: 'data/src/year=2020/real.json', Size: 8 },
            ],
            IsTruncated: false,
        }))
        s3Mock.on(GetObjectCommand).callsFake((input) => {
            if (input.Key?.includes('ghost.json')) {
                return Promise.resolve({ Body: sdkStreamMixin(Readable.from([''])) })
            }
            return Promise.resolve({ Body: sdkStreamMixin(Readable.from(['{"real":true}'])) })
        })
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                partitionStack: ['year'],
                loggerOptions: { level: 'warn' },
            }),
            testAwsSecrets
        )
        const logger = (cat as unknown as { _logger?: { warn: (...args: unknown[]) => unknown } })._logger
        if (!logger) {
            throw new Error('expected a logger to have been constructed for this test')
        }
        const warnSpy = vi.fn()
        logger.warn = warnSpy

        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        // Both objects are counted as fetched (the raw metric is "did we call GetObject").
        expect(cat.s3ObjectsFetchedTotal).toBe(2)
        // The empty ghost is warned about with its key name and the "empty body" phrasing.
        expect(
            warnSpy.mock.calls.some((args) => {
                const msg = String(args[0])
                return (
                    msg.includes('empty body') &&
                    msg.includes('ghost.json') &&
                    msg.includes('positive listing Size')
                )
            })
        ).toBe(true)

        // Exactly one output part is written, containing only the real record.
        expect(cat.s3ObjectsPutTotal).toBe(1)
        const put = s3Mock.commandCalls(PutObjectCommand)[0].args[0].input
        const plain = gunzipSync(put.Body as Uint8Array).toString('utf8')
        expect(plain).toBe('{"real":true}\n')
    })

    it('warns and skips entries where Size is undefined but Key is present (not a hard error)', async () => {
        // A valid Key with Size undefined is the scenario that previously threw
        // "Invalid S3 Object encountered." despite nothing actually being invalid.
        stubPartitionAndConcatList(() => ({
            Contents: [
                { Key: 'data/src/year=2020/sizeless.json' }, // Size undefined -> warn & skip
                { Key: 'data/src/year=2020/good.json', Size: 8 },
            ],
            IsTruncated: false,
        }))
        s3Mock.on(GetObjectCommand).callsFake((input) => {
            // The "sizeless" entry must never be fetched.
            if (input.Key?.includes('sizeless')) {
                throw new Error('sizeless.json should have been skipped without fetching')
            }
            return Promise.resolve({ Body: sdkStreamMixin(Readable.from(['{"ok":true}'])) })
        })
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                partitionStack: ['year'],
                // Instantiate a logger so the skip warning has somewhere to land.
                loggerOptions: { level: 'warn' },
            }),
            testAwsSecrets
        )
        const logger = (cat as unknown as { _logger?: { warn: (...args: unknown[]) => unknown } })._logger
        if (!logger) {
            throw new Error('expected a logger to have been constructed for this test')
        }
        const warnSpy = vi.fn()
        logger.warn = warnSpy

        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        // Only the "good" object should have been fetched and written.
        expect(cat.s3ObjectsFetchedTotal).toBe(1)
        expect(cat.s3ObjectsPutTotal).toBe(1)
        // And the skip must have been announced on the warn channel with the offending key.
        expect(
            warnSpy.mock.calls.some((args) => {
                const msg = String(args[0])
                return (
                    msg.includes('no fetchable content') &&
                    msg.includes('sizeless.json') &&
                    msg.includes('Size=undefined')
                )
            })
        ).toBe(true)
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
        expect(cat.prefixesWithEmittedOutputTotal).toBe(1)
        expect(cat.totalPrefixesToProcess).toBe(1)

        // Second run against the same instance should zero the per-run counters before starting,
        // not double them.
        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')
        expect(cat.isDone).toBe(true)
        expect(cat.s3ObjectsPutTotal).toBe(1)
        expect(cat.s3ObjectsFetchedTotal).toBe(1)
        expect(cat.prefixesProcessedTotal).toBe(1)
        expect(cat.prefixesWithEmittedOutputTotal).toBe(1)
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
        expect(cat.prefixesWithEmittedOutputTotal).toBe(1)
        expect(cat.totalPrefixesToProcess).toBe(1)
        expect(cat.s3ObjectsPutTotal).toBe(1)
    })

    it('prefixesWithEmittedOutputTotal stays 0 when a leaf finishes concat with no PutObject (all Size=0)', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [{ Key: 'data/src/year=2020/zero.json', Size: 0 }],
            IsTruncated: false,
        }))
        const cat = new S3FileScanCat(false, scannerOptions({ partitionStack: ['year'] }), testAwsSecrets)
        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')
        expect(cat.prefixesProcessedTotal).toBe(1)
        expect(cat.prefixesWithEmittedOutputTotal).toBe(0)
        expect(cat.s3ObjectsPutTotal).toBe(0)
        expect(s3Mock.commandCalls(GetObjectCommand).length).toBe(0)
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
                limits: {
                    maxFileSizeBytes: 16,
                    // Allow GetObject for the 128-byte body; default maxSource would match maxFile and skip fetch.
                    maxSourceObjectSizeBytes: 256,
                },
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

    it('skips GetObject when listing Size exceeds maxSourceObjectSizeBytes but preserves key order', async () => {
        stubPartitionAndConcatList(() => ({
            Contents: [
                { Key: 'data/src/year=2020/huge.json', Size: 10_000_000 },
                { Key: 'data/src/year=2020/small.json', Size: 12 },
            ],
            IsTruncated: false,
        }))
        s3Mock.on(GetObjectCommand).callsFake((input) => {
            if (input.Key?.includes('huge')) {
                throw new Error('huge.json must not be fetched')
            }
            return Promise.resolve({
                Body: sdkStreamMixin(Readable.from(['{"ok":true}'])),
            })
        })
        s3Mock.on(PutObjectCommand).resolves({})

        const cat = new S3FileScanCat(
            false,
            scannerOptions({
                partitionStack: ['year'],
                limits: { maxSourceObjectSizeBytes: 1024 },
            }),
            testAwsSecrets
        )
        await cat.scanAndProcessFiles('bucket', 'data/src', 'data/dst')

        expect(s3Mock.commandCalls(GetObjectCommand).length).toBe(1)
        expect(cat.s3ObjectsFetchedTotal).toBe(1)
        const plain = gunzipSync(
            s3Mock.commandCalls(PutObjectCommand)[0].args[0].input.Body as Uint8Array
        ).toString('utf8')
        expect(plain).toBe('{"ok":true}\n')
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
