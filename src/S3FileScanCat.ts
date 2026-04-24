import { Agent as HttpAgent } from 'node:http'
import { Agent as HttpsAgent } from 'node:https'
import { promisify } from 'node:util'
import { createLogger, Logger } from 'ot-logger-deluxe'
import * as zlib from 'zlib'

// Async gzip so that compressing large buffers doesn't block the event loop; libuv
// runs the work on its thread pool instead of stalling every other S3 request in flight.
const gzipAsync = promisify(zlib.gzip)

import {
    CommonPrefix, GetObjectCommand, GetObjectCommandOutput, ListObjectsV2Command,
    ListObjectsV2CommandInput, ListObjectsV2CommandOutput, PutObjectCommand, PutObjectCommandInput,
    S3Client
} from '@aws-sdk/client-s3'
import { NodeHttpHandler } from '@smithy/node-http-handler'

import { EmptyPrefixError } from './errors/EmptyPrefixError'
import {
    AWSSecrets, ConcatState, PrefixEvalResult, PrefixParams, S3FileScanCatStats, ScannerOptions
} from './interfaces/scanner.interface'
import { formatUtcYmdParts, utcDayStartMs } from './utcDayRange'
import { waitUntil } from './waitUntil'

const MS_PER_DAY = 24 * 60 * 60 * 1000
const MAX_LIST_KEYS = 1000

/** Readable detail for `Error` messages; avoids `[object Object]` on non-Error throws. */
function describeUnknownError(value: unknown): string {
    if (value instanceof Error) {
        return value.message
    }
    try {
        return JSON.stringify(value)
    } catch {
        return String(value)
    }
}

export class S3FileScanCat {
    private _logger?: Logger
    /** S3 path separator. Immutable; constant across the lifetime of the instance. */
    private readonly _delimiter: string = '/'
    // Queues are implemented as arrays with a monotonic read-index pointer so that taking an item
    // is O(1) instead of Array.shift()'s O(n). Without this, draining thousands of leaf prefixes
    // is O(n^2). The arrays are cleared in `_resetRunState` to release references.
    private _keyParams: PrefixParams[] = []
    private _keyParamsHead: number = 0
    private _allPrefixes: string[] = []
    private _allPrefixesHead: number = 0
    private _scanPrefixForPartitionsProcessCount: number = 0
    private _s3PrefixListObjectsProcessCount: number = 0
    private _totalPrefixesToProcess: number = 0
    private _s3ObjectBodyProcessInProgress: number = 0
    private _s3ObjectPutProcessCount: number = 0
    private _prefixesProcessedTotal: number = 0
    /** Leaves whose concat pass wrote at least one output part (`_s3ObjectsPutTotal` rose for that leaf). */
    private _prefixesWithEmittedOutputTotal: number = 0
    /** Incremented after `GetObject` + `transformToString()` completes (per source key read). */
    private _s3ObjectsFetchedTotal: number = 0
    /** Incremented only after each successful `PutObject` (output part). */
    private _s3ObjectsPutTotal: number = 0
    private _isDone: boolean = false
    /** Set on first background failure so wait loops can exit and the scan rejects cleanly. */
    private _fatalScanError: Error | undefined = undefined
    /**
     * Tracks every in-flight `_scanPrefixForPartitions` promise so that when the scan aborts
     * (fatal error or normal completion) we can await them all before returning. Without this
     * the caller would see a rejection while background scans were still mutating shared state.
     */
    private _inFlightPartitionScans: Set<Promise<void>> = new Set()
    /** Guards against concurrent/re-entrant calls to `scanAndProcessFiles`. */
    private _isRunning: boolean = false
    /** Once true, every future `scanAndProcessFiles` call rejects instead of issuing S3 traffic. */
    private _isClosed: boolean = false
    private _s3ObjectBodyProcessInProgressLimit: number
    /** Class-level cap on aggregate `_getAndProcessObjectBody` workers; see ScannerLimits. */
    private _s3ObjectBodyProcessTotalLimit: number
    /** Class-level cap on concurrent `PutObject` calls; `Infinity` by default. */
    private _s3ObjectPutProcessLimit: number
    /** Max concurrent `concatFilesAtPrefix` calls in phase 2 (default 1 = sequential). */
    private _concatFilesAtPrefixProcessLimit: number
    /** Number of `concatFilesAtPrefix` calls currently in flight; see phase-2 worker spawn. */
    private _concatFilesAtPrefixProcessCount: number = 0
    /**
     * Tracks every in-flight `concatFilesAtPrefix` promise so phase-2 can drain them on fatal
     * error or normal completion before returning (mirrors `_inFlightPartitionScans`).
     */
    private _inFlightConcatCalls: Set<Promise<void>> = new Set()
    private _maxFileSizeBytes: number
    /** Upper bound on listing `Size` before we will issue GetObject (defaults to maxFileSizeBytes). */
    private _maxSourceObjectSizeBytes: number
    /** Passed to every `waitUntil` in this class so stuck predicates cannot spin forever. */
    private _waitUntilTimeoutMs: number
    private _scannerOptions: ScannerOptions
    private _awsAccess: AWSSecrets
    private _s3Client: S3Client
    /**
     * Held on the instance so `close()` can tear down the keep-alive socket pools. Without this,
     * Node's event loop would stay alive after a caller's work is done because the agents keep
     * their pooled TCP connections open.
     */
    private _httpsAgent: HttpsAgent
    private _httpAgent: HttpAgent
    /**
     * @param useAccelerateEndpoint Whether to use the S3 Transfer Acceleration endpoint.
     * @param scannerOptions Scanner configuration (partition stack, bounds, limits, logging).
     * @param awsAccess AWS credentials used to construct the S3 client.
     * @param region AWS region for the S3 client. Defaults to `'us-east-1'` for backward
     *   compatibility with earlier versions that hard-coded the region. Callers working with
     *   buckets in other regions should pass the bucket's actual region to avoid cross-region
     *   redirect latency (or outright failures on strict endpoints).
     */
    constructor(
        useAccelerateEndpoint: boolean,
        scannerOptions: ScannerOptions,
        awsAccess: AWSSecrets,
        region: string = 'us-east-1'
    ) {
        if (scannerOptions.loggerOptions !== undefined) {
            this._logger = createLogger({
                ...scannerOptions.loggerOptions,
                name: 'file-scan-cat',
            })
        }
        this._s3ObjectBodyProcessInProgressLimit =
            scannerOptions.limits?.s3ObjectBodyProcessInProgressLimit !== undefined
                ? scannerOptions.limits?.s3ObjectBodyProcessInProgressLimit
                : 100
        // New concurrency limits (phase-2 parallelism). Validation happens here so that bad
        // config fails fast at construction instead of deep inside the scan. Fractional values
        // are rejected because every gate compares `count < limit` and then bumps the counter
        // by exactly 1 — a limit of 1.5 would allow 2 concurrent workers (since 1 < 1.5) and
        // silently violate the cap.
        this._concatFilesAtPrefixProcessLimit =
            scannerOptions.limits?.concatFilesAtPrefixProcessLimit !== undefined
                ? scannerOptions.limits.concatFilesAtPrefixProcessLimit
                : 1
        if (
            !Number.isInteger(this._concatFilesAtPrefixProcessLimit) ||
            this._concatFilesAtPrefixProcessLimit < 1
        ) {
            throw new RangeError(
                `scannerOptions.limits.concatFilesAtPrefixProcessLimit must be an integer >= 1; received ${scannerOptions.limits?.concatFilesAtPrefixProcessLimit}`
            )
        }
        // Default to Infinity (opt-in) so existing callers — including any who drive
        // `concatFilesAtPrefix` directly across calls — see no change. When explicitly set,
        // the value must be a positive integer (same rationale as above) and >= the per-leaf
        // limit so a single leaf cannot deadlock on itself (the inner `waitUntil` gates on
        // both caps).
        this._s3ObjectBodyProcessTotalLimit =
            scannerOptions.limits?.s3ObjectBodyProcessTotalLimit !== undefined
                ? scannerOptions.limits.s3ObjectBodyProcessTotalLimit
                : Number.POSITIVE_INFINITY
        if (scannerOptions.limits?.s3ObjectBodyProcessTotalLimit !== undefined) {
            const v = this._s3ObjectBodyProcessTotalLimit
            if (v !== Number.POSITIVE_INFINITY && (!Number.isInteger(v) || v < 1)) {
                throw new RangeError(
                    `scannerOptions.limits.s3ObjectBodyProcessTotalLimit must be a positive integer or Infinity; received ${v}`
                )
            }
            if (v < this._s3ObjectBodyProcessInProgressLimit) {
                throw new RangeError(
                    `scannerOptions.limits.s3ObjectBodyProcessTotalLimit (${v}) must be >= s3ObjectBodyProcessInProgressLimit (${this._s3ObjectBodyProcessInProgressLimit}).`
                )
            }
        }
        this._s3ObjectPutProcessLimit =
            scannerOptions.limits?.s3ObjectPutProcessLimit !== undefined
                ? scannerOptions.limits.s3ObjectPutProcessLimit
                : Number.POSITIVE_INFINITY
        if (scannerOptions.limits?.s3ObjectPutProcessLimit !== undefined) {
            const v = this._s3ObjectPutProcessLimit
            if (v !== Number.POSITIVE_INFINITY && (!Number.isInteger(v) || v < 1)) {
                throw new RangeError(
                    `scannerOptions.limits.s3ObjectPutProcessLimit must be a positive integer or Infinity; received ${v}`
                )
            }
        }
        this._maxFileSizeBytes =
            scannerOptions.limits?.maxFileSizeBytes !== undefined
                ? scannerOptions.limits?.maxFileSizeBytes
                : 128 * 1024 * 1024
        this._maxSourceObjectSizeBytes =
            scannerOptions.limits?.maxSourceObjectSizeBytes !== undefined
                ? scannerOptions.limits.maxSourceObjectSizeBytes
                : this._maxFileSizeBytes
        this._waitUntilTimeoutMs =
            scannerOptions.limits?.waitUntilTimeoutMs !== undefined
                ? scannerOptions.limits.waitUntilTimeoutMs
                : 30 * 60 * 1000
        const requestSocketTimeoutMs =
            scannerOptions.limits?.requestSocketTimeoutMs !== undefined
                ? scannerOptions.limits.requestSocketTimeoutMs
                : 120_000
        this._scannerOptions = scannerOptions
        this._awsAccess = awsAccess
        // Build the keep-alive agents as named references so `close()` can destroy them later.
        // keepAlive is a default from AWS SDK; we preserve it for performance.
        this._httpsAgent = new HttpsAgent({
            maxSockets: 500,
            keepAlive: true,
            keepAliveMsecs: 1000,
        })
        this._httpAgent = new HttpAgent({
            maxSockets: 500,
            keepAlive: true,
            keepAliveMsecs: 1000,
        })
        this._s3Client = new S3Client({
            region,
            credentials: {
                accessKeyId: this._awsAccess.accessKeyId,
                secretAccessKey: this._awsAccess.secretAccessKey,
            },
            useAccelerateEndpoint,
            // Use a custom request handler so that we can adjust the HTTPS Agent and
            // socket behavior.
            requestHandler: new NodeHttpHandler({
                httpsAgent: this._httpsAgent,
                httpAgent: this._httpAgent,
                socketTimeout: requestSocketTimeoutMs,
            }),
        })
    }

    /**
     * Release the underlying S3 client and keep-alive socket pools. Once called, further
     * `scanAndProcessFiles` invocations reject.
     *
     * Node will not exit cleanly while this instance's HTTP(S) agents are holding pooled
     * sockets (because `keepAlive: true` pins them until their idle timer fires). Always call
     * `close()` when the caller is done using the scanner.
     *
     * Throws if `scanAndProcessFiles` is still running so the client is not torn down under
     * active work — await the scan promise first.
     *
     * Idempotent: safe to call multiple times after a successful close. Does not wait for
     * in-flight S3 calls other than the scan guard above.
     */
    close(): void {
        if (this._isClosed) {
            return
        }
        if (this._isRunning) {
            throw new Error(
                'S3FileScanCat.close() cannot run while scanAndProcessFiles is in progress; await the scan promise first.'
            )
        }
        this._isClosed = true
        try {
            this._s3Client.destroy()
        } catch (err) {
            void this._logger?.warn(
                `S3Client.destroy threw while closing S3FileScanCat: ${err instanceof Error ? err.message : String(err)}`
            )
        }
        try {
            this._httpsAgent.destroy()
        } catch (err) {
            void this._logger?.warn(
                `https agent destroy threw while closing S3FileScanCat: ${err instanceof Error ? err.message : String(err)}`
            )
        }
        try {
            this._httpAgent.destroy()
        } catch (err) {
            void this._logger?.warn(
                `http agent destroy threw while closing S3FileScanCat: ${err instanceof Error ? err.message : String(err)}`
            )
        }
    }

    /** Returns true if `close()` has been called on this instance. */
    get isClosed(): boolean {
        return this._isClosed
    }

    /**
     * @deprecated Misnamed. The value is the number of partition-scan workers in flight, not a
     * list-objects count. Prefer `partitionScansInProgress` or `getStats().partitionScansInProgress`.
     * This getter will be removed in a future major version.
     */
    get s3BuildPrefixListObjectsProcessCount(): number {
        return this._scanPrefixForPartitionsProcessCount
    }

    /** Partition-scan workers (`_scanPrefixForPartitions`) currently in flight during phase 1. */
    get partitionScansInProgress(): number {
        return this._scanPrefixForPartitionsProcessCount
    }

    /**
     * @deprecated Misnamed. The value is the aggregate number of list-objects requests currently
     * in flight across all active `concatFilesAtPrefix` calls — 0 or 1 when
     * `concatFilesAtPrefixProcessLimit === 1`, and up to that limit when multiple leaves run in
     * parallel. Prefer `concatListObjectsInProgress` or `getStats().concatListObjectsInProgress`.
     * This getter will be removed in a future major version.
     */
    get s3PrefixListObjectsProcessCount(): number {
        return this._s3PrefixListObjectsProcessCount
    }

    /**
     * Sum of list-objects requests currently in flight across all active `concatFilesAtPrefix`
     * calls. Effectively 0 or 1 while `concatFilesAtPrefixProcessLimit === 1`; can grow up to
     * that limit once cross-leaf parallelism is enabled.
     */
    get concatListObjectsInProgress(): number {
        return this._s3PrefixListObjectsProcessCount
    }

    /** Leaf `concatFilesAtPrefix` calls currently in flight (phase-2 workers). */
    get concatFilesInProgress(): number {
        return this._concatFilesAtPrefixProcessCount
    }

    get totalPrefixesToProcess(): number {
        return this._totalPrefixesToProcess
    }

    get s3ObjectBodyProcessInProgress(): number {
        return this._s3ObjectBodyProcessInProgress
    }

    get s3ObjectPutProcessCount(): number {
        return this._s3ObjectPutProcessCount
    }

    get prefixCountForProcessing(): number {
        return this._allPrefixesRemaining()
    }

    get prefixesProcessedTotal(): number {
        return this._prefixesProcessedTotal
    }

    /** Leaves that produced at least one `PutObject` this run (never greater than `prefixesProcessedTotal`). */
    get prefixesWithEmittedOutputTotal(): number {
        return this._prefixesWithEmittedOutputTotal
    }

    /**
     * Source objects fully read via GetObject this run (after `transformToString()`). See
     * `S3FileScanCatStats.s3ObjectsFetchedTotal` for nuance vs. listing skips and empty bodies.
     */
    get s3ObjectsFetchedTotal(): number {
        return this._s3ObjectsFetchedTotal
    }

    /** Successful output `PutObject` count this run. See `S3FileScanCatStats.s3ObjectsPutTotal`. */
    get s3ObjectsPutTotal(): number {
        return this._s3ObjectsPutTotal
    }

    get isDone(): boolean {
        return this._isDone
    }

    /**
     * Returns a frozen, atomic snapshot of all scanner metrics. Prefer this over calling the
     * individual getters when sampling progress in a monitoring loop: all fields are read in
     * the same tick, so internal counters cannot drift between reads.
     *
     * `s3ObjectsFetchedTotal` counts completed GetObject reads; `s3ObjectsPutTotal` counts
     * successful PutObject uploads — they are not paired 1:1 (see `S3FileScanCatStats`).
     */
    getStats(): Readonly<S3FileScanCatStats> {
        return Object.freeze({
            partitionScansInProgress: this._scanPrefixForPartitionsProcessCount,
            concatFilesInProgress: this._concatFilesAtPrefixProcessCount,
            concatListObjectsInProgress: this._s3PrefixListObjectsProcessCount,
            totalPrefixesToProcess: this._totalPrefixesToProcess,
            prefixesProcessedTotal: this._prefixesProcessedTotal,
            prefixesWithEmittedOutputTotal: this._prefixesWithEmittedOutputTotal,
            prefixesRemainingInQueue: this._allPrefixesRemaining(),
            s3ObjectBodyWorkersInProgress: this._s3ObjectBodyProcessInProgress,
            s3ObjectPutWorkersInProgress: this._s3ObjectPutProcessCount,
            s3ObjectsFetchedTotal: this._s3ObjectsFetchedTotal,
            s3ObjectsPutTotal: this._s3ObjectsPutTotal,
            isRunning: this._isRunning,
            isDone: this._isDone,
            isClosed: this._isClosed,
        })
    }

    async scanAndProcessFiles(bucket: string, srcPrefix: string, destPrefix: string): Promise<void> {
        if (this._isClosed) {
            throw new Error(
                'S3FileScanCat.scanAndProcessFiles called after close(); construct a new S3FileScanCat.'
            )
        }
        if (this._isRunning) {
            throw new Error(
                'scanAndProcessFiles is already running on this instance; await the existing call or construct a new S3FileScanCat.'
            )
        }
        // Reset per-run state so that prior runs (successful or failed) cannot leak
        // queue contents, counters, `_isDone`, or `_fatalScanError` into this invocation.
        this._resetRunState()
        this._isRunning = true
        try {
            return await this._runScanAndProcessFiles(bucket, srcPrefix, destPrefix)
        } finally {
            this._isRunning = false
        }
    }

    private async _runScanAndProcessFiles(
        bucket: string,
        srcPrefix: string,
        destPrefix: string
    ): Promise<void> {
        void this._logger?.trace(
            `BEGIN scanConcatenateCopy srcPrefix=${srcPrefix}:destPrefix=${destPrefix}:partitionStack=${this._scannerOptions.partitionStack}`
        )
        const destPath = destPrefix
        /** Canonical scan root for path math and `PrefixParams.prefix` (always ends with `/`). */
        const normalizedSrcRoot = srcPrefix.endsWith('/') ? srcPrefix : `${srcPrefix}/`
        if (this._scannerOptions.bounds?.startDate && this._scannerOptions.bounds.endDate) {
            const stack = this._scannerOptions.partitionStack
            const expectedDateParts = ['year', 'month', 'day'] as const
            if (
                stack.length < expectedDateParts.length ||
                stack[0] !== expectedDateParts[0] ||
                stack[1] !== expectedDateParts[1] ||
                stack[2] !== expectedDateParts[2]
            ) {
                throw new Error(
                    `When bounds are set, partitionStack must begin with ['year','month','day']; received ${JSON.stringify(stack)}`
                )
            }
            const remainingStack = stack.slice(expectedDateParts.length)
            const startMs = utcDayStartMs(this._scannerOptions.bounds.startDate)
            const endMs = utcDayStartMs(this._scannerOptions.bounds.endDate)
            if (startMs > endMs) {
                throw new RangeError(
                    `bounds.startDate must be on or before bounds.endDate (UTC); received startDate=${this._scannerOptions.bounds.startDate} endDate=${this._scannerOptions.bounds.endDate}`
                )
            }
            for (let t = startMs; t <= endMs; t += MS_PER_DAY) {
                const { year, month, day } = formatUtcYmdParts(t)
                const datePrefix = `${normalizedSrcRoot}year=${year}/month=${month}/day=${day}`
                if (remainingStack.length === 0) {
                    // Day is already the leaf partition; skip further scanning and
                    // feed the prefix straight into the concat phase.
                    this._allPrefixes.push(datePrefix)
                } else {
                    this._keyParams.push({
                        bucket,
                        // Same normalized root used to build `curPrefix` / date paths (see #3).
                        prefix: normalizedSrcRoot,
                        curPrefix: datePrefix,
                        // Fresh copy per iteration because _scanPrefixForPartitions mutates via shift().
                        partitionStack: remainingStack.slice(),
                    })
                }
            }
        } else {
            this._keyParams.push({
                bucket,
                prefix: normalizedSrcRoot,
                curPrefix: normalizedSrcRoot,
                // Clone the stack because _scanPrefixForPartitions mutates it via shift();
                // without this, re-running on the same instance would see an empty stack.
                partitionStack: this._scannerOptions.partitionStack.slice(),
            })
        }

        let waits = 0
        try {
            while (this._keyParamsRemaining() > 0 || this._scanPrefixForPartitionsProcessCount > 0) {
                await waitUntil(
                    () => {
                        if (this._fatalScanError) {
                            return true
                        }
                        if (
                            this._scanPrefixForPartitionsProcessCount === 0 ||
                            (this._keyParamsRemaining() > 0 &&
                                this._scanPrefixForPartitionsProcessCount <
                                    this._scannerOptions.limits.scanPrefixForPartitionsProcessLimit)
                        ) {
                            return true
                        } else if (waits++ % 20 === 0) {
                            void this._logger?.debug(
                                `Waiting until a partition-scan worker slot is free (srcPrefix=${srcPrefix}, inFlight=${this._scanPrefixForPartitionsProcessCount}, limit=${this._scannerOptions.limits.scanPrefixForPartitionsProcessLimit})`
                            )
                        }
                    },
                    { timeout: this._waitUntilTimeoutMs }
                )
                if (this._fatalScanError) {
                    break
                }
                const keyParam = this._takeKeyParam()
                if (keyParam) {
                    // Track the promise so a fatal error can't leave background scans
                    // mutating shared state after scanAndProcessFiles has returned.
                    const scanPromise: Promise<void> = this._scanPrefixForPartitions(keyParam)
                        .catch((err: unknown) => {
                            void this._logger?.error(
                                `Partition scan failed for prefix=${keyParam.curPrefix}: ${err instanceof Error ? err.message : String(err)}`
                            )
                            this._setFatalScanError(err)
                        })
                        .finally(() => {
                            this._inFlightPartitionScans.delete(scanPromise)
                        })
                    this._inFlightPartitionScans.add(scanPromise)
                }
            }
        } finally {
            // Always drain in-flight partition scans before proceeding (or rethrowing).
            // Promise.allSettled never rejects, so this is safe in a finally block.
            if (this._inFlightPartitionScans.size > 0) {
                await Promise.allSettled([...this._inFlightPartitionScans])
            }
        }
        if (this._fatalScanError) {
            throw this._fatalScanError
        }
        this._totalPrefixesToProcess = this._allPrefixesRemaining()
        if (this._totalPrefixesToProcess <= 0) {
            throw new EmptyPrefixError()
        }
        // Phase 2 fan-out. Mirrors phase 1 in intent but uses a Promise.race over the in-flight
        // set instead of `waitUntil` polling — with `concatFilesAtPrefixProcessLimit === 1` and
        // thousands of small leaves, a 10ms poll loop would add multi-second overhead for work
        // that is otherwise essentially instant.
        try {
            while (
                (this._allPrefixesRemaining() > 0 || this._inFlightConcatCalls.size > 0) &&
                !this._fatalScanError
            ) {
                // Spawn as many leaves as the cap allows before blocking.
                while (
                    !this._fatalScanError &&
                    this._allPrefixesRemaining() > 0 &&
                    this._concatFilesAtPrefixProcessCount < this._concatFilesAtPrefixProcessLimit
                ) {
                    const prefix = this._takeAllPrefix()
                    if (prefix === undefined) {
                        break
                    }
                    this._concatFilesAtPrefixProcessCount++
                    const concatPromise: Promise<void> = this.concatFilesAtPrefix(
                        bucket,
                        prefix,
                        srcPrefix,
                        destPath
                    )
                        .catch((err: unknown) => {
                            void this._logger?.error(
                                `Leaf concat failed for prefix=${prefix}: ${err instanceof Error ? err.message : String(err)}`
                            )
                            this._setFatalScanError(err)
                        })
                        .finally(() => {
                            this._concatFilesAtPrefixProcessCount--
                            this._inFlightConcatCalls.delete(concatPromise)
                        })
                    this._inFlightConcatCalls.add(concatPromise)
                }
                // Wait for at least one leaf to finish before considering the next batch. Each
                // promise is `.catch`-wrapped above, so Promise.race never rejects here.
                if (this._inFlightConcatCalls.size > 0) {
                    await Promise.race([...this._inFlightConcatCalls])
                }
            }
        } finally {
            // Drain in-flight leaves regardless of outcome so callers never see
            // `scanAndProcessFiles` resolve/reject while background concat is still running.
            if (this._inFlightConcatCalls.size > 0) {
                await Promise.allSettled([...this._inFlightConcatCalls])
            }
        }
        if (this._fatalScanError) {
            throw this._fatalScanError
        }

        void this._logger?.info(`Finished concatenating and compressing JSON S3 Objects for prefix=${srcPrefix}`)
        await this._logger?.flush()
        this._isDone = true
    }

    async concatFilesAtPrefix(bucket: string, prefix: string, srcPrefix: string, destPrefix: string): Promise<void> {
        void this._logger?.trace(`BEGIN _concatFilesAtPrefix prefix=${prefix}`)
        const putsBeforeLeaf = this._s3ObjectsPutTotal
        let waits = 0
        const concatState: ConcatState = {
            buffer: undefined,
            continuationToken: undefined,
            fileNumber: 0,
            _bufferMutex: Promise.resolve(),
            nextSeqToAppend: 0,
            pendingBodies: new Map(),
            bodiesInFlight: 0,
            inFlightFlushes: new Set(),
        }

        // Monotonic listing-order sequence number assigned to each fetchable key.
        // Keys skipped due to zero size do not consume a seq slot (there is nothing
        // to append for them), which keeps the contiguous-drain logic simple.
        let nextSeq = 0

        // Track every spawned body-processor so we can guarantee they have all
        // settled before this method returns. Without this, a fatal error would
        // leave workers mutating `concatState` / counters after we've thrown.
        const inFlightBodies = new Set<Promise<void>>()

        const listObjRequest: ListObjectsV2CommandInput = {
            Bucket: bucket,
            Prefix: prefix.endsWith('/') ? prefix : `${prefix}/`,
            MaxKeys: MAX_LIST_KEYS,
        }
        try {
            do {
                if (this._fatalScanError) {
                    break
                }
                if (concatState.continuationToken) {
                    listObjRequest.ContinuationToken = concatState.continuationToken
                } else {
                    listObjRequest.ContinuationToken = undefined
                }
                // Wrap the whole per-iteration unit of work (list call + Contents processing)
                // in try/finally so `_s3PrefixListObjectsProcessCount` is always decremented,
                // even if ListObjectsV2 rejects or the Contents loop throws.
                this._s3PrefixListObjectsProcessCount++
                try {
                    let response: ListObjectsV2CommandOutput
                    try {
                        void this._logger?.trace(
                            `STATUS _concatFilesAtPrefix prefix=${prefix} - this._s3PrefixListObjectsProcessCount: ${this._s3PrefixListObjectsProcessCount}`
                        )
                        response = await this._s3Client.send(new ListObjectsV2Command(listObjRequest))
                    } catch (e) {
                        void this._logger?.error(
                            `Failed to list objects for ${listObjRequest.Prefix}, Error: ${e}`
                        )
                        throw e
                    }
                    // Always update the continuation token based on IsTruncated /
                    // NextContinuationToken, regardless of whether this page returned Contents.
                    // S3 can (rarely) return a truncated page with no Contents, and nesting the
                    // token update under `if (response.Contents)` would cause the loop to exit
                    // early and silently drop the rest of the key set.
                    if (response.IsTruncated === true && response.NextContinuationToken !== undefined) {
                        concatState.continuationToken = response.NextContinuationToken
                    } else {
                        concatState.continuationToken = undefined
                    }
                    if (response.Contents) {
                        for (const s3Object of response.Contents) {
                            // Classify the listing entry. Key is the only hard requirement:
                            // without it there's nothing to fetch and no way to build the
                            // destination path, which is an unambiguous data-integrity problem.
                            // Size can legitimately be absent in some SDK/mock responses, so we
                            // treat undefined the same as 0 (warn + skip) instead of erroring.
                            if (s3Object.Key === undefined || s3Object.Key === '') {
                                throw new Error(
                                    `Invalid S3 Object encountered (missing Key): ${JSON.stringify(s3Object)}`
                                )
                            }
                            if (s3Object.Size === undefined || s3Object.Size === 0) {
                                void this._logger?.warn(
                                    `S3 Object had no fetchable content (Size=${s3Object.Size}) key=${s3Object.Key}; skipping.`
                                )
                                continue
                            }
                            if (s3Object.Size > this._maxSourceObjectSizeBytes) {
                                void this._logger?.warn(
                                    `Skipping GetObject: listing Size (${s3Object.Size}) exceeds maxSourceObjectSizeBytes (${this._maxSourceObjectSizeBytes}) for key=${s3Object.Key}; preserving ordering with an empty slot.`
                                )
                                await waitUntil(
                                    () =>
                                        this._tryAcquireBodySlot(concatState, prefix, () => waits++),
                                    { timeout: this._waitUntilTimeoutMs }
                                )
                                if (this._fatalScanError) {
                                    break
                                }
                                // The slot was reserved synchronously inside the acquire
                                // predicate so a sibling leaf cannot observe the gate as open
                                // before we have bumped the counters.
                                const seq = nextSeq++
                                const objectKey = s3Object.Key
                                const skipPromise: Promise<void> = this._skipListedObjectOversizedForConcat(
                                    bucket,
                                    concatState,
                                    prefix,
                                    srcPrefix,
                                    destPrefix,
                                    seq,
                                    objectKey
                                )
                                    .catch((err: unknown) => {
                                        void this._logger?.error(
                                            `skipListedObjectOversized failed for key=${objectKey}: ${err instanceof Error ? err.message : String(err)}`
                                        )
                                        this._setFatalScanError(err)
                                    })
                                    .finally(() => {
                                        inFlightBodies.delete(skipPromise)
                                    })
                                inFlightBodies.add(skipPromise)
                                continue
                            }
                            await waitUntil(
                                () => this._tryAcquireBodySlot(concatState, prefix, () => waits++),
                                { timeout: this._waitUntilTimeoutMs }
                            )
                            if (this._fatalScanError) {
                                break
                            }
                            // Assign the listing-order sequence BEFORE spawning so that even
                            // if workers finish out of order the append path can reassemble
                            // them in S3's lexicographic/chronological order.
                            const seq = nextSeq++
                            // We purposely do not await on this - the waitUntil above limits the number of these
                            // calls that are in progress at any given moment. The promise is tracked in
                            // `inFlightBodies` so the finally block can await it before returning.
                            const objectKey = s3Object.Key
                            const bodyPromise: Promise<void> = this._getAndProcessObjectBody(
                                bucket,
                                objectKey,
                                concatState,
                                prefix,
                                srcPrefix,
                                destPrefix,
                                seq
                            )
                                .catch((err: unknown) => {
                                    void this._logger?.error(
                                        `getObject/process failed for key=${objectKey}: ${err instanceof Error ? err.message : String(err)}`
                                    )
                                    this._setFatalScanError(err)
                                })
                                .finally(() => {
                                    inFlightBodies.delete(bodyPromise)
                                })
                            inFlightBodies.add(bodyPromise)
                        }
                    } else {
                        void this._logger?.warn(`Prefix had no contents: ${prefix}`)
                    }
                } finally {
                    this._s3PrefixListObjectsProcessCount--
                    void this._logger?.trace(
                        `STATUS _concatFilesAtPrefix prefix=${prefix} - _s3PrefixListObjectsProcessCount: ${this._s3PrefixListObjectsProcessCount}`
                    )
                }
            } while (concatState.continuationToken && !this._fatalScanError)
        } finally {
            // Always drain in-flight body processors before returning so they cannot
            // mutate `concatState` or the shared counters after this method resolves.
            if (inFlightBodies.size > 0) {
                await Promise.allSettled([...inFlightBodies])
            }
            // Bodies have settled but may have spawned detached flushes (gzip + PutObject)
            // that are still running. Drain those too, otherwise callers could see puts
            // land in S3 after concatFilesAtPrefix has already resolved.
            if (concatState.inFlightFlushes.size > 0) {
                await Promise.allSettled([...concatState.inFlightFlushes])
            }
        }
        if (this._fatalScanError) {
            throw this._fatalScanError
        }
        // Now do a final flush (under the same mutex as in-flight appends).
        // `inFlightBodies` has been drained above, so this mutex acquisition is
        // only defensive against a future refactor that reintroduces concurrency.
        const unlockFinal = await this._acquireConcatBufferLock(concatState)
        try {
            if (!concatState.continuationToken && concatState.buffer && concatState.buffer.length > 0) {
                await this._flushBuffer(
                    bucket,
                    concatState.buffer,
                    prefix,
                    srcPrefix,
                    destPrefix,
                    concatState.fileNumber++
                )
                concatState.buffer = undefined
            }
        } finally {
            unlockFinal()
        }
        void this._logger?.trace(
            `END _concatFilesAtPrefix prefix=${prefix} - _s3PrefixListObjectsProcessCount: ${this._s3PrefixListObjectsProcessCount}`
        )
        this._prefixesProcessedTotal++
        if (this._s3ObjectsPutTotal > putsBeforeLeaf) {
            this._prefixesWithEmittedOutputTotal++
        }
    }

    /**
     * Place `objectBodyStr` at `seq` and drain contiguous pending slots into the concat buffer.
     * Used after GetObject and for oversized listing skips (empty string preserves ordering).
     */
    private async _depositListingOrderBodyFromString(
        bucket: string,
        concatState: ConcatState,
        prefix: string,
        srcPrefix: string,
        destPrefix: string,
        seq: number,
        objectBodyStr: string,
        s3Key: string
    ): Promise<void> {
        const unlockBuf = await this._acquireConcatBufferLock(concatState)
        try {
            concatState.pendingBodies.set(seq, objectBodyStr)
            while (concatState.pendingBodies.has(concatState.nextSeqToAppend)) {
                const rawBody = concatState.pendingBodies.get(concatState.nextSeqToAppend) as string
                concatState.pendingBodies.delete(concatState.nextSeqToAppend)
                concatState.nextSeqToAppend++
                if (rawBody.length === 0) {
                    continue
                }
                const bodyWithNl = rawBody.endsWith('\n') ? rawBody : `${rawBody}\n`
                const addSize = bodyWithNl.length
                if (addSize > this._maxFileSizeBytes) {
                    void this._logger?.warn(
                        `Source object body (${rawBody.length} bytes, key=${s3Key}) exceeds maxFileSizeBytes (${this._maxFileSizeBytes}); emitting as a single oversized output part.`
                    )
                }
                if (
                    concatState.buffer &&
                    concatState.buffer.length + addSize > this._maxFileSizeBytes
                ) {
                    const bufferToFlush = concatState.buffer
                    const flushFileNumber = concatState.fileNumber++
                    concatState.buffer = undefined
                    const flushPromise: Promise<void> = this._flushBuffer(
                        bucket,
                        bufferToFlush,
                        prefix,
                        srcPrefix,
                        destPrefix,
                        flushFileNumber
                    )
                        .catch((err: unknown) => {
                            void this._logger?.error(
                                `Flush failed for prefix=${prefix} fileNumber=${flushFileNumber}: ${err instanceof Error ? err.message : String(err)}`
                            )
                            this._setFatalScanError(err)
                        })
                        .finally(() => {
                            concatState.inFlightFlushes.delete(flushPromise)
                        })
                    concatState.inFlightFlushes.add(flushPromise)
                }
                concatState.buffer = (concatState.buffer ?? '') + bodyWithNl
            }
        } finally {
            unlockBuf()
        }
    }

    /**
     * Same bodiesInFlight / class rollup as `_getAndProcessObjectBody` without issuing GetObject
     * when listing Size exceeds `maxSourceObjectSizeBytes` (memory bound).
     */
    private async _skipListedObjectOversizedForConcat(
        bucket: string,
        concatState: ConcatState,
        prefix: string,
        srcPrefix: string,
        destPrefix: string,
        seq: number,
        s3Key: string
    ): Promise<void> {
        // Counters were reserved synchronously by `_tryAcquireBodySlot` before spawn;
        // we only need to release them here.
        try {
            if (this._fatalScanError) {
                return
            }
            await this._depositListingOrderBodyFromString(
                bucket,
                concatState,
                prefix,
                srcPrefix,
                destPrefix,
                seq,
                '',
                s3Key
            )
        } finally {
            concatState.bodiesInFlight--
            this._s3ObjectBodyProcessInProgress--
        }
    }

    async _getAndProcessObjectBody(
        bucket: string,
        s3Key: string,
        concatState: ConcatState,
        prefix: string,
        srcPrefix: string,
        destPrefix: string,
        seq: number
    ): Promise<void> {
        // Counters were reserved synchronously by `_tryAcquireBodySlot` before this spawn, so
        // the gate + bump happen in a single sync step (no microtask race across parallel
        // leaves). We only release here. The per-call counter remains authoritative for
        // in-leaf ordering; the class-level one is retained as an aggregate rollup for the
        // public `s3ObjectBodyProcessInProgress` getter.
        void this._logger?.trace(
            `BEGIN _getObjectBody objectKey=${s3Key} - _s3ObjectBodyProcessCount: ${this._s3ObjectBodyProcessInProgress}`
        )
        try {
            // If a sibling worker already failed, abandon this one before issuing S3 calls
            // or mutating `concatState`. concatFilesAtPrefix awaits all spawned bodies in its
            // finally block, so this early return is safe.
            if (this._fatalScanError) {
                return
            }
            let response: undefined | GetObjectCommandOutput
            let retryCount = 0
            // With exponential backoff the last retry waits ~13 minutes.  This gives the system a chance to recover after a failure but also allows us to move on if we can resolve this in a reasonable amount of time
            const MAX_RETRIES = 14
            let lastError: unknown
            while (!response) {
                if (this._fatalScanError) {
                    return
                }
                try {
                    response = await this._s3Client.send(
                        new GetObjectCommand({
                            Bucket: bucket,
                            Key: s3Key,
                        })
                    )
                    lastError = undefined
                } catch (error) {
                    void this._logger?.error(`[ERROR] S3 getObjectFailed: ${error}`)
                    lastError = error
                    // Bail out immediately on client errors that cannot be resolved by
                    // retrying (403 AccessDenied, 404 NoSuchKey, etc.). Without this the
                    // full ~13-minute backoff ladder fires for errors the caller already
                    // knows will keep failing.
                    if (!this._isRetryableError(error)) {
                        void this._logger?.warn(
                            `Non-retryable S3 error for key=${s3Key}; skipping retries. Error: ${error instanceof Error ? error.message : String(error)}`
                        )
                        break
                    }
                    if (retryCount < MAX_RETRIES) {
                        void this._logger?.warn(
                            `STATUS RETRY! _getAndProcessObjectBody s3Key=${s3Key} - _s3ObjectBodyProcessCount: ${this._s3ObjectBodyProcessInProgress} - retryCount: ${retryCount}`
                        )
                        await this._sleep(this._getWaitTimeForRetry(retryCount++))
                    } else {
                        break
                    }
                }
            }
            if (this._fatalScanError) {
                return
            }
            void this._logger?.trace(
                `STATUS _getObjectBody s3Key=${s3Key} - _s3ObjectBodyProcessCount: ${this._s3ObjectBodyProcessInProgress}`
            )
            if (response === undefined) {
                const detail =
                    lastError === undefined ? 'no error details captured' : describeUnknownError(lastError)
                throw new Error(`Unexpected S3 GetObject failure for key=${s3Key}: ${detail}`)
            } else if (!response.Body) {
                throw new Error(`Missing response data for object ${s3Key}`)
            }
            // When S3 includes ContentLength, honor maxSourceObjectSizeBytes again before buffering
            // the whole body — listing Size can be stale or wrong (#3).
            const contentLength = response.ContentLength
            if (typeof contentLength === 'number' && contentLength > this._maxSourceObjectSizeBytes) {
                void this._logger?.warn(
                    `Skipping GetObject body read: ContentLength (${contentLength}) exceeds maxSourceObjectSizeBytes (${this._maxSourceObjectSizeBytes}) for key=${s3Key} (listing Size may be stale). Preserving ordering with an empty slot.`
                )
                await this._depositListingOrderBodyFromString(
                    bucket,
                    concatState,
                    prefix,
                    srcPrefix,
                    destPrefix,
                    seq,
                    '',
                    s3Key
                )
                return
            }
            const objectBodyStr = await response.Body.transformToString()
            this._s3ObjectsFetchedTotal++

            if (objectBodyStr.length === 0) {
                // The listing phase already filters out `Size === 0` entries (fix #19), so an
                // empty body here means GetObject contradicted what ListObjectsV2 reported --
                // usually a sign that the object was mutated or deleted between list and get,
                // or that a producer wrote a size but no body. Surface it instead of silently
                // dropping the record; the slot below will still advance via pendingBodies
                // so downstream ordering is preserved.
                void this._logger?.warn(
                    `S3 GetObject returned an empty body for key=${s3Key} despite a positive listing Size; skipping append.`
                )
            }

            await this._depositListingOrderBodyFromString(
                bucket,
                concatState,
                prefix,
                srcPrefix,
                destPrefix,
                seq,
                objectBodyStr,
                s3Key
            )
        } finally {
            concatState.bodiesInFlight--
            this._s3ObjectBodyProcessInProgress--
            void this._logger?.trace(
                `END _getObjectBody objectKey=${s3Key} - _s3ObjectBodyProcessCount: ${this._s3ObjectBodyProcessInProgress} - _s3ObjectsFetchedTotal: ${this._s3ObjectsFetchedTotal}`
            )
        }
    }

    _setFatalScanError(err: unknown): void {
        if (this._fatalScanError !== undefined) {
            return
        }
        this._fatalScanError = err instanceof Error ? err : new Error(String(err))
    }

    /**
     * Reset every per-run field so the instance can be safely reused across
     * multiple `scanAndProcessFiles` invocations (including after a failure).
     * Fields tied to configuration (S3 client, logger, limits) are preserved.
     */
    private _resetRunState(): void {
        this._keyParams = []
        this._keyParamsHead = 0
        this._allPrefixes = []
        this._allPrefixesHead = 0
        this._scanPrefixForPartitionsProcessCount = 0
        this._s3PrefixListObjectsProcessCount = 0
        this._totalPrefixesToProcess = 0
        this._s3ObjectBodyProcessInProgress = 0
        this._s3ObjectPutProcessCount = 0
        this._prefixesProcessedTotal = 0
        this._prefixesWithEmittedOutputTotal = 0
        this._s3ObjectsFetchedTotal = 0
        this._s3ObjectsPutTotal = 0
        this._isDone = false
        this._fatalScanError = undefined
        this._inFlightPartitionScans = new Set()
        this._concatFilesAtPrefixProcessCount = 0
        this._inFlightConcatCalls = new Set()
    }

    /** Number of items still to be taken from the `_keyParams` queue. */
    private _keyParamsRemaining(): number {
        return this._keyParams.length - this._keyParamsHead
    }

    /** O(1) replacement for `this._keyParams.shift()`, using an index pointer. */
    private _takeKeyParam(): PrefixParams | undefined {
        if (this._keyParamsHead >= this._keyParams.length) {
            return undefined
        }
        const item = this._keyParams[this._keyParamsHead]
        // Release the reference so the GC can collect consumed entries; prevents the array
        // from retaining every processed PrefixParams for the duration of the run.
        // (The ts-ignore is just for the `delete` on a typed array index.)
        ;(this._keyParams as (PrefixParams | undefined)[])[this._keyParamsHead] = undefined
        this._keyParamsHead++
        return item
    }

    /** Number of items still to be taken from the `_allPrefixes` queue. */
    private _allPrefixesRemaining(): number {
        return this._allPrefixes.length - this._allPrefixesHead
    }

    /** O(1) replacement for `this._allPrefixes.shift()`, using an index pointer. */
    private _takeAllPrefix(): string | undefined {
        if (this._allPrefixesHead >= this._allPrefixes.length) {
            return undefined
        }
        const item = this._allPrefixes[this._allPrefixesHead]
        ;(this._allPrefixes as (string | undefined)[])[this._allPrefixesHead] = undefined
        this._allPrefixesHead++
        return item
    }

    /**
     * Atomic check-and-increment used as the `waitUntil` predicate for body-worker slots.
     * Runs synchronously, so the gate evaluation and the counter bump are inseparable — even
     * with multiple `concatFilesAtPrefix` calls racing, no two can both observe the gate as
     * open without the second seeing the first's increment. Returns true on fatal error so the
     * caller can abort (no counters are bumped on the fatal path).
     *
     * Releasing is done by `_getAndProcessObjectBody` / `_skipListedObjectOversizedForConcat`
     * in their `finally` blocks.
     */
    private _tryAcquireBodySlot(
        concatState: ConcatState,
        prefix: string,
        bumpWaits: () => number
    ): boolean {
        if (this._fatalScanError) {
            return true
        }
        if (
            concatState.bodiesInFlight < this._s3ObjectBodyProcessInProgressLimit &&
            this._s3ObjectBodyProcessInProgress < this._s3ObjectBodyProcessTotalLimit
        ) {
            concatState.bodiesInFlight++
            this._s3ObjectBodyProcessInProgress++
            return true
        }
        if (bumpWaits() % 20 === 0) {
            void this._logger?.trace(
                `Waiting until GetObject body workers drop below limits (leafPrefix=${prefix}, leafInFlight=${concatState.bodiesInFlight}/${this._s3ObjectBodyProcessInProgressLimit}, totalInFlight=${this._s3ObjectBodyProcessInProgress}/${this._s3ObjectBodyProcessTotalLimit})`
            )
        }
        return false
    }

    /** Serialize buffer reads/writes/flushes across concurrent `_getAndProcessObjectBody` calls. */
    private async _acquireConcatBufferLock(concatState: ConcatState): Promise<() => void> {
        if (concatState._bufferMutex === undefined) {
            concatState._bufferMutex = Promise.resolve()
        }
        const previous = concatState._bufferMutex
        let resolveUnlock!: () => void
        concatState._bufferMutex = new Promise<void>((resolve) => {
            resolveUnlock = resolve
        })
        await previous
        return () => {
            resolveUnlock()
        }
    }

    _sleep(milliseconds: number): Promise<void> {
        return new Promise((resolve) => {
            setTimeout(resolve, milliseconds)
        })
    }

    /**
     * Exponential backoff with equal jitter.
     *
     * Returns an integer delay in ms in the closed range `[cap/2, cap]` where `cap = 2^retryCount * 100`, so:
     *  - retryCount=0  -> 50-100ms   (previously 0ms, which defeated backoff entirely)
     *  - retryCount=1  -> 100-200ms
     *  - retryCount=13 -> ~6.8-13.65 min (matches the prior documented upper bound)
     *
     * Equal jitter keeps a guaranteed minimum wait (unlike full jitter, which can return 0)
     * while still spreading concurrent retries to avoid synchronized thundering-herd storms
     * against the same S3 endpoint.
     */
    _getWaitTimeForRetry(retryCount: number): number {
        const cap = Math.pow(2, retryCount) * 100
        const half = cap / 2
        return half + Math.floor(Math.random() * (half + 1))
    }

    /**
     * Returns false when the error is a client-side failure that retrying cannot fix
     * (4xx except 408 RequestTimeout and 429 TooManyRequests). When `httpStatusCode` is
     * missing (mocks, proxies, or non-AWS errors), uses Smithy `$fault`, exception `name`,
     * and conservative message heuristics so we do not burn the full backoff ladder on
     * obvious non-retries; plain `Error` with no signals stays retryable for transport glitches.
     */
    _isRetryableError(err: unknown): boolean {
        if (err == null || typeof err !== 'object') {
            return true
        }
        const e = err as {
            $metadata?: { httpStatusCode?: number }
            $fault?: string
            name?: string
            message?: string
        }
        const status = e.$metadata?.httpStatusCode
        if (typeof status === 'number') {
            if (status === 408 || status === 429) {
                return true
            }
            if (status >= 400 && status < 500) {
                return false
            }
            return true
        }

        const name = e.name ?? ''
        const message = typeof e.message === 'string' ? e.message : ''

        const nonRetryableNames = new Set([
            'AccessDenied',
            'UnauthorizedOperation',
            'NoSuchKey',
            'NoSuchBucket',
            'NoSuchVersion',
            'InvalidObjectState',
            'InvalidRequest',
            'MalformedKey',
            'KeyTooLongError',
            'InvalidRange',
        ])
        if (nonRetryableNames.has(name)) {
            return false
        }

        const retryableNames = new Set([
            'ThrottlingException',
            'TooManyRequestsException',
            'RequestTimeout',
            'RequestTimeoutException',
            'ServiceUnavailable',
            'SlowDown',
            'InternalError',
            'PriorRequestNotComplete',
        ])
        if (retryableNames.has(name)) {
            return true
        }

        if (e.$fault === 'client') {
            return false
        }
        if (e.$fault === 'server') {
            return true
        }

        if (/throttl|slow\s*down|ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|socket hang up|request timed out/i.test(message)) {
            return true
        }
        if (
            /access denied|not authorized|no such key|no such bucket|forbidden|invalid access key|signature does not match/i.test(
                message
            )
        ) {
            return false
        }

        if (!(err instanceof Error)) {
            return false
        }

        return true
    }

    async _flushBuffer(
        bucket: string,
        buffer: string,
        prefix: string,
        srcPrefix: string,
        destPrefix: string,
        fileNumber: number
    ): Promise<void> {
        void this._logger?.trace(`BEGIN _flushBuffer destination=${destPrefix}`)
        const fileName = this._buildDestinationKey(prefix, srcPrefix, destPrefix, fileNumber)
        await this._saveToS3(bucket, buffer, fileName)
        void this._logger?.trace(`END _flushBuffer destination=${destPrefix}`)
    }

    /**
     * Build the destination S3 key for a flushed part, translating a leaf `prefix` from
     * under `srcPrefix` to the corresponding location under `destPrefix`.
     *
     * Path-aware: validates that `prefix` really is a path-descendant of `srcPrefix`
     * (so e.g. `srcPrefix='data/src'` does not silently match `'data/source/...'`), and
     * normalizes trailing slashes on all three inputs. Throws with a clear error on any
     * mismatch so misconfiguration surfaces instead of writing under the wrong key.
     */
    _buildDestinationKey(
        prefix: string,
        srcPrefix: string,
        destPrefix: string,
        fileNumber: number
    ): string {
        // Strip trailing slashes so we can work with canonical path-segment comparisons.
        const src = srcPrefix.replace(/\/+$/, '')
        const dst = destPrefix.replace(/\/+$/, '')
        const leaf = prefix.replace(/\/+$/, '')

        let relative: string
        if (src === '') {
            // Empty srcPrefix: treat the whole leaf as the relative path.
            relative = leaf
        } else if (leaf === src) {
            relative = ''
        } else if (leaf.startsWith(`${src}/`)) {
            relative = leaf.slice(src.length + 1)
        } else {
            throw new Error(
                `Leaf prefix '${prefix}' is not a path-descendant of srcPrefix '${srcPrefix}'; cannot build destination key.`
            )
        }

        // Build ${dst}/[relative/]${fileNumber}.json.gz without introducing empty path segments.
        const destStem = dst === '' ? '' : `${dst}/`
        return relative.length > 0
            ? `${destStem}${relative}/${fileNumber}.json.gz`
            : `${destStem}${fileNumber}.json.gz`
    }

    async _saveToS3(bucket: string, buffer: string, key: string): Promise<void> {
        void this._logger?.trace(`BEGIN _saveToS3 key=${key} - _s3ObjectPutProcessCount: ${this._s3ObjectPutProcessCount}`)

        // Class-level PutObject cap. The `tryAcquire` predicate performs the check-and-bump
        // atomically (synchronously) so racing flush promises cannot both observe the gate as
        // open. With the default `Infinity` the predicate always acquires on the first call and
        // this is a no-op. When fatal, we short-circuit without bumping.
        let putSlotAcquired = false
        if (Number.isFinite(this._s3ObjectPutProcessLimit)) {
            let putWaits = 0
            await waitUntil(
                () => {
                    if (this._fatalScanError) {
                        return true
                    }
                    if (this._s3ObjectPutProcessCount < this._s3ObjectPutProcessLimit) {
                        this._s3ObjectPutProcessCount++
                        putSlotAcquired = true
                        return true
                    }
                    if (putWaits++ % 20 === 0) {
                        void this._logger?.trace(
                            `Waiting until PutObject workers drop below limit (inFlight=${this._s3ObjectPutProcessCount}, limit=${this._s3ObjectPutProcessLimit})`
                        )
                    }
                    return false
                },
                { timeout: this._waitUntilTimeoutMs }
            )
            if (this._fatalScanError && !putSlotAcquired) {
                // Honor the same abort semantics as other workers: skip the PUT and let the
                // outer flush tracker's allSettled drain it. No counters to revert.
                void this._logger?.trace(
                    `Abandoning _saveToS3 key=${key} due to prior fatal scan error.`
                )
                return
            }
        } else {
            // Unbounded: bump eagerly so both branches share the release path below.
            this._s3ObjectPutProcessCount++
            putSlotAcquired = true
        }

        try {
            const body = await gzipAsync(buffer)
            const object: PutObjectCommandInput = {
                Body: body,
                Bucket: bucket,
                Key: key,
            }
            try {
                await this._s3Client.send(new PutObjectCommand(object))
            } catch (e) {
                void this._logger?.error(`Failed to save object to S3: ${key}`)
                throw e
            }
            // Only bump the "successfully put" tally on the success path; the in-progress
            // counter is always decremented below.
            this._s3ObjectsPutTotal++
        } finally {
            if (putSlotAcquired) {
                this._s3ObjectPutProcessCount--
            }
            void this._logger?.trace(
                `END _saveToS3 key=${key} - _s3ObjectPutProcessCount: ${this._s3ObjectPutProcessCount} - _s3ObjectsPutTotal: ${this._s3ObjectsPutTotal}`
            )
        }
    }

    async _scanPrefixForPartitions(keyParams: PrefixParams): Promise<void> {
        this._scanPrefixForPartitionsProcessCount++
        try {
            // If a sibling scan has already failed, abandon this one without touching
            // shared state or issuing additional S3 calls.
            if (this._fatalScanError) {
                return
            }
            void this._logger?.trace(`_scanPrefixForPartitions ${keyParams.curPrefix}::${keyParams.partitionStack}`)
            const curPart = keyParams.partitionStack.shift()
            if (!curPart) {
                throw new Error('Unexpected end of partition stack!')
            }

            if (!keyParams.curPrefix.endsWith(this._delimiter)) {
                keyParams.curPrefix += this._delimiter
            }

            const listObjRequest: ListObjectsV2CommandInput = {
                Bucket: keyParams.bucket,
                Prefix: keyParams.curPrefix,
                Delimiter: this._delimiter,
            }
            let continuationToken: undefined | string
            do {
                if (this._fatalScanError) {
                    return
                }
                listObjRequest.ContinuationToken = continuationToken
                let response: ListObjectsV2CommandOutput
                try {
                    response = await this._s3Client.send(new ListObjectsV2Command(listObjRequest))
                } catch (e) {
                    void this._logger?.error(`Failed to list objects at prefix ${keyParams.curPrefix}`)
                    throw e
                }
                if (this._fatalScanError) {
                    return
                }
                // Always advance pagination from IsTruncated / NextContinuationToken, even when this
                // page has no CommonPrefixes. S3 can return truncated pages with only Contents (objects
                // directly under the prefix) or an empty page; nesting token updates under
                // `if (CommonPrefixes)` caused premature loop exit and dropped subsequent prefix pages.
                if (response.IsTruncated === true && response.NextContinuationToken !== undefined) {
                    continuationToken = response.NextContinuationToken
                } else {
                    continuationToken = undefined
                }
                if (response.CommonPrefixes && response.CommonPrefixes.length > 0) {
                    response.CommonPrefixes.forEach((commonPrefix) => {
                        const prefixEval = this._evaluatePrefix(keyParams, commonPrefix, curPart)
                        if (prefixEval.partition) {
                            this._allPrefixes.push(prefixEval.partition)
                        } else if (prefixEval.keyParams) {
                            this._keyParams.push(prefixEval.keyParams)
                        } else {
                            const lastSeg = commonPrefix.Prefix
                                ? this._lastSegmentOfListPrefix(commonPrefix.Prefix)
                                : undefined
                            if (
                                lastSeg !== undefined &&
                                !this._pathSegmentMatchesPartitionCurPart(lastSeg, curPart)
                            ) {
                                void this._logger?.warn(
                                    `Skipping CommonPrefix that does not match partition dimension '${curPart}': ${commonPrefix.Prefix}`
                                )
                            } else {
                                throw new Error(`Unexpected partition info encountered. ${JSON.stringify(keyParams)}`)
                            }
                        }
                    })
                } else if (response.Contents && response.Contents.length > 0) {
                    void this._logger?.warn(
                        `ListObjectsV2 with Delimiter returned no CommonPrefixes but ${response.Contents.length} key(s) under prefix=${keyParams.curPrefix} (curPart=${curPart}); partition scan only follows sub-prefixes, so these objects are skipped.`
                    )
                }
            } while (continuationToken)
        } finally {
            this._scanPrefixForPartitionsProcessCount--
        }
    }

    /**
     * Final path segment before the trailing delimiter in a list-style prefix
     * (e.g. `data/src/year=2020/` → `year=2020`).
     */
    _lastSegmentOfListPrefix(prefixKey: string): string | undefined {
        const parts = prefixKey.split(this._delimiter)
        if (parts.length <= 1) {
            return undefined
        }
        return parts[parts.length - 2]
    }

    /**
     * True when `segment` is this partition level's directory name for `curPart`.
     * Uses Hive-style `name=value` boundaries so `year` does not match `yearly=…`
     * (substring false positives from the old `startsWith(curPart)` check).
     */
    _pathSegmentMatchesPartitionCurPart(segment: string, curPart: string): boolean {
        const dimension = curPart.replace(/=+$/, '').trim()
        if (dimension.length === 0) {
            return false
        }
        return segment === dimension || segment.startsWith(`${dimension}=`)
    }

    _evaluatePrefix(keyParams: PrefixParams, commonPrefix: CommonPrefix, curPart: string): PrefixEvalResult {
        void this._logger?.trace(`_keysForPrefix ${keyParams.curPrefix}`)
        const result: PrefixEvalResult = {}
        if (!commonPrefix.Prefix) {
            return result
        }
        const lastPart = this._lastSegmentOfListPrefix(commonPrefix.Prefix)
        if (lastPart === undefined || !this._pathSegmentMatchesPartitionCurPart(lastPart, curPart)) {
            return result
        }
        const nextPart = `${keyParams.curPrefix}${lastPart}`
        if (keyParams.partitionStack.length > 0) {
            const partitionStackClone = Object.assign([], keyParams.partitionStack)
            result.keyParams = {
                bucket: keyParams.bucket,
                prefix: keyParams.prefix,
                curPrefix: nextPart,
                partitionStack: partitionStackClone,
            }
        } else {
            result.partition = nextPart
        }
        return result
    }
}
