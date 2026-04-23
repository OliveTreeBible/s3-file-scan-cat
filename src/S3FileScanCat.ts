import { Agent as HttpAgent } from 'node:http'
import { Agent as HttpsAgent } from 'node:https'
import { createLogger, Logger } from 'ot-logger-deluxe'
import * as zlib from 'zlib'

import {
    CommonPrefix, GetObjectCommand, GetObjectCommandOutput, ListObjectsV2Command,
    ListObjectsV2CommandInput, ListObjectsV2CommandOutput, PutObjectCommand, PutObjectCommandInput,
    S3Client
} from '@aws-sdk/client-s3'
import { NodeHttpHandler } from '@smithy/node-http-handler'

import { EmptyPrefixError } from './errors/EmptyPrefixError'
import {
    AWSSecrets, ConcatState, PrefixEvalResult, PrefixParams, ScannerOptions
} from './interfaces/scanner.interface'
import { formatUtcYmdParts, utcDayStartMs } from './utcDayRange'
import { waitUntil } from './waitUntil'

const INFINITE_TIMEOUT = 2147483647 // Largest practical timeout (matches prior behavior)
const MS_PER_DAY = 24 * 60 * 60 * 1000
const MAX_LIST_KEYS = 1000

export class S3FileScanCat {
    private _logger?: Logger
    private _delimiter: string = '/'
    private _keyParams: PrefixParams[] = []
    private _allPrefixes: string[] = []
    private _scanPrefixForPartitionsProcessCount: number = 0
    private _s3PrefixListObjectsProcessCount: number = 0
    private _totalPrefixesToProcess: number = 0
    private _s3ObjectBodyProcessInProgress: number = 0
    private _s3ObjectPutProcessCount: number = 0
    private _prefixesProcessedTotal: number = 0
    private _s3ObjectsFetchedTotal: number = 0
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
    private _s3ObjectBodyProcessInProgressLimit: number
    private _maxFileSizeBytes: number
    private _scannerOptions: ScannerOptions
    private _awsAccess: AWSSecrets
    private _s3Client: S3Client
    constructor(useAccelerateEndpoint: boolean, scannerOptions: ScannerOptions, awsAccess: AWSSecrets) {
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
        this._maxFileSizeBytes =
            scannerOptions.limits?.maxFileSizeBytes !== undefined
                ? scannerOptions.limits?.maxFileSizeBytes
                : 128 * 1024 * 1024
        this._scannerOptions = scannerOptions
        this._awsAccess = awsAccess
        this._s3Client = new S3Client({
            region: 'us-east-1',
            credentials: {
                accessKeyId: this._awsAccess.accessKeyId,
                secretAccessKey: this._awsAccess.secretAccessKey,
            },
            useAccelerateEndpoint,
            // Use a custom request handler so that we can adjust the HTTPS Agent and
            // socket behavior.
            requestHandler: new NodeHttpHandler({
                httpsAgent: new HttpsAgent({
                    maxSockets: 500,

                    // keepAlive is a default from AWS SDK. We want to preserve this for
                    // performance reasons.
                    keepAlive: true,
                    keepAliveMsecs: 1000,
                }),
                httpAgent: new HttpAgent({
                    maxSockets: 500,

                    // keepAlive is a default from AWS SDK. We want to preserve this for
                    // performance reasons.
                    keepAlive: true,
                    keepAliveMsecs: 1000,
                }),
                socketTimeout: 5000,
            }),
        })
    }

    get s3BuildPrefixListObjectsProcessCount(): number {
        return this._scanPrefixForPartitionsProcessCount
    }

    get s3PrefixListObjectsProcessCount(): number {
        return this._s3PrefixListObjectsProcessCount
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
        return this._allPrefixes.length
    }

    get prefixesProcessedTotal(): number {
        return this._prefixesProcessedTotal
    }

    get s3ObjectsFetchedTotal(): number {
        return this._s3ObjectsFetchedTotal
    }

    get s3ObjectsPutTotal(): number {
        return this._s3ObjectsPutTotal
    }

    get isDone(): boolean {
        return this._isDone
    }

    async scanAndProcessFiles(bucket: string, srcPrefix: string, destPrefix: string): Promise<void> {
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
            const normalizedSrc = srcPrefix.endsWith('/') ? srcPrefix : `${srcPrefix}/`
            const startMs = utcDayStartMs(this._scannerOptions.bounds.startDate)
            const endMs = utcDayStartMs(this._scannerOptions.bounds.endDate)
            for (let t = startMs; t <= endMs; t += MS_PER_DAY) {
                const { year, month, day } = formatUtcYmdParts(t)
                const datePrefix = `${normalizedSrc}year=${year}/month=${month}/day=${day}`
                if (remainingStack.length === 0) {
                    // Day is already the leaf partition; skip further scanning and
                    // feed the prefix straight into the concat phase.
                    this._allPrefixes.push(datePrefix)
                } else {
                    this._keyParams.push({
                        bucket,
                        prefix: srcPrefix,
                        curPrefix: datePrefix,
                        // Fresh copy per iteration because _scanPrefixForPartitions mutates via shift().
                        partitionStack: remainingStack.slice(),
                        bounds: this._scannerOptions.bounds,
                    })
                }
            }
        } else {
            this._keyParams.push({
                bucket,
                prefix: srcPrefix,
                curPrefix: srcPrefix /* This changes as we traverse down the path, srcPrefix is where we start */,
                // Clone the stack because _scanPrefixForPartitions mutates it via shift();
                // without this, re-running on the same instance would see an empty stack.
                partitionStack: this._scannerOptions.partitionStack.slice(),
                bounds: this._scannerOptions.bounds,
            })
        }

        let waits = 0
        try {
            while (this._keyParams.length > 0 || this._scanPrefixForPartitionsProcessCount > 0) {
                await waitUntil(() => {
                    if (this._fatalScanError) {
                        return true
                    }
                    if (
                        this._scanPrefixForPartitionsProcessCount === 0 ||
                        (this._keyParams.length > 0 &&
                            this._scanPrefixForPartitionsProcessCount <
                                this._scannerOptions.limits.scanPrefixForPartitionsProcessLimit)
                    ) {
                        return true
                    } else if (waits++ % 20 === 0) {
                        void this._logger?.debug(`Waiting on ListObjects to complete prefix=${srcPrefix} `)
                    }
                })
                if (this._fatalScanError) {
                    break
                }
                const keyParam = this._keyParams.shift()
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
        this._totalPrefixesToProcess = this._allPrefixes.length
        if (this._allPrefixes.length > 0) {
            while (this._allPrefixes.length > 0) {
                const prefix = this._allPrefixes.shift()
                if (prefix) {
                    await this.concatFilesAtPrefix(bucket, prefix, srcPrefix, destPath)
                }
            }
        } else {
            throw new EmptyPrefixError()
        }

        //  Wait until all processes are completed.
        await waitUntil(
            () => this._totalPrefixesToProcess === this._prefixesProcessedTotal,
            INFINITE_TIMEOUT
        )
        void this._logger?.info(`Finished concatenating and compressing JSON S3 Objects for prefix=${srcPrefix}`)
        await this._logger?.flush()
        this._isDone = true
    }

    async concatFilesAtPrefix(bucket: string, prefix: string, srcPrefix: string, destPrefix: string): Promise<void> {
        void this._logger?.trace(`BEGIN _concatFilesAtPrefix prefix=${prefix}`)
        let waits = 0
        const concatState: ConcatState = {
            buffer: undefined,
            continuationToken: undefined,
            fileNumber: 0,
            _bufferMutex: Promise.resolve(),
        }

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
                this._s3PrefixListObjectsProcessCount++
                let response: ListObjectsV2CommandOutput
                try {
                    void this._logger?.trace(
                        `STATUS _concatFilesAtPrefix prefix=${prefix} - this._s3PrefixListObjectsProcessCount: ${this._s3PrefixListObjectsProcessCount}`
                    )
                    response = await this._s3Client.send(new ListObjectsV2Command(listObjRequest))
                } catch (e) {
                    void this._logger?.error(`Failed to list objects for ${listObjRequest.Prefix}, Error: ${e}`)
                    throw e
                }
                if (response.Contents) {
                    if (response.IsTruncated === true && response.NextContinuationToken !== undefined) {
                        concatState.continuationToken = response.NextContinuationToken
                    } else {
                        concatState.continuationToken = undefined
                    }
                    for (const s3Object of response.Contents) {
                        if (s3Object.Size && s3Object.Key) {
                            await waitUntil(() => {
                                if (this._fatalScanError) {
                                    return true
                                }
                                if (this._s3ObjectBodyProcessInProgress < this._s3ObjectBodyProcessInProgressLimit) {
                                    return true
                                } else if (waits++ % 20 === 0) {
                                    void this._logger?.trace(`Waiting on ListObjects to complete prefix=${srcPrefix} `)
                                }
                            })
                            if (this._fatalScanError) {
                                break
                            }
                            // We purposely do not await on this - the waitUntil above limits the number of these
                            // calls that are in progress at any given moment. The promise is tracked in
                            // `inFlightBodies` so the finally block can await it before returning.
                            const bodyPromise: Promise<void> = this._getAndProcessObjectBody(
                                bucket,
                                s3Object.Key,
                                concatState,
                                prefix,
                                srcPrefix,
                                destPrefix
                            )
                                .catch((err: unknown) => {
                                    void this._logger?.error(
                                        `getObject/process failed for key=${s3Object.Key}: ${err instanceof Error ? err.message : String(err)}`
                                    )
                                    this._setFatalScanError(err)
                                })
                                .finally(() => {
                                    inFlightBodies.delete(bodyPromise)
                                })
                            inFlightBodies.add(bodyPromise)
                        } else if (s3Object.Size === 0) {
                            void this._logger?.warn(`S3 Object had zero size ${JSON.stringify(s3Object.Key)}`)
                        } else {
                            throw new Error('Invalid S3 Object encountered.')
                        }
                    }
                } else {
                    void this._logger?.warn(`Prefix had no contents: ${prefix}`)
                }
                this._s3PrefixListObjectsProcessCount--
                void this._logger?.trace(
                    `STATUS _concatFilesAtPrefix prefix=${prefix} - _s3PrefixListObjectsProcessCount: ${this._s3PrefixListObjectsProcessCount}`
                )
            } while (concatState.continuationToken && !this._fatalScanError)
        } finally {
            // Always drain in-flight body processors before returning so they cannot
            // mutate `concatState` or the shared counters after this method resolves.
            if (inFlightBodies.size > 0) {
                await Promise.allSettled([...inFlightBodies])
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
    }

    async _getAndProcessObjectBody(
        bucket: string,
        s3Key: string,
        concatState: ConcatState,
        prefix: string,
        srcPrefix: string,
        destPrefix: string
    ): Promise<void> {
        this._s3ObjectBodyProcessInProgress++
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
                throw new Error(
                    `[ERROR]: Unexpected S3 getObject error encountered ${s3Key}:${lastError ? lastError : ''}`
                )
            } else if (!response.Body) {
                throw new Error(`[ERROR]: Missing response data for object ${s3Key}`)
            }
            const objectBodyStr = await response.Body.transformToString()
            this._s3ObjectsFetchedTotal++

            if (objectBodyStr.length > 0) {
                const unlockBuf = await this._acquireConcatBufferLock(concatState)
                try {
                    if (
                        concatState.buffer &&
                        concatState.buffer.length + objectBodyStr.length > this._maxFileSizeBytes
                    ) {
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
                    if (concatState.buffer) {
                        concatState.buffer += '\n' + objectBodyStr
                    } else {
                        concatState.buffer = objectBodyStr
                    }
                } finally {
                    unlockBuf()
                }
            }
        } finally {
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
        this._allPrefixes = []
        this._scanPrefixForPartitionsProcessCount = 0
        this._s3PrefixListObjectsProcessCount = 0
        this._totalPrefixesToProcess = 0
        this._s3ObjectBodyProcessInProgress = 0
        this._s3ObjectPutProcessCount = 0
        this._prefixesProcessedTotal = 0
        this._s3ObjectsFetchedTotal = 0
        this._s3ObjectsPutTotal = 0
        this._isDone = false
        this._fatalScanError = undefined
        this._inFlightPartitionScans = new Set()
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

    // Provides an exponential backoff for wait times when s3 commands fail.
    _getWaitTimeForRetry(retryCount: number) {
        if (retryCount === 0) return 0
        return Math.pow(2, retryCount) * 100
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
        const fileName = `${prefix.replace(srcPrefix, destPrefix)}/${fileNumber}.json.gz`
        await this._saveToS3(bucket, buffer, fileName)
        void this._logger?.trace(`END _flushBuffer destination=${destPrefix}`)
    }

    async _saveToS3(bucket: string, buffer: string, key: string): Promise<void> {
        void this._logger?.trace(`BEGIN _saveToS3 key=${key} - _s3ObjectPutProcessCount: ${this._s3ObjectPutProcessCount}`)

        this._s3ObjectPutProcessCount++

        const body = zlib.gzipSync(buffer)
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
        this._s3ObjectPutProcessCount--
        this._s3ObjectsPutTotal++
        void this._logger?.trace(
            `END _saveToS3 key=${key} - _s3ObjectPutProcessCount: ${this._s3ObjectPutProcessCount} - _s3ObjectsPutTotal: ${this._s3ObjectsPutTotal}`
        )
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
                if (response.CommonPrefixes && response.CommonPrefixes.length > 0) {
                    if (response.IsTruncated && response.NextContinuationToken) {
                        continuationToken = response.NextContinuationToken
                    } else {
                        continuationToken = undefined
                    }
                    response.CommonPrefixes.forEach((commonPrefix) => {
                        const prefixEval = this._evaluatePrefix(keyParams, commonPrefix, curPart)
                        if (prefixEval.partition) {
                            this._allPrefixes.push(prefixEval.partition)
                        } else if (prefixEval.keyParams) {
                            this._keyParams.push(prefixEval.keyParams)
                        } else {
                            throw new Error(`Unexpected partition info encountered. ${JSON.stringify(keyParams)}`)
                        }
                    })
                } else {
                    throw new Error(`Unexpected empty response from S3. ${JSON.stringify(keyParams)}`)
                }
            } while (continuationToken)
        } finally {
            this._scanPrefixForPartitionsProcessCount--
        }
    }

    _evaluatePrefix(keyParams: PrefixParams, commonPrefix: CommonPrefix, curPart: string): PrefixEvalResult {
        void this._logger?.trace(`_keysForPrefix ${keyParams.curPrefix}`)
        const result: PrefixEvalResult = {}
        if (commonPrefix.Prefix) {
            const parts = commonPrefix.Prefix.split(this._delimiter)
            if (parts.length > 1) {
                const lastPart = parts[parts.length - 2]
                if (lastPart.startsWith(curPart)) {
                    const nextPart = `${keyParams.curPrefix}${lastPart}`
                    if (keyParams.partitionStack.length > 0) {
                        const partitionStackClone = Object.assign([], keyParams.partitionStack)
                        result.keyParams = {
                            bucket: keyParams.bucket,
                            prefix: keyParams.prefix,
                            curPrefix: nextPart,
                            partitionStack: partitionStackClone,
                            bounds: keyParams.bounds,
                        }
                    } else {
                        result.partition = nextPart
                    }
                }
            }
        }
        return result
    }
}
