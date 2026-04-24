import type { LoggerOptions } from 'ot-logger-deluxe'

/** Options passed to ot-logger-deluxe; `name` is always set to `file-scan-cat` by the scanner. */
export type ScannerLoggerOptions = Omit<LoggerOptions, 'name'>

export interface ScannerConfig {
    aws: AWSConfig
    scanner: ScannerOptions
}

export interface ScannerBounds {
    /** Inclusive UTC calendar day (`YYYY-MM-DD`). Must be ≤ `endDate`. */
    startDate: string
    /** Inclusive UTC calendar day (`YYYY-MM-DD`). Must be ≥ `startDate`. */
    endDate: string
}

export interface ScannerOptions {
    loggerOptions?: ScannerLoggerOptions
    partitionStack: string[]
    bounds?: ScannerBounds
    limits: ScannerLimits
}

export interface ScannerLimits {
    scanPrefixForPartitionsProcessLimit: number
    s3ObjectBodyProcessInProgressLimit: number
    maxFileSizeBytes: number
}

export interface AWSConfig {
    s3: AWSS3Config
}

export interface AWSS3Config {
    bucket: string
    useAccelerateEndpoint: boolean
    scannerPrefix: string
    destinationPrefix: string
}

export interface PrefixParams {
    bucket: string
    /** Scan root, normalized with a trailing `/` (same convention as `curPrefix` path building). */
    prefix: string
    curPrefix: string
    partitionStack: string[]
}

export interface ConcatState {
    continuationToken: undefined | string
    buffer: undefined | string
    fileNumber: number
    /** Tail of a promise chain; serializes buffer append/flush for concurrent object workers. */
    _bufferMutex?: Promise<void>
    /**
     * Monotonic sequence number of the next key to append. Guarantees that output bodies are
     * concatenated in the order S3 returned them (lexicographic / chronological), independent of
     * the order in which the concurrent workers actually finish fetching.
     */
    nextSeqToAppend: number
    /**
     * Bodies that have already been fetched but whose listing-order predecessor hasn't landed yet.
     * Drained contiguously from `nextSeqToAppend` whenever a worker enters the buffer critical section.
     */
    pendingBodies: Map<number, string>
    /**
     * Number of `_getAndProcessObjectBody` workers currently running for THIS concatFilesAtPrefix
     * invocation. Used for per-call backpressure against `s3ObjectBodyProcessInProgressLimit`, so
     * that parallel `concatFilesAtPrefix` calls (if ever introduced) do not steal each other's
     * concurrency budget or wait on each other's in-flight bodies. The class-level
     * `_s3ObjectBodyProcessInProgress` counter is retained as a rollup for observability only.
     */
    bodiesInFlight: number
    /**
     * Detached flush promises spawned by workers outside the buffer mutex. Holding the mutex
     * across `gzip` + `PutObject` previously blocked every other worker for the entire flush
     * round trip. Workers now snapshot the buffer under the mutex, release it, and let the
     * flush run asynchronously; `concatFilesAtPrefix` awaits this set via `Promise.allSettled`
     * before returning so callers never see output still in flight after the scan resolves.
     */
    inFlightFlushes: Set<Promise<void>>
}

export interface PrefixEvalResult {
    keyParams?: PrefixParams
    partition?: string
}

export interface GenericMatch {
    [key: string]: string
}

export interface MatchedDate extends GenericMatch {
    year: string
    month: string
    day: string
}

export interface AWSSecrets {
    accessKeyId: string
    secretAccessKey: string
}

/**
 * Snapshot of scanner runtime metrics, returned by `S3FileScanCat.getStats()`.
 *
 * All counters are scoped to the most recent `scanAndProcessFiles` invocation unless noted
 * otherwise. The snapshot is atomic (all fields read in the same tick) and frozen.
 */
export interface S3FileScanCatStats {
    /** Partition-scan workers (`_scanPrefixForPartitions`) currently in flight during phase 1. */
    partitionScansInProgress: number
    /**
     * Whether a list-objects request is currently in flight inside `concatFilesAtPrefix`.
     * Because concatFilesAtPrefix currently runs one leaf at a time this is effectively 0 or 1;
     * the field is a number so that a future parallelization does not change its shape.
     */
    concatListObjectsInProgress: number
    /** Leaf prefixes enqueued for the concat phase when the current run entered phase 2. */
    totalPrefixesToProcess: number
    /** Leaf prefixes processed so far in the current run. */
    prefixesProcessedTotal: number
    /** Leaf prefixes still queued, waiting to enter `concatFilesAtPrefix`. */
    prefixesRemainingInQueue: number
    /** GetObject body workers currently in flight; aggregate rollup across all concatFilesAtPrefix calls. */
    s3ObjectBodyWorkersInProgress: number
    /** PutObject calls currently in flight. */
    s3ObjectPutWorkersInProgress: number
    /** Total S3 objects successfully fetched in the current run. */
    s3ObjectsFetchedTotal: number
    /** Total S3 objects successfully put (output parts written) in the current run. */
    s3ObjectsPutTotal: number
    /** True while `scanAndProcessFiles` is executing on this instance. */
    isRunning: boolean
    /** True when the most recent `scanAndProcessFiles` completed successfully. */
    isDone: boolean
    /** True when `close()` has been called. */
    isClosed: boolean
}
