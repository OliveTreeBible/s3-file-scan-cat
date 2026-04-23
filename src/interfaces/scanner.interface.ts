import type { LoggerOptions } from 'ot-logger-deluxe'

/** Options passed to ot-logger-deluxe; `name` is always set to `file-scan-cat` by the scanner. */
export type ScannerLoggerOptions = Omit<LoggerOptions, 'name'>

export interface ScannerConfig {
    aws: AWSConfig
    scanner: ScannerOptions
}

export interface ScannerBounds {
    startDate: string
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
    prefix: string
    curPrefix: string
    partitionStack: string[]
    bounds: ScannerBounds | undefined
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
