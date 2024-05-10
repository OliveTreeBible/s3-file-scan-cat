import { LogLevel } from 'typescript-logging'

export interface ScannerConfig {
    aws: AWSConfig
    scanner: ScannerOptions
}

export interface ScannerBounds {
    startDate: string
    endDate: string
}

export interface ScannerOptions {
    logOptions?: LoggerOptions
    partitionStack: string[]
    bounds?: ScannerBounds
    limits: ScannerLimits
}

export interface LoggerOptions {
    logLevel: LogLevel
    logGroupingPattern: string
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
    useAccelerateEndpoint: boolean
    prefix: string
    curPrefix: string
    partitionStack: string[]
    bounds: ScannerBounds | undefined
}

export interface ConcatState {
    continuationToken: undefined | string
    buffer: undefined | string
    fileNumber: number
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
