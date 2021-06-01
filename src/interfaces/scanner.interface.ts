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
    maxBuildPrefixList: number
    prefixListObjectsLimit: number
    objectFetchBatchSize: number
    objectBodyFetchLimit: number
    objectBodyPutLimit: number
    maxFileSizeBytes: number
}

export interface AWSConfig {
    s3: AWSS3Config
}

export interface AWSS3Config {
    bucket: string
    scannerPrefix: string
    destinationPrefix: string
}

export interface PrefixParams {
    bucket: string
    prefix: string
    partitionStack: string[]
    bounds: ScannerBounds | undefined
}

export interface ConcatState {
    continuationToken: undefined | AWS.S3.Types.Token
    buffer: string
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
