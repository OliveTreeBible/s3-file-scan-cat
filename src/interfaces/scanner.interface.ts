export interface ScannerConfig {
    aws: AWSConfig
    scanner: ScannerOptions
}

export interface ScannerBounds {
    startDate: string
    endDate: string
}

export interface ScannerOptions {
    headless: boolean
    partition_stack: string[]
    max_file_size_bytes: number
    bounds?: ScannerBounds
    fetch_limits: S3FetchLimits
}

export interface S3FetchLimits {
    max_build_prefix_list: number
    max_prefix_list_objects: number
    max_object_fetch: number
    max_object_body_fetch: number
    max_object_puts: number
    min_percent_ram_free: number
}

export interface AWSConfig {
    s3: AWSS3Config
}

export interface AWSS3Config {
    bucket: string
    scanner_prefix: string
    destination_prefix: string
}

export interface PrefixParams {
    prefix: string
    partitionStack: string[]
    bounds: ScannerBounds | undefined
}

export interface ConcatState {
    continuationToken: AWS.S3.Types.Token
    buffer: string
    fileNumber: number
}

export interface PrefixEvalResult {
    keyParams?: PrefixParams
    partition?: string
}

export interface GenericMatch {
    [key: string]: string;
}

export interface MatchedDate extends GenericMatch {
    year: string;
    month: string;
    day: string;
}

export interface AWSSecrets {
    accessKeyId: string
    secretAccessKey: string
}