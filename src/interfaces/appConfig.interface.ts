export interface AppConfig {
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
