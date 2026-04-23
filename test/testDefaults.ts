import type { AWSSecrets, ScannerLimits, ScannerOptions } from '../src/interfaces/scanner.interface.js'

export const testLimits: ScannerLimits = {
    scanPrefixForPartitionsProcessLimit: 10,
    s3ObjectBodyProcessInProgressLimit: 10,
    maxFileSizeBytes: 128 * 1024 * 1024,
}

export function scannerOptions(partial: Partial<ScannerOptions> = {}): ScannerOptions {
    const { limits, ...rest } = partial
    return {
        partitionStack: partial.partitionStack ?? ['year'],
        limits: { ...testLimits, ...limits },
        ...rest,
    }
}

export const testAwsSecrets: AWSSecrets = {
    accessKeyId: 'ACCESS_KEY_TEST',
    secretAccessKey: 'SECRET_KEY_TEST',
}
