import { ScannerBounds } from './appConfig.interface'

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

export interface GenericMatch {
    [key: string]: string
}

export interface MatchedDate extends GenericMatch {
    year: string
    month: string
    day: string
}

export interface PrefixEvalResult {
    keyParams?: PrefixParams
    partition?: string
}
