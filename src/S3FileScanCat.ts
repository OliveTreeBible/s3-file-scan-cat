import * as waitUntil from 'async-wait-until'
import * as AWS from 'aws-sdk'
import { ListObjectsV2Request } from 'aws-sdk/clients/s3'
import { error } from 'console'
import * as moment from 'moment'
import { Logger, LogLevel } from 'typescript-logging'
import * as zlib from 'zlib'

import { EmptyPrefixError } from './errors/EmptyPrefixError'
import {
    AWSSecrets, ConcatState, MatchedDate, PrefixEvalResult, PrefixParams, ScannerOptions
} from './interfaces/scanner.interface'
import { createLogger } from './utils/logger'

const INFINITE_TIMEOUT = 2147483647 // This is actually 24 days but it is also the largest timeout allowed

type NullableAWSListObjectsResponse = undefined | AWS.Response<AWS.S3.ListObjectsV2Output, AWS.AWSError>
type NullableAWSObjectList = undefined | AWS.S3.ObjectList
type NullableAWSListObjectsOutput = undefined | AWS.S3.ListObjectsV2Output
type NullableMoment = undefined | moment.Moment
type NullableString = undefined | string

export class S3FileScanCat {
    private _log?: Logger
    private _delimeter: string
    private _keyParams: PrefixParams[]
    private _allPrefixes: string[]
    private _s3BuildPrefixListObjectsProcessCount: number
    private _s3PrefixListObjectsProcessCount: number
    private _totalPrefixesToProcess: number
    private _s3ObjectBodyProcessCount: number
    private _s3ObjectPutProcessCount: number
    private _prefixesProcessedTotal: number
    private _s3ObjectsFetchedTotal: number
    private _s3ObjectsPutTotal: number
    private _isDone: boolean
    private _objectFetchBatchSize: number
    private _prefixListObjectsLimit: number
    private _objectBodyFetchLimit: number
    private _objectBodyPutLimit: number
    private _maxFileSizeBytes: number
    private _scannerOptions: ScannerOptions
    constructor(scannerOptions: ScannerOptions, awsAccess: AWSSecrets) {
        if (scannerOptions.logOptions)
            this._log = createLogger(
                scannerOptions.logOptions.logGroupingPattern,
                scannerOptions.logOptions.logLevel,
                'app.s3-file-scan-cat'
            )
        this._delimeter = '/'
        this._keyParams = []
        this._allPrefixes = []
        this._s3BuildPrefixListObjectsProcessCount = 0
        this._s3PrefixListObjectsProcessCount = 0
        this._totalPrefixesToProcess = 0
        this._s3ObjectBodyProcessCount = 0
        this._s3ObjectPutProcessCount = 0
        this._prefixesProcessedTotal = 0
        this._s3ObjectsFetchedTotal = 0
        this._s3ObjectsPutTotal = 0
        this._isDone = false
        this._objectFetchBatchSize = scannerOptions.limits?.objectFetchBatchSize !== undefined ? scannerOptions.limits?.objectFetchBatchSize :  100
        this._prefixListObjectsLimit = scannerOptions.limits?.prefixListObjectsLimit !== undefined ? scannerOptions.limits?.prefixListObjectsLimit : 100
        this._objectBodyFetchLimit = scannerOptions.limits?.objectBodyFetchLimit !== undefined ? scannerOptions.limits?.objectBodyFetchLimit : 100
        this._objectBodyPutLimit = scannerOptions.limits?.objectBodyPutLimit !== undefined ? scannerOptions.limits?.objectBodyPutLimit : 100
        this._maxFileSizeBytes = scannerOptions.limits?.maxFileSizeBytes !== undefined ? scannerOptions.limits?.maxFileSizeBytes : 128 * 1024 * 1024
        this._scannerOptions = scannerOptions
        AWS.config.update({
            region: 'us-east-1',
            accessKeyId: awsAccess.accessKeyId,
            secretAccessKey: awsAccess.secretAccessKey,
        })
        AWS.config.apiVersions = {
            s3: '2006-03-01',
        }
    }

    get s3BuildPrefixListObjectsProcessCount(): number {
        return this._s3BuildPrefixListObjectsProcessCount
    }

    get s3PrefixListObjectsProcessCount(): number {
        return this._s3PrefixListObjectsProcessCount
    }

    get totalPrefixesToProcess(): number {
        return this._totalPrefixesToProcess
    }

    get s3ObjectBodyProcessCount(): number {
        return this._s3ObjectBodyProcessCount
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
        this.log(
            LogLevel.Trace,
            `BEGIN scanConcatenateCopy srcPrefix=${srcPrefix}:destPrefix=${destPrefix}:partitionStack=${this._scannerOptions.partitionStack}`
        )
        const destPath = destPrefix

        this._keyParams.push({
            bucket,
            prefix: srcPrefix,
            curPrefix: srcPrefix /* This changes as we traverse down the path, srcPrefix is where we start */,
            partitionStack: this._scannerOptions.partitionStack,
            bounds: this._scannerOptions.bounds,
        })
        while (this._keyParams.length > 0 || this._s3BuildPrefixListObjectsProcessCount > 0) {
            await waitUntil(
                () => this._s3BuildPrefixListObjectsProcessCount < this._scannerOptions.limits.maxBuildPrefixList,
                INFINITE_TIMEOUT
            )
            const keyParam = this._keyParams.shift()
            if (keyParam) this._buildFullPrefixList(keyParam)
        }
        this._prefixesProcessedTotal = 0
        this._totalPrefixesToProcess = this._allPrefixes.length
        if (this._allPrefixes.length > 0) {
            while (this._allPrefixes.length > 0) {
                const prefix = this._allPrefixes.shift()
                if (prefix) {
                    this.concatFilesAtPrefix(bucket, prefix, srcPrefix, destPath)
                }
            }
        } else {
            throw new EmptyPrefixError()
        }

        //  Wait until all processes are completed.
        await waitUntil(() => this._totalPrefixesToProcess === this._prefixesProcessedTotal, INFINITE_TIMEOUT)
        this.log(LogLevel.Info, `Finished concatenating and compressing JSON S3 Objects for prefix=${srcPrefix}`)
        this._isDone = true
    }

    async concatFilesAtPrefix(bucket: string, prefix: string, srcPrefix: string, destPrefix: string): Promise<void> {
        this.log(LogLevel.Info, `BEGIN _concatFilesAtPrefix prefix=${prefix}`)
        const concatState: ConcatState = {
            buffer: undefined,
            continuationToken: undefined,
            fileNumber: 0,
        }
        const listObjRequest: ListObjectsV2Request = {
            Bucket: bucket,
            Prefix: prefix.endsWith('/') ? prefix : `${prefix}/`,
            MaxKeys: this._objectFetchBatchSize,
        }
        do {
            if (concatState.continuationToken) {
                listObjRequest.ContinuationToken = concatState.continuationToken
            } else {
                listObjRequest.ContinuationToken = undefined
            }
            await waitUntil(
                () => this._s3PrefixListObjectsProcessCount < this._prefixListObjectsLimit,
                INFINITE_TIMEOUT
            )
            this._s3PrefixListObjectsProcessCount++
            let response: AWS.Response<AWS.S3.ListObjectsV2Output, AWS.AWSError>
            try {
                const s3 = new AWS.S3()
                response = (await s3.listObjectsV2(listObjRequest).promise()).$response
            } catch (e) {
                this.log(LogLevel.Error, `Failed to list objects for ${listObjRequest.Prefix}, Error: ${e}`)
                throw e
            }

            if (response.error) {
                throw error
            } else if (response.data) {
                let data: NullableAWSListObjectsOutput = response.data
                if (data.Contents) {
                    let contents: NullableAWSObjectList = data.Contents
                    if (data.IsTruncated === true && data.NextContinuationToken !== undefined) {
                        concatState.continuationToken = data.NextContinuationToken
                    } else {
                        concatState.continuationToken = undefined
                    }
                    data = undefined
                    let objectBodyPromises: undefined | Promise<AWS.S3.Body>[] = []
                    for (const s3Object of contents) {
                        if (s3Object.Size && s3Object.Key) {
                            objectBodyPromises.push(this._getObjectBody(bucket, s3Object.Key))
                        } else if (s3Object.Size === 0) {
                            this.log(LogLevel.Warn, `S3 Object had zero size ${JSON.stringify(s3Object.Key)}`)
                        } else {
                            throw new Error('Invalid S3 Object encountered.')
                        }
                    }
                    contents = undefined

                    const objectBodies = await Promise.all(objectBodyPromises)
                    objectBodyPromises = undefined

                    objectBodies.forEach((objectBody) => {
                        let objectBodyStr: string
                        if (typeof objectBody !== 'string') {
                            objectBodyStr = objectBody.toString()
                        } else {
                            objectBodyStr = objectBody
                        }
                        if (objectBodyStr.length > 0) {
                            if (
                                concatState.buffer &&
                                concatState.buffer.length + objectBodyStr.length >
                                    this._maxFileSizeBytes
                            ) {
                                this._flushBuffer(
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
                        }
                    })
                    if (!concatState.continuationToken && concatState.buffer && concatState.buffer.length > 0) {
                        this._flushBuffer(
                            bucket,
                            concatState.buffer,
                            prefix,
                            srcPrefix,
                            destPrefix,
                            concatState.fileNumber++
                        )
                        concatState.buffer = undefined
                    }
                }
            } else {
                throw new Error(`Unexpected Error: List S3 Objects request had missing response.`)
            }
            this._s3PrefixListObjectsProcessCount--
        } while (concatState.continuationToken)
        this.log(LogLevel.Info, `END _concatFilesAtPrefix prefix=${prefix}`)
        this._prefixesProcessedTotal++
    }

    async _getObjectBody(bucket: string, s3Key: AWS.S3.ObjectKey): Promise<AWS.S3.Body> {
        this.log(LogLevel.Trace, `BEGIN _getObjectBody objectKey=${s3Key}`)
        let body: AWS.S3.Body = ''
        const getObjectRequest: AWS.S3.Types.GetObjectRequest = {
            Bucket: bucket,
            Key: s3Key,
        }
        await waitUntil(() => this._s3ObjectBodyProcessCount < this._objectBodyFetchLimit, INFINITE_TIMEOUT)
        this._s3ObjectBodyProcessCount++
        let response
        let retryCount = 0
        // With exponential backoff the last retry waits ~13 minutes.  This gives the system a chance to recover after a failure but also allows us to move on if we can resolve this in a reasonable amount of time
        const MAX_RETRIES = 14
        let lastError: unknown
        const s3 = new AWS.S3()
        while (!response) {
            try {
                response = (await s3.getObject(getObjectRequest).promise()).$response
                lastError = undefined
            } catch (error) {
                console.log(`[ERROR] S3 getObjectFailed: ${error}`)
                lastError = error
                if (retryCount < MAX_RETRIES) {
                    await this._sleep(this._getWaitTimeForRetry(retryCount++))
                } else {
                    break
                }
            }
        }

        this._s3ObjectBodyProcessCount--
        if (response === undefined) {
            console.log(`[ERROR]: Unexpected S3 getObject error encountered ${s3Key}:${lastError ? lastError : ''}`)
        } else if (response.error) {
            console.log(`[ERROR]: S3 getObject error encountered ${response.error}`)
        } else if (!response.data) {
            console.log(`[ERROR]: Missing response data for object ${s3Key}`)
        } else if (!response.data.Body) {
            console.log(`[ERROR]: Missing response data for object ${s3Key}`)
        } else {
            body = response.data.Body
        }
        this._s3ObjectsFetchedTotal++
        return body
    }

    _sleep(milliseconds: number): Promise<void> {
        return new Promise((resolve, reject) => {
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
    ) {
        this.log(LogLevel.Trace, `BEGIN _flushBuffer destination=${destPrefix}`)
        const fileName = `${prefix.replace(srcPrefix, destPrefix)}/${fileNumber}.json.gz`
        await this._saveToS3(bucket, buffer, fileName)
    }

    async _saveToS3(bucket: string, buffer: string, key: string): Promise<void> {
        this.log(LogLevel.Trace, `BEGIN _saveToS3 key=${key}`)
        await waitUntil(() => this._s3ObjectPutProcessCount < this._objectBodyPutLimit, INFINITE_TIMEOUT)

        this._s3ObjectPutProcessCount++

        const body = zlib.gzipSync(buffer)
        const object: AWS.S3.Types.PutObjectRequest = {
            Body: body,
            Bucket: bucket,
            Key: key,
        }
        let response: AWS.Response<AWS.S3.PutObjectAclOutput, AWS.AWSError>
        try {
            const s3 = new AWS.S3()
            response = (await s3.putObject(object).promise()).$response
        } catch (e) {
            this.log(LogLevel.Error, `Failed to save object to S3: ${key}`)
            throw e
        }
        this._s3ObjectPutProcessCount--
        if (response.error) {
            throw response.error
        }
        this._s3ObjectsPutTotal++
        this.log(LogLevel.Trace, `END _saveToS3 key=${key}`)
    }

    async _buildFullPrefixList(keyParams: PrefixParams): Promise<void> {
        this._s3BuildPrefixListObjectsProcessCount++
        this.log(LogLevel.Trace, `_buildFullPrefixList ${keyParams.curPrefix}::${keyParams.partitionStack}`)
        if (this._isPrefixInBounds(keyParams)) {
            const curPart = keyParams.partitionStack.shift()
            if (!curPart) {
                throw new Error('Unexpected end of partition stack!')
            }

            if (!keyParams.curPrefix.endsWith(this._delimeter)) {
                keyParams.curPrefix += this._delimeter
            }

            const listObjRequest: ListObjectsV2Request = {
                Bucket: keyParams.bucket,
                Prefix: keyParams.curPrefix,
                Delimiter: this._delimeter,
            }
            let continuationToken: undefined | string
            do {
                listObjRequest.ContinuationToken = continuationToken
                let response: AWS.Response<AWS.S3.ListObjectsV2Output, AWS.AWSError>
                try {
                    const s3 = new AWS.S3()
                    response = (await s3.listObjectsV2(listObjRequest).promise()).$response
                } catch (e) {
                    this.log(LogLevel.Error, `Failed to list objects at prefix ${keyParams.curPrefix}`)
                    throw e
                }
                if (response.error) {
                    throw error
                } else if (response.data && response.data.CommonPrefixes && response.data.CommonPrefixes.length > 0) {
                    if (response.data.IsTruncated && response.data.NextContinuationToken) {
                        continuationToken = response.data.NextContinuationToken
                    } else {
                        continuationToken = undefined
                    }
                    response.data.CommonPrefixes.forEach((commonPrefix) => {
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
        }
        this._s3BuildPrefixListObjectsProcessCount--
    }

    _isPrefixInBounds(keyParams: PrefixParams) {
        let inBounds
        if (keyParams.bounds) {
            let startDate: NullableMoment = moment(keyParams.bounds.startDate)
            const [startYear, startMonth, startDay] = startDate.format('YYYY-MM-DD').split('-')
            startDate = undefined
            let endDate: NullableMoment = moment(keyParams.bounds.endDate)
            const [endYear, endMonth, endDay] = endDate.format('YYYY-MM-DD').split('-')
            endDate = undefined
            const dateMatcher = new RegExp(
                `(${keyParams.prefix})(/year=(?<year>[0-9]{4}))?(/month=(?<month>[0-1][0-9]))?(/day=(?<day>[0-3][0-9]))?`
            )
            const dateParts = keyParams.curPrefix.match(dateMatcher)?.groups as MatchedDate
            if (dateParts) {
                if (dateParts.year && dateParts.month && dateParts.day) {
                    if (
                        (dateParts.year > startYear ||
                            (dateParts.year === startYear && dateParts.month > startMonth) ||
                            (dateParts.year === startYear &&
                                dateParts.month === startMonth &&
                                dateParts.day >= startDay)) &&
                        (dateParts.year < endYear ||
                            (dateParts.year === endYear && dateParts.month < endMonth) ||
                            (dateParts.year === endYear && dateParts.month === endMonth && dateParts.day <= endDay))
                    ) {
                        inBounds = true
                    } else {
                        inBounds = false
                    }
                } else if (dateParts.year && dateParts.month) {
                    if (
                        (dateParts.year > startYear ||
                            (dateParts.year === startYear && dateParts.month >= startMonth)) &&
                        (dateParts.year < endYear || (dateParts.year === endYear && dateParts.month <= endMonth))
                    ) {
                        inBounds = true
                    } else {
                        inBounds = false
                    }
                } else if (dateParts.year) {
                    if (dateParts.year >= startYear && dateParts.year <= endYear) {
                        inBounds = true
                    } else {
                        inBounds = false
                    }
                } else {
                    // Having none of the date parts implies the prefix is
                    // currently date agnositc and thus, it is in bounds.
                    // Similar arguments can be made for the previous
                    // else if statements - dctrotz 07/23/2020
                    inBounds = true
                }
            } else {
                throw new Error('Invalid prefix provided.')
            }
        } else {
            // No bounds provided means all prefxes are within the bounds
            inBounds = true
        }
        return inBounds
    }

    log(logLevel: LogLevel, logMessage: string) {
        if (this._log) {
            switch (logLevel) {
                case LogLevel.Trace:
                    this._log.trace(logMessage)
                    break
                case LogLevel.Debug:
                    this._log.debug(logMessage)
                    break
                case LogLevel.Info:
                    this._log.info(logMessage)
                    break
                case LogLevel.Warn:
                    this._log.warn(logMessage)
                    break
                case LogLevel.Error:
                    this._log.error(logMessage)
                    break
                case LogLevel.Fatal:
                    this._log.fatal(logMessage)
                    break
            }
        }
    }

    _evaluatePrefix(keyParams: PrefixParams, commonPrefix: AWS.S3.CommonPrefix, curPart: string): PrefixEvalResult {
        this.log(LogLevel.Trace, `_keysForPrefix ${keyParams.curPrefix}`)
        const result: PrefixEvalResult = {}
        if (commonPrefix.Prefix) {
            const parts = commonPrefix.Prefix.split(this._delimeter)
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
