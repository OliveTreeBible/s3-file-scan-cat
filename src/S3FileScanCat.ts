import * as waitUntil from 'async-wait-until'
import * as AWS from 'aws-sdk'
import { ListObjectsV2Request } from 'aws-sdk/clients/s3'
import { error } from 'console'
import * as moment from 'moment'
import { gzip } from 'node-gzip'
import { mem, MemInfo } from 'node-os-utils'
import { Logger } from 'typescript-logging'

import { S3FetchLimits, ScannerOptions, ConcatState, MatchedDate, PrefixEvalResult, PrefixParams, AWSSecrets } from './interfaces/scanner.interface'
import { loggerFactory } from './utils/logger'

const INFINITE_TIMEOUT = 2147483647 // This is actually 24 days but it is also the largest timeout allowed

type NullableAWSListObjectsResponse = undefined | AWS.Response<AWS.S3.ListObjectsV2Output, AWS.AWSError>
type NullableAWSObjectList = undefined | AWS.S3.ObjectList
type NullableAWSListObjectsOutput = undefined | AWS.S3.ListObjectsV2Output
type NullableMoment = undefined | moment.Moment
type NullableString = undefined | string

export class S3FileScanCat {
    private _s3: AWS.S3
    private _log: Logger
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
    private _memInfo: undefined | MemInfo
    constructor(
        private bucket: string,
        private awsAccess: AWSSecrets,
        private fetchLimits: S3FetchLimits,
        private headless: boolean
    ) {
        this._log = loggerFactory.getLogger('app.partitioner')
        this._delimeter = '/'
        this.bucket = bucket
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
        AWS.config.update({
            region: 'us-east-1',
            accessKeyId: awsAccess.accessKeyId,
            secretAccessKey: awsAccess.secretAccessKey,
        })
        AWS.config.apiVersions = {
            s3: '2006-03-01',
        }
        this._s3 = new AWS.S3()
        setInterval(() => this._checkUsage(), 1000)
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

    get percentMemoryFree(): number {
        if (this._memInfo) return this._memInfo.freeMemPercentage
        return 0.0
    }

    async scanAndProcessFiles(srcPrefix: string, destPrefix: string, scannerOptions: ScannerOptions): Promise<void> {
        this._log.trace({
            msg: `BEGIN scanConcatenateCopy srcPrefix=${srcPrefix}:destPrefix=${destPrefix}:partitionStack=${scannerOptions.partition_stack}`,
        })
        const destPath = destPrefix
        const maxFileSizeBytes = scannerOptions.max_file_size_bytes
        this._keyParams.push({
            prefix: srcPrefix,
            partitionStack: scannerOptions.partition_stack,
            bounds: scannerOptions.bounds,
        })
        while (this._keyParams.length > 0 || this._s3BuildPrefixListObjectsProcessCount > 0) {
            await waitUntil(
                () => this._s3BuildPrefixListObjectsProcessCount < this.fetchLimits.max_build_prefix_list,
                INFINITE_TIMEOUT
            )
            const keyParam = this._keyParams.shift()
            if (keyParam) this._buildFullPrefixList(keyParam)
        }
        this._totalPrefixesToProcess = this._allPrefixes.length
        if (this._allPrefixes.length > 0) {
            while (this._allPrefixes.length > 0) {
                const prefix = this._allPrefixes.shift()
                if (prefix) {
                    this._concatFilesAtPrefix(prefix, srcPrefix, destPath, maxFileSizeBytes, {
                        buffer: '',
                        continuationToken: '',
                        fileNumber: 0,
                    })
                }
            }
            this._log.info(`Finished concatenating and compressing JSON S3 Objects for prefix=${srcPrefix}`)
        } else {
            throw new Error('Failed to get common prefixes for directory listing in S3.')
        }

        //  Wait until all processes are completed.
        await waitUntil(() => this._totalPrefixesToProcess === this._prefixesProcessedTotal, INFINITE_TIMEOUT)
        this._s3 = new AWS.S3()
        this._isDone = true
    }
    _processAtPriorityCanProceed(priority: number): boolean {
        if (this._memInfo) return this._memInfo.freeMemPercentage - priority * 5 > this.fetchLimits.min_percent_ram_free
        return false
    }

    async _checkUsage(): Promise<void> {
        this._memInfo = await mem.info()
    }

    async _concatFilesAtPrefix(
        prefix: string,
        srcPrefix: string,
        destPrefix: string,
        maxFileSizeBytes: number,
        concatState: ConcatState
    ): Promise<void> {
        this._log.info({ msg: `BEGIN _concatFilesAtPrefix prefix=${prefix}` })
        const listObjRequest: ListObjectsV2Request = {
            Bucket: this.bucket,
            Prefix: prefix.endsWith('/') ? prefix : `${prefix}/`,
            MaxKeys: this.fetchLimits.max_object_fetch,
        }
        do {
            if (concatState.continuationToken.length > 0) {
                listObjRequest.ContinuationToken = concatState.continuationToken
            }
            await waitUntil(
                () =>
                    this._processAtPriorityCanProceed(0) &&
                    this._s3PrefixListObjectsProcessCount < this.fetchLimits.max_prefix_list_objects,
                INFINITE_TIMEOUT
            )
            this._s3PrefixListObjectsProcessCount++
            let response: NullableAWSListObjectsResponse = (await this._s3.listObjectsV2(listObjRequest).promise())
                .$response
            if (response.error) {
                throw error
            } else if (response.data) {
                let data: NullableAWSListObjectsOutput = response.data
                response = undefined // These build up over time, let GC get to this sooner (same with the data and contents)
                if (data.Contents) {
                    let doneFetching = true
                    let contents: NullableAWSObjectList = data.Contents
                    if (data.IsTruncated === true && data.NextContinuationToken !== undefined) {
                        concatState.continuationToken = data.NextContinuationToken
                        doneFetching = false
                    }
                    data = undefined
                    let objectBodyPromises: undefined | Promise<AWS.S3.Body>[] = []
                    for (const s3Object of contents) {
                        if (s3Object.Size && s3Object.Key) {
                            objectBodyPromises.push(this._getObjectBody(s3Object.Key))
                        } else if (s3Object.Size === 0) {
                            this._log.warn({ msg: `S3 Object had zero size ${JSON.stringify(s3Object.Key)}` })
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
                        if (concatState.buffer.length + objectBodyStr.length > maxFileSizeBytes) {
                            this._flushBuffer(
                                concatState.buffer,
                                prefix,
                                srcPrefix,
                                destPrefix,
                                concatState.fileNumber++
                            )
                            concatState.buffer = ''
                        }
                        if (concatState.buffer.length > 0) {
                            concatState.buffer += '\n' + objectBodyStr
                        } else {
                            concatState.buffer += objectBodyStr
                        }
                    })
                    if (doneFetching && concatState.buffer.length > 0) {
                        this._flushBuffer(concatState.buffer, prefix, srcPrefix, destPrefix, concatState.fileNumber++)
                        concatState.buffer = ''
                        concatState.continuationToken = ''
                    }
                }
            } else {
                throw new Error(`Unexpected Error: List S3 Objects request had missing response.`)
            }
            this._s3PrefixListObjectsProcessCount--
        } while (concatState.continuationToken.length > 0)
        this._log.info({ msg: `END _concatFilesAtPrefix prefix=${prefix}` })
        this._prefixesProcessedTotal++
    }

    async _getObjectBody(s3Key: AWS.S3.ObjectKey): Promise<AWS.S3.Body> {
        this._log.trace({ msg: `BEGIN _getObjectBody objectKey=${s3Key}` })
        let body: AWS.S3.Body
        const getObjectRequest: AWS.S3.Types.GetObjectRequest = {
            Bucket: this.bucket,
            Key: s3Key,
        }
        await waitUntil(
            () =>
                this._processAtPriorityCanProceed(0) &&
                this._s3ObjectBodyProcessCount < this.fetchLimits.max_object_body_fetch,
            INFINITE_TIMEOUT
        )
        this._s3ObjectBodyProcessCount++
        const response = (await this._s3.getObject(getObjectRequest).promise()).$response
        this._s3ObjectBodyProcessCount--
        if (response.error) {
            throw response.error
        } else if (!response.data) {
            throw new Error(`Missing response data for object ${s3Key}`)
        } else if (!response.data.Body) {
            throw new Error(`Missing response data for object ${s3Key}`)
        } else {
            body = response.data.Body
        }
        this._s3ObjectsFetchedTotal++
        return body
    }

    async _flushBuffer(buffer: string, prefix: string, srcPrefix: string, destPrefix: string, fileNumber: number) {
        this._log.trace({ msg: `BEGIN _flushBuffer destination=${destPrefix}` })
        const fileName = `${prefix.replace(srcPrefix, destPrefix)}/${fileNumber}.json.gz`
        await this._saveToS3(buffer, fileName)
    }

    async _saveToS3(buffer: string, key: string): Promise<void> {
        this._log.trace({ msg: `BEGIN _saveToS3 key=${key}` })
        const body = await gzip(buffer)
        const object: AWS.S3.Types.PutObjectRequest = {
            Body: body,
            Bucket: this.bucket,
            Key: key,
        }
        await waitUntil(
            () =>
                this._processAtPriorityCanProceed(0) &&
                this._s3ObjectPutProcessCount < this.fetchLimits.max_object_puts,
            INFINITE_TIMEOUT
        )
        this._s3ObjectPutProcessCount++
        const response = (await this._s3.putObject(object).promise()).$response
        this._s3ObjectPutProcessCount--
        if (response.error) {
            throw response.error
        }
        this._s3ObjectsPutTotal++
        this._log.trace({ msg: `END _saveToS3 key=${key}` })
    }

    async _buildFullPrefixList(keyParams: PrefixParams): Promise<void> {
        this._s3BuildPrefixListObjectsProcessCount++
        this._log.trace({ msg: `_buildFullPrefixList ${keyParams.prefix}::${keyParams.partitionStack}` })
        if (this._isPrefixInBounds(keyParams)) {
            const curPart = keyParams.partitionStack.shift()
            if (!curPart) {
                throw new Error('Unexpected end of partition stack!')
            }

            if (!keyParams.prefix.endsWith(this._delimeter)) {
                keyParams.prefix += this._delimeter
            }

            const listObjRequest: ListObjectsV2Request = {
                Bucket: this.bucket,
                Prefix: keyParams.prefix,
                Delimiter: this._delimeter,
            }
            const response = (await this._s3.listObjectsV2(listObjRequest).promise()).$response
            if (response.error) {
                throw error
            } else if (response.data && response.data.CommonPrefixes && response.data.CommonPrefixes.length > 0) {
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
            let basePath: NullableString = keyParams.prefix.split('/')[0]
            const dateMatcher = new RegExp(
                `${basePath}\\/?(year=(?<year>[0-9]{4}))?(\\/month=(?<month>[0-1][0-9]))?(\\/day=(?<day>[0-3][0-9]))?`
            )
            basePath = undefined
            const dateParts = keyParams.prefix.match(dateMatcher)?.groups as MatchedDate
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

    _evaluatePrefix(keyParams: PrefixParams, commonPrefix: AWS.S3.CommonPrefix, curPart: string): PrefixEvalResult {
        this._log.trace({ msg: `_keysForPrefix ${keyParams.prefix}` })
        const result: PrefixEvalResult = {}
        if (commonPrefix.Prefix) {
            const parts = commonPrefix.Prefix.split(this._delimeter)
            if (parts.length > 1) {
                const lastPart = parts[parts.length - 2]
                if (lastPart.startsWith(curPart)) {
                    const nextPart = `${keyParams.prefix}${lastPart}`
                    if (keyParams.partitionStack.length > 0) {
                        const partitionStackClone = Object.assign([], keyParams.partitionStack)
                        result.keyParams = {
                            prefix: nextPart,
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
