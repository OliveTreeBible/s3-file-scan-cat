import * as waitUntil from 'async-wait-until'
import * as AWS from 'aws-sdk'
import { ListObjectsV2Request } from 'aws-sdk/clients/s3'
import { error } from 'console'
import * as moment from 'moment'
import { Logger, LogLevel } from 'typescript-logging'

import { EmptyPrefixError } from './errors/EmptyPrefixError'
import {
    AWSSecrets, ConcatState, PrefixEvalResult, PrefixParams, ScannerOptions
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
    private _scannerOptions: ScannerOptions
    constructor(scannerOptions: ScannerOptions, awsAccess: AWSSecrets) {
        if (scannerOptions.logOptions) {
            const logOptions = scannerOptions.logOptions
            let logLevel = logOptions.logLevel
            if(typeof logLevel === 'string' && !Number.isInteger(logLevel)) {
                switch((logLevel as string).toLowerCase()) {
                    case 'trace': {
                        logLevel = 0
                        break
                    }
                    case 'debug': {
                        logLevel = 1
                        break
                    }
                    case 'info': {
                        logLevel = 2
                        break
                    }
                    case 'warn': {
                        logLevel = 3
                        break
                    }
                    case 'error': {
                        logLevel = 4
                        break
                    }
                    case 'fatal': {
                        logLevel = 5
                        break
                    }
                    default: 
                        logLevel = 0
                }
            }
            this._log = createLogger(
                logOptions.logGroupingPattern,
                logLevel,
                'app.s3-file-scan-cat'
            )
        }
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
        this._objectFetchBatchSize = 100
        this._prefixListObjectsLimit = 100
        this._objectBodyFetchLimit = 100
        this._objectBodyPutLimit = 100
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

    async scanAndProcessFiles(bucket: string, srcPrefix: string): Promise<void> {
        this.log(
            LogLevel.Trace,
            `BEGIN scanConcatenateCopy srcPrefix=${srcPrefix}:partitionStack=${this._scannerOptions.partitionStack}`
        )

        this._keyParams.push({
            bucket,
            prefix: srcPrefix,
            curPrefix: srcPrefix, /* This changes as we traverse down the path, srcPrefix is where we start */
            partitionStack: this._scannerOptions.partitionStack,
            bounds: this._scannerOptions.bounds,
        })
        this._allPrefixes = ['/content_style']
        this._totalPrefixesToProcess = this._allPrefixes.length
        if (this._allPrefixes.length > 0) {
            while (this._allPrefixes.length > 0) {
                const prefix = this._allPrefixes.shift()
                if (prefix) {
                    this._copyFilesAtPrefix(bucket, prefix, {
                        continuationToken: undefined,
                        fileNumber: 0,
                    })
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

    async _copyFilesAtPrefix(
        bucket: string,
        prefix: string,
        concatState: ConcatState
    ): Promise<void> {
        this.log(LogLevel.Info, `BEGIN _concatFilesAtPrefix prefix=${prefix}`)
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
            } catch(e) {
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
                    let objectCopyPromises: undefined | Promise<boolean>[] = []
                    for (const s3Object of contents) {
                        if (s3Object.Size && s3Object.Key) {
                            objectCopyPromises.push(this._copyObject(bucket, s3Object.Key))
                        } else if (s3Object.Size === 0) {
                            this.log(LogLevel.Warn, `S3 Object had zero size ${JSON.stringify(s3Object.Key)}`)
                        } else {
                            throw new Error('Invalid S3 Object encountered.')
                        }
                    }
                    contents = undefined

                    await Promise.all(objectCopyPromises)
                }
            } else {
                throw new Error(`Unexpected Error: List S3 Objects request had missing response.`)
            }
            this._s3PrefixListObjectsProcessCount--
        } while (concatState.continuationToken)
        this.log(LogLevel.Info, `END _concatFilesAtPrefix prefix=${prefix}`)
        this._prefixesProcessedTotal++
    }

    async _copyObject(bucket: string, s3Key: AWS.S3.ObjectKey): Promise<boolean> {
        const body = await this._getObjectBody(bucket, s3Key)
        const fileName = s3Key.split('/').pop()
        if(fileName === undefined) {
            this.log(LogLevel.Error, `Object ${s3Key} missing fileName??`)
            return false
        }
        const newKey = `content/managed_annotations/annotations/content_style/${fileName}`
        await this._putObject(bucket, newKey, body)
        this.log(LogLevel.Info, `Copied ${s3Key}`)
        return true
    }

    async _getObjectBody(bucket: string, s3Key: AWS.S3.ObjectKey): Promise<AWS.S3.Body> {
        this.log(LogLevel.Trace, `BEGIN _getObjectBody objectKey=${s3Key}`)
        let body: AWS.S3.Body = ""
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
        while(!response) {
            try{
                response = (await s3.getObject(getObjectRequest).promise()).$response
                lastError = undefined
            } catch(error) {
                console.log(`[ERROR] S3 getObjectFailed: ${error}`)
                lastError = error
                if(retryCount < MAX_RETRIES) {
                    await this._sleep(this._getWaitTimeForRetry(retryCount++))
                } else {
                    break
                }
            }
        }
        
        
        this._s3ObjectBodyProcessCount--
        if(response === undefined) {
            console.log(`[ERROR]: Unexpected S3 getObject error encountered ${s3Key}:${lastError? lastError:""}`)
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

    async _putObject(bucket: string, s3Key: AWS.S3.ObjectKey, content:AWS.S3.Body): Promise<void> {
        this.log(LogLevel.Trace, `BEGIN _putObject key=${s3Key}`)
        await waitUntil(() => this._s3ObjectPutProcessCount < this._objectBodyPutLimit, INFINITE_TIMEOUT)
        
        this._s3ObjectPutProcessCount++

        const object: AWS.S3.Types.PutObjectRequest = {
            Body: content,
            Bucket: bucket,
            Key: s3Key,
        }
        let response: AWS.Response<AWS.S3.PutObjectAclOutput, AWS.AWSError>
        try{
            const s3 = new AWS.S3()
            response = (await s3.putObject(object).promise()).$response
        } catch(e) {
            this.log(LogLevel.Error, `Failed to save object to S3: ${s3Key}`)
            throw e
        }
        this._s3ObjectPutProcessCount--
        if (response.error) {
            throw response.error
        }
        this._s3ObjectsPutTotal++
        this.log(LogLevel.Trace, `END _putObject key=${s3Key}`)
    }

    _sleep(milliseconds: number): Promise<void> {
        return new Promise((resolve, reject) => {
            setTimeout(resolve, milliseconds)
        })
    }

    // Provides an exponential backoff for wait times when s3 commands fail.
    _getWaitTimeForRetry(retryCount: number) {
        if(retryCount === 0)
            return 0
        return Math.pow(2, retryCount) * 100
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
                    this._log.warn(logMessage )
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
