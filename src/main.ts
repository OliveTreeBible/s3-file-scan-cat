import * as fs from 'fs'

import { AWSSecrets, ScannerConfig } from './interfaces/scanner.interface'
import { S3FileScanCat } from './S3FileScanCat'

const scannerConfig = JSON.parse(fs.readFileSync('./config/copy_config.json').toString('utf8')) as ScannerConfig
const awsSecrets = JSON.parse(fs.readFileSync('./config/private/secrets.json').toString('utf8')).aws as AWSSecrets

const s3Scanner = new S3FileScanCat(scannerConfig.scanner, awsSecrets)
s3Scanner
    .scanAndProcessFiles('ot-s3-managed-data-p', '/content_style', )
    .then(() => {
        process.exit(0)
    })
    .catch((error) => {
        console.error(`Failed: ${error}`)
        process.exit(-1)
    })