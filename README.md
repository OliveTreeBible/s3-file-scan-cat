# s3-file-scan-cat
A utility library that exists to concatenate multiple small JSON files into a single compressed gunzip file.

## Install

This is a [Node.js](https://nodejs.org/en/) module available through the
[npm registry](https://www.npmjs.com/).

Before installing, [download and install Node.js](https://nodejs.org/en/download/).
Node.js 0.6 or higher is required.

Installation is done using the
[`npm install` command](https://docs.npmjs.com/getting-started/installing-npm-packages-locally):

```sh
$ npm install s3-file-scan-cat
```
## Example

#### Configuration
Configuration is divided between three main sections, AWS S3 configuration (`bucket`, etc.), AWS secrets (`accessKeyId`, `secretAccessKey`), and the scanner.

Breaking these three sections into two JSON files is recommended.  One files contains the credentials for AWS and should never be committed to a repository and the second file contains everything else and can be committed to a repository depending on your deployment strategy.

##### Example: secrets.json
```
{
  "aws": {
    "accessKeyId": "_secret_key_",
    "secretAccessKey": "_secret_access_key_"
  }
}
```

##### Example: appConfig.json
```
{
  "aws" : {
    "s3": {
      "bucket": "bucket-name",
      "scannerPrefix": "src-prefix",
      "destinationPrefix": "dest-prefix"
    }
  },
  "scanner": {
    "logLevel": 4,
    "partitionStack" : [
      "year",
      "month",
      "day",
      "part-04",
      "part-05"
    ],
    "limits" : {
      "maxBuildPrefixList":   100,
      "prefixListObjectsLimit": 100,
      "objectFetchBatchSize":        200,
      "objectBodyFetchLimit":   300,
      "objectBodyPutLimit":         250,
      "minPercentRamFree":    25.0,
      "maxFileSizeBytes": 134217728
    },
    "bounds": {
      "startDate": "2020-01-01",
      "endDate": "2020-01-01"
    }
  }
}
```

#### Performing the scan

```
import * as fs from 'fs';
import { AWSSecrets, S3FileScanCat, ScannerConfig } from 's3-file-scan-cat';

const scannerConfig = JSON.parse(fs.readFileSync('./config/manager_config.json').toString('utf8')) as ScannerConfig
const awsSecrets = JSON.parse(fs.readFileSync('./config/private/secrets.json').toString('utf8')).aws as AWSSecrets

const s3Scanner = new S3FileScanCat(scannerConfig.scanner, awsSecrets)
s3Scanner
    .scanAndProcessFiles(scannerConfig.aws.s3.bucket, scannerConfig.aws.s3.scannerPrefix, scannerConfig.aws.s3.destinationPrefix)
    .then(() => {
        process.exit(0)
    })
    .catch((error) => {
        console.error(`Failed: ${error}`)
        process.exit(-1)
    })
```