# s3-file-scan-cat

A utility library that exists to concatenate multiple small JSON files into a single compressed gzip file.

## Requirements

[Node.js](https://nodejs.org/) **20** or newer.

## Install

This package is published on the [npm registry](https://www.npmjs.com/).

```sh
npm install s3-file-scan-cat
```

## Upgrading to v2

- **Constructor**: `new S3FileScanCat(useAccelerateEndpoint, scannerOptions, awsSecrets)` — the fourth argument (`slackConfig`) was removed. Configure Slack via [ot-logger-deluxe](https://www.npmjs.com/package/ot-logger-deluxe) v2 `slack` inside `scanner.loggerOptions`.
- **Logging**: `scanner.logOptions` (v1 / `OTLoggerDeluxeOptions`) is replaced by **`scanner.loggerOptions`**, which matches **`LoggerOptions`** from `ot-logger-deluxe` except the logger `name` (this library always sets `name` to `file-scan-cat`). See the ot-logger-deluxe README for `level`, `pretty`, `slack` (full webhook URLs), and related fields.
- **Re-exports**: Types such as `LoggerOptions` and `Logger` are re-exported from this package for convenience.

## Example

### Configuration

Configuration is divided between AWS S3 settings (`bucket`, prefixes, etc.), AWS credentials (`accessKeyId`, `secretAccessKey`), and the scanner.

Keeping credentials in a separate file from the rest of the config is recommended: one file for secrets (never commit) and one for the rest (commit per your deployment policy).

#### Example: secrets.json

```json
{
  "aws": {
    "accessKeyId": "_secret_key_",
    "secretAccessKey": "_secret_access_key_"
  }
}
```

#### Example: appConfig.json

```json
{
  "aws": {
    "s3": {
      "bucket": "bucket-name",
      "useAccelerateEndpoint": false,
      "scannerPrefix": "src-prefix",
      "destinationPrefix": "dest-prefix"
    }
  },
  "scanner": {
    "loggerOptions": {
      "level": "info",
      "pretty": false,
      "slack": {
        "defaultWebhookUrl": "https://hooks.slack.com/services/..."
      }
    },
    "partitionStack": ["year", "month", "day", "part-04", "part-05"],
    "limits": {
      "scanPrefixForPartitionsProcessLimit": 10,
      "s3ObjectBodyProcessInProgressLimit": 500,
      "maxFileSizeBytes": 134217728
    },
    "bounds": {
      "startDate": "2020-01-01",
      "endDate": "2020-01-01"
    }
  }
}
```

Omit `loggerOptions` entirely to disable library logging. When `bounds.startDate` and `bounds.endDate` are set, use **`YYYY-MM-DD`** strings; they are interpreted as **UTC** calendar days.

### Performing the scan

```ts
import * as fs from 'fs'
import { AWSSecrets, S3FileScanCat, ScannerConfig } from 's3-file-scan-cat'

const scannerConfig = JSON.parse(
    fs.readFileSync('./config/manager_config.json', 'utf8')
) as ScannerConfig
const awsSecrets = JSON.parse(fs.readFileSync('./config/private/secrets.json', 'utf8')).aws as AWSSecrets

const s3Scanner = new S3FileScanCat(
    scannerConfig.aws.s3.useAccelerateEndpoint,
    scannerConfig.scanner,
    awsSecrets
)
s3Scanner
    .scanAndProcessFiles(
        scannerConfig.aws.s3.bucket,
        scannerConfig.aws.s3.scannerPrefix,
        scannerConfig.aws.s3.destinationPrefix
    )
    .then(() => {
        process.exit(0)
    })
    .catch((error) => {
        console.error(`Failed: ${error}`)
        process.exit(1)
    })
```

For environment-driven logging, you can build options with `createLoggerFromEnv` from `ot-logger-deluxe` and pass the relevant fields into `loggerOptions`, or rely on env vars as documented in that package.
