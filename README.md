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
      "concatFilesAtPrefixProcessLimit": 4,
      "s3ObjectBodyProcessTotalLimit": 500,
      "s3ObjectPutProcessLimit": 64,
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

### Tuning concurrency

Phase 1 (partition discovery) fans out up to `scanPrefixForPartitionsProcessLimit` `ListObjectsV2` workers.

Phase 2 (concatenate-and-write) supports three optional knobs that default to the original strictly-sequential behavior, so upgrades are safe without any config changes:

- **`concatFilesAtPrefixProcessLimit`** — max leaf prefixes being concatenated at once. Default `1` (sequential). Raising this is the primary speedup lever when you have many small leaves.
- **`s3ObjectBodyProcessTotalLimit`** — class-wide cap on in-flight `GetObject` bodies across all concurrent leaves. Default `Infinity`. Must be >= `s3ObjectBodyProcessInProgressLimit`. Use this together with `concatFilesAtPrefixProcessLimit > 1` to bound concurrent `GetObject` work on the shared S3 HTTP(S) agents (default `maxSockets` is **500 total** across list, get, and put).
- **`s3ObjectPutProcessLimit`** — cap on concurrent `PutObject` calls across the whole scanner. Default `Infinity`. Pair with `concatFilesAtPrefixProcessLimit > 1` to avoid flush-time bursts against the destination prefix.

Memory/socket ceilings to watch: peak concat-buffer memory is roughly `concatFilesAtPrefixProcessLimit × maxFileSizeBytes`, and peak source-body memory is bounded by `s3ObjectBodyProcessTotalLimit × maxSourceObjectSizeBytes`. Sockets are shared: phase 1 runs up to `scanPrefixForPartitionsProcessLimit` concurrent `ListObjectsV2` calls, phase 2 can run about one listing stream per in-flight leaf (`concatFilesAtPrefixProcessLimit`), and `GetObject` / `PutObject` concurrency is capped by `s3ObjectBodyProcessTotalLimit` / `s3ObjectPutProcessLimit`. Keep that **aggregate** demand comfortably below the `500` `maxSockets` budget so lists, gets, and puts don't starve one another.

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
    .catch((error) => {
        console.error(`Failed: ${error}`)
        process.exitCode = 1
    })
    .finally(() => {
        s3Scanner.close()
    })
```

v2 uses explicit keep-alive HTTP(S) agents; `close()` destroys the S3 client and those agents after the scan promise settles (do not call `close()` while `scanAndProcessFiles` is still running). Use `.finally` as above, or `try` / `finally` with `await`, so long-running scripts and tests can shut down cleanly without depending on `process.exit()` to drop open sockets.

For environment-driven logging, you can build options with `createLoggerFromEnv` from `ot-logger-deluxe` and pass the relevant fields into `loggerOptions`, or rely on env vars as documented in that package.
