import { LFService, LoggerFactory, LoggerFactoryOptions, LogGroupRule, LogLevel } from 'typescript-logging'

const options = new LoggerFactoryOptions()
options.addLogGroupRule(new LogGroupRule(new RegExp('app.+'), LogLevel.Fatal))

export const loggerFactory: LoggerFactory = LFService.createNamedLoggerFactory('S3MANAGER', options)
