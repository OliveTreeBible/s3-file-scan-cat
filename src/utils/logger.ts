import {
    LFService, LoggerFactory, LoggerFactoryOptions, LogGroupRule, LogLevel
} from 'typescript-logging'

let loggerFactory: LoggerFactory

export function createLogger(groupingRule: string, logLevel: LogLevel, loggerName: string) {
    const options = new LoggerFactoryOptions()
    options.addLogGroupRule(new LogGroupRule(new RegExp(groupingRule), logLevel))
    if(loggerFactory === undefined) {
        loggerFactory = LFService.createNamedLoggerFactory('s3fsc-logger-factory', options)
    }
    return loggerFactory.getLogger(loggerName)
}
