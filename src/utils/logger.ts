import { LFService, LoggerFactoryOptions, LogGroupRule, LogLevel } from 'typescript-logging'

export function createLogger(groupingRuleExp: RegExp, logLevel: LogLevel, loggerName: string) {
    const options = new LoggerFactoryOptions()
    options.addLogGroupRule(new LogGroupRule(groupingRuleExp, logLevel))
    return LFService.createNamedLoggerFactory('s3fsc-logger-factory', options).getLogger(loggerName)
}
