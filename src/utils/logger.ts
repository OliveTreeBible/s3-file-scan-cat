import { LFService, LoggerFactoryOptions, LogGroupRule, LogLevel } from 'typescript-logging';

export function createLogger(groupingRule: string, logLevel: LogLevel, loggerName: string) {
    const options = new LoggerFactoryOptions()
    options.addLogGroupRule(new LogGroupRule(new RegExp(groupingRule), logLevel))
    return LFService.createNamedLoggerFactory('s3fsc-logger-factory', options).getLogger(loggerName)
}
