/**
 * Structured logging utility with log levels, formatting, and file logging
 */
import chalk from 'chalk';
import fs from 'fs';
import path from 'path';
import { getMalachiteLogsDir } from './platform.js';

export enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
  SILENT = 4,
}

export class Logger {
  private level: LogLevel;
  private prefix: string;
  private logFile: string | null = null;
  private logStream: fs.WriteStream | null = null;

  constructor(level: LogLevel = LogLevel.INFO, prefix: string = '') {
    this.level = level;
    this.prefix = prefix;
  }

  setLevel(level: LogLevel): void {
    this.level = level;
  }

  getLevel(): LogLevel {
    return this.level;
  }

  /**
   * Enable logging to file
   */
  enableFileLogging(logDir?: string): void {
    try {
      // Default to ~/.malachite/logs if no directory specified
      const defaultLogDir = getMalachiteLogsDir();
      const logsPath = logDir ? path.resolve(process.cwd(), logDir) : defaultLogDir;
      if (!fs.existsSync(logsPath)) {
        fs.mkdirSync(logsPath, { recursive: true });
      }

      // Create log filename with timestamp
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
      this.logFile = path.join(logsPath, `import-${timestamp}.log`);

      // Create write stream
      this.logStream = fs.createWriteStream(this.logFile, { flags: 'a' });

      this.writeToFile(`Log started at ${new Date().toISOString()}`);
      this.writeToFile(`Log file: ${this.logFile}`);
      this.writeToFile('='.repeat(80));
    } catch (error) {
      console.error('Failed to enable file logging:', error);
    }
  }

  /**
   * Get the current log file path
   */
  getLogFile(): string | null {
    return this.logFile;
  }

  /**
   * Close the log file stream
   */
  closeLogFile(): void {
    if (this.logStream) {
      this.writeToFile('='.repeat(80));
      this.writeToFile(`Log ended at ${new Date().toISOString()}`);
      this.logStream.end();
      this.logStream = null;
    }
  }

  /**
   * Write to log file (without formatting)
   */
  private writeToFile(message: string): void {
    if (this.logStream) {
      const timestamp = new Date().toISOString();
      this.logStream.write(`[${timestamp}] ${message}\n`);
    }
  }

  private shouldLog(level: LogLevel): boolean {
    return level >= this.level;
  }

  private formatMessage(message: string, prefix?: string): string {
    const finalPrefix = prefix || this.prefix;
    return finalPrefix ? `${finalPrefix} ${message}` : message;
  }

  private logToFile(level: string, message: string): void {
    if (this.logStream) {
      const cleanMessage = message.replace(/\x1b\[[0-9;]*m/g, ''); // Remove ANSI codes
      this.writeToFile(`[${level}] ${cleanMessage}`);
    }
  }

  debug(message: string, ...args: any[]): void {
    if (this.shouldLog(LogLevel.DEBUG)) {
      const formatted = chalk.gray(`[DEBUG] ${this.formatMessage(message)}`);
      console.log(formatted, ...args);
      this.logToFile('DEBUG', this.formatMessage(message));
    }
  }

  info(message: string, ...args: any[]): void {
    if (this.shouldLog(LogLevel.INFO)) {
      const formatted = this.formatMessage(message);
      console.log(formatted, ...args);
      this.logToFile('INFO', formatted);
    }
  }

  success(message: string, ...args: any[]): void {
    if (this.shouldLog(LogLevel.INFO)) {
      const formatted = chalk.green(`✓ ${this.formatMessage(message)}`);
      console.log(formatted, ...args);
      this.logToFile('SUCCESS', this.formatMessage(message));
    }
  }

  warn(message: string, ...args: any[]): void {
    if (this.shouldLog(LogLevel.WARN)) {
      const formatted = chalk.yellow(`⚠️  ${this.formatMessage(message)}`);
      console.warn(formatted, ...args);
      this.logToFile('WARN', this.formatMessage(message));
    }
  }

  error(message: string, ...args: any[]): void {
    if (this.shouldLog(LogLevel.ERROR)) {
      const formatted = chalk.red(`✗ ${this.formatMessage(message)}`);
      console.error(formatted, ...args);
      this.logToFile('ERROR', this.formatMessage(message));
    }
  }

  fatal(message: string, ...args: any[]): void {
    if (this.shouldLog(LogLevel.ERROR)) {
      const formatted = chalk.red.bold(`🛑 ${this.formatMessage(message)}`);
      console.error(formatted, ...args);
      this.logToFile('FATAL', this.formatMessage(message));
    }
  }

  // Progress and status messages (always shown unless SILENT)
  progress(message: string, ...args: any[]): void {
    if (this.level < LogLevel.SILENT) {
      const formatted = chalk.cyan(`→ ${this.formatMessage(message)}`);
      console.log(formatted, ...args);
      this.logToFile('PROGRESS', this.formatMessage(message));
    }
  }

  // Section headers
  section(title: string): void {
    if (this.shouldLog(LogLevel.INFO)) {
      const formatted = chalk.bold(`\n=== ${title} ===`);
      console.log(formatted);
      this.logToFile('SECTION', title);
    }
  }

  // Blank line
  blank(): void {
    if (this.level < LogLevel.SILENT) {
      console.log('');
    }
  }

  // Raw output (bypasses log level for important user-facing info)
  raw(message: string, ...args: any[]): void {
    if (this.level < LogLevel.SILENT) {
      console.log(message, ...args);
      this.logToFile('INFO', message);
    }
  }
}

// Global logger instance
let globalLogger: Logger = new Logger(LogLevel.INFO);

export function setGlobalLogger(logger: Logger): void {
  globalLogger = logger;
}

export function getLogger(): Logger {
  return globalLogger;
}

// Convenience exports
export const log = {
  debug: (msg: string, ...args: any[]) => globalLogger.debug(msg, ...args),
  info: (msg: string, ...args: any[]) => globalLogger.info(msg, ...args),
  success: (msg: string, ...args: any[]) => globalLogger.success(msg, ...args),
  warn: (msg: string, ...args: any[]) => globalLogger.warn(msg, ...args),
  error: (msg: string, ...args: any[]) => globalLogger.error(msg, ...args),
  fatal: (msg: string, ...args: any[]) => globalLogger.fatal(msg, ...args),
  progress: (msg: string, ...args: any[]) => globalLogger.progress(msg, ...args),
  section: (title: string) => globalLogger.section(title),
  blank: () => globalLogger.blank(),
  raw: (msg: string, ...args: any[]) => globalLogger.raw(msg, ...args),
  setLevel: (level: LogLevel) => globalLogger.setLevel(level),
  getLevel: () => globalLogger.getLevel(),
  enableFileLogging: (logDir?: string) => globalLogger.enableFileLogging(logDir),
  getLogFile: () => globalLogger.getLogFile(),
  closeLogFile: () => globalLogger.closeLogFile(),
};
