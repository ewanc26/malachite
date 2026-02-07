import os from 'node:os';
import path from 'node:path';
import { log } from './logger.js';

/**
 * Detect the current platform
 */
export type Platform = 'windows' | 'macos' | 'linux';

export function getPlatform(): Platform {
  const platform = os.platform();
  let result: Platform;
  
  if (platform === 'win32') result = 'windows';
  else if (platform === 'darwin') result = 'macos';
  else result = 'linux';
  
  log.debug(`[platform.ts] getPlatform() detected: ${result} (node os.platform() = ${platform})`);
  return result;
}

/**
 * Get the malachite state directory path.
 * Always uses ~/.malachite regardless of OS platform for consistency.
 */
export function getMalachiteStateDir(): string {
  const home = os.homedir();
  const stateDir = path.join(home, '.malachite');
  log.debug(`[platform.ts] getMalachiteStateDir(): ${stateDir}`);
  return stateDir;
}

/**
 * Get the malachite cache directory path
 */
export function getMalachiteCacheDir(): string {
  const baseDir = getMalachiteStateDir();
  const cacheDir = path.join(baseDir, 'cache');
  log.debug(`[platform.ts] getMalachiteCacheDir(): ${cacheDir}`);
  return cacheDir;
}

/**
 * Get the malachite logs directory path
 */
export function getMalachiteLogsDir(): string {
  const baseDir = getMalachiteStateDir();
  const logsDir = path.join(baseDir, 'logs');
  log.debug(`[platform.ts] getMalachiteLogsDir(): ${logsDir}`);
  return logsDir;
}

/**
 * Get a locale-aware number formatter
 */
export function getNumberFormatter(): Intl.NumberFormat {
  try {
    return new Intl.NumberFormat(undefined, { useGrouping: true });
  } catch (e) {
    // Fallback for environments without Intl
    return {
      format: (n: number) => n.toLocaleString(),
    } as Intl.NumberFormat;
  }
}

/**
 * Format a number using the system locale
 */
export function formatLocaleNumber(n: number): string {
  const formatted = getNumberFormatter().format(n);
  log.debug(`[platform.ts] formatLocaleNumber(${n}) => ${formatted}`);
  return formatted;
}

/**
 * Get a locale-aware duration formatter (respects system locale for separators, abbreviations)
 */
export function formatLocaleDuration(ms: number): string {
  const days = Math.floor(ms / (24 * 60 * 60 * 1000));
  const hours = Math.floor((ms % (24 * 60 * 60 * 1000)) / (60 * 60 * 1000));
  const minutes = Math.floor((ms % (60 * 60 * 1000)) / (60 * 1000));
  const seconds = Math.floor((ms % (60 * 1000)) / 1000);

  const parts: string[] = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0) parts.push(`${minutes}m`);
  if (seconds > 0 && ms < 60 * 1000) parts.push(`${seconds}s`);

  if (parts.length === 0) return '< 1s';

  // Use locale-aware separator (typically space or comma depending on region)
  const platform = getPlatform();
  const separator = platform === 'windows' ? ' ' : ' ';
  return parts.join(separator);
}
