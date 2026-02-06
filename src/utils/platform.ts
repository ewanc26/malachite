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
 * Get the malachite state directory path, respecting platform conventions.
 * Windows: %APPDATA%\malachite (or fallback to ~/.malachite)
 * macOS: ~/Library/Application Support/malachite
 * Linux: ~/.config/malachite (XDG Base Directory spec)
 */
export function getMalachiteStateDir(): string {
  const platform = getPlatform();
  const home = os.homedir();
  let stateDir: string;

  switch (platform) {
    case 'windows': {
      const appdata = process.env.APPDATA;
      if (appdata) {
        stateDir = path.join(appdata, 'malachite');
        log.debug(`[platform.ts] getMalachiteStateDir() using APPDATA on Windows: ${stateDir}`);
      } else {
        stateDir = path.join(home, '.malachite');
        log.debug(`[platform.ts] getMalachiteStateDir() APPDATA not set, falling back to: ${stateDir}`);
      }
      return stateDir;
    }
    case 'macos': {
      stateDir = path.join(home, 'Library', 'Application Support', 'malachite');
      log.debug(`[platform.ts] getMalachiteStateDir() on macOS: ${stateDir}`);
      return stateDir;
    }
    case 'linux': {
      const xdgConfig = process.env.XDG_CONFIG_HOME;
      if (xdgConfig) {
        stateDir = path.join(xdgConfig, 'malachite');
        log.debug(`[platform.ts] getMalachiteStateDir() using XDG_CONFIG_HOME on Linux: ${stateDir}`);
      } else {
        stateDir = path.join(home, '.config', 'malachite');
        log.debug(`[platform.ts] getMalachiteStateDir() XDG not set, using default on Linux: ${stateDir}`);
      }
      return stateDir;
    }
  }
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
