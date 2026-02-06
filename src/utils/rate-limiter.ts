import type { Config } from '../types.js';
import { formatLocaleNumber } from './platform.js';
import { log } from './logger.js';

/**
 * Calculate rate-limited batch parameters
 * Ensures we don't exceed daily limits while maintaining efficiency
 */
export function calculateRateLimitedBatches(
  totalRecords: number,
  config: Config
): {
  batchSize: number;
  batchDelay: number;
  estimatedDays: number;
  recordsPerDay: number;
  needsRateLimiting: boolean;
} {
  log.debug(`[rate-limiter.ts] calculateRateLimitedBatches(totalRecords=${totalRecords})`);
  
  const dailyLimit = Math.floor(config.RECORDS_PER_DAY_LIMIT * config.SAFETY_MARGIN);
  log.debug(`[rate-limiter.ts] Daily limit: ${formatLocaleNumber(dailyLimit)} (raw limit=${config.RECORDS_PER_DAY_LIMIT}, safety margin=${config.SAFETY_MARGIN})`);
  
  // Check if we need rate limiting
  const needsRateLimiting = totalRecords > dailyLimit;
  log.info(`[rate-limiter.ts] Rate limiting needed: ${needsRateLimiting} (${formatLocaleNumber(totalRecords)} records > ${formatLocaleNumber(dailyLimit)} daily limit)`);
  
  if (!needsRateLimiting) {
    // Can import everything in one go
    log.debug(`[rate-limiter.ts] No rate limiting needed - can import all ${formatLocaleNumber(totalRecords)} records in one batch`);
    return {
      batchSize: config.DEFAULT_BATCH_SIZE,
      batchDelay: config.DEFAULT_BATCH_DELAY,
      estimatedDays: 1,
      recordsPerDay: totalRecords,
      needsRateLimiting: false,
    };
  }
  
  // Calculate how many days needed
  const estimatedDays = Math.ceil(totalRecords / dailyLimit);
  const recordsPerDay = Math.floor(totalRecords / estimatedDays);
  log.info(`[rate-limiter.ts] Estimated duration: ${estimatedDays} day(s), spreading ${formatLocaleNumber(recordsPerDay)} records/day`);
  
  // Calculate batch parameters
  // We want to spread records evenly throughout the day
  const minutesPerDay = 24 * 60;
  const batchesPerDay = Math.ceil(recordsPerDay / config.DEFAULT_BATCH_SIZE);
  const delayBetweenBatches = Math.floor((minutesPerDay * 60 * 1000) / batchesPerDay);
  log.debug(`[rate-limiter.ts] Batches per day: ${batchesPerDay}, delay between batches: ${delayBetweenBatches}ms`);
  
  // Ensure batch delay is at least minimum
  const batchDelay = Math.max(delayBetweenBatches, config.MIN_BATCH_DELAY);
  if (batchDelay !== delayBetweenBatches) {
    log.debug(`[rate-limiter.ts] Batch delay adjusted from ${delayBetweenBatches}ms to minimum ${config.MIN_BATCH_DELAY}ms`);
  }
  
  // Adjust batch size if needed to hit the target
  const adjustedBatchSize = Math.min(
    Math.ceil(recordsPerDay / Math.floor((minutesPerDay * 60 * 1000) / batchDelay)),
    config.MAX_BATCH_SIZE
  );
  log.debug(`[rate-limiter.ts] Final batch parameters: size=${adjustedBatchSize}, delay=${batchDelay}ms (default was size=${config.DEFAULT_BATCH_SIZE}, delay=${config.DEFAULT_BATCH_DELAY}ms)`);
  
  return {
    batchSize: adjustedBatchSize,
    batchDelay,
    estimatedDays,
    recordsPerDay,
    needsRateLimiting: true,
  };
}

// --------------------------------------------------
// RateLimiter class (persisted state + header handling)
// --------------------------------------------------
import fs from 'node:fs';
import path from 'node:path';
import { getMalachiteStateDir } from './platform.js';
import { parseRateLimitHeaders } from './rate-limit-headers.js';

export class RateLimiter {
  stateFile: string;
  safety: number;

  constructor(opts?: { safety?: number }) {
    this.safety = opts?.safety ?? 1.0;
    const stateDir = path.join(getMalachiteStateDir(), 'state');
    this.stateFile = path.join(stateDir, 'rate-limit.json');
    log.debug(`[RateLimiter] constructor: stateFile=${this.stateFile}, safety=${this.safety}`);
    this.ensureStateDir();
  }

  ensureStateDir() {
    const dir = path.dirname(this.stateFile);
    if (!fs.existsSync(dir)) {
      log.debug(`[RateLimiter] ensureStateDir() creating: ${dir}`);
      fs.mkdirSync(dir, { recursive: true });
    } else {
      log.debug(`[RateLimiter] ensureStateDir() directory already exists: ${dir}`);
    }
  }

  persistState(obj: Record<string, any>) {
    log.debug(`[RateLimiter] persistState() writing to ${this.stateFile}: ${JSON.stringify(obj)}`);
    fs.writeFileSync(this.stateFile, JSON.stringify(obj, null, 2), 'utf8');
  }

  readState(): Record<string, any> {
    try {
      const raw = fs.readFileSync(this.stateFile, 'utf8');
      const state = JSON.parse(raw);
      log.debug(`[RateLimiter] readState() loaded from ${this.stateFile}: ${JSON.stringify(state)}`);
      return state;
    } catch (e) {
      log.debug(`[RateLimiter] readState() no state file found (${this.stateFile}), returning empty object`);
      return {};
    }
  }

  /**
   * Update internal state based on server rate-limit headers
   */
  updateFromHeaders(headers: Record<string, string>) {
    log.debug(`[RateLimiter] updateFromHeaders() called`);
    const parsed = parseRateLimitHeaders(headers);
    const limit = parsed.limit || 0;
    const remainingRaw = parsed.remaining || 0;
    const windowSeconds = parsed.windowSeconds || 0;

    const remaining = Math.floor(remainingRaw * this.safety);
    log.info(`[RateLimiter] Rate limit info: limit=${limit}, remaining_raw=${remainingRaw}, remaining_with_safety=${remaining}, window=${windowSeconds}s`);

    const obj = {
      limit,
      remaining,
      windowSeconds,
      updatedAt: Math.floor(Date.now() / 1000),
    };

    this.persistState(obj);
    return obj;
  }

  /**
   * Decrement remaining permits and persist. If not enough remaining, returns false.
   */
  async waitForPermit(count = 1): Promise<boolean> {
    const state = this.readState();
    const rem = typeof state.remaining === 'number' ? state.remaining : 0;
    log.debug(`[RateLimiter] waitForPermit(${count}) called, current remaining: ${rem}/${state.limit}`);
    
    if (rem >= count) {
      state.remaining = Math.max(0, rem - count);
      log.debug(`[RateLimiter] waitForPermit() quota available, decrementing ${rem} -> ${state.remaining}`);
      this.persistState(state);
      return true;
    }

    // Not enough quota; if windowSeconds is set, sleep until reset
    if (state.windowSeconds && state.windowSeconds > 0) {
      const waitMs = state.windowSeconds * 1000 + 1000;
      const waitSeconds = Math.floor(waitMs / 1000);
      log.warn(`[RateLimiter] Quota exhausted! Remaining ${rem}/${state.limit}. Waiting ${waitSeconds}s for quota reset (window=${state.windowSeconds}s)`);
      await new Promise((res) => setTimeout(res, waitMs));
      // After waiting, set remaining to limit * safety
      state.remaining = Math.floor((state.limit || 0) * this.safety) - count;
      if (state.remaining < 0) state.remaining = 0;
      log.debug(`[RateLimiter] waitForPermit() quota reset complete, remaining now: ${state.remaining}`);
      this.persistState(state);
      return true;
    }

    log.warn(`[RateLimiter] waitForPermit() failed: quota exhausted and no reset window info available`);
    return false;
  }
}

/**
 * Calculate daily batches and pause times
 */
export function calculateDailySchedule(
  totalRecords: number,
  batchSize: number,
  batchDelay: number,
  recordsPerDay: number
) {
  const schedule = [];

  // How many batches fit into a 24h window using the actual delay?
  const batchesPerDay = Math.floor((24 * 60 * 60 * 1000) / batchDelay);

  // Max records we could process in one day given the spacing
  const maxRecordsPerDay = batchesPerDay * batchSize;

  // Respect the external rate limit (recordsPerDay)
  const dailyCap = Math.min(maxRecordsPerDay, recordsPerDay);

  let processed = 0;
  let day = 1;

  while (processed < totalRecords) {
    const recordsStart = processed;
    const dailyCount = Math.min(dailyCap, totalRecords - processed);
    const recordsEnd = recordsStart + dailyCount;
    const isLastDay = recordsEnd >= totalRecords;

    schedule.push({
      day,
      recordsStart,
      recordsEnd,
      recordsCount: dailyCount,
      pauseAfter: !isLastDay,
      pauseDuration: isLastDay ? 0 : 24 * 60 * 60 * 1000
    });

    processed = recordsEnd;
    day++;
  }

  return schedule;
}


/**
 * Format time duration in human-readable format
 */
export function formatTimeRemaining(ms: number): string {
  const days = Math.floor(ms / (24 * 60 * 60 * 1000));
  const hours = Math.floor((ms % (24 * 60 * 60 * 1000)) / (60 * 60 * 1000));
  const minutes = Math.floor((ms % (60 * 60 * 1000)) / (60 * 1000));
  
  if (days > 0) {
    return `${days}d ${hours}h ${minutes}m`;
  } else if (hours > 0) {
    return `${hours}h ${minutes}m`;
  } else if (minutes > 0) {
    return `${minutes}m`;
  } else {
    return '< 1m';
  }
}

/**
 * Display rate limit warning
 */
export function displayRateLimitWarning(): void {
  console.log('');
  console.log('⚠️  IMPORTANT: Rate Limits');
  console.log('   Exceeding 10K records/day can rate limit your ENTIRE PDS.');
  console.log('   This affects ALL users on your PDS, not just your account.');
  console.log('   Import automatically limits to 10K records/day with pauses.');
  console.log('   See: https://docs.bsky.app/blog/rate-limits-pds-v3');
  console.log('');
}

/**
 * Display rate limiting info
 */
export function displayRateLimitInfo(
  totalRecords: number,
  batchSize: number,
  batchDelay: number,
  estimatedDays: number,
  recordsPerDay: number
): void {
  console.log('\n📊 Rate Limiting Information:');
  console.log(`   Total records: ${formatLocaleNumber(totalRecords)}`);
  console.log(`   Daily limit: ${formatLocaleNumber(recordsPerDay)} records/day`);
  console.log(`   Estimated duration: ${estimatedDays} day${estimatedDays > 1 ? 's' : ''}`);
  console.log(`   Batch size: ${batchSize} records`);
  console.log(`   Batch delay: ${(batchDelay / 1000).toFixed(1)}s`);
  
  if (estimatedDays > 1) {
    console.log('\n   The import will automatically pause between days.');
    console.log('   You can safely close and restart the importer - it will resume from where it left off.');
  }
  console.log('');
}
