import type { Config } from '../types.js';

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
  const dailyLimit = Math.floor(config.RECORDS_PER_DAY_LIMIT * config.SAFETY_MARGIN);
  
  // Check if we need rate limiting
  const needsRateLimiting = totalRecords > dailyLimit;
  
  if (!needsRateLimiting) {
    // Can import everything in one go
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
  
  // Calculate batch parameters
  // We want to spread records evenly throughout the day
  const minutesPerDay = 24 * 60;
  const batchesPerDay = Math.ceil(recordsPerDay / config.DEFAULT_BATCH_SIZE);
  const delayBetweenBatches = Math.floor((minutesPerDay * 60 * 1000) / batchesPerDay);
  
  // Ensure batch delay is at least minimum
  const batchDelay = Math.max(delayBetweenBatches, config.MIN_BATCH_DELAY);
  
  // Adjust batch size if needed to hit the target
  const adjustedBatchSize = Math.min(
    Math.ceil(recordsPerDay / Math.floor((minutesPerDay * 60 * 1000) / batchDelay)),
    config.MAX_BATCH_SIZE
  );
  
  return {
    batchSize: adjustedBatchSize,
    batchDelay,
    estimatedDays,
    recordsPerDay,
    needsRateLimiting: true,
  };
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
  console.log('\n⚠️  ═══════════════════════════════════════════════════════════════════════════════');
  console.log('⚠️  IMPORTANT: Bluesky AppView Rate Limits');
  console.log('⚠️  ═══════════════════════════════════════════════════════════════════════════════');
  console.log('⚠️');
  console.log('⚠️  Exceeding 10K records per day can rate limit your ENTIRE PDS on Bluesky\'s');
  console.log('⚠️  AppView. This affects ALL users on your PDS, not just your account!');
  console.log('⚠️');
  console.log('⚠️  This importer automatically limits imports to 1K records per day by default');
  console.log('⚠️  with automatic batching and pauses to stay within safe limits.');
  console.log('⚠️');
  console.log('⚠️  See: https://docs.bsky.app/blog/rate-limits-pds-v3');
  console.log('⚠️  ═══════════════════════════════════════════════════════════════════════════════\n');
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
  console.log(`   Total records: ${totalRecords.toLocaleString()}`);
  console.log(`   Daily limit: ${recordsPerDay.toLocaleString()} records/day`);
  console.log(`   Estimated duration: ${estimatedDays} day${estimatedDays > 1 ? 's' : ''}`);
  console.log(`   Batch size: ${batchSize} records`);
  console.log(`   Batch delay: ${(batchDelay / 1000).toFixed(1)}s`);
  
  if (estimatedDays > 1) {
    console.log('\n   The import will automatically pause between days.');
    console.log('   You can safely close and restart the importer - it will resume from where it left off.');
  }
  console.log('');
}
