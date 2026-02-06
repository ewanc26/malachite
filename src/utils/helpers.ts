/**
 * Utility functions for the Last.fm importer
 */
import type { Config, PlayRecord } from '../types.js';

/**
 * Get user's locale from environment or default to system locale
 */
function getUserLocale(): string {
  // Try to get locale from environment variables
  const envLang = process.env.LANG?.split('.')[0] ||
    process.env.LC_ALL?.split('.')[0];

  // Filter out invalid locales (like "C" or "POSIX")
  if (envLang && envLang !== 'C' && envLang !== 'POSIX') {
    // FIX: Replace underscore with hyphen to satisfy BCP 47 (e.g., en_GB -> en-GB) [cite: 413, 414]
    return envLang.replace('_', '-');
  }

  // Try system locale
  try {
    const systemLocale = Intl.DateTimeFormat().resolvedOptions().locale;
    if (systemLocale && systemLocale !== 'C') {
      return systemLocale;
    }
  } catch (e) {
    // Ignore errors
  }

  // Default to UK format
  return 'en-GB';
}

/**
 * Format a date in a locale-aware way
 * @param date - Date string or Date object
 * @param includeTime - Whether to include time in the output
 * @returns Formatted date string
 */
export function formatDate(date: string | Date, includeTime: boolean = false): string {
  const dateObj = typeof date === 'string' ? new Date(date) : date;
  const locale = getUserLocale();
  
  if (includeTime) {
    return dateObj.toLocaleString(locale, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      timeZoneName: 'short'
    });
  } else {
    return dateObj.toLocaleDateString(locale, {
      year: 'numeric',
      month: 'short',
      day: 'numeric'
    });
  }
}

/**
 * Format a date range in a locale-aware way
 * @param startDate - Start date
 * @param endDate - End date
 * @returns Formatted date range string
 */
export function formatDateRange(startDate: string | Date, endDate: string | Date): string {
  return `${formatDate(startDate)} to ${formatDate(endDate)}`;
}

/**
 * Format duration in human-readable format
 */
export function formatDuration(milliseconds: number): string {
  const seconds = Math.floor(milliseconds / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  
  if (hours > 0) {
    const mins = minutes % 60;
    return `${hours}h ${mins}m`;
  } else if (minutes > 0) {
    const secs = seconds % 60;
    return `${minutes}m ${secs}s`;
  } else {
    return `${seconds}s`;
  }
}

/**
 * Calculate optimal batch size based on total records and rate limits
 * Uses a logarithmic scaling approach to balance throughput with API safety
 */
export function calculateOptimalBatchSize(totalRecords: number, batchDelay: number, config: Config): number {
  const {
    MIN_RECORDS_FOR_SCALING,
    BASE_BATCH_SIZE,
    MAX_BATCH_SIZE,
    SCALING_FACTOR,
    DEFAULT_BATCH_DELAY
  } = config;
  
  const delay = batchDelay || DEFAULT_BATCH_DELAY;
  
  // For very small datasets, use minimal batches
  if (totalRecords <= 50) {
    return 3;
  }
  
  // For small to medium datasets, use conservative batching
  if (totalRecords <= MIN_RECORDS_FOR_SCALING) {
    return BASE_BATCH_SIZE;
  }
  
  // Logarithmic scaling
  const logScale = Math.log2(totalRecords / MIN_RECORDS_FOR_SCALING);
  const calculatedSize = Math.floor(BASE_BATCH_SIZE + (logScale * SCALING_FACTOR));
  
  // Apply maximum cap
  let optimalSize = Math.min(calculatedSize, MAX_BATCH_SIZE);
  
  // Adjust based on batch delay
  if (delay < 1500 && optimalSize > 15) {
    optimalSize = Math.floor(optimalSize * 0.75);
  }
  
  // Ensure batch size is at least 3
  return Math.max(3, optimalSize);
}

/**
 * Normalize a string for comparison (lowercase, remove extra spaces, punctuation)
 * Used for duplicate detection and record matching
 */
export function normalizeString(str: string): string {
  return str
    .toLowerCase()
    .replace(/[^\w\s]/g, '') // Remove punctuation
    .replace(/\s+/g, ' ')     // Normalize whitespace
    .trim();
}

/**
 * Sort records chronologically
 * @param records - Records to sort
 * @param reverseChronological - If true, sort newest first; otherwise sort oldest first
 * @returns Sorted records
 */
export function sortRecords(records: PlayRecord[], reverseChronological = false): PlayRecord[] {
  if (records.length === 0) {
    return records;
  }

  console.log(`Sorting records ${reverseChronological ? 'newest' : 'oldest'} first...`);

  records.sort((a, b) => {
    const timeA = new Date(a.playedTime).getTime();
    const timeB = new Date(b.playedTime).getTime();
    return reverseChronological ? timeB - timeA : timeA - timeB;
  });

  const firstPlay = formatDate(records[0].playedTime);
  const lastPlay = formatDate(records[records.length - 1].playedTime);
  console.log(`✓ Sorted ${records.length} records`);
  console.log(`  First: ${firstPlay}`);
  console.log(`  Last: ${lastPlay}\n`);

  return records;
}

/**
 * Logs rate limiting and batching information to the console.
 * Note: This function cannot import log from logger.ts to avoid circular dependencies,
 * so it uses console.log directly. The CLI controls the log level, so this output
 * is appropriately controlled.
 */
export function showRateLimitInfo(
  totalRecords: number,
  batchSize: number,
  batchDelay: number,
  estimatedDays: number,
  dailyLimit: number
): void {
  console.log('\n📊 Rate Limiting Information:');
  console.log(`   Total records: ${totalRecords.toLocaleString()}`);
  console.log(`   Daily limit: ${dailyLimit.toLocaleString()} records/day`);
  console.log(`   Estimated duration: ${estimatedDays} day${estimatedDays > 1 ? 's' : ''}`);
  console.log(`   Batch size: ${batchSize} records`);
  console.log(`   Batch delay: ${(batchDelay / 1000).toFixed(1)}s`);
  
  if (estimatedDays > 1) {
    console.log('\n   The import will automatically pause between days.');
    console.log('   You can safely close and restart the importer - it will resume from where it left off.');
  }
  console.log('');
}