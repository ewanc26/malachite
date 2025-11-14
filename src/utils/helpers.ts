/**
 * Utility functions for the Last.fm importer
 */
import type { Config } from '../types.js';

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
 * Logs rate limiting and batching information to the console.
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