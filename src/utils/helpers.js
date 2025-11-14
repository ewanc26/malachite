/**
 * Utility functions for the Last.fm importer
 */

/**
 * Format duration in human-readable format
 */
export function formatDuration(milliseconds) {
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
export function calculateOptimalBatchSize(totalRecords, batchDelay, config) {
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
