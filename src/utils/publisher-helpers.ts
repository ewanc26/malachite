import type { PublishResult } from '../types.js';
import { log } from './logger.js';

/**
 * Maximum operations allowed per applyWrites call
 * PDS allows up to 200 operations per call. Each create operation costs 3 rate limit points.
 * We use the full limit for maximum performance.
 * See: https://github.com/bluesky-social/atproto/blob/main/packages/pds/src/api/com/atproto/repo/applyWrites.ts
 */
export const MAX_APPLY_WRITES_OPS = 200;

/**
 * Handle cancellation during import
 * Returns a PublishResult with cancellation status
 */
export function handleCancellation(
  successCount: number,
  errorCount: number,
  totalRecords: number
): PublishResult {
  log.blank();
  log.warn('Import cancelled by user');
  log.info(`Processed: ${successCount.toLocaleString()}/${totalRecords.toLocaleString()} records`);
  log.info(`Remaining: ${(totalRecords - successCount).toLocaleString()} records`);
  return { successCount, errorCount, cancelled: true };
}
