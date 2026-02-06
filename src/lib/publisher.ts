import type { AtpAgent } from '@atproto/api';
import { formatDuration, formatDate } from '../utils/helpers.js';
import { isImportCancelled } from '../utils/killswitch.js';
import { RateLimiter } from '../utils/rate-limiter.js';
import { isRateLimitError } from '../utils/rate-limit-headers.js';
import { formatLocaleNumber } from '../utils/platform.js';
import { generateTIDFromISO } from '../utils/tid.js';
import type { PlayRecord, Config, PublishResult } from '../types.js';
import { log } from '../utils/logger.js';
import {
  ImportState,
  updateImportState,
  completeImport,
  getResumeStartIndex,
} from '../utils/import-state.js';

/**
 * Maximum operations allowed per applyWrites call
 * PDS allows up to 200 operations per call. Each create operation costs 3 rate limit points.
 * We use the full limit for maximum performance.
 * See: https://github.com/bluesky-social/atproto/blob/main/packages/pds/src/api/com/atproto/repo/applyWrites.ts
 */
const MAX_APPLY_WRITES_OPS = 200;

/**
 * Publish records using com.atproto.repo.applyWrites for efficient batching
 * with adaptive rate limiting and stateful resume support
 */
export async function publishRecordsWithApplyWrites(
  agent: AtpAgent | null,
  records: PlayRecord[],
  batchSize: number,
  batchDelay: number,
  config: Config,
  dryRun = false,
  syncMode = false,
  importState: ImportState | null = null
): Promise<PublishResult> {
  const { RECORD_TYPE } = config;
  const totalRecords = records.length;

  if (dryRun) {
    return handleDryRun(records, batchSize, batchDelay, config, syncMode);
  }

  if (!agent) {
    throw new Error('Agent is required for publishing');
  }

  // Start with aggressive settings
  let currentBatchSize = Math.min(batchSize, MAX_APPLY_WRITES_OPS);
  let currentBatchDelay = batchDelay;
  
  // Adaptive rate limiting state
  let consecutiveSuccesses = 0;
  let consecutiveFailures = 0;
  const MAX_CONSECUTIVE_FAILURES = 3;
  const POINTS_PER_RECORD = 3; // approximate cost per create operation

  // Persistent rate limiter (reads/writes ~/.malachite/state/rate-limit.json)
  const rl = new RateLimiter({ safety: config.SAFETY_MARGIN });

  log.section('Conservative Adaptive Import');
  log.info(`Initial batch size: ${currentBatchSize} records (conservative)`);
  log.info(`Initial delay: ${currentBatchDelay}ms (2 seconds - very safe)`);
  log.debug(`[publisher.ts] MAX_APPLY_WRITES_OPS=${MAX_APPLY_WRITES_OPS}, POINTS_PER_RECORD=${POINTS_PER_RECORD}`);
  log.debug(`[publisher.ts] Safety margin: ${config.SAFETY_MARGIN}, Records per day limit: ${formatLocaleNumber(config.RECORDS_PER_DAY_LIMIT)}`);
  log.info(`Will automatically adjust based on server response`);
  log.info(`Using conservative settings to protect your PDS`);
  log.blank();
  log.info(`Publishing ${formatLocaleNumber(totalRecords)} records using adaptive batching...`);
  log.warn('Press Ctrl+C to stop gracefully after current batch');
  log.blank();

  let successCount = 0;
  let errorCount = 0;
  const startTime = Date.now();

  // Resume from saved state if available
  let startIndex = importState ? getResumeStartIndex(importState) : 0;
  if (importState && startIndex > 0) {
    log.info(`Resuming from record ${startIndex + 1} (${(startIndex / totalRecords * 100).toFixed(1)}% complete)`);
    log.debug(`[publisher.ts] Import state loaded, resuming at index=${startIndex}`);
    log.blank();
  }

  let i = startIndex;
  while (i < totalRecords) {
    // Check killswitch before processing batch
    if (isImportCancelled()) {
      return handleCancellation(successCount, errorCount, totalRecords);
    }

    const batch = records.slice(i, Math.min(i + currentBatchSize, totalRecords));
    const batchNum = Math.floor(i / currentBatchSize) + 1;
    const progress = ((i / totalRecords) * 100).toFixed(1);

    log.progress(
      `[${progress}%] Batch ${batchNum} (records ${i + 1}-${Math.min(i + currentBatchSize, totalRecords)}) [size: ${currentBatchSize}, delay: ${currentBatchDelay}ms]`
    );
    log.debug(`[publisher.ts] Starting batch: index=${i}, size=${batch.length}`);

    const batchStartTime = Date.now();

    // Build writes array for applyWrites with TID-based rkeys
    const writes = await Promise.all(
      batch.map(async (record) => ({
        $type: 'com.atproto.repo.applyWrites#create',
        collection: RECORD_TYPE,
        rkey: await generateTIDFromISO(record.playedTime, 'inject:playlist'),
        value: record,
      }))
    );

    // Reserve quota (points) for this batch before sending. This will wait until
    // server reset if quota is exhausted (persisted across runs).
    const batchPoints = batch.length * POINTS_PER_RECORD;
    log.debug(`[publisher.ts] Reserving quota: batch_size=${batch.length}, points=${batchPoints} (${POINTS_PER_RECORD} per record)`);
    const permit = await rl.waitForPermit(batchPoints);
    if (!permit) {
      const backoffMs = Math.min(currentBatchDelay * 2, 30000);
      log.warn(`Rate limiter cannot grant permit; backing off ${backoffMs}ms`);
      await new Promise((resolve) => setTimeout(resolve, backoffMs));
    }

    try {
      // Call applyWrites with the batch
      log.debug(`[publisher.ts] Sending applyWrites request for ${batch.length} records to PDS...`);
      const response = await agent.com.atproto.repo.applyWrites({
        repo: agent.session?.did || '',
        writes: writes as any,
      });

      // Success!
      const batchSuccessCount = response.data.results?.length || batch.length;
      successCount += batchSuccessCount;
      consecutiveSuccesses++;
      consecutiveFailures = 0;

      const batchDuration = Date.now() - batchStartTime;
      log.debug(`[publisher.ts] Batch success: ${batchSuccessCount}/${batch.length} records published in ${batchDuration}ms`);

      // Save state after successful batch
      if (importState) {
        updateImportState(importState, i + batch.length - 1, batchSuccessCount, 0);
      }

      // Speed up if we're doing well (after 5 consecutive successes)
      if (consecutiveSuccesses >= 5 && currentBatchDelay > config.MIN_BATCH_DELAY) {
        const oldDelay = currentBatchDelay;
        currentBatchDelay = Math.max(
          config.MIN_BATCH_DELAY,
          Math.floor(currentBatchDelay * 0.8)
        );
        if (oldDelay !== currentBatchDelay) {
          log.info(`⚡ Speeding up! Delay: ${oldDelay}ms → ${currentBatchDelay}ms`);
        }
        consecutiveSuccesses = 0;
      }

      // Update limiter from any headers the server returned
      try {
        const respHeaders = (response as any)?.headers || (response as any)?.data?.headers;
        if (respHeaders) {
          log.debug(`[publisher.ts] Updating rate limiter from response headers`);
          rl.updateFromHeaders(respHeaders as Record<string,string>);
        }
      } catch (e) {
        log.debug(`[publisher.ts] Could not extract headers from response: ${e}`);
      }

      i += batch.length;

    } catch (error) {
      const err = error as any;
      consecutiveFailures++;
      consecutiveSuccesses = 0;

      const rateLimitError = isRateLimitError(err);
      log.debug(`[publisher.ts] Batch error: rateLimitError=${rateLimitError}, consecutiveFailures=${consecutiveFailures}`);

      if (rateLimitError) {
        log.warn('Rate limit hit! Inspecting server headers...');
        const headers = err.response?.headers || err.headers || err.data?.headers || {};
        log.debug(`[publisher.ts] Rate limit error headers: ${JSON.stringify(Object.keys(headers))}`);
        // Wait for permit for this batch (this will wait until reset if necessary)
        const batchPoints = batch.length * POINTS_PER_RECORD;
        log.debug(`[publisher.ts] Waiting for rate limit reset, requesting ${batchPoints} points...`);
        const waitOk = await rl.waitForPermit(batchPoints);
        if (!waitOk) {
          const backoffMs = Math.min(Math.pow(2, consecutiveFailures) * 1000, 60000);
          log.info(`Fallback backoff: waiting ${backoffMs}ms before retry...`);
          await new Promise((resolve) => setTimeout(resolve, backoffMs));
        }
        continue;
        
      } else {
        // Other error - log and continue
        errorCount += batch.length;
        log.error(`Batch failed: ${err.message}`);
        log.debug(`[publisher.ts] Error details: status=${err.response?.status}, code=${err.code}`);
        
        // Log failed records
        batch.slice(0, 3).forEach((record) => {
          log.debug(`Failed: ${record.trackName} by ${record.artists[0]?.artistName}`);
        });
        if (batch.length > 3) {
          log.debug(`... and ${batch.length - 3} more failed`);
        }

        // Save state with errors
        if (importState) {
          updateImportState(importState, i + batch.length - 1, 0, batch.length);
        }

        // If too many consecutive failures, slow down
        if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
          currentBatchDelay = Math.min(currentBatchDelay * 2, 10000);
          currentBatchSize = Math.max(Math.floor(currentBatchSize / 2), 10);
          log.warn(`📉 Multiple failures (${consecutiveFailures}): adjusted to ${currentBatchSize} records, ${currentBatchDelay}ms delay`);
          log.debug(`[publisher.ts] Slowing down due to consecutive failures`);
        }
        
        i += batch.length; // Skip failed batch
      }
    }

    const elapsed = formatDuration(Date.now() - startTime);
    const recordsPerSecond = successCount / ((Date.now() - startTime) / 1000);
    const remainingRecords = totalRecords - i;
    const estimatedRemaining = remainingRecords / Math.max(recordsPerSecond, 1);
    
    log.debug(
      `[publisher.ts] Stats - Elapsed: ${elapsed} | Speed: ${recordsPerSecond.toFixed(1)} rec/s | Success: ${successCount}/${totalRecords} | Remaining: ~${formatDuration(estimatedRemaining * 1000)}`
    );
    log.blank();

    // Check again before waiting
    if (isImportCancelled()) {
      return handleCancellation(successCount, errorCount, totalRecords);
    }

    // Wait before next batch (except for last batch)
    if (i < totalRecords) {
      await new Promise((resolve) => setTimeout(resolve, currentBatchDelay));
    }
  }

  // Mark import as complete
  if (importState) {
    completeImport(importState);
    log.debug('Import state saved as completed');
  }

  return { successCount, errorCount, cancelled: false };
}



/**
 * Handle dry run mode
 */
function handleDryRun(
  records: PlayRecord[],
  batchSize: number,
  batchDelay: number,
  config: Config,
  syncMode: boolean
): PublishResult {
  const totalRecords = records.length;

  // Ensure batch size doesn't exceed applyWrites limit
  batchSize = Math.min(batchSize, MAX_APPLY_WRITES_OPS);

  // Calculate estimated duration
  const estimatedBatches = Math.ceil(totalRecords / batchSize);
  const estimatedDuration = estimatedBatches * batchDelay;
  
  // Check if we'll exceed daily limit
  const recordsPerDay = config.RECORDS_PER_DAY_LIMIT || Number.MAX_SAFE_INTEGER;
  const needsRateLimiting = totalRecords > recordsPerDay;
  
  if (needsRateLimiting) {
    const estimatedDays = Math.ceil(totalRecords / recordsPerDay);
    log.warn('⚠️  Large Import Detected');
    log.warn(`This import exceeds the daily limit of ${formatLocaleNumber(recordsPerDay)} records`);
    log.warn(`Estimated duration: ${estimatedDays} days with automatic pauses`);
    log.blank();
  }

  log.section(`DRY RUN MODE ${syncMode ? '(SYNC)' : ''}`);
  if (syncMode) {
    log.info('Sync mode: Only new records will be published');
  }
  log.info(`Total: ${formatLocaleNumber(totalRecords)} records`);
  log.info(`Batch: ${Math.min(batchSize, MAX_APPLY_WRITES_OPS)} records per call`);
  
  if (needsRateLimiting) {
    const estimatedDays = Math.ceil(totalRecords / recordsPerDay);
    log.info(`Duration: ${estimatedDays} days with automatic pauses`);
  } else {
    log.info(`Time: ~${formatDuration(estimatedDuration)}`);
  }
  log.blank();

  // Show first 5 records as preview
  const previewCount = Math.min(5, totalRecords);
  log.info(`Preview (first ${previewCount} records):`);
  log.blank();

  for (let i = 0; i < previewCount; i++) {
    const record = records[i];
    const artistName = record.artists[0]?.artistName || 'Unknown Artist';
    
    log.raw(`${i + 1}. ${artistName} - ${record.trackName}`);
    
    // Album/Release
    if (record.releaseName) {
      log.raw(`   Album: ${record.releaseName}`);
    }
    
    // Timestamp
    log.raw(`   Played: ${formatDate(record.playedTime, true)}`);
    
    // Source and URL
    log.raw(`   Source: ${record.musicServiceBaseDomain}`);
    log.raw(`   URL: ${record.originUrl}`);
    
    // MusicBrainz IDs (if available)
    const mbids: string[] = [];
    if (record.artists[0]?.artistMbId) mbids.push(`Artist: ${record.artists[0].artistMbId}`);
    if (record.recordingMbId) mbids.push(`Track: ${record.recordingMbId}`);
    if (record.releaseMbId) mbids.push(`Album: ${record.releaseMbId}`);
    
    if (mbids.length > 0) {
      log.raw(`   MusicBrainz IDs: ${mbids.join(', ')}`);
    }
    
    // Record metadata
    log.raw(`   Record Type: ${record.$type}`);
    log.raw(`   Client: ${record.submissionClientAgent}`);
    
    log.blank();
  }

  if (totalRecords > previewCount) {
    log.info(`... and ${formatLocaleNumber(totalRecords - previewCount)} more records`);
    log.blank();
  }

  log.section('DRY RUN COMPLETE');
  log.info('No records were published.');
  log.info('Remove --dry-run to publish for real.');

  return { successCount: totalRecords, errorCount: 0, cancelled: false };
}

/**
 * Handle cancellation
 */
function handleCancellation(
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
