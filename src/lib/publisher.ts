import type { AtpAgent } from '@atproto/api';
import { formatDuration, formatDate } from '../utils/helpers.js';
import { isImportCancelled } from '../utils/killswitch.js';
import {
  calculateDailySchedule,
  displayRateLimitWarning,
  displayRateLimitInfo,
  calculateRateLimitedBatches,
} from '../utils/rate-limiter.js';
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

  log.section('Conservative Adaptive Import');
  log.info(`Initial batch size: ${currentBatchSize} records (conservative)`);
  log.info(`Initial delay: ${currentBatchDelay}ms (2 seconds - very safe)`);
  log.info(`Will automatically adjust based on server response`);
  log.info(`Using conservative settings to protect your PDS`);
  log.blank();
  log.info(`Publishing ${totalRecords.toLocaleString()} records using adaptive batching...`);
  log.warn('Press Ctrl+C to stop gracefully after current batch');
  log.blank();

  let successCount = 0;
  let errorCount = 0;
  const startTime = Date.now();

  // Resume from saved state if available
  let startIndex = importState ? getResumeStartIndex(importState) : 0;
  if (importState && startIndex > 0) {
    log.info(`Resuming from record ${startIndex + 1} (${(startIndex / totalRecords * 100).toFixed(1)}% complete)`);
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

    try {
      // Call applyWrites with the batch
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
      log.debug(`Batch complete in ${batchDuration}ms (${batchSuccessCount} successful)`);

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

      i += batch.length;

    } catch (error) {
      const err = error as any;
      const isRateLimitError = 
        err.status === 429 || 
        err.message?.includes('rate limit') ||
        err.message?.includes('too many requests');

      consecutiveFailures++;
      consecutiveSuccesses = 0;

      if (isRateLimitError) {
        log.warn('Rate limit hit! Backing off...');
        
        // Exponential backoff
        const backoffMultiplier = Math.pow(2, consecutiveFailures);
        currentBatchDelay = Math.min(
          currentBatchDelay * backoffMultiplier,
          60000 // Max 60 seconds
        );
        
        // Also reduce batch size
        currentBatchSize = Math.max(
          Math.floor(currentBatchSize / 2),
          10 // Minimum 10 records
        );

        log.info(`📉 Adjusted: batch size → ${currentBatchSize}, delay → ${currentBatchDelay}ms`);
        log.info(`⏳ Waiting ${currentBatchDelay}ms before retry...`);
        
        await new Promise((resolve) => setTimeout(resolve, currentBatchDelay));
        
        // Don't advance i, retry this batch
        continue;
        
      } else {
        // Other error - log and continue
        errorCount += batch.length;
        log.error(`Batch failed: ${err.message}`);
        
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
          log.warn(`📉 Multiple failures: adjusted to ${currentBatchSize} records, ${currentBatchDelay}ms delay`);
        }
        
        i += batch.length; // Skip failed batch
      }
    }

    const elapsed = formatDuration(Date.now() - startTime);
    const recordsPerSecond = successCount / ((Date.now() - startTime) / 1000);
    const remainingRecords = totalRecords - i;
    const estimatedRemaining = remainingRecords / Math.max(recordsPerSecond, 1);
    
    log.debug(
      `Elapsed: ${elapsed} | Speed: ${recordsPerSecond.toFixed(1)} rec/s | Remaining: ~${formatDuration(estimatedRemaining * 1000)}`
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

  // Calculate rate limiting info
  const rateLimitParams = calculateRateLimitedBatches(totalRecords, config);

  if (rateLimitParams.needsRateLimiting) {
    displayRateLimitWarning();
    batchSize = rateLimitParams.batchSize;
    batchDelay = rateLimitParams.batchDelay;

    // Ensure batch size doesn't exceed applyWrites limit
    batchSize = Math.min(batchSize, MAX_APPLY_WRITES_OPS);

    displayRateLimitInfo(
      totalRecords,
      batchSize,
      batchDelay,
      rateLimitParams.estimatedDays,
      rateLimitParams.recordsPerDay
    );

    if (rateLimitParams.estimatedDays > 1) {
      // Only show daily schedule in verbose/debug mode
      if (log.getLevel() <= 0) { // DEBUG level
        const dailySchedule = calculateDailySchedule(
          totalRecords,
          batchSize,
          batchDelay,
          rateLimitParams.recordsPerDay
        );

        console.log('📅 Multi-Day Import Schedule:\n');
        dailySchedule.forEach((day) => {
          console.log(`   Day ${day.day}:`);
          console.log(`     Records ${day.recordsStart + 1}-${day.recordsEnd} (${day.recordsCount} total)`);
          if (day.pauseAfter) {
            console.log(`     → Pause 24h after completion`);
          }
        });
        console.log('');
      }
    }
  }

  log.section(`DRY RUN MODE ${syncMode ? '(SYNC)' : ''}`);
  if (syncMode) {
    log.info('Sync mode: Only new records will be published');
  }
  log.info(`Total: ${totalRecords.toLocaleString()} records`);
  log.info(`Batch: ${Math.min(batchSize, MAX_APPLY_WRITES_OPS)} records per call`);
  
  if (rateLimitParams.estimatedDays > 1) {
    log.info(`Duration: ${rateLimitParams.estimatedDays} days with automatic pauses`);
  } else {
    log.info(`Time: ~${formatDuration(Math.ceil(totalRecords / batchSize) * batchDelay)}`);
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
    log.info(`... and ${(totalRecords - previewCount).toLocaleString()} more records`);
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
