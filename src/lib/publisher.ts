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

/**
 * Maximum operations allowed per applyWrites call
 * PDS allows up to 200 operations per call. Each create operation costs 3 rate limit points.
 * We use the full limit for maximum performance.
 * See: https://github.com/bluesky-social/atproto/blob/main/packages/pds/src/api/com/atproto/repo/applyWrites.ts
 */
const MAX_APPLY_WRITES_OPS = 200;

/**
 * Publish records using com.atproto.repo.applyWrites for efficient batching
 * with adaptive rate limiting
 */
export async function publishRecordsWithApplyWrites(
  agent: AtpAgent | null,
  records: PlayRecord[],
  batchSize: number,
  batchDelay: number,
  config: Config,
  dryRun = false,
  syncMode = false
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

  console.log(`\n🚀 Starting adaptive import with aggressive settings...`);
  console.log(`   Initial batch size: ${currentBatchSize} records`);
  console.log(`   Initial delay: ${currentBatchDelay}ms`);
  console.log(`   Will automatically adjust based on server response\n`);

  let successCount = 0;
  let errorCount = 0;
  const startTime = Date.now();

  console.log(`Publishing ${totalRecords} records using adaptive batching...`);
  console.log(`\n🚨 Press Ctrl+C to stop gracefully after current batch\n`);

  let i = 0;
  while (i < totalRecords) {
    // Check killswitch before processing batch
    if (isImportCancelled()) {
      return handleCancellation(successCount, errorCount, totalRecords);
    }

    const batch = records.slice(i, Math.min(i + currentBatchSize, totalRecords));
    const batchNum = Math.floor(i / currentBatchSize) + 1;
    const progress = ((i / totalRecords) * 100).toFixed(1);

    console.log(
      `[${progress}%] Batch ${batchNum} (records ${i + 1}-${Math.min(i + currentBatchSize, totalRecords)}) [size: ${currentBatchSize}, delay: ${currentBatchDelay}ms]`
    );

    const batchStartTime = Date.now();

    // Build writes array for applyWrites with TID-based rkeys
    const writes = batch.map((record) => ({
      $type: 'com.atproto.repo.applyWrites#create',
      collection: RECORD_TYPE,
      rkey: generateTIDFromISO(record.playedTime),
      value: record,
    }));

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
      console.log(
        `  ✓ Complete in ${batchDuration}ms (${batchSuccessCount} successful)`
      );

      // Speed up if we're doing well (after 5 consecutive successes)
      if (consecutiveSuccesses >= 5 && currentBatchDelay > config.MIN_BATCH_DELAY) {
        const oldDelay = currentBatchDelay;
        currentBatchDelay = Math.max(
          config.MIN_BATCH_DELAY,
          Math.floor(currentBatchDelay * 0.8)
        );
        if (oldDelay !== currentBatchDelay) {
          console.log(`  ⚡ Speeding up! Delay: ${oldDelay}ms → ${currentBatchDelay}ms`);
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
        console.error(`  ⚠️  Rate limit hit! Backing off...`);
        
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

        console.log(`  📉 Adjusted: batch size → ${currentBatchSize}, delay → ${currentBatchDelay}ms`);
        console.log(`  ⏳ Waiting ${currentBatchDelay}ms before retry...`);
        
        await new Promise((resolve) => setTimeout(resolve, currentBatchDelay));
        
        // Don't advance i, retry this batch
        continue;
        
      } else {
        // Other error - log and continue
        errorCount += batch.length;
        console.error(`  ✗ Batch failed: ${err.message}`);
        
        // Log failed records
        batch.slice(0, 3).forEach((record) => {
          console.error(`     - ${record.trackName} by ${record.artists[0]?.artistName}`);
        });
        if (batch.length > 3) {
          console.error(`     ... and ${batch.length - 3} more`);
        }

        // If too many consecutive failures, slow down
        if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
          currentBatchDelay = Math.min(currentBatchDelay * 2, 10000);
          currentBatchSize = Math.max(Math.floor(currentBatchSize / 2), 10);
          console.log(`  📉 Multiple failures: adjusted to ${currentBatchSize} records, ${currentBatchDelay}ms delay`);
        }
        
        i += batch.length; // Skip failed batch
      }
    }

    const elapsed = formatDuration(Date.now() - startTime);
    const recordsPerSecond = successCount / ((Date.now() - startTime) / 1000);
    const remainingRecords = totalRecords - i;
    const estimatedRemaining = remainingRecords / Math.max(recordsPerSecond, 1);
    
    console.log(
      `  ⏱  Elapsed: ${elapsed} | Speed: ${recordsPerSecond.toFixed(1)} rec/s | Remaining: ~${formatDuration(estimatedRemaining * 1000)}\n`
    );

    // Check again before waiting
    if (isImportCancelled()) {
      return handleCancellation(successCount, errorCount, totalRecords);
    }

    // Wait before next batch (except for last batch)
    if (i < totalRecords) {
      await new Promise((resolve) => setTimeout(resolve, currentBatchDelay));
    }
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

  console.log(`\n=== DRY RUN MODE ${syncMode ? '(SYNC)' : ''} ===`);
  if (syncMode) {
    console.log(`Sync mode enabled: Only new records will be published`);
  }
  console.log(`Would publish ${totalRecords} records using applyWrites`);
  console.log(`Batch size: ${Math.min(batchSize, MAX_APPLY_WRITES_OPS)} records per applyWrites call`);

  if (rateLimitParams.estimatedDays > 1) {
    console.log(
      `Import would span ${rateLimitParams.estimatedDays} days with automatic pauses\n`
    );
  } else {
    console.log(`Estimated time: ${formatDuration(Math.ceil(totalRecords / batchSize) * batchDelay)}\n`);
  }

  // Show first 5 records as preview
  const previewCount = Math.min(5, totalRecords);
  console.log(`Preview of first ${previewCount} records (in processing order):\n`);

  for (let i = 0; i < previewCount; i++) {
    const record = records[i];
    console.log(`${i + 1}. ${record.artists[0]?.artistName} - ${record.trackName}`);
    console.log(`   Album: ${record.releaseName || 'N/A'}`);
    console.log(`   Played: ${formatDate(record.playedTime, true)}`);
    console.log(`   URL: ${record.originUrl}`);

    // Show MusicBrainz IDs if available
    const mbids = [];
    if (record.artists[0]?.artistMbId)
      mbids.push(`Artist: ${record.artists[0].artistMbId}`);
    if (record.recordingMbId) mbids.push(`Recording: ${record.recordingMbId}`);
    if (record.releaseMbId) mbids.push(`Release: ${record.releaseMbId}`);

    if (mbids.length > 0) {
      console.log(`   MBIDs: ${mbids.join(', ')}`);
    }
    console.log('');
  }

  if (totalRecords > previewCount) {
    console.log(`... and ${totalRecords - previewCount} more records\n`);
  }

  console.log('=== DRY RUN COMPLETE ===');
  console.log('No records were actually published.');
  console.log('Remove --dry-run flag to publish for real.\n');

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
  console.log(`\n🛑 Import cancelled by user`);
  console.log(`   Processed: ${successCount}/${totalRecords} records`);
  console.log(`   Remaining: ${totalRecords - successCount} records\n`);
  return { successCount, errorCount, cancelled: true };
}
