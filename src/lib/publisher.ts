import type { AtpAgent } from '@atproto/api';
import { formatDuration } from '../utils/helpers.js';
import { isImportCancelled } from '../utils/killswitch.js';
import {
  calculateDailySchedule,
  displayRateLimitWarning,
  displayRateLimitInfo,
  calculateRateLimitedBatches,
} from '../utils/rate-limiter.js';
import type { PlayRecord, Config, PublishResult } from '../types.js';

/**
 * Publish records in batches with rate limiting and multi-day support
 */
export async function publishRecords(
  agent: AtpAgent | null,
  records: PlayRecord[],
  batchSize: number,
  batchDelay: number,
  config: Config,
  dryRun = false
): Promise<PublishResult> {
  const { RECORD_TYPE } = config;
  const totalRecords = records.length;

  if (dryRun) {
    return handleDryRun(records, batchSize, batchDelay, config);
  }

  if (!agent) {
    throw new Error('Agent is required for publishing');
  }

  // Calculate rate-limited batch parameters
  const rateLimitParams = calculateRateLimitedBatches(totalRecords, config);

  // Override with calculated parameters if rate limiting is needed
  if (rateLimitParams.needsRateLimiting) {
    displayRateLimitWarning();
    batchSize = rateLimitParams.batchSize;
    batchDelay = rateLimitParams.batchDelay;
  }

  displayRateLimitInfo(
    totalRecords,
    batchSize,
    batchDelay,
    rateLimitParams.estimatedDays,
    rateLimitParams.recordsPerDay
  );

  // Calculate daily schedule if multi-day import
  const dailySchedule =
    rateLimitParams.estimatedDays > 1
      ? calculateDailySchedule(
          totalRecords,
          batchSize,
          batchDelay,
          rateLimitParams.recordsPerDay
        )
      : null;

  let successCount = 0;
  let errorCount = 0;
  const startTime = Date.now();

  const totalBatches = Math.ceil(totalRecords / batchSize);
  const estimatedTime = formatDuration(totalBatches * batchDelay);

  console.log(`Publishing ${totalRecords} records in batches of ${batchSize}...`);
  console.log(`Total batches: ${totalBatches}`);
  if (!dailySchedule) {
    console.log(`Estimated time: ${estimatedTime}`);
  }
  console.log(`\n🚨 Press Ctrl+C to stop gracefully after current batch\n`);

  // If multi-day, process day by day
  if (dailySchedule) {
    for (const day of dailySchedule) {
      console.log(`\n╔═══════════════════════════════════════════════════════════════╗`);
      console.log(`║  DAY ${day.day} of ${rateLimitParams.estimatedDays}`);
      console.log(`║  Records: ${day.recordsStart + 1}-${day.recordsEnd} (${day.recordsCount} total)`);
      console.log(`╚═══════════════════════════════════════════════════════════════╝\n`);

      const dayRecords = records.slice(day.recordsStart, day.recordsEnd);
      const result = await processDayBatch(
        agent,
        dayRecords,
        batchSize,
        batchDelay,
        RECORD_TYPE,
        day.recordsStart,
        totalRecords,
        startTime
      );

      successCount += result.successCount;
      errorCount += result.errorCount;

      if (result.cancelled) {
        return { successCount, errorCount, cancelled: true };
      }

      // Pause between days
      if (day.pauseAfter) {
        console.log(`\n⏸️  Pausing for 24 hours before continuing...`);
        console.log(`   Next batch will start at: ${new Date(Date.now() + day.pauseDuration).toLocaleString()}`);
        console.log(`   Progress: ${successCount}/${totalRecords} records completed\n`);
        console.log(`   💡 You can safely stop (Ctrl+C) and restart later.\n`);

        await new Promise((resolve) => setTimeout(resolve, day.pauseDuration));
      }
    }
  } else {
    // Single day import - process normally
    const result = await processDayBatch(
      agent,
      records,
      batchSize,
      batchDelay,
      RECORD_TYPE,
      0,
      totalRecords,
      startTime
    );

    successCount = result.successCount;
    errorCount = result.errorCount;

    if (result.cancelled) {
      return { successCount, errorCount, cancelled: true };
    }
  }

  return { successCount, errorCount, cancelled: false };
}

/**
 * Process a batch of records (for a single day or entire import)
 */
async function processDayBatch(
  agent: AtpAgent,
  records: PlayRecord[],
  batchSize: number,
  batchDelay: number,
  recordType: string,
  globalOffset: number,
  totalRecords: number,
  startTime: number
): Promise<PublishResult> {
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < records.length; i += batchSize) {
    // Check killswitch before processing batch
    if (isImportCancelled()) {
      return handleCancellation(successCount, errorCount, totalRecords);
    }

    const batch = records.slice(i, i + batchSize);
    const globalIndex = globalOffset + i;
    const batchNum = Math.floor(globalIndex / batchSize) + 1;
    const progress = (((globalOffset + i) / totalRecords) * 100).toFixed(1);

    console.log(
      `[${progress}%] Batch ${batchNum} (records ${globalOffset + i + 1}-${Math.min(globalOffset + i + batchSize, globalOffset + records.length)})`
    );

    // Process batch records
    const batchStartTime = Date.now();
    for (const record of batch) {
      // Check killswitch during batch processing
      if (isImportCancelled()) {
        console.log(`  ⚠️  Stopping mid-batch...`);
        break;
      }

      try {
        await agent.com.atproto.repo.createRecord({
          repo: agent.session?.did || '',
          collection: recordType,
          record,
        });
        successCount++;
      } catch (error) {
        errorCount++;
        const err = error as Error;
        console.error(`  ✗ Failed: ${record.trackName} - ${err.message}`);
      }
    }

    const batchDuration = Date.now() - batchStartTime;
    const elapsed = formatDuration(Date.now() - startTime);
    const remaining = formatDuration(
      ((totalRecords - (globalOffset + i + batchSize)) / batchSize) * batchDelay
    );

    console.log(
      `  ✓ Complete in ${batchDuration}ms (${successCount} successful, ${errorCount} failed)`
    );

    // Only show time estimates if not cancelled
    if (!isImportCancelled()) {
      console.log(`  ⏱  Elapsed: ${elapsed} | Remaining: ~${remaining}\n`);
    }

    // Check again before waiting
    if (isImportCancelled()) {
      return handleCancellation(successCount, errorCount, totalRecords);
    }

    // Wait before next batch (except for last batch)
    if (i + batchSize < records.length) {
      await new Promise((resolve) => setTimeout(resolve, batchDelay));
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
  config: Config
): PublishResult {
  const totalRecords = records.length;

  // Calculate rate limiting info
  const rateLimitParams = calculateRateLimitedBatches(totalRecords, config);

  if (rateLimitParams.needsRateLimiting) {
    displayRateLimitWarning();
    batchSize = rateLimitParams.batchSize;
    batchDelay = rateLimitParams.batchDelay;

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

  console.log(`\n=== DRY RUN MODE ===`);
  console.log(`Would publish ${totalRecords} records in batches of ${batchSize}`);

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
    console.log(`   Played: ${record.playedTime}`);
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
