import type { AtpAgent } from '@atproto/api';
import { formatDuration, formatDate } from '../utils/helpers.js';
import { isImportCancelled } from '../utils/killswitch.js';
import { RateLimiter } from '../utils/rate-limiter.js';
import { DynamicBatchCalculator } from '../utils/dynamic-batch-calculator.js';
import { ProactiveRatePacer } from '../utils/proactive-rate-pacer.js';
import { isRateLimitError, normalizeHeaders } from '../utils/rate-limit-headers.js';
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
 * Maximum operations allowed per applyWrites call (PDS hard limit)
 */
const MAX_PDS_BATCH_SIZE = 200;

/**
 * Points cost per record in ATProto
 */
const POINTS_PER_RECORD = 3;

/**
 * Publish records using PROACTIVE rate limiting - never hits rate limits
 * 
 * NEW STRATEGY (Proactive):
 * - Calculate optimal delays to maintain sustainable rate
 * - Spread requests evenly over time
 * - Never approach headroom threshold
 * - Adapt delays based on quota health
 * 
 * SLIDING WINDOW INSIGHT:
 * ATProto rate limits use sliding windows where points used at time T
 * become available at time T + window_duration. By pacing our requests
 * appropriately, we create a steady state where old points become available
 * as we need new ones - maintaining constant throughput without ever
 * hitting limits.
 * 
 * EXAMPLE:
 * Server: 5000 points/hour
 * Target rate: 80% of max = 0.37 rec/s
 * Batch 50 records → wait 135s → steady state at 4000 points
 * Never hits 750-point headroom threshold!
 */
export async function publishRecordsWithApplyWrites(
  agent: AtpAgent | null,
  records: PlayRecord[],
  _initialBatchSize: number, // Ignored - kept for backwards compatibility
  _batchDelay: number, // Ignored - kept for backwards compatibility
  config: Config,
  dryRun = false,
  syncMode = false,
  importState: ImportState | null = null
): Promise<PublishResult> {
  const { RECORD_TYPE } = config;
  const totalRecords = records.length;

  if (dryRun) {
    return handleDryRun(records, config, syncMode);
  }

  if (!agent) {
    throw new Error('Agent is required for publishing');
  }

  // Initialize systems
  const rl = new RateLimiter({ headroom: 0.15 });
  const calculator = new DynamicBatchCalculator(); // For performance metrics
  const pacer = new ProactiveRatePacer(); // NEW: Proactive pacing
  
  log.section('Proactive Rate-Limited Import');
  log.info(`🎯 Proactive pacing strategy: Never hit rate limits`);
  log.info(`📊 Batch size: Optimized for sustainable throughput`);
  log.info(`⏱️  Delay: Calculated to maintain steady rate below limit`);
  log.info(`🔄 Sliding window: Points recover as we use them`);
  log.blank();
  
  // Check if we already know server capacity
  const serverCapacity = rl.getServerCapacity();
  let currentBatchSize: number;
  let currentDelay: number;
  
  if (serverCapacity) {
    // We have server info - calculate optimal batch size
    const actualRemaining = rl.getActualRemaining();
    currentBatchSize = pacer.calculateOptimalBatchSize(
      serverCapacity.limit,
      serverCapacity.windowSeconds,
      actualRemaining,
      MAX_PDS_BATCH_SIZE
    );
    
    // Initial delay will be calculated after first batch
    currentDelay = 500;
    
    const quotaPercent = ((actualRemaining / serverCapacity.limit) * 100).toFixed(1);
    log.info(`ℹ️  Using saved server info: ${serverCapacity.limit} points/${serverCapacity.windowSeconds}s`);
    log.info(`ℹ️  Current quota: ${actualRemaining}/${serverCapacity.limit} (${quotaPercent}%)`);
    log.info(`ℹ️  Starting with optimal batch: ${currentBatchSize} records`);
    
    // Show estimated time to completion
    const eta = pacer.estimateTimeToCompletion(
      totalRecords,
      serverCapacity.limit,
      serverCapacity.windowSeconds,
      actualRemaining
    );
    log.info(`⏱️  Estimated time: ~${formatDuration(eta * 1000)} at sustainable rate`);
  } else {
    // OPTIMIZED: Smart probe - 50 records (150 points) is safe for all standard rate limits
    // Previous: 10 records was too conservative, causing slow first imports
    // Minimum server: 1000 points/hour → 150 points is only 15% → Very safe
    currentBatchSize = 50;
    currentDelay = 500;
    log.info(`🔍 Smart probe: ${currentBatchSize} records (150 points)`);
    log.info(`ℹ️  Safe for all standard rate limits (min 1000 points/hour)`);
    log.info(`ℹ️  Will optimize after learning server capacity`);
  }
  
  log.blank();
  log.info(`Publishing ${formatLocaleNumber(totalRecords)} records...`);
  log.warn('Press Ctrl+C to stop gracefully after current batch');
  log.blank();

  let successCount = 0;
  let errorCount = 0;
  const startTime = Date.now();

  // Resume support
  let startIndex = importState ? getResumeStartIndex(importState) : 0;
  if (importState && startIndex > 0) {
    log.info(`Resuming from record ${startIndex + 1} (${(startIndex / totalRecords * 100).toFixed(1)}% complete)`);
    log.blank();
  }

  let i = startIndex;
  let batchCounter = 0;
  
  while (i < totalRecords) {
    // Check killswitch
    if (isImportCancelled()) {
      return handleCancellation(successCount, errorCount, totalRecords);
    }

    // Get current server capacity and quota
    const capacity = rl.getServerCapacity();
    const actualRemaining = rl.getActualRemaining();
    
    if (!capacity) {
      // Still learning - use probe batch
      log.debug('[Publisher] No capacity info yet, using probe batch');
    } else {
      // Calculate optimal batch size using ACTUAL remaining (not safe quota)
      // The pacer will handle low quota by using recovery mode
      const optimalSize = pacer.calculateOptimalBatchSize(
        capacity.limit,
        capacity.windowSeconds,
        actualRemaining,
        MAX_PDS_BATCH_SIZE
      );
      
      // Apply adaptive scaling from performance metrics
      const adaptiveScale = calculator.calculateAdaptiveScale();
      const scaledSize = Math.floor(optimalSize * adaptiveScale.scale);
      const finalSize = Math.max(1, Math.min(scaledSize, MAX_PDS_BATCH_SIZE));
      
      // Update batch size if changed significantly
      if (Math.abs(finalSize - currentBatchSize) > 5) {
        log.info(`📊 Batch size: ${currentBatchSize} → ${finalSize} records`);
        if (adaptiveScale.scale !== 1.0) {
          log.info(`   └─ Adaptive: ×${adaptiveScale.scale.toFixed(2)} (${adaptiveScale.reason})`);
        }
        currentBatchSize = finalSize;
      }
    }

    const batch = records.slice(i, Math.min(i + currentBatchSize, totalRecords));
    batchCounter++;
    const progress = ((i / totalRecords) * 100).toFixed(1);

    log.progress(
      `[${progress}%] Batch ${batchCounter} (${i + 1}-${Math.min(i + currentBatchSize, totalRecords)}) [${currentBatchSize} records]`
    );

    const batchStartTime = Date.now();

    // Build writes array
    const writes = await Promise.all(
      batch.map(async (record) => ({
        $type: 'com.atproto.repo.applyWrites#create',
        collection: RECORD_TYPE,
        rkey: await generateTIDFromISO(record.playedTime, 'inject:playlist'),
        value: record,
      }))
    );

    // Reserve quota
    const batchPoints = batch.length * POINTS_PER_RECORD;
    await rl.waitForPermit(batchPoints);

    try {
      // Send batch
      const response = await agent.com.atproto.repo.applyWrites({
        repo: agent.session?.did || '',
        writes: writes as any,
      });

      // Success!
      const batchSuccessCount = response.data.results?.length || batch.length;
      successCount += batchSuccessCount;
      const batchDuration = Date.now() - batchStartTime;
      
      // Record success metrics
      calculator.recordSuccess(batch.length, batchDuration);

      // Save state
      if (importState) {
        updateImportState(importState, i + batch.length - 1, batchSuccessCount, 0);
      }

      // Update rate limiter from response headers
      try {
        let respHeaders: Record<string, string> | undefined;
        if ((response as any)?.headers) {
          respHeaders = (response as any).headers;
        }
        
        if (respHeaders && Object.keys(respHeaders).length > 0) {
          const normalized = normalizeHeaders(respHeaders);
          const hasRateLimitHeaders = Object.keys(normalized).some(k => k.includes('ratelimit'));
          
          if (hasRateLimitHeaders) {
            rl.updateFromHeaders(normalized);
            
            // After first response, recalculate optimal settings
            if (!rl.hasServerInfo() && batchCounter === 1) {
            const newCap = rl.getServerCapacity();
            if (newCap) {
            const actualQuota = rl.getActualRemaining();
            const newBatchSize = pacer.calculateOptimalBatchSize(
            newCap.limit,
            newCap.windowSeconds,
            actualQuota,
            MAX_PDS_BATCH_SIZE
            );
            
            const quotaPercent = ((actualQuota / newCap.limit) * 100).toFixed(1);
            log.info(`🎓 Learned server capacity! Optimizing for sustainable throughput`);
            log.info(`   Server: ${newCap.limit} points/${newCap.windowSeconds}s`);
            log.info(`   Current quota: ${actualQuota}/${newCap.limit} (${quotaPercent}%)`);
            log.info(`   Optimal batch: ${newBatchSize} records`);
            
            currentBatchSize = newBatchSize;
            
            // Show estimated completion time
            const eta = pacer.estimateTimeToCompletion(
            totalRecords - successCount,
            newCap.limit,
              newCap.windowSeconds,
              actualQuota
              );
                log.info(`   ETA: ~${formatDuration(eta * 1000)}`);
                }
              }
          }
        }
      } catch (e) {
        log.error(`Error updating from headers: ${e}`);
      }

      i += batch.length;
      
      // PROACTIVE PACING: Calculate optimal delay for next batch
      const cap = rl.getServerCapacity();
      if (cap && i < totalRecords) {
        const actualQuota = rl.getActualRemaining();
        const pacing = pacer.calculateDelay(
          batch.length,
          cap.limit,
          cap.windowSeconds,
          actualQuota
        );
        
        // Update delay if changed significantly
        if (Math.abs(pacing.delayMs - currentDelay) > 200) {
          log.info(`⏱️  Pacing: ${currentDelay}ms → ${pacing.delayMs}ms (${pacing.reason})`);
          currentDelay = pacing.delayMs;
        }
      }

    } catch (error) {
      const err = error as any;
      const batchDuration = Date.now() - batchStartTime;
      
      // Record failure metrics
      calculator.recordFailure(batch.length, batchDuration);

      const rateLimitError = isRateLimitError(err);

      if (rateLimitError) {
        log.warn('⚠️  Rate limit hit (unexpected with proactive pacing) - updating from error headers...');
        
        // Extract and update from error headers
        let headers: Record<string, string> | undefined;
        if (err?.response?.headers) {
          headers = err.response.headers;
        } else if (err?.headers) {
          headers = err.headers;
        }
        
        if (headers && Object.keys(headers).length > 0) {
          const normalized = normalizeHeaders(headers);
          const hasRateLimitHeaders = Object.keys(normalized).some(k => k.includes('ratelimit'));
          if (hasRateLimitHeaders) {
            rl.updateFromHeaders(normalized);
          }
        }
        
        // Wait for permit and retry
        await rl.waitForPermit(batchPoints);
        continue;
        
      } else {
        // Other error - log and skip batch
        errorCount += batch.length;
        log.error(`Batch failed: ${err.message}`);
        
        batch.slice(0, 3).forEach((record) => {
          log.debug(`Failed: ${record.trackName} by ${record.artists[0]?.artistName}`);
        });
        if (batch.length > 3) {
          log.debug(`... and ${batch.length - 3} more failed`);
        }

        if (importState) {
          updateImportState(importState, i + batch.length - 1, 0, batch.length);
        }
        
        i += batch.length;
      }
    }

    // Progress logging
    const elapsed = formatDuration(Date.now() - startTime);
    const recordsPerSecond = successCount / ((Date.now() - startTime) / 1000);
    const remainingRecords = totalRecords - i;
    const estimatedRemaining = remainingRecords / Math.max(recordsPerSecond, 1);
    
    log.debug(
      `Stats - ${elapsed} | ${recordsPerSecond.toFixed(1)} rec/s | ${successCount}/${totalRecords} | ~${formatDuration(estimatedRemaining * 1000)} remaining`
    );
    log.blank();

    // Check killswitch again
    if (isImportCancelled()) {
      return handleCancellation(successCount, errorCount, totalRecords);
    }

    // Wait before next batch (proactive pacing)
    if (i < totalRecords) {
      await new Promise((resolve) => setTimeout(resolve, currentDelay));
    }
  }

  // Complete
  if (importState) {
    completeImport(importState);
  }

  // Show final performance summary
  log.blank();
  log.section('Performance Summary');
  log.info(calculator.getPerformanceSummary());
  const totalDuration = (Date.now() - startTime) / 1000;
  const avgRate = successCount / totalDuration;
  log.info(`Overall: ${formatLocaleNumber(successCount)} records in ${formatDuration(totalDuration * 1000)} (~${avgRate.toFixed(1)} rec/s)`);

  return { successCount, errorCount, cancelled: false };
}

/**
 * Handle dry run mode
 */
function handleDryRun(
  records: PlayRecord[],
  _config: Config,
  syncMode: boolean
): PublishResult {
  const totalRecords = records.length;

  log.section(`DRY RUN MODE ${syncMode ? '(SYNC)' : ''}`);
  if (syncMode) {
    log.info('Sync mode: Only new records will be published');
  }
  log.info(`Total: ${formatLocaleNumber(totalRecords)} records`);
  log.info(`Strategy: Proactive pacing (maintains sustainable rate)`);
  log.info(`Batch size: Optimized from server capacity after first request`);
  log.info(`Delay: Calculated to never hit rate limits`);
  log.blank();

  // Show preview
  const previewCount = Math.min(5, totalRecords);
  log.info(`Preview (first ${previewCount} records):`);
  log.blank();

  for (let i = 0; i < previewCount; i++) {
    const record = records[i];
    const artistName = record.artists[0]?.artistName || 'Unknown Artist';
    
    log.raw(`${i + 1}. ${artistName} - ${record.trackName}`);
    if (record.releaseName) {
      log.raw(`   Album: ${record.releaseName}`);
    }
    log.raw(`   Played: ${formatDate(record.playedTime, true)}`);
    log.raw(`   Source: ${record.musicServiceBaseDomain}`);
    log.raw(`   URL: ${record.originUrl}`);
    
    const mbids: string[] = [];
    if (record.artists[0]?.artistMbId) mbids.push(`Artist: ${record.artists[0].artistMbId}`);
    if (record.recordingMbId) mbids.push(`Track: ${record.recordingMbId}`);
    if (record.releaseMbId) mbids.push(`Album: ${record.releaseMbId}`);
    
    if (mbids.length > 0) {
      log.raw(`   MusicBrainz IDs: ${mbids.join(', ')}`);
    }
    
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
  log.info('');
  log.info('💡 TIP: First batch will probe server capacity, then optimize automatically');

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
