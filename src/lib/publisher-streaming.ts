import type { SafeAgent } from './safe-agent.js';
import { formatDuration } from '../utils/helpers.js';
import { isImportCancelled } from '../utils/killswitch.js';
import { generateTIDFromISO } from '../utils/tid.js';
import type { Config, PublishResult } from '../types.js';
import { log } from '../utils/logger.js';
import { StreamDB } from '../utils/stream-db.js';
import {
  updateImportState,
  completeImport,
  getResumeStartIndex,
  ImportState,
} from '../utils/import-state.js';
import { AdaptiveRateLimiter } from '../utils/adaptive-rate-limiter.js';
import { handleCancellation } from '../utils/publisher-helpers.js';
import { MAX_APPLY_WRITES_OPS } from '../constants.js';

/**
 * Publish records using streaming database with adaptive rate limiting
 */
export async function publishRecordsStreaming(
  agent: SafeAgent | null,
  db: StreamDB,
  batchSize: number,
  batchDelay: number,
  config: Config,
  dryRun = false,
  importState: ImportState | null = null
): Promise<PublishResult> {
  const { RECORD_TYPE } = config;
  const totalRecords = db.count();
  
  if (dryRun) {
    return handleDryRun(db, totalRecords, batchSize, batchDelay);
  }
  
  if (!agent) {
    throw new Error('Agent is required for publishing');
  }
  
  // Initialize adaptive rate limiter
  const rateLimiter = new AdaptiveRateLimiter(
    config,
    Math.min(batchSize, MAX_APPLY_WRITES_OPS),
    batchDelay,
    MAX_APPLY_WRITES_OPS
  );
  
  log.section('Streaming Adaptive Import');
  log.info(`Initial batch size: ${rateLimiter.getCurrentBatchSize()} records`);
  log.info(`Initial delay: ${rateLimiter.getCurrentDelay()}ms`);
  log.info(`Publishing ${totalRecords.toLocaleString()} records using streaming...`);
  log.warn('Press Ctrl+C to stop gracefully after current batch');
  log.blank();
  
  let successCount = 0;
  let errorCount = 0;
  const startTime = Date.now();
  
  // Resume from saved state if available
  let recordsProcessed = importState ? getResumeStartIndex(importState) : 0;
  if (importState && recordsProcessed > 0) {
    log.info(`Resuming from record ${recordsProcessed + 1} (${(recordsProcessed / totalRecords * 100).toFixed(1)}% complete)`);
    log.blank();
  }
  
  let batchNum = 0;
  let retryCount = 0;
  const MAX_RETRIES = 3;
  
  // Process records in batches from database
  for (const batch of db.getBatches(rateLimiter.getCurrentBatchSize())) {
    // Skip batches we've already processed
    if (recordsProcessed >= batch.length) {
      recordsProcessed -= batch.length;
      continue;
    }
    
    // Check killswitch
    if (isImportCancelled()) {
      return handleCancellation(successCount, errorCount, totalRecords);
    }
    
    // Check circuit breaker
    if (rateLimiter.isCircuitBreakerOpen()) {
      log.error('🔒 Circuit breaker is open - too many failures detected');
      log.warn('⏸️  Entering recovery cooldown period...');
      
      // Wait for circuit breaker timeout
      await rateLimiter.wait(() => isImportCancelled());
      
      if (isImportCancelled()) {
        return handleCancellation(successCount, errorCount, totalRecords);
      }
      
      log.info('🔓 Circuit breaker recovery period complete - resuming operations');
    }
    
    batchNum++;
    const progress = ((successCount / totalRecords) * 100).toFixed(1);
    const batchStats = rateLimiter.getStats();
    
    log.progress(
      `[${progress}%] Batch ${batchNum} (${batch.length} records) [size: ${batchStats.batchSize}, delay: ${batchStats.delay}ms, success: ${batchStats.successRate}%]`
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
    
    let batchSuccess = false;
    
    while (retryCount <= MAX_RETRIES && !batchSuccess) {
      try {
        const response = await agent.com.atproto.repo.applyWrites({
          repo: agent.session?.did || '',
          writes: writes as any,
        });
        
        const batchSuccessCount = response.data.results?.length || batch.length;
        const responseTime = Date.now() - batchStartTime;
        
        successCount += batchSuccessCount;
        batchSuccess = true;
        retryCount = 0; // Reset retry counter on success
        
        // Report success to rate limiter
        await rateLimiter.onSuccess(responseTime);
        
        log.debug(`Batch complete in ${responseTime}ms (${batchSuccessCount} successful)`);
        
        // Save state
        if (importState) {
          updateImportState(importState, successCount - 1, batchSuccessCount, 0);
        }
        
      } catch (error) {
        const err = error as any;
        
        // Detect rate limiting
        const detection = rateLimiter.detectRateLimit(err);
        
        if (detection.isRateLimited) {
          log.warn(`⚠️  Rate limit detected (confidence: ${detection.confidence})`);
          
          // Report rate limit to limiter and get wait time
          const waitTime = await rateLimiter.onRateLimit(err);
          
          retryCount++;
          
          if (retryCount <= MAX_RETRIES) {
            log.info(`⏳ Waiting ${Math.ceil(waitTime / 1000)}s before retry ${retryCount}/${MAX_RETRIES}...`);
            
            // Wait with cancellation support
            const completed = await rateLimiter.wait(() => isImportCancelled());
            
            if (!completed) {
              return handleCancellation(successCount, errorCount, totalRecords);
            }
            
            // Update batch size for retry
            const newBatchSize = rateLimiter.getCurrentBatchSize();
            if (batch.length > newBatchSize) {
              log.info(`📉 Reducing batch size for retry: ${batch.length} → ${newBatchSize}`);
            }
            
            continue; // Retry the batch
          } else {
            log.error(`❌ Max retries (${MAX_RETRIES}) exceeded for batch`);
            errorCount += batch.length;
            
            if (importState) {
              updateImportState(importState, successCount + errorCount - 1, 0, batch.length);
            }
            
            break; // Move to next batch
          }
          
        } else {
          // Non-rate-limit error
          log.error(`Batch failed: ${err.message}`);
          
          // Report error to limiter
          await rateLimiter.onError(err);
          
          retryCount++;
          
          if (retryCount <= MAX_RETRIES) {
            const backoffDelay = Math.min(1000 * Math.pow(2, retryCount), 10000);
            log.warn(`Retrying batch after ${backoffDelay}ms (attempt ${retryCount}/${MAX_RETRIES})`);
            
            await new Promise(resolve => setTimeout(resolve, backoffDelay));
            continue;
          } else {
            errorCount += batch.length;
            
            if (importState) {
              updateImportState(importState, successCount + errorCount - 1, 0, batch.length);
            }
            
            break;
          }
        }
      }
    }
    
    // Reset retry count for next batch
    retryCount = 0;
    
    // Display progress stats
    const elapsed = formatDuration(Date.now() - startTime);
    const recordsPerSecond = successCount / ((Date.now() - startTime) / 1000);
    const remainingRecords = totalRecords - successCount - errorCount;
    const estimatedRemaining = remainingRecords / Math.max(recordsPerSecond, 1);
    const progressStats = rateLimiter.getStats();
    
    log.debug(
      `Elapsed: ${elapsed} | Speed: ${recordsPerSecond.toFixed(1)} rec/s | Remaining: ~${formatDuration(estimatedRemaining * 1000)} | Success: ${progressStats.successRate}%`
    );
    
    // Show limiter stats periodically
    if (batchNum % 10 === 0) {
      log.debug(
        `Rate Limiter: ${progressStats.rateLimits} rate limits, avg response: ${progressStats.avgResponseTime}ms, circuit: ${progressStats.circuitBreakerOpen ? 'OPEN' : 'closed'}`
      );
    }
    
    log.blank();
    
    // Check again before waiting
    if (isImportCancelled()) {
      return handleCancellation(successCount, errorCount, totalRecords);
    }
    
    // Wait before next batch (with cancellation support)
    if (successCount + errorCount < totalRecords) {
      const completed = await rateLimiter.wait(() => isImportCancelled());
      
      if (!completed) {
        return handleCancellation(successCount, errorCount, totalRecords);
      }
    }
  }
  
  // Mark import as complete
  if (importState) {
    completeImport(importState);
    log.debug('Import state saved as completed');
  }
  
  // Display final stats
  const finalStats = rateLimiter.getStats();
  log.blank();
  log.info('📊 Final Rate Limiter Statistics:');
  log.info(`   Total requests: ${finalStats.totalRequests}`);
  log.info(`   Success rate: ${finalStats.successRate}%`);
  log.info(`   Rate limits encountered: ${finalStats.rateLimits}`);
  log.info(`   Average response time: ${finalStats.avgResponseTime}ms`);
  log.info(`   Final batch size: ${finalStats.batchSize}`);
  log.info(`   Final delay: ${finalStats.delay}ms`);
  log.blank();
  
  return { successCount, errorCount, cancelled: false };
}

/**
 * Handle dry run mode
 */
function handleDryRun(
  db: StreamDB,
  totalRecords: number,
  batchSize: number,
  batchDelay: number
): PublishResult {
  log.section('DRY RUN MODE (STREAMING)');
  log.info(`Total: ${totalRecords.toLocaleString()} records`);
  log.info(`Batch: ${Math.min(batchSize, MAX_APPLY_WRITES_OPS)} records per call`);
  log.info(`Time: ~${formatDuration(Math.ceil(totalRecords / batchSize) * batchDelay)}`);
  log.blank();
  
  // Show first 5 records
  const previewCount = Math.min(5, totalRecords);
  log.info(`Preview (first ${previewCount} records):`);
  log.blank();
  
  let count = 0;
  for (const batch of db.getBatches(previewCount)) {
    for (const record of batch) {
      if (count >= previewCount) break;
      
      count++;
      const artistName = record.artists[0]?.artistName || 'Unknown Artist';
      log.raw(`${count}. ${artistName} - ${record.trackName}`);
      log.raw(`   Played: ${record.playedTime}`);
      log.raw(`   Source: ${record.musicServiceBaseDomain}`);
      log.blank();
    }
    if (count >= previewCount) break;
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


