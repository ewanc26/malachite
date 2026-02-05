import type { SafeAgent } from './safe-agent.js';
import type { Config } from '../types.js';
import * as ui from '../utils/ui.js';
import { log } from '../utils/logger.js';
import { isImportCancelled } from '../utils/killswitch.js';
import { prompt } from '../utils/input.js';
import { MAX_DELETE_BATCH_SIZE } from '../constants.js';
import { AdaptiveRateLimiter } from '../utils/adaptive-rate-limiter.js';

/**
 * Nuke all play records from the user's repository
 * This is a DESTRUCTIVE operation that deletes ALL records in the play collection
 * 
 * Based on pdsls deletion patterns:
 * - Uses operations per applyWrites call (AT Protocol standard)
 * - Simple batch processing with fixed delays
 * - Delete operations cost 1 rate limit point each (vs 3 for creates)
 */
export async function nukeAllRecords(
  agent: SafeAgent,
  config: Config,
  dryRun: boolean = false
): Promise<{ totalRecords: number; deletedRecords: number; cancelled: boolean }> {
  const { RECORD_TYPE } = config;
  const did = agent.session?.did;
  
  if (!did) {
    throw new Error('No authenticated session found');
  }
  
  ui.header('NUKE ALL RECORDS');
  log.blank();
  log.error('⚠️  THIS WILL DELETE ALL RECORDS IN YOUR PLAY COLLECTION');
  log.error('⚠️  This action is IRREVERSIBLE and CANNOT be undone!');
  log.blank();
  
  // First, count total records
  ui.startSpinner('Counting records...');
  let totalRecords = 0;
  let cursor: string | undefined = undefined;
  const recordUris: string[] = [];
  
  try {
    do {
      const response: any = await agent.com.atproto.repo.listRecords({
        repo: did,
        collection: RECORD_TYPE,
        limit: 100,
        cursor: cursor,
      });
      
      for (const record of response.data.records) {
        recordUris.push(record.uri);
        totalRecords++;
      }
      
      cursor = response.data.cursor;
      
      if (totalRecords % 1000 === 0) {
        ui.updateSpinner(`Counting records... ${totalRecords.toLocaleString()} found`);
      }
    } while (cursor);
    
    ui.succeedSpinner(`Found ${totalRecords.toLocaleString()} records to delete`);
  } catch (error) {
    ui.failSpinner('Failed to count records');
    throw error;
  }
  
  if (totalRecords === 0) {
    log.info('No records found to delete');
    return { totalRecords: 0, deletedRecords: 0, cancelled: false };
  }
  
  log.blank();
  log.warn(`Preparing to delete ${totalRecords.toLocaleString()} records`);
  log.blank();
  
  if (dryRun) {
    log.section('DRY RUN MODE');
    log.info('No records were deleted.');
    log.info('Remove --dry-run to actually delete records.');
    return { totalRecords, deletedRecords: 0, cancelled: false };
  }
  
  // Require explicit confirmation
  log.blank();
  const typeToConfirm = 'DELETE ALL';
  log.error(`Type '${typeToConfirm}' to confirm deletion:`);
  const confirmationInput = await prompt('');
  
  if (confirmationInput !== typeToConfirm) {
    log.warn('Deletion cancelled - confirmation did not match');
    return { totalRecords, deletedRecords: 0, cancelled: true };
  }
  
  log.blank();
  log.section('Deleting Records');
  log.info('Press Ctrl+C to cancel (will stop after current batch)');
  log.blank();
  
  // Delete records in batches with adaptive rate limiting
  const progressBar = ui.createProgressBar(totalRecords, 'Deleting records');
  let deletedRecords = 0;
  const startTime = Date.now();
  
  log.info('Using extremely conservative rate limiting for deletions');
  log.info('AT Protocol deletion rate limits are very strict');
  log.info('Starting with 10 records/batch and 30s delays');
  log.info('⏱️  This will be slow - expect ~1 hour per 1000 records');
  log.blank();
  
  // Use extremely conservative settings for deletions
  // AT Protocol is VERY strict with deletion rate limits
  const rateLimiter = new AdaptiveRateLimiter(
    config,
    10, // Very small batch size (10 records per batch)
    30000, // Very long delay between batches (30 seconds)
    MAX_DELETE_BATCH_SIZE // Max for deletions (10) - much lower than creates (200)
  );
  
  let batchNum = 0;
  let totalErrors = 0;
  
  // Process in batches
  for (let i = 0; i < recordUris.length; i += rateLimiter.getCurrentBatchSize()) {
    // Check for cancellation
    if (isImportCancelled()) {
      progressBar.stop();
      log.blank();
      log.warn('Deletion cancelled by user');
      log.info(`Progress: ${deletedRecords.toLocaleString()}/${totalRecords.toLocaleString()} deleted`);
      log.info(`Remaining: ${(totalRecords - deletedRecords).toLocaleString()} records not deleted`);
      log.blank();
      const stats = rateLimiter.getStats();
      showFinalStats(batchNum, totalErrors, stats.rateLimits, startTime);
      return { totalRecords, deletedRecords, cancelled: true };
    }
    
    batchNum++;
    const currentBatchSize = rateLimiter.getCurrentBatchSize();
    const batch = recordUris.slice(i, Math.min(i + currentBatchSize, recordUris.length));
    
    // Build delete operations
    const writes = batch.map(uri => ({
      $type: 'com.atproto.repo.applyWrites#delete',
      collection: RECORD_TYPE,
      rkey: uri.split('/').pop()!,
    }));
    
    const batchStartTime = Date.now();
    
    try {
      await agent.com.atproto.repo.applyWrites({
        repo: did,
        writes: writes as any,
      });
      
      const responseTime = Date.now() - batchStartTime;
      deletedRecords += batch.length;
      
      // Report success to rate limiter
      await rateLimiter.onSuccess(responseTime);
      
      // Update progress
      const elapsed = (Date.now() - startTime) / 1000;
      const speed = deletedRecords / Math.max(elapsed, 0.1);
      progressBar.update(deletedRecords, { speed });
      
      // Show stats periodically (less often to reduce noise)
      if (batchNum % 50 === 0) {
        progressBar.stop();
        log.blank();
        const stats = rateLimiter.getStats();
        const eta = (totalRecords - deletedRecords) / Math.max(speed, 0.1);
        const etaMinutes = Math.ceil(eta / 60);
        log.debug(
          `Batch ${batchNum}: ${deletedRecords.toLocaleString()}/${totalRecords.toLocaleString()} ` +
          `(${Math.round((deletedRecords / totalRecords) * 100)}%) ` +
          `- ETA: ~${etaMinutes}m, batch: ${stats.batchSize}, delay: ${stats.delay}ms`
        );
        progressBar.update(deletedRecords, { speed });
      }
      
      // Wait before next batch
      const delayTime = rateLimiter.getCurrentDelay();
      if (delayTime > 0) {
        await sleep(delayTime, () => isImportCancelled());
        
        if (isImportCancelled()) {
          progressBar.stop();
          log.blank();
          const stats = rateLimiter.getStats();
          showFinalStats(batchNum, totalErrors, stats.rateLimits, startTime);
          return { totalRecords, deletedRecords, cancelled: true };
        }
      }
      
    } catch (error) {
      const err = error as any;
      totalErrors++;
      
      // Report error to rate limiter (it will handle backoff)
      const rateLimit = rateLimiter.detectRateLimit(err);
      
      progressBar.stop();
      log.blank();
      
      if (rateLimit.isRateLimited) {
        log.error(`⚠️  Rate limit detected (batch ${batchNum})`);
        
        // Let rate limiter handle the backoff
        const backoff = await rateLimiter.onRateLimit(err);
        const backoffMinutes = Math.ceil(backoff / 60000);
        const backoffSeconds = Math.ceil((backoff % 60000) / 1000);
        
        if (backoff > 60000) {
          log.error(`⏳ Rate limited: Waiting ${backoffMinutes}m ${backoffSeconds}s before retry`);
          log.info('The AT Protocol has strict deletion limits');
          log.info('Consider batching deletes across multiple days');
        } else {
          log.info(`⏳ Waiting ${Math.ceil(backoff / 1000)}s before retry...`);
        }
        
        await sleep(backoff, () => isImportCancelled());
        
        if (isImportCancelled()) {
          log.blank();
          const stats = rateLimiter.getStats();
          showFinalStats(batchNum, totalErrors, stats.rateLimits, startTime);
          return { totalRecords, deletedRecords, cancelled: true };
        }
        
        // Retry the same batch
        log.info('⚡ Retrying batch...');
        i -= currentBatchSize; // Go back to retry this batch
        continue;
        
      } else {
        // Non-rate-limit error
        log.error(`❌ Batch ${batchNum} failed: ${err.message}`);
        log.warn(`Skipping ${batch.length} records`);
        
        // Light backoff on other errors
        await rateLimiter.onError(err);
        const delayTime = rateLimiter.getCurrentDelay();
        await sleep(delayTime, () => isImportCancelled());
        
        if (isImportCancelled()) {
          log.blank();
          const stats = rateLimiter.getStats();
          showFinalStats(batchNum, totalErrors, stats.rateLimits, startTime);
          return { totalRecords, deletedRecords, cancelled: true };
        }
      }
    }
  }
  
  progressBar.stop();
  log.blank();
  
  if (deletedRecords === totalRecords) {
    log.success(`Successfully deleted all ${deletedRecords.toLocaleString()} records`);
  } else {
    log.warn(`Partial deletion: ${deletedRecords.toLocaleString()} of ${totalRecords.toLocaleString()} deleted`);
    log.warn(`${totalRecords - deletedRecords} records failed to delete`);
  }
  
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log.info(`Operation completed in ${elapsed}s`);
  
  const stats = rateLimiter.getStats();
  showFinalStats(batchNum, totalErrors, stats.rateLimits, startTime);
  
  return { totalRecords, deletedRecords, cancelled: false };
}

/**
 * Sleep with cancellation support
 */
async function sleep(ms: number, checkCancelled: () => boolean): Promise<void> {
  const checkInterval = 100; // Check every 100ms
  const checks = Math.ceil(ms / checkInterval);
  
  for (let i = 0; i < checks; i++) {
    if (checkCancelled()) {
      return;
    }
    await new Promise(resolve => setTimeout(resolve, Math.min(checkInterval, ms - (i * checkInterval))));
  }
}

/**
 * Display final statistics
 */
function showFinalStats(
  totalBatches: number,
  totalErrors: number,
  rateLimitHits: number,
  startTime: number
): void {
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const successRate = totalBatches > 0 ? (((totalBatches - totalErrors) / totalBatches) * 100).toFixed(1) : '0.0';
  
  log.blank();
  log.info('📊 Deletion Statistics:');
  log.info(`   Total batches: ${totalBatches}`);
  log.info(`   Success rate: ${successRate}%`);
  log.info(`   Errors: ${totalErrors}`);
  log.info(`   Rate limits: ${rateLimitHits}`);
  log.info(`   Duration: ${elapsed}s`);
  log.blank();
}

/**
 * Interactive nuke confirmation
 * Provides multiple confirmations to prevent accidental deletion
 */
export async function nukeWithConfirmation(
  agent: SafeAgent,
  config: Config,
  skipConfirmation: boolean = false
): Promise<{ totalRecords: number; deletedRecords: number; cancelled: boolean }> {
  if (!skipConfirmation) {
    ui.header('⚠️  WARNING: DESTRUCTIVE ACTION');
    log.blank();
    log.error('You are about to DELETE ALL records in your play collection');
    log.error('This will remove your ENTIRE listening history from ATProto');
    log.error('This action is PERMANENT and CANNOT be undone');
    log.blank();
    
    const proceedInput = await prompt('Do you want to continue? (yes/no): ');
    
    if (proceedInput.toLowerCase() !== 'yes') {
      log.info('Operation cancelled');
      return { totalRecords: 0, deletedRecords: 0, cancelled: true };
    }
  }
  
  // Perform the nuke
  return await nukeAllRecords(agent, config, false);
}
