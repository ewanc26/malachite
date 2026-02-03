import type { SafeAgent } from './safe-agent.js';
import type { Config } from '../types.js';
import * as ui from '../utils/ui.js';
import { log } from '../utils/logger.js';
import { isImportCancelled } from '../utils/killswitch.js';
import { prompt } from '../utils/input.js';
import { AdaptiveRateLimiter } from '../utils/adaptive-rate-limiter.js';

/**
 * Maximum operations allowed per applyWrites call for deletions
 * Using a lower limit than imports (50 vs 200) to be extra conservative
 */
const MAX_DELETE_OPS = 50;

/**
 * Nuke all play records from the user's repository
 * This is a DESTRUCTIVE operation that deletes ALL records in the play collection
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
  
  ui.header('⚠️  NUKE ALL RECORDS');
  log.blank();
  log.warn('THIS WILL DELETE ALL RECORDS IN YOUR PLAY COLLECTION');
  log.warn('This action is IRREVERSIBLE and CANNOT be undone!');
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
  log.warn(`About to delete ${totalRecords.toLocaleString()} records from ${RECORD_TYPE}`);
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
  log.warn('Press Ctrl+C to cancel');
  log.blank();
  
  // Initialize adaptive rate limiter for deletions
  // VERY CONSERVATIVE settings for deletions to avoid rate limits
  const rateLimiter = new AdaptiveRateLimiter(
    config,
    20,           // Start with only 20 records per batch (much more conservative)
    2000,         // 2 second initial delay (10x more conservative than before)
    MAX_DELETE_OPS // Cap at 50 instead of 200
  );
  
  log.info(`Initial batch size: ${rateLimiter.getCurrentBatchSize()} records (very conservative)`);
  log.info(`Initial delay: ${rateLimiter.getCurrentDelay()}ms (2 seconds between batches)`);
  log.info(`Deletions are rate-limited more aggressively than imports`);
  log.blank();
  
  // Delete records in batches
  let progressBar = ui.createProgressBar(totalRecords, 'Deleting records');
  let deletedRecords = 0;
  const startTime = Date.now();
  
  let batchNum = 0;
  let retryCount = 0;
  const MAX_RETRIES = 3;
  
  // Process in batches using adaptive batch size
  for (let i = 0; i < recordUris.length; ) {
    // Check for cancellation
    if (isImportCancelled()) {
      progressBar.stop();
      log.blank();
      log.warn('Deletion cancelled by user');
      log.info(`Deleted: ${deletedRecords.toLocaleString()}/${totalRecords.toLocaleString()} records`);
      log.info(`Remaining: ${(totalRecords - deletedRecords).toLocaleString()} records`);
      
      // Show final stats
      const finalStats = rateLimiter.getStats();
      log.blank();
      log.info('📊 Rate Limiter Statistics:');
      log.info(`   Total requests: ${finalStats.totalRequests}`);
      log.info(`   Success rate: ${finalStats.successRate}%`);
      log.info(`   Rate limits encountered: ${finalStats.rateLimits}`);
      log.blank();
      
      return { totalRecords, deletedRecords, cancelled: true };
    }
    
    // Check circuit breaker
    if (rateLimiter.isCircuitBreakerOpen()) {
      progressBar.stop();
      log.blank();
      log.error('🔒 Circuit breaker is open - too many failures detected');
      log.warn('⏸️  Entering recovery cooldown period...');
      
      // Wait for circuit breaker timeout
      await rateLimiter.wait(() => isImportCancelled());
      
      if (isImportCancelled()) {
        progressBar.stop();
        return { totalRecords, deletedRecords, cancelled: true };
      }
      
      log.info('🔓 Circuit breaker recovery period complete - resuming operations');
      progressBar = ui.createProgressBar(totalRecords, 'Deleting records');
      progressBar.update(deletedRecords);
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
    let batchSuccess = false;
    
    while (retryCount <= MAX_RETRIES && !batchSuccess) {
      try {
        await agent.com.atproto.repo.applyWrites({
          repo: did,
          writes: writes as any,
        });
        
        const responseTime = Date.now() - batchStartTime;
        
        deletedRecords += batch.length;
        batchSuccess = true;
        retryCount = 0;
        
        // Report success to rate limiter
        await rateLimiter.onSuccess(responseTime);
        
        // Update progress
        const elapsed = (Date.now() - startTime) / 1000;
        const speed = deletedRecords / Math.max(elapsed, 0.1);
        progressBar.update(deletedRecords, { speed });
        
        // Move to next batch
        i += batch.length;
        
      } catch (error) {
        const err = error as any;
        
        // Detect rate limiting
        const detection = rateLimiter.detectRateLimit(err);
        
        if (detection.isRateLimited) {
          progressBar.stop();
          log.blank();
          log.warn(`⚠️  Rate limit detected (confidence: ${detection.confidence})`);
          
          // Report rate limit to limiter and get wait time
          const waitTime = await rateLimiter.onRateLimit(err);
          
          retryCount++;
          
          if (retryCount <= MAX_RETRIES) {
            log.info(`⏳ Waiting ${Math.ceil(waitTime / 1000)}s before retry ${retryCount}/${MAX_RETRIES}...`);
            
            // Wait with cancellation support
            const completed = await rateLimiter.wait(() => isImportCancelled());
            
            if (!completed) {
              return { totalRecords, deletedRecords, cancelled: true };
            }
            
            log.info('⚡ Resuming deletion...');
            progressBar = ui.createProgressBar(totalRecords, 'Deleting records');
            progressBar.update(deletedRecords);
            
            continue; // Retry the batch
          } else {
            log.error(`❌ Max retries (${MAX_RETRIES}) exceeded for batch`);
            log.warn(`Skipping ${batch.length} records`);
            
            // Move to next batch
            i += batch.length;
            break;
          }
          
        } else {
          // Non-rate-limit error
          progressBar.stop();
          log.blank();
          log.error(`Batch deletion failed: ${err.message}`);
          
          // Report error to limiter
          await rateLimiter.onError(err);
          
          retryCount++;
          
          if (retryCount <= MAX_RETRIES) {
            const backoffDelay = Math.min(1000 * Math.pow(2, retryCount), 10000);
            log.warn(`Retrying batch after ${backoffDelay}ms (attempt ${retryCount}/${MAX_RETRIES})`);
            
            await new Promise(resolve => setTimeout(resolve, backoffDelay));
            progressBar = ui.createProgressBar(totalRecords, 'Deleting records');
            progressBar.update(deletedRecords);
            continue;
          } else {
            log.error(`❌ Max retries exceeded, skipping ${batch.length} records`);
            
            // Move to next batch
            i += batch.length;
            break;
          }
        }
      }
    }
    
    // Reset retry count for next batch
    retryCount = 0;
    
    // Show rate limiter stats periodically
    if (batchNum % 20 === 0) {
      const stats = rateLimiter.getStats();
      progressBar.stop();
      log.blank();
      log.debug(
        `Stats: ${stats.successRate}% success, ${stats.rateLimits} rate limits, ` +
        `${stats.avgResponseTime}ms avg, batch: ${stats.batchSize}, delay: ${stats.delay}ms`
      );
      progressBar = ui.createProgressBar(totalRecords, 'Deleting records');
      progressBar.update(deletedRecords);
    }
    
    // Wait before next batch (with cancellation support)
    if (deletedRecords < totalRecords) {
      const completed = await rateLimiter.wait(() => isImportCancelled());
      
      if (!completed) {
        progressBar.stop();
        return { totalRecords, deletedRecords, cancelled: true };
      }
    }
  }
  
  progressBar.stop();
  log.blank();
  
  if (deletedRecords === totalRecords) {
    log.success(`✓ Successfully deleted all ${deletedRecords.toLocaleString()} records`);
  } else {
    log.warn(`Deleted ${deletedRecords.toLocaleString()} of ${totalRecords.toLocaleString()} records`);
    log.warn(`${totalRecords - deletedRecords} records may have failed to delete`);
  }
  
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log.info(`Completed in ${elapsed}s`);
  
  // Display final rate limiter stats
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
  
  return { totalRecords, deletedRecords, cancelled: false };
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