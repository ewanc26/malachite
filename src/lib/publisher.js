import { formatDuration } from '../utils/helpers.js';
import { isImportCancelled } from '../utils/killswitch.js';

/**
 * Publish records in batches with rate limiting and killswitch support
 */
export async function publishRecords(agent, records, batchSize, batchDelay, config, dryRun = false) {
  const { RECORD_TYPE } = config;
  const totalRecords = records.length;
  let successCount = 0;
  let errorCount = 0;
  const startTime = Date.now();
  
  if (dryRun) {
    return handleDryRun(records, batchSize, batchDelay);
  }
  
  const totalBatches = Math.ceil(totalRecords / batchSize);
  const estimatedTime = formatDuration(totalBatches * batchDelay);
  
  console.log(`Publishing ${totalRecords} records in batches of ${batchSize}...`);
  console.log(`Total batches: ${totalBatches}`);
  console.log(`Estimated time: ${estimatedTime}`);
  console.log(`\n🚨 Press Ctrl+C to stop gracefully after current batch\n`);
  
  for (let i = 0; i < totalRecords; i += batchSize) {
    // Check killswitch before processing batch
    if (isImportCancelled()) {
      return handleCancellation(successCount, errorCount, totalRecords);
    }
    
    const batch = records.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    const progress = ((i / totalRecords) * 100).toFixed(1);
    
    console.log(`[${progress}%] Batch ${batchNum}/${totalBatches} (records ${i + 1}-${Math.min(i + batchSize, totalRecords)})`);
    
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
          repo: agent.session.did,
          collection: RECORD_TYPE,
          record,
        });
        successCount++;
      } catch (error) {
        errorCount++;
        console.error(`  ✗ Failed: ${record.trackName} - ${error.message}`);
      }
    }
    
    const batchDuration = Date.now() - batchStartTime;
    const elapsed = formatDuration(Date.now() - startTime);
    const remaining = formatDuration(((totalRecords - i - batchSize) / batchSize) * batchDelay);
    
    console.log(`  ✓ Complete in ${batchDuration}ms (${successCount} successful, ${errorCount} failed)`);
    
    // Only show time estimates if not cancelled
    if (!isImportCancelled()) {
      console.log(`  ⏱  Elapsed: ${elapsed} | Remaining: ~${remaining}\n`);
    }
    
    // Check again before waiting
    if (isImportCancelled()) {
      return handleCancellation(successCount, errorCount, totalRecords);
    }
    
    // Wait before next batch (except for last batch)
    if (i + batchSize < totalRecords) {
      await new Promise(resolve => setTimeout(resolve, batchDelay));
    }
  }
  
  return { successCount, errorCount, cancelled: false };
}

/**
 * Handle dry run mode
 */
function handleDryRun(records, batchSize, batchDelay) {
  const totalRecords = records.length;
  
  console.log(`\n=== DRY RUN MODE ===`);
  console.log(`Would publish ${totalRecords} records in batches of ${batchSize}`);
  console.log(`Estimated time: ${formatDuration(Math.ceil(totalRecords / batchSize) * batchDelay)}\n`);
  
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
    if (record.artists[0]?.artistMbId) mbids.push(`Artist: ${record.artists[0].artistMbId}`);
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
function handleCancellation(successCount, errorCount, totalRecords) {
  console.log(`\n🛑 Import cancelled by user`);
  console.log(`   Processed: ${successCount}/${totalRecords} records`);
  console.log(`   Remaining: ${totalRecords - successCount} records\n`);
  return { successCount, errorCount, cancelled: true };
}
