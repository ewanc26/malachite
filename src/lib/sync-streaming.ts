import type { SafeAgent } from './safe-agent.js';
import type { PlayRecord, Config } from '../types.js';
import { StreamDB } from '../utils/stream-db.js';
import { log } from '../utils/logger.js';
import { isImportCancelled } from '../utils/killswitch.js';
import * as ui from '../utils/ui.js';
import { createRecordKey, BatchSizeOptimizer } from '../utils/sync-helpers.js';

/**
 * Fetch existing records from Teal and store them in the database
 * Returns count of records fetched
 */
export async function fetchExistingRecordsStreaming(
  agent: SafeAgent,
  config: Config,
  db: StreamDB
): Promise<number> {
  log.section('Fetching Existing Records from Teal');
  const { RECORD_TYPE } = config;
  const did = agent.session?.did;
  
  if (!did) {
    throw new Error('No authenticated session found');
  }
  
  let cursor: string | undefined = undefined;
  let totalFetched = 0;
  const startTime = Date.now();
  
  // Adaptive batch sizing
  const optimizer = new BatchSizeOptimizer(25);
  let requestCount = 0;
  
  try {
    do {
      if (isImportCancelled()) {
        log.warn('Fetch cancelled by user');
        throw new Error('Operation cancelled by user');
      }
      
      requestCount++;
      const requestStart = Date.now();
      const batchSize = optimizer.getBatchSize();
      
      log.debug(`Request #${requestCount}: Fetching batch of ${batchSize}...`);
      
      const response: any = await agent.com.atproto.repo.listRecords({
        repo: did,
        collection: RECORD_TYPE,
        limit: batchSize,
        cursor: cursor,
      });
      
      const requestLatency = Date.now() - requestStart;
      const records = response.data.records;
      
      log.debug(`Request #${requestCount}: Got ${records.length} records in ${requestLatency}ms`);
      
      // Insert records into database in batch
      const playRecords = records.map((record: any) => record.value as unknown as PlayRecord);
      db.insertMany(playRecords);
      
      totalFetched += records.length;
      cursor = response.data.cursor;
      
      // Update optimizer with request latency
      optimizer.onRequest(requestLatency);
      
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      const rate = (totalFetched / (Date.now() - startTime) * 1000).toFixed(0);
      log.progress(`Fetched ${totalFetched.toLocaleString()} records (${rate} rec/s, batch: ${batchSize}, ${elapsed}s)...`);
    } while (cursor);
    
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const avgRate = (totalFetched / (Date.now() - startTime) * 1000).toFixed(0);
    log.success(`Found ${totalFetched.toLocaleString()} existing records in ${elapsed}s (avg ${avgRate} rec/s)`);
    log.blank();
    
    return totalFetched;
  } catch (error) {
    const err = error as Error;
    log.error(`Failed to fetch existing records: ${err.message}`);
    throw error;
  }
}

/**
 * Filter records in the input database against existing records database
 * Creates a new database with only new records
 */
export function filterNewRecordsStreaming(
  inputDb: StreamDB,
  existingDb: StreamDB,
  outputDb: StreamDB
): { total: number; existing: number; new: number } {
  log.section('Identifying New Records');
  
  const total = inputDb.count();
  let existing = 0;
  let newCount = 0;
  
  // Process in batches to avoid memory issues
  const BATCH_SIZE = 1000;
  
  for (const batch of inputDb.getBatches(BATCH_SIZE)) {
    const newRecords: PlayRecord[] = [];
    
    for (const record of batch) {
      if (existingDb.exists(record)) {
        existing++;
      } else {
        newRecords.push(record);
        newCount++;
      }
    }
    
    // Insert new records into output database
    if (newRecords.length > 0) {
      outputDb.insertMany(newRecords);
    }
    
    // Progress logging
    if ((existing + newCount) % 10000 === 0) {
      log.progress(`Processed ${(existing + newCount).toLocaleString()}/${total.toLocaleString()} records...`);
    }
  }
  
  log.info(`Total: ${total.toLocaleString()} records`);
  log.info(`Existing: ${existing.toLocaleString()} already in Teal`);
  log.info(`New: ${newCount.toLocaleString()} to import`);
  log.blank();
  
  return { total, existing, new: newCount };
}

/**
 * Find and remove duplicate records from Teal (streaming version)
 */
export async function removeDuplicatesStreaming(
  agent: SafeAgent,
  config: Config,
  dryRun: boolean = false
): Promise<{ totalDuplicates: number; recordsRemoved: number }> {
  ui.header('Checking for Duplicate Records');
  
  const { RECORD_TYPE } = config;
  const did = agent.session?.did;
  
  if (!did) {
    throw new Error('No authenticated session found');
  }
  
  // Track seen records by key
  const seenKeys = new Map<string, string>(); // key -> uri
  const duplicateUris: string[] = [];
  
  let cursor: string | undefined = undefined;
  let totalRecords = 0;
  
  ui.startSpinner('Scanning for duplicates...');
  
  try {
    do {
      const response: any = await agent.com.atproto.repo.listRecords({
        repo: did,
        collection: RECORD_TYPE,
        limit: 100,
        cursor: cursor,
      });
      
      for (const record of response.data.records) {
        const playRecord = record.value as unknown as PlayRecord;
        const key = createRecordKey(playRecord);
        
        if (seenKeys.has(key)) {
          // This is a duplicate - mark for deletion
          duplicateUris.push(record.uri);
        } else {
          // First occurrence - remember it
          seenKeys.set(key, record.uri);
        }
        
        totalRecords++;
      }
      
      cursor = response.data.cursor;
      
      if (totalRecords % 1000 === 0) {
        ui.updateSpinner(`Scanning for duplicates... ${totalRecords.toLocaleString()} scanned, ${duplicateUris.length.toLocaleString()} duplicates found`);
      }
    } while (cursor);
    
    ui.stopSpinner();
    
    if (duplicateUris.length === 0) {
      ui.succeedSpinner('No duplicates found!');
      return { totalDuplicates: 0, recordsRemoved: 0 };
    }
    
    ui.warning(`Found ${duplicateUris.length.toLocaleString()} duplicate records`);
    console.log('');
    
    if (dryRun) {
      ui.info('DRY RUN: No records were removed.');
      return { totalDuplicates: duplicateUris.length, recordsRemoved: 0 };
    }
    
    // Remove duplicates
    console.log('');
    const progressBar = ui.createProgressBar(duplicateUris.length, 'Removing duplicates');
    let recordsRemoved = 0;
    const startTime = Date.now();
    
    for (const uri of duplicateUris) {
      try {
        await agent.com.atproto.repo.deleteRecord({
          repo: did,
          collection: RECORD_TYPE,
          rkey: uri.split('/').pop()!,
        });
        recordsRemoved++;
        
        const elapsed = (Date.now() - startTime) / 1000;
        const speed = recordsRemoved / Math.max(elapsed, 0.1);
        progressBar.update(recordsRemoved, { speed });
      } catch (error) {
        // Silently continue on errors
      }
      
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    progressBar.stop();
    console.log('');
    ui.success(`Removed ${recordsRemoved.toLocaleString()} duplicate records`);
    ui.info(`Kept ${seenKeys.size.toLocaleString()} unique records`);
    
    return { totalDuplicates: duplicateUris.length, recordsRemoved };
  } catch (error) {
    ui.failSpinner('Failed to check for duplicates');
    throw error;
  }
}


