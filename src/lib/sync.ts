import type { AtpAgent } from '@atproto/api';
import type { PlayRecord, Config } from '../types.js';
import { formatDate, formatDateRange } from '../utils/helpers.js';
import * as ui from '../utils/ui.js';
import { log } from '../utils/logger.js';
import { isImportCancelled } from '../utils/killswitch.js';
import { isCacheValid, loadCache, saveCache, getCacheInfo } from '../utils/teal-cache.js';
import { createRecordKey, BatchSizeOptimizer } from '../utils/sync-helpers.js';

interface ExistingRecord {
  uri: string;
  cid: string;
  value: PlayRecord;
}

interface DuplicateGroup {
  key: string;
  records: ExistingRecord[];
}

/**
 * Fetch all existing play records from Teal
 * Returns a Map where each key can have multiple records (for duplicate detection)
 */
export async function fetchExistingRecords(
  agent: AtpAgent,
  config: Config,
  forceRefresh: boolean = false
): Promise<Map<string, ExistingRecord>> {
  log.section('Checking Existing Records');
  const { RECORD_TYPE } = config;
  const did = agent.session?.did;

  if (!did) {
    throw new Error('No authenticated session found');
  }

  // Check cache first (unless force refresh)
  if (!forceRefresh && isCacheValid(did)) {
    const cacheInfo = getCacheInfo(did);
    log.info(`📂 Loading from cache (${cacheInfo.age!.toFixed(1)}h old, ${cacheInfo.records!.toLocaleString()} records)...`);
    
    const cached = loadCache(did);
    if (cached) {
      // Convert cached records to the format we need
      const existingRecords = new Map<string, ExistingRecord>();
      for (const [, record] of cached.entries()) {
        const playRecord = record.value as PlayRecord;
        const key = createRecordKey(playRecord);
        existingRecords.set(key, record as ExistingRecord);
      }
      
      log.success(`✓ Loaded ${existingRecords.size.toLocaleString()} records from cache`);
      log.blank();
      return existingRecords;
    }
  }

  // Cache miss or force refresh - fetch from Teal
  if (forceRefresh) {
    log.info('🔄 Force refresh - fetching from Teal...');
  } else {
    log.info('Fetching records from Teal to avoid duplicates...');
  }
  
  const existingRecords = new Map<string, ExistingRecord>();
  const cacheMap = new Map<string, { uri: string; cid: string; value: any }>();
  let cursor: string | undefined = undefined;
  let totalFetched = 0;
  const startTime = Date.now();

  // Adaptive batch sizing
  const optimizer = new BatchSizeOptimizer(25); // Start conservative
  let requestCount = 0;

  try {
    // Fetch records in batches using listRecords with adaptive sizing
    do {
      // Check for cancellation
      if (isImportCancelled()) {
        log.warn('Fetch cancelled by user');
        throw new Error('Operation cancelled by user');
      }

      requestCount++;
      const requestStart = Date.now();
      const batchSize = optimizer.getBatchSize();
      
      log.debug(`Request #${requestCount}: Fetching batch of ${batchSize}...`);
      
      const response = await agent.com.atproto.repo.listRecords({
        repo: did,
        collection: RECORD_TYPE,
        limit: batchSize,
        cursor: cursor,
      });

      const requestLatency = Date.now() - requestStart;
      const records = response.data.records;

      log.debug(`Request #${requestCount}: Got ${records.length} records in ${requestLatency}ms`);

      // Batch process records for better performance
      for (const record of records) {
        const playRecord = record.value as unknown as PlayRecord;
        const key = createRecordKey(playRecord);
        const existingRecord = {
          uri: record.uri,
          cid: record.cid,
          value: playRecord,
        };
        existingRecords.set(key, existingRecord);
        // Also store for cache (using URI as key for cache)
        cacheMap.set(record.uri, existingRecord);
      }

      totalFetched += records.length;
      cursor = response.data.cursor;

      // Update optimizer with request latency
      optimizer.onRequest(requestLatency);

      // Show progress on every batch
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      const rate = (totalFetched / (Date.now() - startTime) * 1000).toFixed(0);
      log.progress(`Fetched ${totalFetched.toLocaleString()} records (${rate} rec/s, batch: ${batchSize}, ${elapsed}s)...`);
    } while (cursor);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const avgRate = (totalFetched / (Date.now() - startTime) * 1000).toFixed(0);
    log.success(`Found ${existingRecords.size.toLocaleString()} existing records in ${elapsed}s (avg ${avgRate} rec/s)`);
    
    // Save to cache
    log.debug('Saving records to cache...');
    saveCache(did, cacheMap);
    
    log.blank();
    return existingRecords;
  } catch (error) {
    const err = error as Error;
    log.error(`Failed to fetch existing records: ${err.message}`);
    throw error;
  }
}

/**
 * Fetch all existing play records as an array (for duplicate detection)
 * This version keeps ALL records, including duplicates
 */
export async function fetchAllRecords(
  agent: AtpAgent,
  config: Config
): Promise<ExistingRecord[]> {
  const { RECORD_TYPE } = config;
  const did = agent.session?.did;

  if (!did) {
    throw new Error('No authenticated session found');
  }

  ui.startSpinner('Fetching existing records from Teal...');
  const allRecords: ExistingRecord[] = [];
  let cursor: string | undefined = undefined;
  let totalFetched = 0;
  const startTime = Date.now();

  // Adaptive batch sizing
  const optimizer = new BatchSizeOptimizer(25); // Start conservative
  let requestCount = 0;

  try {
    // Fetch records in batches using listRecords with adaptive sizing
    do {
      // Check for cancellation
      if (isImportCancelled()) {
        ui.failSpinner('Fetch cancelled by user');
        throw new Error('Operation cancelled by user');
      }

      requestCount++;
      const requestStart = Date.now();
      const batchSize = optimizer.getBatchSize();

      const response = await agent.com.atproto.repo.listRecords({
        repo: did,
        collection: RECORD_TYPE,
        limit: batchSize,
        cursor: cursor,
      });

      const requestLatency = Date.now() - requestStart;
      const records = response.data.records;
      for (const record of records) {
        const playRecord = record.value as unknown as PlayRecord;
        allRecords.push({
          uri: record.uri,
          cid: record.cid,
          value: playRecord,
        });
      }

      totalFetched += records.length;
      cursor = response.data.cursor;

      // Update optimizer with request latency
      optimizer.onRequest(requestLatency);

      // Update spinner with progress more frequently
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      const rate = (totalFetched / (Date.now() - startTime) * 1000).toFixed(0);
      ui.updateSpinner(`Fetching records... ${totalFetched.toLocaleString()} found (${rate} rec/s, batch: ${batchSize}, ${elapsed}s)`);
    } while (cursor);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const avgRate = (totalFetched / (Date.now() - startTime) * 1000).toFixed(0);
    ui.succeedSpinner(`Found ${allRecords.length.toLocaleString()} total records in ${elapsed}s (avg ${avgRate} rec/s)`);
    return allRecords;
  } catch (error) {
    ui.failSpinner('Failed to fetch existing records');
    throw error;
  }
}

// Re-export for backward compatibility
export { createRecordKey };

/**
 * Deduplicate input records before submission
 * Keeps the first occurrence of each duplicate
 */
export function deduplicateInputRecords(records: PlayRecord[]): { unique: PlayRecord[]; duplicates: number } {
  const seen = new Map<string, PlayRecord>();
  const duplicates: PlayRecord[] = [];

  for (const record of records) {
    const key = createRecordKey(record);
    if (!seen.has(key)) {
      seen.set(key, record);
    } else {
      duplicates.push(record);
    }
  }

  return {
    unique: Array.from(seen.values()),
    duplicates: duplicates.length
  };
}

/**
 * Filter out records that already exist in Teal
 */
export function filterNewRecords(
  lastfmRecords: PlayRecord[],
  existingRecords: Map<string, ExistingRecord>
): PlayRecord[] {
  log.section('Identifying New Records');

  const newRecords: PlayRecord[] = [];
  const duplicates: PlayRecord[] = [];

  for (const record of lastfmRecords) {
    const key = createRecordKey(record);
    if (existingRecords.has(key)) {
      duplicates.push(record);
    } else {
      newRecords.push(record);
    }
  }

  log.info(`Total: ${lastfmRecords.length.toLocaleString()} records`);
  log.info(`Existing: ${duplicates.length.toLocaleString()} already in Teal`);
  log.info(`New: ${newRecords.length.toLocaleString()} to import`);
  log.blank();

  // Show some examples of duplicates if any (only in verbose mode)
  if (log.getLevel() <= 0 && duplicates.length > 0) { // DEBUG level
    const exampleCount = Math.min(3, duplicates.length);
    log.debug('Examples of existing records (skipped):');
    duplicates.slice(0, exampleCount).forEach((record, i) => {
      log.debug(`  ${i + 1}. ${record.artists[0]?.artistName} - ${record.trackName}`);
      log.debug(`     ${formatDate(record.playedTime, true)}`);
    });
    if (duplicates.length > exampleCount) {
      log.debug(`  ... and ${(duplicates.length - exampleCount).toLocaleString()} more`);
    }
    log.blank();
  }

  return newRecords;
}

/**
 * Get time range of records
 */
export function getRecordTimeRange(records: PlayRecord[]): { earliest: Date; latest: Date } | null {
  if (records.length === 0) {
    return null;
  }

  const times = records.map(r => new Date(r.playedTime).getTime());
  const earliest = new Date(Math.min(...times));
  const latest = new Date(Math.max(...times));

  return { earliest, latest };
}

/**
 * Display sync statistics
 */
export function displaySyncStats(
  lastfmRecords: PlayRecord[],
  existingRecords: Map<string, ExistingRecord>,
  newRecords: PlayRecord[]
): void {
  const lastfmRange = getRecordTimeRange(lastfmRecords);
  const existingArray = Array.from(existingRecords.values()).map(r => r.value);
  const existingRange = getRecordTimeRange(existingArray);

  log.section('Sync Statistics');
  log.info(`Last.fm export: ${lastfmRecords.length.toLocaleString()} records`);
  if (lastfmRange) {
    log.info(`  Range: ${formatDateRange(lastfmRange.earliest, lastfmRange.latest)}`);
  }
  log.blank();

  log.info(`Teal current: ${existingRecords.size.toLocaleString()} records`);
  if (existingRange) {
    log.info(`  Range: ${formatDateRange(existingRange.earliest, existingRange.latest)}`);
  }
  log.blank();

  log.info(`New to import: ${newRecords.length.toLocaleString()}`);
  log.info(`Duplicates: ${(lastfmRecords.length - newRecords.length).toLocaleString()} skipped`);
  log.info(`Match rate: ${((1 - newRecords.length / lastfmRecords.length) * 100).toFixed(1)}%`);
  log.blank();
}

/**
 * Find duplicate records in the existing records
 * Returns groups of duplicates (where each group has 2+ records with the same key)
 */
export function findDuplicates(
  allRecords: ExistingRecord[]
): DuplicateGroup[] {
  const keyGroups = new Map<string, ExistingRecord[]>();
  
  // Group records by their key
  for (const record of allRecords) {
    const key = createRecordKey(record.value);
    if (!keyGroups.has(key)) {
      keyGroups.set(key, []);
    }
    keyGroups.get(key)!.push(record);
  }
  
  // Filter to only groups with duplicates (2+ records)
  const duplicates: DuplicateGroup[] = [];
  for (const [key, records] of keyGroups) {
    if (records.length > 1) {
      duplicates.push({ key, records });
    }
  }
  
  return duplicates;
}

/**
 * Remove duplicate records from Teal, keeping only the first occurrence
 */
export async function removeDuplicates(
  agent: AtpAgent,
  config: Config,
  dryRun: boolean = false
): Promise<{ totalDuplicates: number; recordsRemoved: number }> {
  ui.header('Checking for Duplicate Records');
  
  // Fetch ALL records (including duplicates)
  const allRecords = await fetchAllRecords(agent, config);
  
  ui.startSpinner('Analyzing records for duplicates...');
  const duplicateGroups = findDuplicates(allRecords);
  
  if (duplicateGroups.length === 0) {
    ui.succeedSpinner('No duplicates found!');
    return { totalDuplicates: 0, recordsRemoved: 0 };
  }
  
  ui.stopSpinner();
  
  // Count total duplicate records (excluding the one we keep per group)
  const totalDuplicates = duplicateGroups.reduce((sum, group) => sum + (group.records.length - 1), 0);
  
  ui.warning(`Found ${duplicateGroups.length.toLocaleString()} duplicate groups (${totalDuplicates.toLocaleString()} records to remove)`);
  console.log('');
  
  // Show examples
  const exampleCount = Math.min(5, duplicateGroups.length);
  ui.subheader('Examples of Duplicates:');
  for (let i = 0; i < exampleCount; i++) {
    const group = duplicateGroups[i];
    const firstRecord = group.records[0].value;
    console.log(`  ${i + 1}. ${firstRecord.artists[0]?.artistName} - ${firstRecord.trackName}`);
    console.log(`     ${formatDate(firstRecord.playedTime, true)} · ${group.records.length - 1} duplicate(s)`);
  }
  
  if (duplicateGroups.length > exampleCount) {
    console.log(`     ... and ${duplicateGroups.length - exampleCount} more groups`);
  }
  console.log('');
  
  if (dryRun) {
    ui.info('DRY RUN: No records were removed.');
    return { totalDuplicates, recordsRemoved: 0 };
  }
  
  // Remove duplicates (keep first, delete rest)
  console.log('');
  const progressBar = ui.createProgressBar(totalDuplicates, 'Removing duplicates');
  let recordsRemoved = 0;
  const startTime = Date.now();
  
  for (const group of duplicateGroups) {
    // Keep the first record, delete the rest
    const toDelete = group.records.slice(1);
    
    for (const record of toDelete) {
      try {
        await agent.com.atproto.repo.deleteRecord({
          repo: agent.session?.did || '',
          collection: record.value.$type,
          rkey: record.uri.split('/').pop()!,
        });
        recordsRemoved++;
        
        // Update progress bar
        const elapsed = (Date.now() - startTime) / 1000;
        const speed = recordsRemoved / Math.max(elapsed, 0.1);
        progressBar.update(recordsRemoved, { speed });
        
      } catch (error) {
        // Silently continue on errors
      }
      
      // Small delay between deletions
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
  
  progressBar.stop();
  console.log('');
  ui.success(`Removed ${recordsRemoved.toLocaleString()} duplicate records`);
  ui.info(`Kept ${duplicateGroups.length.toLocaleString()} unique records`);
  
  return { totalDuplicates, recordsRemoved };
}
