import type { AtpAgent } from '@atproto/api';
import type { PlayRecord, Config } from '../types.js';
import { formatDate, formatDateRange } from '../utils/helpers.js';
import * as ui from '../utils/ui.js';

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
  config: Config
): Promise<Map<string, ExistingRecord>> {
  console.log('\n=== Fetching Existing Teal Records ===');
  const { RECORD_TYPE } = config;
  const did = agent.session?.did;

  if (!did) {
    throw new Error('No authenticated session found');
  }

  const existingRecords = new Map<string, ExistingRecord>();
  let cursor: string | undefined = undefined;
  let totalFetched = 0;

  try {
    // Fetch records in batches using listRecords
    do {
      const response = await agent.com.atproto.repo.listRecords({
        repo: did,
        collection: RECORD_TYPE,
        limit: 100,
        cursor: cursor,
      });

      for (const record of response.data.records) {
        const playRecord = record.value as PlayRecord;
        // Create a unique key based on track, artist, and timestamp
        const key = createRecordKey(playRecord);
        // Note: This will overwrite duplicates, but that's OK for sync mode
        // For duplicate detection, we'll need to fetch all records again
        existingRecords.set(key, {
          uri: record.uri,
          cid: record.cid,
          value: playRecord,
        });
      }

      totalFetched += response.data.records.length;
      cursor = response.data.cursor;

      // Show progress
      if (totalFetched % 500 === 0 && totalFetched > 0) {
        console.log(`  Fetched ${totalFetched} records...`);
      }
    } while (cursor);

    console.log(`✓ Found ${existingRecords.size} existing records\n`);
    return existingRecords;
  } catch (error) {
    const err = error as Error;
    console.error('✗ Failed to fetch existing records:', err.message);
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

  try {
    // Fetch records in batches using listRecords
    do {
      const response = await agent.com.atproto.repo.listRecords({
        repo: did,
        collection: RECORD_TYPE,
        limit: 100,
        cursor: cursor,
      });

      for (const record of response.data.records) {
        const playRecord = record.value as PlayRecord;
        allRecords.push({
          uri: record.uri,
          cid: record.cid,
          value: playRecord,
        });
      }

      totalFetched += response.data.records.length;
      cursor = response.data.cursor;

      // Update spinner with progress
      if (totalFetched % 500 === 0 && totalFetched > 0) {
        ui.updateSpinner(`Fetching records... ${totalFetched.toLocaleString()} found`);
      }
    } while (cursor);

    ui.succeedSpinner(`Found ${allRecords.length.toLocaleString()} total records`);
    return allRecords;
  } catch (error) {
    ui.failSpinner('Failed to fetch existing records');
    throw error;
  }
}

/**
 * Create a unique key for a play record based on its essential properties
 * This is used to identify duplicates
 */
export function createRecordKey(record: PlayRecord): string {
  const artist = record.artists[0]?.artistName || '';
  const track = record.trackName;
  const timestamp = record.playedTime;

  // Normalize strings to handle case and whitespace differences
  const normalizedArtist = artist.toLowerCase().trim();
  const normalizedTrack = track.toLowerCase().trim();

  return `${normalizedArtist}|||${normalizedTrack}|||${timestamp}`;
}

/**
 * Filter out records that already exist in Teal
 */
export function filterNewRecords(
  lastfmRecords: PlayRecord[],
  existingRecords: Map<string, ExistingRecord>
): PlayRecord[] {
  console.log('\n=== Identifying New Records ===');

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

  console.log(`  Total Last.fm records: ${lastfmRecords.length}`);
  console.log(`  Already in Teal: ${duplicates.length}`);
  console.log(`  New records to import: ${newRecords.length}\n`);

  // Show some examples of duplicates if any
  if (duplicates.length > 0 && duplicates.length <= 5) {
    console.log('Examples of existing records (skipped):');
    duplicates.slice(0, 5).forEach((record, i) => {
      console.log(`  ${i + 1}. ${record.artists[0]?.artistName} - ${record.trackName}`);
      console.log(`     Played: ${formatDate(record.playedTime, true)}`);
    });
    console.log('');
  } else if (duplicates.length > 5) {
    console.log('Examples of existing records (skipped):');
    duplicates.slice(0, 5).forEach((record, i) => {
      console.log(`  ${i + 1}. ${record.artists[0]?.artistName} - ${record.trackName}`);
      console.log(`     Played: ${formatDate(record.playedTime, true)}`);
    });
    console.log(`  ... and ${duplicates.length - 5} more duplicates\n`);
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

  console.log('=== Sync Statistics ===');
  console.log(`Last.fm Export:`);
  console.log(`  Total records: ${lastfmRecords.length}`);
  if (lastfmRange) {
    console.log(`  Date range: ${formatDateRange(lastfmRange.earliest, lastfmRange.latest)}`);
  }
  console.log('');

  console.log(`Teal (Current):`);
  console.log(`  Total records: ${existingRecords.size}`);
  if (existingRange) {
    console.log(`  Date range: ${formatDateRange(existingRange.earliest, existingRange.latest)}`);
  }
  console.log('');

  console.log(`Sync Result:`);
  console.log(`  Records to import: ${newRecords.length}`);
  console.log(`  Duplicates skipped: ${lastfmRecords.length - newRecords.length}`);
  console.log(`  Match rate: ${((1 - newRecords.length / lastfmRecords.length) * 100).toFixed(1)}%`);
  console.log('');
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
