import type { AtpAgent } from '@atproto/api';
import type { PlayRecord, Config } from '../types.js';

interface ExistingRecord {
  uri: string;
  cid: string;
  value: PlayRecord;
}

/**
 * Fetch all existing play records from Teal
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
      console.log(`     Played: ${record.playedTime}`);
    });
    console.log('');
  } else if (duplicates.length > 5) {
    console.log('Examples of existing records (skipped):');
    duplicates.slice(0, 5).forEach((record, i) => {
      console.log(`  ${i + 1}. ${record.artists[0]?.artistName} - ${record.trackName}`);
      console.log(`     Played: ${record.playedTime}`);
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
    console.log(`  Date range: ${lastfmRange.earliest.toLocaleDateString()} to ${lastfmRange.latest.toLocaleDateString()}`);
  }
  console.log('');

  console.log(`Teal (Current):`);
  console.log(`  Total records: ${existingRecords.size}`);
  if (existingRange) {
    console.log(`  Date range: ${existingRange.earliest.toLocaleDateString()} to ${existingRange.latest.toLocaleDateString()}`);
  }
  console.log('');

  console.log(`Sync Result:`);
  console.log(`  Records to import: ${newRecords.length}`);
  console.log(`  Duplicates skipped: ${lastfmRecords.length - newRecords.length}`);
  console.log(`  Match rate: ${((1 - newRecords.length / lastfmRecords.length) * 100).toFixed(1)}%`);
  console.log('');
}
