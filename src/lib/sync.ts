import type { AtpAgent } from '@atproto/api';
import type { PlayRecord, Config } from '../types.js';
import { fetchRepoViaCAR, getPdsUrlFromAgent, getAgentToken } from '../utils/car-fetch.js';
import { formatDate, formatDateRange } from '../utils/helpers.js';
import * as ui from '../utils/ui.js';
import { log } from '../utils/logger.js';
import { isCacheValid, loadCache, saveCache, getCacheInfo } from '../utils/teal-cache.js';

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
 * Fetch all existing play records from Teal via a single CAR export.
 * Uses com.atproto.sync.getRepo (sync namespace) — separate, generous
 * rate-limit envelope; burns zero AppView write-quota points.
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

  // Serve from cache when valid and not forcing a refresh
  if (!forceRefresh && isCacheValid(did)) {
    const cacheInfo = getCacheInfo(did);
    log.info(`📂 Loading from cache (${cacheInfo.age!.toFixed(1)}h old, ${cacheInfo.records!.toLocaleString()} records)...`);
    const cached = loadCache(did);
    if (cached) {
      const existingRecords = new Map<string, ExistingRecord>();
      for (const [, record] of cached.entries()) {
        const playRecord = record.value as PlayRecord;
        existingRecords.set(createRecordKey(playRecord), record as ExistingRecord);
      }
      log.success(`✓ Loaded ${existingRecords.size.toLocaleString()} records from cache`);
      log.blank();
      return existingRecords;
    }
  }

  if (forceRefresh) {
    log.info('🔄 Force refresh — fetching repo via CAR export...');
  } else {
    log.info('📦 Fetching repo via CAR export (no rate-limit points consumed)...');
  }

  const pdsUrl = getPdsUrlFromAgent(agent);
  const token = await getAgentToken(agent);
  const carStart = Date.now();
  const carRecords = await fetchRepoViaCAR(pdsUrl, did, RECORD_TYPE, undefined, token);
  const carElapsed = ((Date.now() - carStart) / 1000).toFixed(1);

  const existingRecords = new Map<string, ExistingRecord>();
  const cacheMap = new Map<string, { uri: string; cid: string; value: any }>();

  for (const rec of carRecords) {
    const playRecord = rec.value as PlayRecord;
    const entry = { uri: rec.uri, cid: rec.cid, value: playRecord };
    existingRecords.set(createRecordKey(playRecord), entry);
    cacheMap.set(rec.uri, entry);
  }

  log.success(`✓ Loaded ${existingRecords.size.toLocaleString()} records via CAR in ${carElapsed}s`);
  saveCache(did, cacheMap);
  log.blank();
  return existingRecords;
}

/**
 * Fetch ALL existing play records as an array (including duplicates) via CAR export.
 * Used by the deduplicate flow.
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

  ui.startSpinner('📦 Fetching repo via CAR export...');

  const pdsUrl = getPdsUrlFromAgent(agent);
  const token = await getAgentToken(agent);
  const carRecords = await fetchRepoViaCAR(pdsUrl, did, RECORD_TYPE, undefined, token);
  const allRecords: ExistingRecord[] = carRecords.map((rec) => ({
    uri: rec.uri,
    cid: rec.cid,
    value: rec.value as PlayRecord,
  }));

  ui.succeedSpinner(`Found ${allRecords.length.toLocaleString()} records via CAR`);
  return allRecords;
}

/**
 * Create a unique key for a play record based on its essential properties.
 */
export function createRecordKey(record: PlayRecord): string {
  const artist = (record.artists[0]?.artistName ?? '').toLowerCase().trim();
  const track = record.trackName.toLowerCase().trim();
  return `${artist}|||${track}|||${record.playedTime}`;
}

/**
 * Deduplicate input records before submission.
 * Keeps the first occurrence of each duplicate.
 */
export function deduplicateInputRecords(records: PlayRecord[]): { unique: PlayRecord[]; duplicates: number } {
  const seen = new Map<string, PlayRecord>();
  let duplicates = 0;

  for (const record of records) {
    const key = createRecordKey(record);
    if (!seen.has(key)) {
      seen.set(key, record);
    } else {
      duplicates++;
    }
  }

  return { unique: Array.from(seen.values()), duplicates };
}

/**
 * Filter out records that already exist in Teal.
 */
export function filterNewRecords(
  lastfmRecords: PlayRecord[],
  existingRecords: Map<string, ExistingRecord>
): PlayRecord[] {
  log.section('Identifying New Records');

  const newRecords: PlayRecord[] = [];
  const duplicates: PlayRecord[] = [];

  for (const record of lastfmRecords) {
    if (existingRecords.has(createRecordKey(record))) {
      duplicates.push(record);
    } else {
      newRecords.push(record);
    }
  }

  log.info(`Total: ${lastfmRecords.length.toLocaleString()} records`);
  log.info(`Existing: ${duplicates.length.toLocaleString()} already in Teal`);
  log.info(`New: ${newRecords.length.toLocaleString()} to import`);
  log.blank();

  if (log.getLevel() <= 0 && duplicates.length > 0) {
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
 * Get time range of records.
 */
export function getRecordTimeRange(records: PlayRecord[]): { earliest: Date; latest: Date } | null {
  if (records.length === 0) return null;
  const times = records.map(r => new Date(r.playedTime).getTime());
  return {
    earliest: new Date(Math.min(...times)),
    latest: new Date(Math.max(...times)),
  };
}

/**
 * Display sync statistics.
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
 * Find duplicate records in the existing records.
 * Returns groups of duplicates (each group has 2+ records with the same key).
 */
export function findDuplicates(allRecords: ExistingRecord[]): DuplicateGroup[] {
  const keyGroups = new Map<string, ExistingRecord[]>();

  for (const record of allRecords) {
    const key = createRecordKey(record.value);
    if (!keyGroups.has(key)) keyGroups.set(key, []);
    keyGroups.get(key)!.push(record);
  }

  const duplicates: DuplicateGroup[] = [];
  for (const [key, records] of keyGroups) {
    if (records.length > 1) duplicates.push({ key, records });
  }
  return duplicates;
}

/**
 * Remove duplicate records from Teal, keeping only the first occurrence.
 */
export async function removeDuplicates(
  agent: AtpAgent,
  config: Config,
  dryRun: boolean = false
): Promise<{ totalDuplicates: number; recordsRemoved: number }> {
  ui.header('Checking for Duplicate Records');

  const allRecords = await fetchAllRecords(agent, config);

  ui.startSpinner('Analyzing records for duplicates...');
  const duplicateGroups = findDuplicates(allRecords);

  if (duplicateGroups.length === 0) {
    ui.succeedSpinner('No duplicates found!');
    return { totalDuplicates: 0, recordsRemoved: 0 };
  }

  ui.stopSpinner();

  const totalDuplicates = duplicateGroups.reduce((sum, group) => sum + (group.records.length - 1), 0);

  ui.warning(`Found ${duplicateGroups.length.toLocaleString()} duplicate groups (${totalDuplicates.toLocaleString()} records to remove)`);
  console.log('');

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

  console.log('');
  const progressBar = ui.createProgressBar(totalDuplicates, 'Removing duplicates');
  let recordsRemoved = 0;
  const startTime = Date.now();

  for (const group of duplicateGroups) {
    for (const record of group.records.slice(1)) {
      try {
        await agent.com.atproto.repo.deleteRecord({
          repo: agent.session?.did || '',
          collection: record.value.$type,
          rkey: record.uri.split('/').pop()!,
        });
        recordsRemoved++;
        const elapsed = (Date.now() - startTime) / 1000;
        progressBar.update(recordsRemoved, { speed: recordsRemoved / Math.max(elapsed, 0.1) });
      } catch {
        // continue on individual failures
      }
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  progressBar.stop();
  console.log('');
  ui.success(`Removed ${recordsRemoved.toLocaleString()} duplicate records`);
  ui.info(`Kept ${duplicateGroups.length.toLocaleString()} unique records`);

  return { totalDuplicates, recordsRemoved };
}
