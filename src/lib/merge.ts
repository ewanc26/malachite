import type { PlayRecord, Config } from '../types.js';
import { parseLastFmCsv, convertToPlayRecord } from './csv.js';
import { parseSpotifyJson, convertSpotifyToPlayRecord } from './spotify.js';
import { formatDate, normalizeString } from '../utils/helpers.js';
import { log } from '../utils/logger.js';

/**
 * Normalized record for comparison
 */
interface NormalizedRecord {
  original: PlayRecord;
  normalizedTrack: string;
  normalizedArtist: string;
  timestamp: number;
  source: 'lastfm' | 'spotify';
}

/**
 * Check if two records represent the same play
 * They match if they have:
 * 1. The same timestamp (within 5 minutes)
 * 2. The same normalized track name
 * 3. The same normalized artist name
 */
function areRecordsDuplicates(a: NormalizedRecord, b: NormalizedRecord): boolean {
  // Check timestamp within 5 minutes (300000 ms)
  const timeDiff = Math.abs(a.timestamp - b.timestamp);
  if (timeDiff > 300000) {
    return false;
  }

  // Check normalized track and artist
  return (
    a.normalizedTrack === b.normalizedTrack &&
    a.normalizedArtist === b.normalizedArtist
  );
}

/**
 * Choose the better record between two duplicates
 * Prefer Last.fm if it has MusicBrainz IDs, otherwise prefer Spotify
 */
function chooseBetterRecord(a: NormalizedRecord, b: NormalizedRecord): PlayRecord {
  // Prefer Last.fm if it has any MusicBrainz IDs
  const aHasMbIds = a.source === 'lastfm' && (
    a.original.recordingMbId ||
    a.original.releaseMbId ||
    (a.original.artists[0]?.artistMbId)
  );

  const bHasMbIds = b.source === 'lastfm' && (
    b.original.recordingMbId ||
    b.original.releaseMbId ||
    (b.original.artists[0]?.artistMbId)
  );

  if (aHasMbIds && !bHasMbIds) return a.original;
  if (bHasMbIds && !aHasMbIds) return b.original;

  // Otherwise prefer Spotify for its better metadata quality
  if (a.source === 'spotify') return a.original;
  if (b.source === 'spotify') return b.original;

  // Default to first record
  return a.original;
}

/**
 * Merge and deduplicate records from multiple sources
 */
function mergeRecords(
  lastfmRecords: PlayRecord[],
  spotifyRecords: PlayRecord[]
): { merged: PlayRecord[]; stats: MergeStats } {
  log.info('Merging Last.fm and Spotify exports...');
  log.blank();
  
  const stats: MergeStats = {
    lastfmTotal: lastfmRecords.length,
    spotifyTotal: spotifyRecords.length,
    duplicatesRemoved: 0,
    lastfmUnique: 0,
    spotifyUnique: 0,
    mergedTotal: 0,
  };

  // Normalize all records
  const normalizedLastFm: NormalizedRecord[] = lastfmRecords.map(record => ({
    original: record,
    normalizedTrack: normalizeString(record.trackName),
    normalizedArtist: normalizeString(record.artists[0]?.artistName || ''),
    timestamp: new Date(record.playedTime).getTime(),
    source: 'lastfm' as const,
  }));

  const normalizedSpotify: NormalizedRecord[] = spotifyRecords.map(record => ({
    original: record,
    normalizedTrack: normalizeString(record.trackName),
    normalizedArtist: normalizeString(record.artists[0]?.artistName || ''),
    timestamp: new Date(record.playedTime).getTime(),
    source: 'spotify' as const,
  }));

  // Combine all records
  const allRecords = [...normalizedLastFm, ...normalizedSpotify];

  // Sort by timestamp
  allRecords.sort((a, b) => a.timestamp - b.timestamp);

  // Deduplicate
  const uniqueRecords: PlayRecord[] = [];
  const seen = new Set<string>();

  for (const record of allRecords) {
    // Create a key for duplicate detection
    const key = `${record.normalizedTrack}|${record.normalizedArtist}|${Math.floor(record.timestamp / 60000)}`; // Round to minute

    if (seen.has(key)) {
      // Find the existing record to compare
      const existingIndex = uniqueRecords.findIndex(r => {
        const normalized: NormalizedRecord = {
          original: r,
          normalizedTrack: normalizeString(r.trackName),
          normalizedArtist: normalizeString(r.artists[0]?.artistName || ''),
          timestamp: new Date(r.playedTime).getTime(),
          source: r.musicServiceBaseDomain === 'last.fm' ? 'lastfm' : 'spotify',
        };
        return areRecordsDuplicates(record, normalized);
      });

      if (existingIndex !== -1) {
        // This is a duplicate - choose the better one
        const existing: NormalizedRecord = {
          original: uniqueRecords[existingIndex],
          normalizedTrack: normalizeString(uniqueRecords[existingIndex].trackName),
          normalizedArtist: normalizeString(uniqueRecords[existingIndex].artists[0]?.artistName || ''),
          timestamp: new Date(uniqueRecords[existingIndex].playedTime).getTime(),
          source: uniqueRecords[existingIndex].musicServiceBaseDomain === 'last.fm' ? 'lastfm' : 'spotify',
        };

        uniqueRecords[existingIndex] = chooseBetterRecord(existing, record);
        stats.duplicatesRemoved++;
        continue;
      }
    }

    seen.add(key);
    uniqueRecords.push(record.original);

    // Track source statistics
    if (record.source === 'lastfm') {
      stats.lastfmUnique++;
    } else {
      stats.spotifyUnique++;
    }
  }

  stats.mergedTotal = uniqueRecords.length;

  // Sort final records chronologically
  uniqueRecords.sort((a, b) => {
    const timeA = new Date(a.playedTime).getTime();
    const timeB = new Date(b.playedTime).getTime();
    return timeA - timeB;
  });

  return { merged: uniqueRecords, stats };
}

/**
 * Statistics about the merge operation
 */
export interface MergeStats {
  lastfmTotal: number;
  spotifyTotal: number;
  duplicatesRemoved: number;
  lastfmUnique: number;
  spotifyUnique: number;
  mergedTotal: number;
}

/**
 * Display merge statistics
 */
function displayMergeStats(stats: MergeStats, merged: PlayRecord[]): void {
  log.blank();
  log.section('Merge Statistics');
  log.info(`Last.fm: ${stats.lastfmTotal.toLocaleString()} records`);
  log.info(`Spotify: ${stats.spotifyTotal.toLocaleString()} records`);
  log.info(`Duplicates: ${stats.duplicatesRemoved.toLocaleString()} removed`);
  log.info(`Result: ${stats.mergedTotal.toLocaleString()} unique records`);

  if (merged.length > 0) {
    const firstPlay = formatDate(merged[0].playedTime);
    const lastPlay = formatDate(merged[merged.length - 1].playedTime);
    log.info(`Range: ${firstPlay} to ${lastPlay}`);
  }
  log.blank();
}

/**
 * Parse and merge Last.fm and Spotify exports
 */
export function parseCombinedExports(
  lastfmPath: string,
  spotifyPath: string,
  config: Config,
  debug = false
): PlayRecord[] {
  log.section('Combined Import Mode');
  log.blank();

  // Parse Last.fm
  log.info('Parsing Last.fm export...');
  const lastfmCsvRecords = parseLastFmCsv(lastfmPath);
  const lastfmRecords = lastfmCsvRecords.map(r => convertToPlayRecord(r, config, debug));

  // Parse Spotify
  log.info('Parsing Spotify export...');
  const spotifyJsonRecords = parseSpotifyJson(spotifyPath);
  const spotifyRecords = spotifyJsonRecords.map(r => convertSpotifyToPlayRecord(r, config, debug));

  // Merge and deduplicate
  const { merged, stats } = mergeRecords(lastfmRecords, spotifyRecords);

  // Display statistics
  displayMergeStats(stats, merged);

  return merged;
}
