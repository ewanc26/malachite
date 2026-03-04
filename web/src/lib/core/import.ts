/**
 * Import orchestration logic — pure TypeScript, no Svelte deps.
 * Handles all five ImportMode flows with progress + cancellation callbacks.
 *
 * All heavy logic (publisher, sync, merge) lives in src/core/ and is shared
 * with the CLI. Only the browser File-loading helpers and this orchestrator
 * are web-specific.
 */

import type { Agent } from '@atproto/api';
import type { ImportMode, LogEntry, PlayRecord } from '$core/types.js';
import { CLIENT_AGENT } from '../config.js';
import { parseLastFmFile, convertToPlayRecord } from './csv.js';
import { parseSpotifyFiles, convertSpotifyToPlayRecord } from './spotify.js';
import { mergePlayRecords, deduplicateInputRecords, sortRecords } from '$core/merge.js';
import {
  fetchExistingRecords,
  filterNewRecords,
  fetchAllRecordsForDedup,
  findDuplicateGroups,
  removeDuplicateRecords,
} from '$core/sync.js';
import { publishRecords, type PublishProgress } from '$core/publisher.js';

export type { PublishProgress };

export interface ImportOptions {
  dryRun: boolean;
  reverseOrder: boolean;
  fresh: boolean;
}

export interface ImportResult {
  success: number;
  errors: number;
  cancelled: boolean;
}

export interface ImportCallbacks {
  onLog: (level: LogEntry['level'], message: string) => void;
  onProgress: (p: PublishProgress) => void;
  isCancelled: () => boolean;
}

export async function runImport(
  agent: Agent,
  mode: ImportMode,
  lastfmFiles: File[],
  spotifyFiles: File[],
  { dryRun, reverseOrder, fresh }: ImportOptions,
  { onLog, onProgress, isCancelled }: ImportCallbacks,
): Promise<ImportResult> {
  // Single AbortController for every network call in this run.
  const ac = new AbortController();
  const poll = setInterval(() => { if (isCancelled()) ac.abort(); }, 50);
  const sig = ac.signal;

  const run = async (): Promise<ImportResult> => {
    // ── Deduplicate mode ─────────────────────────────────────────────────────
    if (mode === 'deduplicate') {
      onLog('section', '── Deduplication ──────────────────────────────────');
      onLog('info', 'Fetching existing records from Teal…');
      const all = await fetchAllRecordsForDedup(
        agent,
        (n) => onLog('progress', `  Fetched ${n.toLocaleString()} records…`),
        sig,
      );
      onLog('success', `Fetched ${all.length.toLocaleString()} records`);

      const groups = findDuplicateGroups(all);
      const totalDups = groups.reduce((s, g) => s + g.records.length - 1, 0);

      if (totalDups === 0) {
        onLog('success', 'No duplicates found — your records are clean.');
        return { success: 0, errors: 0, cancelled: false };
      }
      onLog('warn', `Found ${totalDups.toLocaleString()} duplicate(s) across ${groups.length} groups`);

      if (dryRun) {
        onLog('info', `[DRY RUN] Would remove ${totalDups} duplicate record(s).`);
        return { success: totalDups, errors: 0, cancelled: false };
      }

      onLog('info', 'Removing duplicates…');
      const removed = await removeDuplicateRecords(
        agent,
        groups,
        (n) => onLog('progress', `  Removed ${n}/${totalDups}…`),
        sig,
      );
      onLog('success', `Removed ${removed.toLocaleString()} duplicate(s)`);
      return { success: removed, errors: 0, cancelled: false };
    }

    // ── Load records ─────────────────────────────────────────────────────────
    onLog('section', '── Loading Records ─────────────────────────────────');
    let records: PlayRecord[] = [];

    if (mode === 'combined') {
      const lfRaw = await parseLastFmFile(lastfmFiles[0]);
      onLog('info', `Last.fm: ${lfRaw.length.toLocaleString()} scrobbles`);
      const spRaw = await parseSpotifyFiles(spotifyFiles);
      onLog('info', `Spotify: ${spRaw.length.toLocaleString()} tracks`);
      const { merged, stats } = mergePlayRecords(
        lfRaw.map((r) => convertToPlayRecord(r, CLIENT_AGENT)),
        spRaw.map((r) => convertSpotifyToPlayRecord(r, CLIENT_AGENT)),
      );
      records = merged;
      onLog('success', `Merged: ${stats.mergedTotal.toLocaleString()} unique records (${stats.duplicatesRemoved} removed)`);
    } else if (mode === 'spotify') {
      const spRaw = await parseSpotifyFiles(spotifyFiles);
      records = spRaw.map((r) => convertSpotifyToPlayRecord(r, CLIENT_AGENT));
      onLog('success', `Loaded ${records.length.toLocaleString()} Spotify records`);
    } else {
      const lfRaw = await parseLastFmFile(lastfmFiles[0]);
      records = lfRaw.map((r) => convertToPlayRecord(r, CLIENT_AGENT));
      onLog('success', `Loaded ${records.length.toLocaleString()} Last.fm records`);
    }

    const { unique, duplicates: inputDups } = deduplicateInputRecords(records);
    records = unique;
    if (inputDups > 0) onLog('warn', `Removed ${inputDups.toLocaleString()} duplicate(s) from input`);

    // ── Sync check ───────────────────────────────────────────────────────────
    onLog('section', '── Sync Check ───────────────────────────────────────');
    onLog('info', 'Fetching existing records from Teal…');
    const existing = await fetchExistingRecords(
      agent,
      (n) => onLog('progress', `  Fetched ${n.toLocaleString()} existing records…`),
      fresh,
      sig,
    );
    const before = records.length;
    records = filterNewRecords(records, existing);
    const skipped = before - records.length;
    if (skipped > 0) onLog('info', `Skipped ${skipped.toLocaleString()} already-imported record(s)`);
    onLog('success', `${records.length.toLocaleString()} new record(s) to import`);

    if (records.length === 0) {
      onLog('success', '✓ Nothing to import — all records already exist in Teal!');
      return { success: 0, errors: 0, cancelled: false };
    }
    if (mode !== 'combined') records = sortRecords(records, reverseOrder);

    // ── Publish ──────────────────────────────────────────────────────────────
    onLog('section', '── Publishing ───────────────────────────────────────');
    onLog('warn', 'Do not close this tab while publishing.');
    const res = await publishRecords(agent, records, dryRun, {
      onProgress,
      onLog: (level, msg) => onLog(level as LogEntry['level'], msg),
      isCancelled,
    });
    return { success: res.successCount, errors: res.errorCount, cancelled: res.cancelled };
  };

  try {
    return await run();
  } catch (err: unknown) {
    if (ac.signal.aborted) return { success: 0, errors: 0, cancelled: true };
    throw err;
  } finally {
    clearInterval(poll);
  }
}
