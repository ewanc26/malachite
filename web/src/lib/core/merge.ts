/**
 * Browser-compatible merge logic.
 * Mirrors src/lib/merge.ts without Node.js deps or file I/O.
 */

import type { PlayRecord } from '../types.js';

function normalizeString(s: string): string {
  return s.toLowerCase().replace(/[^\w\s]/g, '').replace(/\s+/g, ' ').trim();
}

interface NormalizedRecord {
  original: PlayRecord;
  normalizedTrack: string;
  normalizedArtist: string;
  timestamp: number;
  source: 'lastfm' | 'spotify';
}

function areDuplicates(a: NormalizedRecord, b: NormalizedRecord): boolean {
  return (
    Math.abs(a.timestamp - b.timestamp) <= 300_000 &&
    a.normalizedTrack === b.normalizedTrack &&
    a.normalizedArtist === b.normalizedArtist
  );
}

function betterRecord(a: NormalizedRecord, b: NormalizedRecord): PlayRecord {
  const hasMb = (n: NormalizedRecord) =>
    n.source === 'lastfm' &&
    (n.original.recordingMbId || n.original.releaseMbId || n.original.artists[0]?.artistMbId);
  if (hasMb(a) && !hasMb(b)) return a.original;
  if (hasMb(b) && !hasMb(a)) return b.original;
  return a.source === 'spotify' ? a.original : b.original;
}

export interface MergeStats {
  lastfmTotal: number;
  spotifyTotal: number;
  duplicatesRemoved: number;
  mergedTotal: number;
}

export function mergePlayRecords(
  lastfmRecords: PlayRecord[],
  spotifyRecords: PlayRecord[]
): { merged: PlayRecord[]; stats: MergeStats } {
  const toNorm = (r: PlayRecord, source: 'lastfm' | 'spotify'): NormalizedRecord => ({
    original: r,
    normalizedTrack: normalizeString(r.trackName),
    normalizedArtist: normalizeString(r.artists[0]?.artistName ?? ''),
    timestamp: new Date(r.playedTime).getTime(),
    source
  });

  const all = [
    ...lastfmRecords.map((r) => toNorm(r, 'lastfm')),
    ...spotifyRecords.map((r) => toNorm(r, 'spotify'))
  ].sort((a, b) => a.timestamp - b.timestamp);

  const unique: PlayRecord[] = [];
  const seen = new Set<string>();
  let dups = 0;

  for (const rec of all) {
    const key = `${rec.normalizedTrack}|${rec.normalizedArtist}|${Math.floor(rec.timestamp / 60_000)}`;
    if (seen.has(key)) {
      const idx = unique.findIndex((u) => {
        const n = toNorm(u, u.musicServiceBaseDomain === 'last.fm' ? 'lastfm' : 'spotify');
        return areDuplicates(rec, n);
      });
      if (idx !== -1) {
        const existing = toNorm(unique[idx], unique[idx].musicServiceBaseDomain === 'last.fm' ? 'lastfm' : 'spotify');
        unique[idx] = betterRecord(existing, rec);
        dups++;
        continue;
      }
    }
    seen.add(key);
    unique.push(rec.original);
  }

  unique.sort((a, b) => new Date(a.playedTime).getTime() - new Date(b.playedTime).getTime());

  return {
    merged: unique,
    stats: {
      lastfmTotal: lastfmRecords.length,
      spotifyTotal: spotifyRecords.length,
      duplicatesRemoved: dups,
      mergedTotal: unique.length
    }
  };
}

/** Remove duplicate records within a single input set (keep first occurrence). */
export function deduplicateInputRecords(records: PlayRecord[]): { unique: PlayRecord[]; duplicates: number } {
  const seen = new Map<string, PlayRecord>();
  let dups = 0;
  for (const r of records) {
    const key = `${(r.artists[0]?.artistName ?? '').toLowerCase()}|||${r.trackName.toLowerCase()}|||${r.playedTime}`;
    if (!seen.has(key)) seen.set(key, r);
    else dups++;
  }
  return { unique: Array.from(seen.values()), duplicates: dups };
}

export function sortRecords(records: PlayRecord[], reverseChronological = false): PlayRecord[] {
  return [...records].sort((a, b) => {
    const diff = new Date(a.playedTime).getTime() - new Date(b.playedTime).getTime();
    return reverseChronological ? -diff : diff;
  });
}
