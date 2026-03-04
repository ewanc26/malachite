/**
 * Last.fm CSV parsing — environment-agnostic.
 * No Node.js deps; file I/O is the caller's responsibility.
 */

import type { LastFmCsvRecord, PlayRecord } from './types.js';
import { RECORD_TYPE } from './config.js';

// ─── delimiter detection ──────────────────────────────────────────────────────

function detectDelimiter(content: string): string {
  const firstLine = content.split('\n')[0];
  const delimiters = [',', ';', '\t', '|'];
  let maxCount = 0;
  let best = ',';
  for (const d of delimiters) {
    const count = firstLine.split(d).length;
    if (count > maxCount) { maxCount = count; best = d; }
  }
  return best;
}

// ─── column normalisation ─────────────────────────────────────────────────────

const COLUMN_MAP: Record<string, string> = {
  uts: 'uts', date: 'uts', timestamp: 'uts', played_at: 'uts', time: 'uts',
  artist: 'artist', artist_name: 'artist', artistname: 'artist',
  artist_mbid: 'artist_mbid', artistmbid: 'artist_mbid', artist_id: 'artist_mbid',
  album: 'album', album_name: 'album', albumname: 'album', release: 'album',
  album_mbid: 'album_mbid', albummbid: 'album_mbid', albumid: 'album_mbid', album_id: 'album_mbid',
  track: 'track', track_name: 'track', trackname: 'track', song: 'track', title: 'track',
  track_mbid: 'track_mbid', trackmbid: 'track_mbid', track_id: 'track_mbid',
  utc_time: 'utc_time', utctime: 'utc_time', datetime: 'utc_time',
};

function normalizeRecord(raw: Record<string, string>): LastFmCsvRecord {
  const normalized: Record<string, string> = {};
  for (const [k, v] of Object.entries(raw)) {
    const mapped = COLUMN_MAP[k.toLowerCase()];
    if (mapped) normalized[mapped] = v;
  }
  if (normalized['uts']) {
    const ts = normalized['uts'].toString();
    if (ts.length >= 13) normalized['uts'] = Math.floor(parseInt(ts) / 1000).toString();
  }
  if (normalized['uts'] && !normalized['utc_time']) {
    normalized['utc_time'] = new Date(parseInt(normalized['uts']) * 1000).toISOString();
  }
  return normalized as unknown as LastFmCsvRecord;
}

// ─── minimal CSV parser ───────────────────────────────────────────────────────

function parseCSV(content: string, delimiter: string): Record<string, string>[] {
  const lines = content.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 2) return [];

  const parseRow = (line: string): string[] => {
    const cells: string[] = [];
    let cur = '';
    let inQuote = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQuote && line[i + 1] === '"') { cur += '"'; i++; }
        else inQuote = !inQuote;
      } else if (ch === delimiter && !inQuote) {
        cells.push(cur.trim()); cur = '';
      } else {
        cur += ch;
      }
    }
    cells.push(cur.trim());
    return cells;
  };

  const headers = parseRow(lines[0]);
  const records: Record<string, string>[] = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = parseRow(lines[i]);
    const record: Record<string, string> = {};
    headers.forEach((h, idx) => { record[h] = cells[idx] ?? ''; });
    records.push(record);
  }
  return records;
}

// ─── public API ───────────────────────────────────────────────────────────────

/**
 * Parse a raw Last.fm CSV string into normalised records.
 * Handles BOM, username comments in the header, and delimiter auto-detection.
 */
export function parseLastFmCsvContent(rawContent: string): LastFmCsvRecord[] {
  let content = rawContent;
  // Strip BOM
  if (content.charCodeAt(0) === 0xfeff) content = content.slice(1);
  // Strip username comment from header line
  const lines = content.split('\n');
  lines[0] = lines[0].split('#')[0].trim();
  content = lines.join('\n');

  const delimiter = detectDelimiter(content);
  const raw = parseCSV(content, delimiter);
  const records = raw.map(normalizeRecord);
  return records.filter((r) => r.artist && r.track && r.uts);
}

/**
 * Convert a normalised Last.fm CSV record to an ATProto play record.
 *
 * @param clientAgent  The `submissionClientAgent` string for this runtime
 *                     (e.g. `malachite/v0.10.0` for CLI, `malachite/v0.3.0 (web)` for web).
 */
export function convertToPlayRecord(csv: LastFmCsvRecord, clientAgent: string): PlayRecord {
  const playedTime = new Date(parseInt(csv.uts) * 1000).toISOString();

  const artists: PlayRecord['artists'] = [];
  if (csv.artist) {
    const a: PlayRecord['artists'][0] = { artistName: csv.artist };
    if (csv.artist_mbid?.trim()) a.artistMbId = csv.artist_mbid;
    artists.push(a);
  }

  const record: PlayRecord = {
    $type: RECORD_TYPE,
    trackName: csv.track,
    artists,
    playedTime,
    submissionClientAgent: clientAgent,
    musicServiceBaseDomain: 'last.fm',
    originUrl: '',
  };

  if (csv.album?.trim()) record.releaseName = csv.album;
  if (csv.album_mbid?.trim()) record.releaseMbId = csv.album_mbid;
  if (csv.track_mbid?.trim()) record.recordingMbId = csv.track_mbid;

  const aEnc = encodeURIComponent(csv.artist);
  const tEnc = encodeURIComponent(csv.track);
  record.originUrl = `https://www.last.fm/music/${aEnc}/_/${tEnc}`;

  return record;
}
