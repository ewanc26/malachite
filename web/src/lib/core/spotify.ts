/**
 * Browser-compatible Spotify JSON parser.
 * Mirrors src/lib/spotify.ts without any Node.js deps.
 */

import type { SpotifyRecord, PlayRecord } from '../types.js';
import { RECORD_TYPE, CLIENT_AGENT } from '../config.js';

export function parseSpotifyJsonContent(records: SpotifyRecord[]): SpotifyRecord[] {
  return records.filter(
    (r) => r.master_metadata_track_name && r.master_metadata_album_artist_name
  );
}

export async function parseSpotifyFiles(files: File[]): Promise<SpotifyRecord[]> {
  let all: SpotifyRecord[] = [];
  for (const file of files) {
    const text = await file.text();
    const parsed = JSON.parse(text) as SpotifyRecord[];
    all = all.concat(parsed);
  }
  return parseSpotifyJsonContent(all);
}

export function convertSpotifyToPlayRecord(r: SpotifyRecord): PlayRecord {
  const artists: PlayRecord['artists'] = [];
  if (r.master_metadata_album_artist_name) {
    artists.push({ artistName: r.master_metadata_album_artist_name });
  }

  const record: PlayRecord = {
    $type: RECORD_TYPE,
    trackName: r.master_metadata_track_name ?? 'Unknown Track',
    artists,
    playedTime: r.ts,
    submissionClientAgent: CLIENT_AGENT,
    musicServiceBaseDomain: 'spotify.com',
    originUrl: ''
  };

  if (r.master_metadata_album_album_name) record.releaseName = r.master_metadata_album_album_name;
  if (r.spotify_track_uri) {
    const id = r.spotify_track_uri.replace('spotify:track:', '');
    record.originUrl = `https://open.spotify.com/track/${id}`;
  }

  return record;
}
