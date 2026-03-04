/**
 * Spotify JSON parsing — environment-agnostic.
 * No Node.js deps; file I/O is the caller's responsibility.
 */

import type { SpotifyRecord, PlayRecord } from './types.js';
import { RECORD_TYPE } from './config.js';

export type { SpotifyRecord };

/**
 * Filter raw Spotify records, keeping only music tracks (not podcasts).
 */
export function parseSpotifyJsonContent(records: SpotifyRecord[]): SpotifyRecord[] {
  return records.filter(
    (r) => r.master_metadata_track_name && r.master_metadata_album_artist_name
  );
}

/**
 * Convert a Spotify record to an ATProto play record.
 *
 * @param clientAgent  The `submissionClientAgent` string for this runtime.
 */
export function convertSpotifyToPlayRecord(r: SpotifyRecord, clientAgent: string): PlayRecord {
  const artists: PlayRecord['artists'] = [];
  if (r.master_metadata_album_artist_name) {
    artists.push({ artistName: r.master_metadata_album_artist_name });
  }

  const record: PlayRecord = {
    $type: RECORD_TYPE,
    trackName: r.master_metadata_track_name ?? 'Unknown Track',
    artists,
    playedTime: r.ts,
    submissionClientAgent: clientAgent,
    musicServiceBaseDomain: 'spotify.com',
    originUrl: '',
  };

  if (r.master_metadata_album_album_name) record.releaseName = r.master_metadata_album_album_name;
  if (r.spotify_track_uri) {
    const id = r.spotify_track_uri.replace('spotify:track:', '');
    record.originUrl = `https://open.spotify.com/track/${id}`;
  }

  return record;
}
