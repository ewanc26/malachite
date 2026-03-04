/**
 * Shared type definitions — environment-agnostic.
 * Used by both the CLI (src/lib/) and the web (web/src/lib/core/).
 */

export interface LastFmCsvRecord {
  uts: string;
  utc_time: string;
  artist: string;
  artist_mbid?: string;
  album: string;
  album_mbid?: string;
  track: string;
  track_mbid?: string;
}

export interface PlayRecordArtist {
  artistName: string;
  artistMbId?: string;
}

export interface PlayRecord {
  $type: string;
  trackName: string;
  artists: PlayRecordArtist[];
  playedTime: string;
  submissionClientAgent: string;
  musicServiceBaseDomain: string;
  releaseName?: string;
  releaseMbId?: string;
  recordingMbId?: string;
  originUrl: string;
}

export interface PublishResult {
  successCount: number;
  errorCount: number;
  cancelled: boolean;
}

export type ImportMode = 'lastfm' | 'spotify' | 'combined' | 'sync' | 'deduplicate';

export interface SpotifyRecord {
  ts: string;
  platform: string;
  ms_played: number;
  conn_country: string;
  master_metadata_track_name: string | null;
  master_metadata_album_artist_name: string | null;
  master_metadata_album_album_name: string | null;
  spotify_track_uri: string | null;
  episode_name: string | null;
  episode_show_name: string | null;
  spotify_episode_uri: string | null;
  reason_start: string;
  reason_end: string;
  shuffle: boolean;
  skipped: boolean;
  offline: boolean;
  offline_timestamp: number | null;
  incognito_mode: boolean;
}

export type LogLevel = 'info' | 'success' | 'warn' | 'error' | 'progress' | 'section';

export interface LogEntry {
  level: LogLevel;
  message: string;
  timestamp: number;
}
