import * as fs from 'fs';
import * as path from 'path';
import type { PlayRecord, Config } from '../types.js';
import { buildClientAgent } from '../config.js';

/**
 * Spotify streaming history record
 */
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

/**
 * Parse Spotify JSON export
 * Supports both single files and directories with multiple JSON files
 */
export function parseSpotifyJson(filePathOrDir: string): SpotifyRecord[] {
  console.log(`Reading Spotify export: ${filePathOrDir}`);
  
  const stats = fs.statSync(filePathOrDir);
  let allRecords: SpotifyRecord[] = [];
  
  if (stats.isDirectory()) {
    // Read all JSON files in the directory
    const files = fs.readdirSync(filePathOrDir)
      .filter(f => f.endsWith('.json') && f.startsWith('Streaming_History_Audio'))
      .map(f => path.join(filePathOrDir, f));
    
    console.log(`Found ${files.length} Spotify JSON files in directory`);
    
    for (const file of files) {
      const fileContent = fs.readFileSync(file, 'utf-8');
      const records = JSON.parse(fileContent) as SpotifyRecord[];
      allRecords = allRecords.concat(records);
      console.log(`  ${path.basename(file)}: ${records.length} records`);
    }
  } else {
    // Single file
    const fileContent = fs.readFileSync(filePathOrDir, 'utf-8');
    allRecords = JSON.parse(fileContent) as SpotifyRecord[];
  }
  
  // Filter out records without track names (podcasts, audiobooks, etc.)
  const trackRecords = allRecords.filter(r => 
    r.master_metadata_track_name && 
    r.master_metadata_album_artist_name
  );
  
  console.log(`✓ Parsed ${trackRecords.length} track records (filtered ${allRecords.length - trackRecords.length} non-music records)\n`);
  return trackRecords;
}

/**
 * Convert Spotify record to ATProto play record
 */
export function convertSpotifyToPlayRecord(spotifyRecord: SpotifyRecord, config: Config, debug = false): PlayRecord {
  const { RECORD_TYPE } = config;

  // Spotify timestamp is already in ISO 8601 format
  const playedTime = spotifyRecord.ts;

  // Build artists array
  const artists: PlayRecord['artists'] = [];
  if (spotifyRecord.master_metadata_album_artist_name) {
    artists.push({
      artistName: spotifyRecord.master_metadata_album_artist_name,
    });
  }

  // Build the play record
  const playRecord: PlayRecord = {
    $type: RECORD_TYPE,
    trackName: spotifyRecord.master_metadata_track_name || 'Unknown Track',
    artists,
    playedTime,
    submissionClientAgent: buildClientAgent(debug),
    musicServiceBaseDomain: 'spotify.com',
    originUrl: '',
  };

  // Add optional fields
  if (spotifyRecord.master_metadata_album_album_name) {
    playRecord.releaseName = spotifyRecord.master_metadata_album_album_name;
  }

  // Generate Spotify URL if we have the track URI
  if (spotifyRecord.spotify_track_uri) {
    const trackId = spotifyRecord.spotify_track_uri.replace('spotify:track:', '');
    playRecord.originUrl = `https://open.spotify.com/track/${trackId}`;
  }

  return playRecord;
}
