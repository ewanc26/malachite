import type { LastFmCsvRecord, PlayRecord, Config } from '../types.js';
import { buildClientAgent } from '../config.js';

/**
 * Normalize column names to expected format
 * Handles various CSV formats from Last.fm and other sources
 */
export function normalizeColumns(record: any): LastFmCsvRecord {
  // Create a mapping of possible column names to our expected format
  const columnMappings: { [key: string]: string } = {
    // Timestamp fields
    'uts': 'uts',
    'date': 'uts',
    'timestamp': 'uts',
    'played_at': 'uts',
    'time': 'uts',
    
    // Artist fields
    'artist': 'artist',
    'artist_name': 'artist',
    'artistname': 'artist',
    
    // Artist MBID fields
    'artist_mbid': 'artist_mbid',
    'artistmbid': 'artist_mbid',
    'artist_id': 'artist_mbid',
    
    // Album fields
    'album': 'album',
    'album_name': 'album',
    'albumname': 'album',
    'release': 'album',
    
    // Album MBID fields
    'album_mbid': 'album_mbid',
    'albummbid': 'album_mbid',
    'albumid': 'album_mbid',
    'album_id': 'album_mbid',
    
    // Track fields
    'track': 'track',
    'track_name': 'track',
    'trackname': 'track',
    'song': 'track',
    'title': 'track',
    
    // Track MBID fields
    'track_mbid': 'track_mbid',
    'trackmbid': 'track_mbid',
    'track_id': 'track_mbid',
    
    // UTC time field
    'utc_time': 'utc_time',
    'utctime': 'utc_time',
    'datetime': 'utc_time',
  };
  
  const normalized: any = {};
  
  // Convert all keys to lowercase for matching
  const recordLowercase: { [key: string]: any } = {};
  for (const [key, value] of Object.entries(record)) {
    recordLowercase[key.toLowerCase()] = value;
  }
  
  // Map columns to expected names
  for (const [originalName, mappedName] of Object.entries(columnMappings)) {
    if (recordLowercase[originalName] !== undefined) {
      normalized[mappedName] = recordLowercase[originalName];
    }
  }
  
  // Handle timestamp conversion
  if (normalized.uts) {
    const timestamp = normalized.uts.toString();
    // If timestamp is in milliseconds (13+ digits), convert to seconds
    if (timestamp.length >= 13) {
      normalized.uts = Math.floor(parseInt(timestamp) / 1000).toString();
    }
  }
  
  // Generate utc_time from uts if not present
  if (normalized.uts && !normalized.utc_time) {
    const date = new Date(parseInt(normalized.uts) * 1000);
    normalized.utc_time = date.toISOString();
  }
  
  return normalized as LastFmCsvRecord;
}

/**
 * Convert Last.fm CSV record to ATProto play record
 */
export function convertToPlayRecord(csvRecord: LastFmCsvRecord, config: Config, debug = false): PlayRecord {
  const { RECORD_TYPE } = config;

  // Parse the timestamp
  const timestamp = parseInt(csvRecord.uts);
  const playedTime = new Date(timestamp * 1000).toISOString();

  // Build artists array
  const artists: PlayRecord['artists'] = [];
  if (csvRecord.artist) {
    const artistData: PlayRecord['artists'][0] = {
      artistName: csvRecord.artist,
    };
    if (csvRecord.artist_mbid && csvRecord.artist_mbid.trim()) {
      artistData.artistMbId = csvRecord.artist_mbid;
    }
    artists.push(artistData);
  }

  // Build the play record
  const playRecord: PlayRecord = {
    $type: RECORD_TYPE,
    trackName: csvRecord.track,
    artists,
    playedTime,
    submissionClientAgent: buildClientAgent(debug),
    musicServiceBaseDomain: 'last.fm',
    originUrl: '',
  };

  // Add optional fields
  if (csvRecord.album && csvRecord.album.trim()) {
    playRecord.releaseName = csvRecord.album;
  }

  if (csvRecord.album_mbid && csvRecord.album_mbid.trim()) {
    playRecord.releaseMbId = csvRecord.album_mbid;
  }

  if (csvRecord.track_mbid && csvRecord.track_mbid.trim()) {
    playRecord.recordingMbId = csvRecord.track_mbid;
  }

  // Generate Last.fm URL
  const artistEncoded = encodeURIComponent(csvRecord.artist);
  const trackEncoded = encodeURIComponent(csvRecord.track);
  playRecord.originUrl = `https://www.last.fm/music/${artistEncoded}/_/${trackEncoded}`;

  return playRecord;
}
