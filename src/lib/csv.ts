import * as fs from 'fs';
import { parse } from 'csv-parse/sync';
import type { LastFmCsvRecord, PlayRecord, Config } from '../types.js';
import { buildClientAgent } from '../config.js';

/**
 * Detect CSV delimiter by checking first line
 */
function detectDelimiter(content: string): string {
  const firstLine = content.split('\n')[0];
  const delimiters = [',', ';', '\t', '|'];
  
  let maxCount = 0;
  let detectedDelimiter = ',';
  
  for (const delimiter of delimiters) {
    const count = firstLine.split(delimiter).length;
    if (count > maxCount) {
      maxCount = count;
      detectedDelimiter = delimiter;
    }
  }
  
  return detectedDelimiter;
}

/**
 * Normalize column names to expected format
 */
function normalizeColumns(record: any): LastFmCsvRecord {
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
 * Parse Last.fm CSV export with dynamic delimiter detection and column mapping
 */
export function parseLastFmCsv(filePath: string): LastFmCsvRecord[] {
  console.log(`Reading CSV file: ${filePath}`);
  let fileContent = fs.readFileSync(filePath, 'utf-8');
  
  // Remove BOM if present
  if (fileContent.charCodeAt(0) === 0xFEFF) {
    fileContent = fileContent.slice(1);
  }
  
  // Clean up header line - remove any trailing content after column names
  const lines = fileContent.split('\n');
  if (lines.length > 0) {
    // Remove anything after # in the header (like username)
    lines[0] = lines[0].split('#')[0].trim();
    fileContent = lines.join('\n');
  }
  
  // Detect delimiter
  const delimiter = detectDelimiter(fileContent);
  console.log(`  Detected delimiter: "${delimiter}"`);
  
  try {
    const rawRecords = parse(fileContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
      delimiter: delimiter,
      relax_quotes: true,
      relax_column_count: true,
    });
    
    // Normalize all records to expected format
    const records = rawRecords.map(normalizeColumns);
    
    // Validate that we have required fields
    const validRecords = records.filter((record: LastFmCsvRecord) => {
      return record.artist && record.track && record.uts;
    });
    
    if (validRecords.length === 0) {
      console.error('\n⚠️  Warning: No valid records found after parsing.');
      console.error('   Required fields: artist, track, and timestamp');
      console.error('   Available columns:', Object.keys(rawRecords[0] || {}));
    }
    
    console.log(`✓ Parsed ${validRecords.length} scrobbles\n`);
    return validRecords;
  } catch (error) {
    console.error('\n🛑 CSV parsing failed:');
    console.error('   ', error);
    console.error('\n   Tip: Make sure your CSV has columns for artist, track, and timestamp');
    throw error;
  }
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
