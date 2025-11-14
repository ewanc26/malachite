import * as fs from 'fs';
import { parse } from 'csv-parse/sync';

/**
 * Parse Last.fm CSV export
 */
export function parseLastFmCsv(filePath) {
  console.log(`Reading CSV file: ${filePath}`);
  const fileContent = fs.readFileSync(filePath, 'utf-8');
  
  const records = parse(fileContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });
  
  console.log(`✓ Parsed ${records.length} scrobbles\n`);
  return records;
}

/**
 * Convert Last.fm CSV record to ATProto play record
 */
export function convertToPlayRecord(csvRecord, config) {
  const { RECORD_TYPE, CLIENT_AGENT } = config;
  
  // Parse the timestamp
  const timestamp = parseInt(csvRecord.uts);
  const playedTime = new Date(timestamp * 1000).toISOString();
  
  // Build artists array
  const artists = [];
  if (csvRecord.artist) {
    const artistData = {
      artistName: csvRecord.artist,
    };
    if (csvRecord.artist_mbid && csvRecord.artist_mbid.trim()) {
      artistData.artistMbId = csvRecord.artist_mbid;
    }
    artists.push(artistData);
  }
  
  // Build the play record
  const playRecord = {
    $type: RECORD_TYPE,
    trackName: csvRecord.track,
    artists,
    playedTime,
    submissionClientAgent: CLIENT_AGENT,
    musicServiceBaseDomain: 'last.fm',
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

/**
 * Sort records chronologically
 */
export function sortRecords(records, reverseChronological = false) {
  console.log(`Sorting records ${reverseChronological ? 'newest' : 'oldest'} first...`);
  
  records.sort((a, b) => {
    const timeA = new Date(a.playedTime).getTime();
    const timeB = new Date(b.playedTime).getTime();
    return reverseChronological ? timeB - timeA : timeA - timeB;
  });
  
  const firstPlay = new Date(records[0].playedTime).toLocaleDateString();
  const lastPlay = new Date(records[records.length - 1].playedTime).toLocaleDateString();
  console.log(`✓ Sorted ${records.length} records`);
  console.log(`  First: ${firstPlay}`);
  console.log(`  Last: ${lastPlay}\n`);
  
  return records;
}
