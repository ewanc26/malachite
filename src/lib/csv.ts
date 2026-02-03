import * as fs from 'fs';
import { parse } from 'csv-parse/sync';
import type { LastFmCsvRecord, PlayRecord, Config } from '../types.js';
import { formatDate } from '../utils/helpers.js';
import { normalizeColumns, convertToPlayRecord } from '../utils/csv-helpers.js';

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
 * Sort records chronologically
 */
export function sortRecords(records: PlayRecord[], reverseChronological = false): PlayRecord[] {
  console.log(`Sorting records ${reverseChronological ? 'newest' : 'oldest'} first...`);

  records.sort((a, b) => {
    const timeA = new Date(a.playedTime).getTime();
    const timeB = new Date(b.playedTime).getTime();
    return reverseChronological ? timeB - timeA : timeA - timeB;
  });

  const firstPlay = formatDate(records[0].playedTime);
  const lastPlay = formatDate(records[records.length - 1].playedTime);
  console.log(`✓ Sorted ${records.length} records`);
  console.log(`  First: ${firstPlay}`);
  console.log(`  Last: ${lastPlay}\n`);

  return records;
}

// Re-export for backward compatibility
export { convertToPlayRecord };
