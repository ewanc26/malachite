/**
 * Last.fm CSV — CLI wrapper.
 * Re-exports the environment-agnostic core and adds a Node.js fs loader.
 */

import * as fs from 'fs';
import type { LastFmCsvRecord, PlayRecord } from '../types.js';
import { parseLastFmCsvContent, convertToPlayRecord as coreConvert } from '../core/csv.js';
import { VERSION } from '../core/config.js';

export { parseLastFmCsvContent };
export type { LastFmCsvRecord };

const CLI_AGENT = `malachite/v${VERSION}`;

/**
 * Read a Last.fm CSV file from disk and return normalised records.
 */
export function parseLastFmCsv(filePath: string): LastFmCsvRecord[] {
  console.log(`Reading CSV file: ${filePath}`);
  const content = fs.readFileSync(filePath, 'utf-8');
  const records = parseLastFmCsvContent(content);
  console.log(`✓ Parsed ${records.length} scrobbles\n`);
  return records;
}

/**
 * Convert a normalised Last.fm CSV record to an ATProto play record.
 * The CLI agent string is injected automatically; pass `debug=true` for
 * future extension (currently has no effect).
 */
export function convertToPlayRecord(
  csv: LastFmCsvRecord,
  _configOrUnused?: unknown,
  _debug?: boolean
): PlayRecord {
  return coreConvert(csv, CLI_AGENT);
}
