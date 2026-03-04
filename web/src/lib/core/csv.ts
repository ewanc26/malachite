/**
 * Last.fm CSV — web layer.
 * Re-exports the shared core logic and adds a browser File API loader.
 */
import type { LastFmCsvRecord } from '$core/types.js';
import { parseLastFmCsvContent, convertToPlayRecord } from '$core/csv.js';

export { parseLastFmCsvContent, convertToPlayRecord };

/** Read a browser File object and parse it as a Last.fm CSV export. */
export async function parseLastFmFile(file: File): Promise<LastFmCsvRecord[]> {
  const text = await file.text();
  return parseLastFmCsvContent(text);
}
