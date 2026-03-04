/**
 * Spotify JSON — web layer.
 * Re-exports the shared core logic and adds a browser File API loader.
 */
import type { SpotifyRecord } from '$core/types.js';
import { parseSpotifyJsonContent, convertSpotifyToPlayRecord } from '$core/spotify.js';

export { parseSpotifyJsonContent, convertSpotifyToPlayRecord };

/** Read one or more browser File objects and parse them as Spotify JSON exports. */
export async function parseSpotifyFiles(files: File[]): Promise<SpotifyRecord[]> {
  let all: SpotifyRecord[] = [];
  for (const file of files) {
    const text = await file.text();
    const parsed = JSON.parse(text) as SpotifyRecord[];
    all = all.concat(parsed);
  }
  return parseSpotifyJsonContent(all);
}
