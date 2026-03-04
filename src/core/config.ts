/**
 * Shared constants — environment-agnostic.
 * No Node.js dependencies; safe for both CLI and browser.
 */

export const RECORD_TYPE = 'fm.teal.alpha.feed.play';
export const SLINGSHOT_RESOLVER = 'https://slingshot.microcosm.blue';
export const MAX_PDS_BATCH_SIZE = 200;
export const POINTS_PER_RECORD = 3;
// Single source of truth for the version string — keep in sync with package.json.
export const VERSION = '0.10.0';
