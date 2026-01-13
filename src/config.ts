import type { Config } from './types.js';

// ⚠️ IMPORTANT: Rate Limit Warning
// Bluesky's AppView has rate limits on PDS instances:
// - Exceeding 10K records per day can rate limit your ENTIRE PDS
// - This affects all users on your PDS, not just your account
// - See: https://docs.bsky.app/blog/rate-limits-pds-v3
//
// Default limit: Very conservative (7,500 records/day) to be safe
export const RECORDS_PER_DAY_LIMIT = 10000;

// Safety margin factor - 75% by default for maximum safety
// Use --aggressive flag to set to 85% for faster imports
export const SAFETY_MARGIN = 0.75;

// Aggressive safety margin (for --aggressive flag)
export const AGGRESSIVE_SAFETY_MARGIN = 0.85;

// Record type
export const RECORD_TYPE = 'fm.teal.alpha.feed.play';

// Build client agent string
export function buildClientAgent(debug = false) {
  if (!debug) {
    return 'malachite/v0.6.1';
  }

  const PLATFORM_LABELS: Record<string, string> = {
    darwin: 'macOS',
    linux: 'Linux',
    win32: 'Windows',
  };

  const platform =
    PLATFORM_LABELS[process.platform] ?? process.platform;

  return `malachite/v0.6.1 (${platform}; Node/${process.version})`;
}

// Default batch configuration - conservative for PDS safety
// Will dynamically adjust based on success/failure
export const DEFAULT_BATCH_SIZE = 100; // Conservative default
export const DEFAULT_BATCH_DELAY = 2000; // Start with 2 seconds between batches

// Minimum safe delay between batches (1 second minimum)
export const MIN_BATCH_DELAY = 1000;

// Maximum batch size (PDS limit is 200 operations per call)
export const MAX_BATCH_SIZE = 200;

// Slingshot resolver URL
export const SLINGSHOT_RESOLVER = 'https://slingshot.microcosm.blue';

const config = {
  RECORD_TYPE,
  MIN_RECORDS_FOR_SCALING: 20,
  BASE_BATCH_SIZE: 200,  // Match DEFAULT_BATCH_SIZE for consistency
  SCALING_FACTOR: 1.5,
  DEFAULT_BATCH_SIZE,
  DEFAULT_BATCH_DELAY,
  MIN_BATCH_DELAY,
  MAX_BATCH_SIZE,
  SLINGSHOT_RESOLVER,
  RECORDS_PER_DAY_LIMIT,
  SAFETY_MARGIN,
  AGGRESSIVE_SAFETY_MARGIN,
};

export default config;
