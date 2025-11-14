import type { Config } from './types.js';

// ⚠️ IMPORTANT: Rate Limit Warning
// Bluesky's AppView has rate limits on PDS instances:
// - Exceeding 10K records per day can rate limit your ENTIRE PDS
// - This affects all users on your PDS, not just your account
// - See: https://docs.bsky.app/blog/rate-limits-pds-v3
//
// Default limit: 1K records per day (automatically batched with pauses)
export const RECORDS_PER_DAY_LIMIT = 1000;

// Safety margin factor (0.9 = use 90% of limit to be safe)
export const SAFETY_MARGIN = 0.9;

// Record type
export const RECORD_TYPE = 'fm.teal.alpha.feed.play';

// Client agent
export const CLIENT_AGENT = 'lastfm-importer/v0.0.2';

// Default batch configuration (will be adjusted for rate limiting)
export const DEFAULT_BATCH_SIZE = 10;
export const DEFAULT_BATCH_DELAY = 2000; // 2 seconds

// Minimum safe delay between batches (1 second)
export const MIN_BATCH_DELAY = 1000;

// Maximum batch size
export const MAX_BATCH_SIZE = 50;

// Slingshot resolver URL
export const SLINGSHOT_RESOLVER = 'https://slingshot.danner.cloud';

const config: Config = {
  RECORD_TYPE,
  MIN_RECORDS_FOR_SCALING: 20,
  BASE_BATCH_SIZE: 10,
  SCALING_FACTOR: 1.5,
  CLIENT_AGENT,
  DEFAULT_BATCH_SIZE,
  DEFAULT_BATCH_DELAY,
  MIN_BATCH_DELAY,
  MAX_BATCH_SIZE,
  SLINGSHOT_RESOLVER,
  RECORDS_PER_DAY_LIMIT,
  SAFETY_MARGIN,
};

export default config;
