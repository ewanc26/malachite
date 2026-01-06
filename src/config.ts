import type { Config } from './types.js';

// ⚠️ IMPORTANT: Rate Limit Warning
// Bluesky's AppView has rate limits on PDS instances:
// - Exceeding 10K records per day can rate limit your ENTIRE PDS
// - This affects all users on your PDS, not just your account
// - See: https://docs.bsky.app/blog/rate-limits-pds-v3
//
// Default limit: Aggressive initial limit that will dynamically adjust
// Start high and back off if we hit rate limits
export const RECORDS_PER_DAY_LIMIT = 10000;

// Safety margin factor - start aggressive, will back off if needed
export const SAFETY_MARGIN = 1.0;

// Record type
export const RECORD_TYPE = 'fm.teal.alpha.feed.play';

// Client agent
export const CLIENT_AGENT = 'lastfm-importer/v0.3.0';

// Default batch configuration - aggressive defaults for maximum speed
// Will dynamically adjust based on success/failure
export const DEFAULT_BATCH_SIZE = 200; // Max allowed by applyWrites
export const DEFAULT_BATCH_DELAY = 500; // Start with 500ms between batches

// Minimum safe delay between batches (500ms for adaptive mode)
export const MIN_BATCH_DELAY = 500;

// Maximum batch size (PDS limit is 200 operations per call)
export const MAX_BATCH_SIZE = 200;

// Slingshot resolver URL
export const SLINGSHOT_RESOLVER = 'https://slingshot.microcosm.blue';

const config: Config = {
  RECORD_TYPE,
  MIN_RECORDS_FOR_SCALING: 20,
  BASE_BATCH_SIZE: 200,  // Match DEFAULT_BATCH_SIZE for consistency
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
