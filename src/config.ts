import type { Config } from './types.js';

// ⚠️ IMPORTANT: Rate Limit Warning
// Bluesky's AppView has rate limits on PDS instances:
// - Exceeding 10K records per day can rate limit your ENTIRE PDS
// - This affects all users on your PDS, not just your account
// - See: https://docs.bsky.app/blog/rate-limits-pds-v3
//
// This importer uses FULLY DYNAMIC batching with zero hardcoded defaults.
// Batch sizes and delays are calculated in real-time based on:
// - Server rate limit capacity (learned from headers)
// - Current quota availability
// - Network performance metrics
// - Success/failure patterns

// Record type
export const RECORD_TYPE = 'fm.teal.alpha.feed.play';

// Build client agent string
export function buildClientAgent(_debug = false) {
  // Always return just the version, regardless of debug mode
  // The debug parameter is kept for backwards compatibility but unused
  return 'malachite/v0.9.2';
}

// DEPRECATED - These are kept for backwards compatibility only
// The actual values are now calculated dynamically at runtime
export const DEFAULT_BATCH_SIZE = 100; // Ignored - dynamically calculated
export const DEFAULT_BATCH_DELAY = 2000; // Ignored - dynamically calculated
export const MIN_BATCH_DELAY = 100; // Minimum safe delay to prevent hammering
export const MAX_BATCH_SIZE = 200; // PDS hard limit for applyWrites

// Slingshot resolver URL
export const SLINGSHOT_RESOLVER = 'https://slingshot.microcosm.blue';

// DEPRECATED - Daily limit concept is replaced by dynamic quota management
// The rate limiter learns actual limits from server headers
export const RECORDS_PER_DAY_LIMIT = 10000; // Kept for backwards compatibility

// DEPRECATED - Safety margins replaced by headroom threshold in RateLimiter
export const SAFETY_MARGIN = 0.75; // Kept for backwards compatibility
export const AGGRESSIVE_SAFETY_MARGIN = 0.85; // Kept for backwards compatibility

const config: Config = {
  RECORD_TYPE,
  MIN_RECORDS_FOR_SCALING: 20, // DEPRECATED - scaling now continuous
  BASE_BATCH_SIZE: DEFAULT_BATCH_SIZE, // DEPRECATED
  SCALING_FACTOR: 1.5, // DEPRECATED
  DEFAULT_BATCH_SIZE, // DEPRECATED - only for backwards compatibility
  DEFAULT_BATCH_DELAY, // DEPRECATED - only for backwards compatibility
  MIN_BATCH_DELAY, // Still used as absolute minimum
  MAX_BATCH_SIZE, // Still used as hard ceiling
  SLINGSHOT_RESOLVER,
  RECORDS_PER_DAY_LIMIT, // DEPRECATED
  SAFETY_MARGIN, // DEPRECATED
  AGGRESSIVE_SAFETY_MARGIN, // DEPRECATED
};

export default config;
