/**
 * Configuration constants for the Last.fm importer
 */

export const DEFAULT_BATCH_SIZE = 10;
export const DEFAULT_BATCH_DELAY = 1500;
export const MIN_BATCH_DELAY = 100;
export const RECORD_TYPE = 'fm.teal.alpha.feed.play';
export const SLINGSHOT_RESOLVER = 'https://slingshot.microcosm.blue/xrpc/com.bad-example.identity.resolveMiniDoc';
export const CLIENT_AGENT = 'lastfm-importer/v0.0.1';

// Batch size calculation constants
export const MIN_RECORDS_FOR_SCALING = 100;
export const BASE_BATCH_SIZE = 5;
export const MAX_BATCH_SIZE = 50;
export const SCALING_FACTOR = 1.5;
