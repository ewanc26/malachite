/**
 * Shared constants used across multiple modules
 * Centralized location for magic numbers and configuration values
 */

/**
 * Network and API constants
 */
export const MAX_APPLY_WRITES_OPS = 200; // Maximum operations per applyWrites call
export const TARGET_LATENCY_MS = 2000;   // Target request latency (2 seconds)
export const MIN_BATCH_SIZE = 10;        // Minimum batch size for operations
export const MAX_BATCH_SIZE = 100;       // Maximum batch size (AT Protocol limit)

/**
 * Rate limiting constants
 */
export const MAX_CONSECUTIVE_FAILURES = 3;        // Failures before slowdown
export const CIRCUIT_BREAKER_THRESHOLD = 5;       // Failures to open circuit breaker
export const CIRCUIT_BREAKER_TIMEOUT = 60000;     // Circuit breaker timeout (1 minute)
export const SPEEDUP_THRESHOLD = 5;               // Consecutive successes before speedup
export const MAX_DELAY = 60000;                   // Maximum delay between batches (60 seconds)

/**
 * Monitoring and metrics constants
 */
export const RESPONSE_TIME_WINDOW = 10;           // Track last N response times
export const ERROR_RATE_WINDOW = 20;              // Track last N requests for error rate
export const HIGH_ERROR_RATE_THRESHOLD = 0.3;     // 30% error rate triggers slowdown
export const SLOW_RESPONSE_THRESHOLD = 5000;      // 5 seconds is considered "slow"

/**
 * Cache configuration
 */
export const CACHE_TTL_HOURS = 24;                // Cache validity period

/**
 * Progress and logging
 */
export const PROGRESS_LOG_INTERVAL = 10000;       // Log progress every N records
export const BATCH_PROGRESS_INTERVAL = 10;        // Show detailed stats every N batches
