/**
 * Rate limit header parsing utilities
 * Shared across rate-limiter.ts and publisher.ts
 */
import { log } from './logger.js';

export interface RateLimitHeaders {
  limit?: number;
  remaining?: number;
  reset?: number; // unix epoch seconds
  windowSeconds?: number;
  policy?: string;
}

/**
 * Parse rate-limit headers from a response, supporting multiple header name variants
 */
export function parseRateLimitHeaders(headers: Record<string, string>): RateLimitHeaders {
  log.debug(`[rate-limit-headers.ts] parseRateLimitHeaders() called with headers: ${JSON.stringify(Object.keys(headers || {}))}`);
  const limit = parseInt(
    headers['ratelimit-limit'] ||
      headers['x-ratelimit-limit'] ||
      headers['RateLimit-Limit'] ||
      '0',
    10
  ) || undefined;

  const remaining = parseInt(
    headers['ratelimit-remaining'] ||
      headers['x-ratelimit-remaining'] ||
      headers['RateLimit-Remaining'] ||
      '0',
    10
  ) || undefined;

  const reset = parseInt(
    headers['ratelimit-reset'] ||
      headers['x-ratelimit-reset'] ||
      headers['RateLimit-Reset'] ||
      '0',
    10
  ) || undefined;

  const policy =
    headers['ratelimit-policy'] ||
    headers['x-ratelimit-policy'] ||
    headers['RateLimit-Policy'];

  // Parse window seconds from policy (e.g. "5000;w=3600")
  let windowSeconds: number | undefined;
  if (policy) {
    const m = /;w=(\d+)/.exec(policy);
    if (m) {
      windowSeconds = parseInt(m[1], 10);
      log.debug(`[rate-limit-headers.ts] Extracted window from policy: ${windowSeconds}s from "${policy}"`);
    }
  }

  // If reset is present (unix epoch seconds), compute approximate window
  if (reset && !windowSeconds) {
    const now = Math.floor(Date.now() / 1000);
    windowSeconds = Math.max(0, reset - now);
    log.debug(`[rate-limit-headers.ts] Computed window from reset time: ${windowSeconds}s`);
  }

  const result: RateLimitHeaders = {
    limit: limit && !isNaN(limit) ? limit : undefined,
    remaining: remaining && !isNaN(remaining) ? remaining : undefined,
    reset: reset && !isNaN(reset) ? reset : undefined,
    windowSeconds,
    policy,
  };

  log.debug(`[rate-limit-headers.ts] parseRateLimitHeaders() result: limit=${result.limit}, remaining=${result.remaining}, reset=${result.reset}, window=${result.windowSeconds}s`);

  return result;
}

/**
 * Check if headers indicate a rate limit error
 */
export function isRateLimitError(error: any): boolean {
  if (error.status === 429) {
    log.debug(`[rate-limit-headers.ts] isRateLimitError() detected: status 429`);
    return true;
  }
  if (error.message?.includes('rate limit')) {
    log.debug(`[rate-limit-headers.ts] isRateLimitError() detected: message contains 'rate limit'`);
    return true;
  }
  if (error.message?.includes('too many requests')) {
    log.debug(`[rate-limit-headers.ts] isRateLimitError() detected: message contains 'too many requests'`);
    return true;
  }
  if (error.message?.includes('Rate Limit')) {
    log.debug(`[rate-limit-headers.ts] isRateLimitError() detected: message contains 'Rate Limit'`);
    return true;
  }
  log.debug(`[rate-limit-headers.ts] isRateLimitError() false for error: ${error.message}`);
  return false;
}
