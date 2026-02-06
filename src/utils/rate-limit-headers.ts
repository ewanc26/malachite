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
 * NOTE: Expects headers to already be normalized to lowercase via normalizeHeaders()
 */
export function parseRateLimitHeaders(headers: Record<string, string>): RateLimitHeaders {
  log.debug(`[rate-limit-headers.ts] parseRateLimitHeaders() called with headers: ${JSON.stringify(Object.keys(headers || {}))}`);
  
  // Headers should already be normalized to lowercase
  const limitStr = headers['ratelimit-limit'] || headers['x-ratelimit-limit'] || '';
  const remainingStr = headers['ratelimit-remaining'] || headers['x-ratelimit-remaining'] || '';
  const resetStr = headers['ratelimit-reset'] || headers['x-ratelimit-reset'] || '';
  const policy = headers['ratelimit-policy'] || headers['x-ratelimit-policy'] || '';
  
  log.debug(`[rate-limit-headers.ts] Raw values: limit="${limitStr}", remaining="${remainingStr}", reset="${resetStr}", policy="${policy}"`);
  
  // Parse integers, checking for valid numbers
  const limit = limitStr ? parseInt(limitStr, 10) : undefined;
  const remaining = remainingStr ? parseInt(remainingStr, 10) : undefined;
  const reset = resetStr ? parseInt(resetStr, 10) : undefined;
  
  // Validate parsed numbers
  const validLimit = limit !== undefined && !isNaN(limit) ? limit : undefined;
  const validRemaining = remaining !== undefined && !isNaN(remaining) ? remaining : undefined;
  const validReset = reset !== undefined && !isNaN(reset) ? reset : undefined;

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
  if (validReset && !windowSeconds) {
    const now = Math.floor(Date.now() / 1000);
    windowSeconds = Math.max(0, validReset - now);
    log.debug(`[rate-limit-headers.ts] Computed window from reset time: ${windowSeconds}s`);
  }

  const result: RateLimitHeaders = {
    limit: validLimit,
    remaining: validRemaining,
    reset: validReset,
    windowSeconds,
    policy: policy || undefined,
  };

  // Log the final parsed result with clear indication of what was found
  const foundHeaders = [];
  if (validLimit !== undefined) foundHeaders.push(`limit=${validLimit}`);
  if (validRemaining !== undefined) foundHeaders.push(`remaining=${validRemaining}`);
  if (validReset !== undefined) foundHeaders.push(`reset=${validReset}`);
  if (windowSeconds !== undefined) foundHeaders.push(`window=${windowSeconds}s`);
  
  if (foundHeaders.length > 0) {
    log.info(`[rate-limit-headers.ts] Parsed rate limit headers: ${foundHeaders.join(', ')}`);
  } else {
    log.warn(`[rate-limit-headers.ts] No valid rate limit headers found in response`);
  }
  
  log.debug(`[rate-limit-headers.ts] parseRateLimitHeaders() result: limit=${result.limit}, remaining=${result.remaining}, reset=${result.reset}, window=${result.windowSeconds}s`);

  return result;
}

/**
 * Normalize header keys to lowercase for consistent access
 * HTTP headers are case-insensitive, but object keys are not
 */
export function normalizeHeaders(headers: Record<string, string>): Record<string, string> {
  const normalized: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    normalized[key.toLowerCase()] = value;
  }
  return normalized;
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
