/**
 * @fileoverview Retry Helper - Exponential Backoff for Network Resilience
 * 
 * Provides retry logic with exponential backoff for handling transient failures.
 * Essential for maintaining reliability over unreliable network connections.
 * 
 * FEATURES:
 * - Exponential backoff: 1s → 2s → 4s → ...
 * - Configurable max attempts and delays
 * - Selective retry based on error type
 * - Detailed logging of retry attempts
 * 
 * USAGE:
 * ```typescript
 * const result = await retryWithBackoff(
 *   () => makeApiCall(),
 *   { maxAttempts: 3, initialDelayMs: 1000 }
 * );
 * ```
 * 
 * @module retry-helper
 */

import { log } from '../logger.js';

/**
 * Configuration options for retry logic
 */
export interface RetryOptions {
  /** Maximum number of attempts (default: 3) */
  maxAttempts?: number;
  
  /** Initial delay in milliseconds (default: 1000) */
  initialDelayMs?: number;
  
  /** Maximum delay cap in milliseconds (default: 30000 = 30s) */
  maxDelayMs?: number;
  
  /** Multiplier for exponential backoff (default: 2) */
  backoffMultiplier?: number;
  
  /** Error types/messages that trigger retry (default: network errors) */
  retryableErrors?: string[];
  
  /** Callback called before each retry (optional) */
  onRetry?: (attempt: number, maxAttempts: number, delay: number, error: Error) => void;
}

/**
 * Default retryable error patterns.
 * These cover common transient network issues.
 */
const DEFAULT_RETRYABLE_ERRORS = [
  'ECONNRESET',      // Connection reset by peer
  'ETIMEDOUT',       // Connection timed out
  'ENOTFOUND',       // DNS lookup failed
  'ECONNREFUSED',    // Connection refused
  'ENETUNREACH',     // Network unreachable
  'EAI_AGAIN',       // DNS temporary failure
  'network',         // Generic network error
  'socket hang up',  // Socket closed unexpectedly
  'timeout',         // Request timed out
  '503',             // Service unavailable
  '502',             // Bad gateway
  '504',             // Gateway timeout
];

/**
 * Execute a function with retry logic and exponential backoff.
 * 
 * ALGORITHM:
 * 1. Try to execute the function
 * 2. On success: return result
 * 3. On failure: check if error is retryable
 * 4. If retryable and attempts remaining:
 *    a. Calculate delay: initial × (multiplier ^ attempt)
 *    b. Cap delay at maxDelayMs
 *    c. Wait for calculated delay
 *    d. Try again (goto step 1)
 * 5. If not retryable or no attempts remaining: throw error
 * 
 * EXAMPLE:
 * ```typescript
 * // Network request with 3 retries
 * const data = await retryWithBackoff(
 *   async () => {
 *     const response = await fetch('https://api.example.com/data');
 *     return response.json();
 *   },
 *   {
 *     maxAttempts: 3,
 *     initialDelayMs: 1000,
 *     backoffMultiplier: 2
 *   }
 * );
 * // Retry delays: 1s, 2s, 4s
 * ```
 * 
 * @param fn Function to execute with retry logic
 * @param options Retry configuration
 * @returns Promise that resolves to function result or rejects if all retries fail
 * @throws Last error encountered if all retries fail
 */
export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  const {
    maxAttempts = 3,
    initialDelayMs = 1000,
    maxDelayMs = 30000,
    backoffMultiplier = 2,
    retryableErrors = DEFAULT_RETRYABLE_ERRORS,
    onRetry
  } = options;
  
  let lastError: Error | undefined;
  
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      // Try to execute the function
      log.debug(`[RetryHelper] Attempt ${attempt}/${maxAttempts}`);
      const result = await fn();
      
      // Success!
      if (attempt > 1) {
        log.info(`[RetryHelper] ✅ Succeeded on attempt ${attempt}/${maxAttempts}`);
      }
      return result;
      
    } catch (error: any) {
      lastError = error;
      
      // Check if this is the last attempt
      if (attempt === maxAttempts) {
        log.error(`[RetryHelper] ❌ All ${maxAttempts} attempts failed`);
        throw error;
      }
      
      // Check if error is retryable
      const errorMessage = error.message?.toLowerCase() || '';
      const errorCode = error.code?.toLowerCase() || '';
      const errorStatus = error.status?.toString() || '';
      
      const isRetryable = retryableErrors.some(
        errType => {
          const pattern = errType.toLowerCase();
          return errorMessage.includes(pattern) ||
                 errorCode.includes(pattern) ||
                 errorStatus.includes(pattern);
        }
      );
      
      if (!isRetryable) {
        log.warn(`[RetryHelper] ⚠️  Non-retryable error: ${error.message}`);
        throw error;
      }
      
      // Calculate delay with exponential backoff
      const baseDelay = initialDelayMs * Math.pow(backoffMultiplier, attempt - 1);
      const delay = Math.min(baseDelay, maxDelayMs);
      
      log.warn(`[RetryHelper] ⚠️  Attempt ${attempt}/${maxAttempts} failed: ${error.message}`);
      log.info(`[RetryHelper] ⏳ Retrying in ${(delay / 1000).toFixed(1)}s...`);
      
      // Call onRetry callback if provided
      if (onRetry) {
        onRetry(attempt, maxAttempts, delay, error);
      }
      
      // Wait before retrying
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  // This should never be reached, but TypeScript needs it
  throw lastError || new Error('Retry failed with unknown error');
}

/**
 * Create a retry wrapper for a function that will be called multiple times.
 * Useful for wrapping API clients or frequently-called functions.
 * 
 * USAGE:
 * ```typescript
 * const apiCall = withRetry(
 *   async (endpoint: string) => fetch(endpoint).then(r => r.json()),
 *   { maxAttempts: 3 }
 * );
 * 
 * // Now you can call it multiple times with automatic retry
 * const data1 = await apiCall('/api/users');
 * const data2 = await apiCall('/api/posts');
 * ```
 * 
 * @param fn Function to wrap with retry logic
 * @param options Retry configuration
 * @returns Wrapped function with retry logic
 */
export function withRetry<TArgs extends any[], TResult>(
  fn: (...args: TArgs) => Promise<TResult>,
  options: RetryOptions = {}
): (...args: TArgs) => Promise<TResult> {
  return (...args: TArgs) => {
    return retryWithBackoff(() => fn(...args), options);
  };
}

/**
 * Check if an error is a retryable network error.
 * Useful for custom retry logic.
 * 
 * @param error Error to check
 * @param additionalPatterns Additional error patterns to check
 * @returns True if error is retryable
 */
export function isRetryableError(
  error: any,
  additionalPatterns: string[] = []
): boolean {
  const patterns = [...DEFAULT_RETRYABLE_ERRORS, ...additionalPatterns];
  const errorMessage = error.message?.toLowerCase() || '';
  const errorCode = error.code?.toLowerCase() || '';
  const errorStatus = error.status?.toString() || '';
  
  return patterns.some(
    pattern => {
      const p = pattern.toLowerCase();
      return errorMessage.includes(p) ||
             errorCode.includes(p) ||
             errorStatus.includes(p);
    }
  );
}

/**
 * Calculate the total time that will be spent on retries
 * (useful for setting timeouts and showing estimates to users)
 * 
 * @param options Retry configuration
 * @returns Total milliseconds that will be spent waiting between retries
 */
export function calculateTotalRetryTime(options: RetryOptions = {}): number {
  const {
    maxAttempts = 3,
    initialDelayMs = 1000,
    maxDelayMs = 30000,
    backoffMultiplier = 2
  } = options;
  
  let totalTime = 0;
  
  for (let attempt = 1; attempt < maxAttempts; attempt++) {
    const baseDelay = initialDelayMs * Math.pow(backoffMultiplier, attempt - 1);
    const delay = Math.min(baseDelay, maxDelayMs);
    totalTime += delay;
  }
  
  return totalTime;
}
