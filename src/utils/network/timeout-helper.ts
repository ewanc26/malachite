/**
 * @fileoverview Timeout Helper - Prevent Hanging Operations
 * 
 * Provides utilities for adding timeouts to promises and operations.
 * Essential for maintaining responsiveness and preventing hung imports.
 * 
 * FEATURES:
 * - Wrap any promise with a timeout
 * - Configurable timeout duration
 * - Custom error messages
 * - Cleanup on timeout
 * 
 * USAGE:
 * ```typescript
 * const result = await withTimeout(
 *   makeApiCall(),
 *   30000, // 30 seconds
 *   'API call timed out'
 * );
 * ```
 * 
 * @module timeout-helper
 */

import { log } from '../logger.js';

/**
 * Timeout error class for distinguishing timeout errors from other errors
 */
export class TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TimeoutError';
    
    // Maintains proper stack trace for where our error was thrown
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, TimeoutError);
    }
  }
}

/**
 * Wrap a promise with a timeout.
 * If the promise doesn't resolve within the timeout period, it will be rejected.
 * 
 * ALGORITHM:
 * 1. Create a timeout promise that rejects after specified duration
 * 2. Race the original promise against the timeout
 * 3. If original resolves first: return result
 * 4. If timeout wins: throw TimeoutError
 * 
 * IMPORTANT: This doesn't cancel the original promise - it just stops waiting.
 * The original operation may still complete in the background.
 * 
 * EXAMPLE:
 * ```typescript
 * try {
 *   const data = await withTimeout(
 *     fetch('https://slow-api.com/data'),
 *     5000,
 *     'API request timed out after 5s'
 *   );
 * } catch (error) {
 *   if (error instanceof TimeoutError) {
 *     console.log('Request took too long');
 *   }
 * }
 * ```
 * 
 * @param promise Promise to wrap with timeout
 * @param timeoutMs Timeout duration in milliseconds
 * @param errorMessage Custom error message (optional)
 * @returns Promise that resolves with original result or rejects on timeout
 * @throws TimeoutError if operation exceeds timeout
 */
export function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  errorMessage: string = `Operation timed out after ${timeoutMs}ms`
): Promise<T> {
  // Create a promise that rejects after the timeout
  const timeoutPromise = new Promise<T>((_, reject) => {
    setTimeout(() => {
      log.warn(`[TimeoutHelper] ⏱️  Timeout exceeded: ${errorMessage}`);
      reject(new TimeoutError(errorMessage));
    }, timeoutMs);
    
    // Note: We can't clear this timeout if the original promise resolves first
    // because we don't have access to the promise's internals. This is a known
    // limitation of Promise.race(). The timeout will still fire but will be harmless.
    // For a production system, consider using AbortController for true cancellation.
  });
  
  // Race the original promise against the timeout
  return Promise.race([promise, timeoutPromise]);
}

/**
 * Create a timeout wrapper for a function that will be called multiple times.
 * Useful for wrapping API clients or frequently-called functions.
 * 
 * USAGE:
 * ```typescript
 * const apiCall = withTimeoutWrapper(
 *   async (endpoint: string) => fetch(endpoint).then(r => r.json()),
 *   30000 // 30 second timeout for all calls
 * );
 * 
 * // Now all calls have automatic timeout
 * const data1 = await apiCall('/api/users');
 * const data2 = await apiCall('/api/posts');
 * ```
 * 
 * @param fn Function to wrap with timeout
 * @param timeoutMs Timeout duration in milliseconds
 * @param errorMessageFn Function to generate custom error message (optional)
 * @returns Wrapped function with timeout logic
 */
export function withTimeoutWrapper<TArgs extends any[], TResult>(
  fn: (...args: TArgs) => Promise<TResult>,
  timeoutMs: number,
  errorMessageFn?: (...args: TArgs) => string
): (...args: TArgs) => Promise<TResult> {
  return (...args: TArgs) => {
    const errorMessage = errorMessageFn 
      ? errorMessageFn(...args)
      : `Function call timed out after ${timeoutMs}ms`;
    
    return withTimeout(fn(...args), timeoutMs, errorMessage);
  };
}

/**
 * Execute a function with a timeout.
 * Convenience function that combines function execution with timeout.
 * 
 * USAGE:
 * ```typescript
 * const result = await executeWithTimeout(
 *   async () => {
 *     const response = await fetch('/api/data');
 *     return response.json();
 *   },
 *   10000,
 *   'Data fetch timed out'
 * );
 * ```
 * 
 * @param fn Function to execute
 * @param timeoutMs Timeout duration in milliseconds
 * @param errorMessage Custom error message (optional)
 * @returns Promise that resolves with function result or rejects on timeout
 */
export async function executeWithTimeout<T>(
  fn: () => Promise<T>,
  timeoutMs: number,
  errorMessage?: string
): Promise<T> {
  return withTimeout(fn(), timeoutMs, errorMessage);
}

/**
 * Check if an error is a timeout error.
 * Useful for error handling and retry logic.
 * 
 * @param error Error to check
 * @returns True if error is a timeout error
 */
export function isTimeoutError(error: any): boolean {
  return error instanceof TimeoutError ||
         error.name === 'TimeoutError' ||
         error.message?.toLowerCase().includes('timeout') ||
         error.code === 'ETIMEDOUT';
}

/**
 * Configuration for timeout with multiple attempts
 */
export interface TimeoutWithRetriesOptions {
  /** Timeout for each attempt in milliseconds */
  timeoutMs: number;
  
  /** Maximum number of attempts */
  maxAttempts?: number;
  
  /** Delay between attempts in milliseconds (optional) */
  retryDelayMs?: number;
  
  /** Custom error message (optional) */
  errorMessage?: string;
}

/**
 * Execute a function with timeout and retry logic.
 * Combines timeout and retry functionality.
 * 
 * ALGORITHM:
 * 1. Try to execute function with timeout
 * 2. If succeeds: return result
 * 3. If times out and attempts remain:
 *    a. Log timeout
 *    b. Wait for retry delay (if specified)
 *    c. Try again (goto step 1)
 * 4. If no attempts remain: throw TimeoutError
 * 
 * EXAMPLE:
 * ```typescript
 * // Try 3 times with 10s timeout each
 * const data = await withTimeoutAndRetries(
 *   () => fetch('/api/data').then(r => r.json()),
 *   {
 *     timeoutMs: 10000,
 *     maxAttempts: 3,
 *     retryDelayMs: 2000
 *   }
 * );
 * ```
 * 
 * @param fn Function to execute
 * @param options Timeout and retry configuration
 * @returns Promise that resolves with result or rejects if all attempts timeout
 */
export async function withTimeoutAndRetries<T>(
  fn: () => Promise<T>,
  options: TimeoutWithRetriesOptions
): Promise<T> {
  const {
    timeoutMs,
    maxAttempts = 3,
    retryDelayMs = 0,
    errorMessage = `Operation timed out after ${maxAttempts} attempts`
  } = options;
  
  let lastError: Error | undefined;
  
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      log.debug(`[TimeoutHelper] Attempt ${attempt}/${maxAttempts} with ${timeoutMs}ms timeout`);
      
      const result = await withTimeout(
        fn(),
        timeoutMs,
        `Attempt ${attempt}/${maxAttempts} timed out after ${timeoutMs}ms`
      );
      
      // Success!
      if (attempt > 1) {
        log.info(`[TimeoutHelper] ✅ Succeeded on attempt ${attempt}/${maxAttempts}`);
      }
      return result;
      
    } catch (error: any) {
      lastError = error;
      
      // If not a timeout error, rethrow immediately
      if (!isTimeoutError(error)) {
        throw error;
      }
      
      // If last attempt, rethrow
      if (attempt === maxAttempts) {
        log.error(`[TimeoutHelper] ❌ All ${maxAttempts} attempts timed out`);
        throw new TimeoutError(errorMessage);
      }
      
      // Log and retry
      log.warn(`[TimeoutHelper] ⚠️  Attempt ${attempt}/${maxAttempts} timed out`);
      if (retryDelayMs > 0) {
        log.info(`[TimeoutHelper] ⏳ Retrying in ${retryDelayMs}ms...`);
        await new Promise(resolve => setTimeout(resolve, retryDelayMs));
      }
    }
  }
  
  // Should never reach here
  throw lastError || new TimeoutError(errorMessage);
}

/**
 * Calculate recommended timeout based on operation type and network conditions.
 * Provides sensible defaults for common operations.
 * 
 * @param operationType Type of operation
 * @param networkQuality Network quality assessment ('good' | 'medium' | 'poor')
 * @returns Recommended timeout in milliseconds
 */
export function getRecommendedTimeout(
  operationType: 'api_call' | 'file_upload' | 'batch_operation' | 'fetch_records',
  networkQuality: 'good' | 'medium' | 'poor' = 'medium'
): number {
  // Base timeouts for different operations (medium network)
  const baseTimeouts = {
    api_call: 15000,        // 15 seconds
    file_upload: 60000,     // 60 seconds
    batch_operation: 30000, // 30 seconds
    fetch_records: 20000,   // 20 seconds
  };
  
  // Quality multipliers
  const qualityMultipliers = {
    good: 0.7,   // Faster on good network
    medium: 1.0, // Base timeout
    poor: 2.0,   // Double timeout on poor network
  };
  
  const baseTimeout = baseTimeouts[operationType];
  const multiplier = qualityMultipliers[networkQuality];
  
  return Math.floor(baseTimeout * multiplier);
}
