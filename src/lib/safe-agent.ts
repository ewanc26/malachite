import { AtpAgent } from '@atproto/api';
import { log } from '../utils/logger.js';

/**
 * Enhanced AtpAgent with automatic rate limit handling
 * 
 * Extends AtpAgent to automatically detect and handle 429 (rate limit) errors
 * by waiting for the duration specified in the ratelimit-reset header before retrying.
 * 
 * This prevents manual rate limit handling in application code and ensures
 * requests are automatically retried after the appropriate wait time.
 */
export class SafeAgent extends AtpAgent {
  /**
   * Resume operation after a rate limit error
   * Waits for the duration specified in the ratelimit-reset header
   * 
   * @param error - The error object containing rate limit information
   */
  async resumeAfterRateLimit(error: any): Promise<void> {
    const resetHeader = error.headers?.['ratelimit-reset'];
    
    if (resetHeader) {
      const resetTime = parseInt(resetHeader, 10) * 1000;
      const waitTime = resetTime - Date.now();
      
      if (waitTime > 0) {
        const waitMinutes = Math.ceil(waitTime / 1000 / 60);
        log.warn(`[Rate Limit] Waiting ${waitMinutes}m before retry...`);
        
        // Add a 2-second buffer to ensure the rate limit has definitely reset
        await new Promise((resolve) => setTimeout(resolve, waitTime + 2000));
        
        log.info('[Rate Limit] Resuming requests...');
      }
    } else {
      // If no reset header, use a default backoff
      log.warn('[Rate Limit] No reset time provided, using default 60s backoff...');
      await new Promise((resolve) => setTimeout(resolve, 60000));
    }
  }

  /**
   * Override the internal call method to intercept and handle rate limit errors
   * 
   * Automatically catches 429 errors and retries after waiting for the rate limit to reset.
   * All other errors are passed through unchanged.
   * 
   * @param method - The XRPC method to call
   * @param params - Optional parameters for the call
   * @param data - Optional data payload
   * @param opts - Optional call options
   * @returns The response from the API call
   */
  override async call(method: string, params?: any, data?: any, opts?: any): Promise<any> {
    try {
      return await super.call(method, params, data, opts);
    } catch (err: any) {
      // Check if this is a rate limit error (429 Too Many Requests)
      // Check both the status property and headers directly
      const isRateLimitError = err.status === 429 || err.headers?.['ratelimit-reset'];
      
      if (isRateLimitError) {
        log.debug(`[Rate Limit] Hit on ${method}, initiating retry logic`);
        
        // Wait for the rate limit to reset
        await this.resumeAfterRateLimit(err);
        
        // Retry the request after waiting
        return this.call(method, params, data, opts);
      }
      
      // For all other errors, throw them as-is
      throw err;
    }
  }
}
