/**
 * IMPROVED Rate Limiter - Based on actual server behavior
 * 
 * KEY LEARNINGS:
 * - Rate limits are typically PER HOUR (3600s), not per day
 * - Example from selfhosted.social:
 *   - ratelimit-limit: 5000 points
 *   - ratelimit-policy: 5000;w=3600 (5000 points per 3600 seconds = 1 hour)
 * - Each applyWrites operation costs ~3 points per record
 * - MUST respect ratelimit-remaining: 0 as a hard stop
 * - MUST wait until ratelimit-reset timestamp before continuing
 */

import fs from 'node:fs';
import path from 'node:path';
import { getMalachiteStateDir } from './platform.js';
import { parseRateLimitHeaders } from './rate-limit-headers.js';
import { log } from './logger.js';

export interface RateLimitState {
  /** Maximum points allowed in the time window */
  limit: number;
  
  /** Points remaining in current window */
  remaining: number;
  
  /** Unix timestamp (seconds) when the window resets */
  resetAt: number;
  
  /** Window duration in seconds (typically 3600 = 1 hour) */
  windowSeconds: number;
  
  /** When this state was last updated (unix timestamp in seconds) */
  updatedAt: number;
  
  /** Safety margin to apply (0.0-1.0) */
  safetyMargin: number;
}

export class RateLimiter {
  private stateFile: string;
  private safetyMargin: number;
  
  constructor(opts?: { safety?: number }) {
    this.safetyMargin = opts?.safety ?? 0.75; // Default 75% safety margin
    const stateDir = path.join(getMalachiteStateDir(), 'state');
    this.stateFile = path.join(stateDir, 'rate-limit.json');
    
    log.debug(`[RateLimiter] constructor: stateFile=${this.stateFile}, safety=${this.safetyMargin}`);
    this.ensureStateDir();
  }
  
  private ensureStateDir(): void {
    const dir = path.dirname(this.stateFile);
    if (!fs.existsSync(dir)) {
      log.debug(`[RateLimiter] Creating state directory: ${dir}`);
      fs.mkdirSync(dir, { recursive: true });
    }
  }
  
  private readState(): RateLimitState | null {
    try {
      const raw = fs.readFileSync(this.stateFile, 'utf8');
      const state = JSON.parse(raw) as RateLimitState;
      log.debug(`[RateLimiter] Loaded state: ${JSON.stringify(state)}`);
      return state;
    } catch (e) {
      log.debug(`[RateLimiter] No existing state file`);
      return null;
    }
  }
  
  private writeState(state: RateLimitState): void {
    log.debug(`[RateLimiter] Writing state: ${JSON.stringify(state)}`);
    fs.writeFileSync(this.stateFile, JSON.stringify(state, null, 2), 'utf8');
  }
  
  /**
   * Update rate limit state from server response headers
   */
  updateFromHeaders(headers: Record<string, string>): void {
    log.debug(`[RateLimiter] updateFromHeaders() called`);
    const parsed = parseRateLimitHeaders(headers);
    
    if (!parsed.limit || parsed.remaining === undefined) {
      log.warn('[RateLimiter] Headers missing limit or remaining - cannot update');
      return;
    }
    
    const now = Math.floor(Date.now() / 1000);
    let resetAt = parsed.reset || (now + (parsed.windowSeconds || 3600));
    let windowSeconds = parsed.windowSeconds || 3600;
    
    // Apply safety margin to remaining
    const remainingWithSafety = Math.floor(parsed.remaining * this.safetyMargin);
    
    const state: RateLimitState = {
      limit: parsed.limit,
      remaining: remainingWithSafety,
      resetAt,
      windowSeconds,
      updatedAt: now,
      safetyMargin: this.safetyMargin
    };
    
    log.info(`[RateLimiter] Updated from headers: ${parsed.limit} limit, ${remainingWithSafety}/${parsed.remaining} remaining (${(this.safetyMargin * 100).toFixed(0)}% safety), resets at ${new Date(resetAt * 1000).toISOString()}`);
    
    this.writeState(state);
  }
  
  /**
   * Check if we have enough quota for the requested points
   * Returns true if quota is available, false otherwise
   */
  async checkQuota(pointsNeeded: number): Promise<boolean> {
    const state = this.readState();
    if (!state) {
      log.warn('[RateLimiter] No state - assuming quota available');
      return true; // No state yet, let first request go through
    }
    
    const now = Math.floor(Date.now() / 1000);
    
    // Check if window has reset
    if (now >= state.resetAt) {
      log.info(`[RateLimiter] Window has reset! Restoring quota to ${state.limit}`);
      state.remaining = Math.floor(state.limit * this.safetyMargin);
      state.resetAt = now + state.windowSeconds;
      state.updatedAt = now;
      this.writeState(state);
      return true;
    }
    
    // Check if we have enough quota
    const hasQuota = state.remaining >= pointsNeeded;
    log.debug(`[RateLimiter] checkQuota(${pointsNeeded}): remaining=${state.remaining}, hasQuota=${hasQuota}`);
    
    return hasQuota;
  }
  
  /**
   * Reserve quota points before making a request
   * Returns true if reservation succeeded, false if quota exhausted
   */
  async reserveQuota(pointsNeeded: number): Promise<boolean> {
    const state = this.readState();
    if (!state) {
      log.warn('[RateLimiter] No state yet - allowing request (will be initialized from response)');
      return true;
    }
    
    const now = Math.floor(Date.now() / 1000);
    
    // Check if window has reset
    if (now >= state.resetAt) {
      log.info(`[RateLimiter] Window reset! Restoring quota`);
      state.remaining = Math.floor(state.limit * this.safetyMargin);
      state.resetAt = now + state.windowSeconds;
      state.updatedAt = now;
      this.writeState(state);
    }
    
    // Check quota
    if (state.remaining < pointsNeeded) {
      const waitTime = state.resetAt - now;
      log.warn(`[RateLimiter] ❌ Quota exhausted! Need ${pointsNeeded} points, only ${state.remaining} remaining`);
      log.warn(`[RateLimiter] Must wait ${waitTime}s (${Math.floor(waitTime / 60)}m ${waitTime % 60}s) until ${new Date(state.resetAt * 1000).toISOString()}`);
      return false;
    }
    
    // Reserve the quota
    state.remaining -= pointsNeeded;
    state.updatedAt = now;
    this.writeState(state);
    
    log.debug(`[RateLimiter] ✅ Reserved ${pointsNeeded} points, ${state.remaining} remaining`);
    return true;
  }
  
  /**
   * Wait until the rate limit window resets
   */
  async waitForReset(): Promise<void> {
    const state = this.readState();
    if (!state) {
      log.warn('[RateLimiter] No state - nothing to wait for');
      return;
    }
    
    const now = Math.floor(Date.now() / 1000);
    const waitTime = Math.max(0, state.resetAt - now);
    
    if (waitTime === 0) {
      log.info('[ImprovedRateLimiter] Window already reset');
      return;
    }
    
    const minutes = Math.floor(waitTime / 60);
    const seconds = waitTime % 60;
    
    log.warn(`[RateLimiter] ⏳ Waiting ${minutes}m ${seconds}s for quota reset...`);
    log.warn(`[RateLimiter] Reset at: ${new Date(state.resetAt * 1000).toISOString()}`);
    
    // Wait with 1 second added as buffer
    await new Promise(resolve => setTimeout(resolve, (waitTime + 1) * 1000));
    
    log.info('[RateLimiter] ✅ Wait complete - quota should be reset');
  }
  
  /**
   * Wait for a permit with the given number of points.
   * This combines reserveQuota and waitForReset logic:
   * - If quota is available, reserves it immediately
   * - If quota is exhausted, waits until reset and then reserves
   */
  async waitForPermit(pointsNeeded: number): Promise<boolean> {
    // Try to reserve quota first
    const reserved = await this.reserveQuota(pointsNeeded);
    if (reserved) {
      return true; // Got the permit immediately
    }
    
    // Quota exhausted - wait for reset
    await this.waitForReset();
    
    // Try again after reset
    return await this.reserveQuota(pointsNeeded);
  }
  
  /**
   * Get current rate limit status for monitoring
   */
  getStatus(): {
    hasState: boolean;
    limit?: number;
    remaining?: number;
    remainingPercent?: number;
    resetAt?: Date;
    secondsUntilReset?: number;
    windowSeconds?: number;
  } {
    const state = this.readState();
    if (!state) {
      return { hasState: false };
    }
    
    const now = Math.floor(Date.now() / 1000);
    const secondsUntilReset = Math.max(0, state.resetAt - now);
    const remainingPercent = (state.remaining / state.limit) * 100;
    
    return {
      hasState: true,
      limit: state.limit,
      remaining: state.remaining,
      remainingPercent,
      resetAt: new Date(state.resetAt * 1000),
      secondsUntilReset,
      windowSeconds: state.windowSeconds
    };
  }
}
