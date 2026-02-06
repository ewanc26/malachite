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
import { parseRateLimitHeaders, normalizeHeaders } from './rate-limit-headers.js';
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
  
  /** Headroom threshold - pause when remaining drops below this % of limit */
  headroomThreshold: number;
}

export class RateLimiter {
  private stateFile: string;
  private safetyMargin: number;
  private headroomThreshold: number;
  
  constructor(opts?: { safety?: number; headroom?: number }) {
    this.safetyMargin = opts?.safety ?? 0.75; // Default 75% safety margin (deprecated, use headroom instead)
    this.headroomThreshold = opts?.headroom ?? 0.15; // Default 15% headroom - pause when we hit this threshold
    const stateDir = path.join(getMalachiteStateDir(), 'state');
    this.stateFile = path.join(stateDir, 'rate-limit.json');
    
    log.info(`[RateLimiter] 💾 State file path: ${this.stateFile}`);
    log.info(`[RateLimiter] 🛡️ Headroom threshold: ${(this.headroomThreshold * 100).toFixed(0)}% - will pause when remaining drops below this`);
    log.debug(`[RateLimiter] constructor: stateFile=${this.stateFile}, safety=${this.safetyMargin}, headroom=${this.headroomThreshold}`);
    this.ensureStateDir();
  }
  
  private ensureStateDir(): void {
    const dir = path.dirname(this.stateFile);
    try {
      if (!fs.existsSync(dir)) {
        log.info(`[RateLimiter] Creating state directory: ${dir}`);
        fs.mkdirSync(dir, { recursive: true });
        log.info(`[RateLimiter] ✅ State directory created`);
      } else {
        log.debug(`[RateLimiter] State directory already exists: ${dir}`);
      }
    } catch (error) {
      log.error(`[RateLimiter] ❌ Failed to create state directory: ${error}`);
      throw error;
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
    try {
      log.debug(`[RateLimiter] Writing state to: ${this.stateFile}`);
      log.debug(`[RateLimiter] State data: ${JSON.stringify(state)}`);
      
      // Ensure directory exists before writing
      this.ensureStateDir();
      
      const stateJson = JSON.stringify(state, null, 2);
      fs.writeFileSync(this.stateFile, stateJson, 'utf8');
      
      // Verify the write succeeded
      if (fs.existsSync(this.stateFile)) {
        log.info(`[RateLimiter] ✅ State file written successfully to: ${this.stateFile}`);
      } else {
        log.error(`[RateLimiter] ❌ State file write failed - file does not exist after write`);
      }
    } catch (error) {
      log.error(`[RateLimiter] ❌ Failed to write state file: ${error}`);
      if (error instanceof Error) {
        log.error(`[RateLimiter] Error details: ${error.message}`);
        log.error(`[RateLimiter] Stack: ${error.stack}`);
      }
      throw error; // Re-throw so caller knows write failed
    }
  }
  
  /**
   * Update rate limit state from server response headers
   */
  updateFromHeaders(headers: Record<string, string>): void {
    log.debug(`[RateLimiter] updateFromHeaders() called`);
    // Normalize header keys to lowercase for consistent parsing
    const normalizedHeaders = normalizeHeaders(headers);
    const parsed = parseRateLimitHeaders(normalizedHeaders);
    
    if (!parsed.limit || parsed.remaining === undefined) {
      log.warn('[RateLimiter] Headers missing limit or remaining - cannot update');
      return;
    }
    
    const now = Math.floor(Date.now() / 1000);
    let resetAt = parsed.reset || (now + (parsed.windowSeconds || 3600));
    let windowSeconds = parsed.windowSeconds || 3600;
    
    // Store the actual remaining from server (no modification)
    // We'll check headroom separately when making decisions
    const state: RateLimitState = {
      limit: parsed.limit,
      remaining: parsed.remaining, // Store actual server value
      resetAt,
      windowSeconds,
      updatedAt: now,
      safetyMargin: this.safetyMargin,
      headroomThreshold: this.headroomThreshold
    };
    
    const headroomPoints = Math.floor(parsed.limit * this.headroomThreshold);
    const percentRemaining = ((parsed.remaining / parsed.limit) * 100).toFixed(1);
    
    log.info(`[RateLimiter] Updated from headers: ${parsed.limit} limit, ${parsed.remaining} remaining (${percentRemaining}%), headroom threshold: ${headroomPoints} points (${(this.headroomThreshold * 100).toFixed(0)}%), resets at ${new Date(resetAt * 1000).toISOString()}`);
    
    // Warn if we're getting close to headroom threshold
    if (parsed.remaining <= headroomPoints) {
      log.warn(`[RateLimiter] ⚠️  Approaching headroom threshold! ${parsed.remaining} <= ${headroomPoints} (${(this.headroomThreshold * 100).toFixed(0)}% of limit)`);
    }
    
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
      state.remaining = state.limit; // Restore to full limit
      state.resetAt = now + state.windowSeconds;
      state.updatedAt = now;
      this.writeState(state);
      return true;
    }
    
    // Calculate headroom threshold - we want to pause BEFORE hitting zero
    const headroomPoints = Math.floor(state.limit * this.headroomThreshold);
    const effectiveRemaining = state.remaining - headroomPoints;
    
    // Check if we have enough quota (with headroom considered)
    const hasQuota = effectiveRemaining >= pointsNeeded;
    
    if (!hasQuota && state.remaining > 0) {
      const percentRemaining = ((state.remaining / state.limit) * 100).toFixed(1);
      log.debug(`[RateLimiter] checkQuota(${pointsNeeded}): remaining=${state.remaining} (${percentRemaining}%), headroom=${headroomPoints}, effective=${effectiveRemaining}, hasQuota=${hasQuota}`);
      log.info(`[RateLimiter] Approaching headroom threshold - preserving ${headroomPoints} points (${(this.headroomThreshold * 100).toFixed(0)}% buffer)`);
    } else {
      log.debug(`[RateLimiter] checkQuota(${pointsNeeded}): remaining=${state.remaining}, hasQuota=${hasQuota}`);
    }
    
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
      state.remaining = state.limit; // Restore to full limit
      state.resetAt = now + state.windowSeconds;
      state.updatedAt = now;
      this.writeState(state);
    }
    
    // Calculate headroom - preserve a buffer before hitting zero
    const headroomPoints = Math.floor(state.limit * this.headroomThreshold);
    const effectiveRemaining = state.remaining - headroomPoints;
    
    // Check quota with headroom
    if (effectiveRemaining < pointsNeeded) {
      const waitTime = state.resetAt - now;
      const percentRemaining = ((state.remaining / state.limit) * 100).toFixed(1);
      
      if (state.remaining > 0) {
        log.warn(`[RateLimiter] ❌ Approaching rate limit headroom! Need ${pointsNeeded} points, have ${state.remaining} (${percentRemaining}%) but preserving ${headroomPoints} point buffer`);
      } else {
        log.warn(`[RateLimiter] ❌ Quota exhausted! Need ${pointsNeeded} points, ${state.remaining} remaining`);
      }
      
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
    headroomPoints?: number;
    effectiveRemaining?: number;
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
    const headroomPoints = Math.floor(state.limit * this.headroomThreshold);
    const effectiveRemaining = state.remaining - headroomPoints;
    
    return {
      hasState: true,
      limit: state.limit,
      remaining: state.remaining,
      remainingPercent,
      headroomPoints,
      effectiveRemaining,
      resetAt: new Date(state.resetAt * 1000),
      secondsUntilReset,
      windowSeconds: state.windowSeconds
    };
  }
}
