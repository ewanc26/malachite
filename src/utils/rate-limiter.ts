/**
 * @fileoverview Fully Dynamic Rate Limiter - Zero Hardcoded Defaults
 * 
 * This module provides intelligent rate limit management for AT Protocol record publishing.
 * Unlike traditional rate limiters that require manual configuration, this implementation:
 * 
 * 1. **Learns Server Capacity**: Discovers limits from first response headers
 * 2. **Tracks Real-Time Quota**: Monitors remaining points continuously
 * 3. **Enforces Headroom Buffer**: Preserves safety margin to prevent exhaustion
 * 4. **Persists State**: Saves state across restarts for consistent behavior
 * 5. **Auto-Recovery**: Detects and handles quota resets automatically
 * 
 * DESIGN PHILOSOPHY:
 * - NO ASSUMPTIONS: Every PDS configuration is learned dynamically
 * - SAFETY FIRST: 15% headroom buffer prevents rate limit hits
 * - STATE PERSISTENCE: Resume support with saved server capacity
 * - TRANSPARENT OPERATION: Clear logging of all decisions
 * 
 * TYPICAL FLOW:
 * 
 * First Run:
 *   1. No saved state exists
 *   2. Return 0 quota (forces conservative probe)
 *   3. Receive first response headers
 *   4. Learn: "5000 points/3600s window"
 *   5. Save state to ~/.malachite/state/rate-limit.json
 *   6. Provide full quota for subsequent requests
 * 
 * Subsequent Runs:
 *   1. Load saved state (5000 points/3600s)
 *   2. Check remaining quota (e.g., 4200)
 *   3. Calculate safe quota: 4200 - (5000 × 0.15) = 3450
 *   4. Provide safe quota for batch sizing
 *   5. Update state after each request
 * 
 * Quota Exhaustion:
 *   1. Remaining drops below headroom (750 for 5000 limit)
 *   2. Return 0 safe quota
 *   3. Wait for reset timestamp
 *   4. Auto-restore to full quota
 *   5. Resume at maximum speed
 * 
 * STATE FILE FORMAT:
 * ```json
 * {
 *   "limit": 5000,              // Server capacity
 *   "remaining": 3240,          // Current quota
 *   "resetAt": 1738938600,      // Reset timestamp (unix seconds)
 *   "windowSeconds": 3600,      // Window duration
 *   "updatedAt": 1738935780,    // Last update timestamp
 *   "headroomThreshold": 0.15   // Safety buffer (15%)
 * }
 * ```
 * 
 * HEADROOM BUFFER:
 * The 15% headroom buffer prevents quota exhaustion:
 * - Without headroom: Risk hitting 0 and triggering rate limits
 * - With headroom: Stop at 750 points (15% of 5000), wait for reset
 * - Result: Never actually hit rate limits, always maintain buffer
 * 
 * @module rate-limiter
 */

import fs from 'node:fs';
import path from 'node:path';
import { getMalachiteStateDir } from './platform.js';
import { parseRateLimitHeaders, normalizeHeaders } from './rate-limit-headers.js';
import { log } from './logger.js';

/**
 * Rate limit state structure persisted to disk.
 * All timestamps are in Unix seconds (not milliseconds).
 */
export interface RateLimitState {
  /** Maximum points allowed in the time window (learned from server) */
  limit: number;
  
  /** Points remaining in current window (actual server value, not adjusted) */
  remaining: number;
  
  /** Unix timestamp (seconds) when the window resets */
  resetAt: number;
  
  /** Window duration in seconds (learned from server policy, typically 3600) */
  windowSeconds: number;
  
  /** Unix timestamp (seconds) when this state was last updated */
  updatedAt: number;
  
  /** Headroom threshold - fraction of limit to preserve as buffer (0.0-1.0) */
  headroomThreshold: number;
}

/**
 * RateLimiter - Intelligent rate limit management with server discovery
 * 
 * CORE CONCEPTS:
 * 
 * 1. **Actual vs Safe Quota**:
 *    - Actual: What the server reports (e.g., 800 points)
 *    - Safe: Actual minus headroom buffer (e.g., 800 - 750 = 50 points)
 *    - We use safe quota for decisions to maintain the buffer
 * 
 * 2. **Headroom Buffer**:
 *    - Default: 15% of server limit
 *    - Purpose: Prevent quota exhaustion before hitting 0
 *    - Example: 5000 limit → 750 point buffer
 *    - When remaining drops to 750, we stop and wait
 * 
 * 3. **State Persistence**:
 *    - Saved after every update to disk
 *    - Allows resuming with known server capacity
 *    - Survives process restarts
 * 
 * 4. **Auto-Recovery**:
 *    - Detects when current time >= resetAt
 *    - Automatically restores remaining to limit
 *    - No manual intervention needed
 * 
 * INITIALIZATION:
 * ```typescript
 * // Default 15% headroom
 * const rl = new RateLimiter();
 * 
 * // Custom headroom (10%)
 * const rl = new RateLimiter({ headroom: 0.10 });
 * ```
 * 
 * TYPICAL USAGE:
 * ```typescript
 * // Check if we have quota
 * const hasQuota = await rl.checkQuota(600); // Need 600 points
 * 
 * // Reserve quota (waits if exhausted)
 * await rl.waitForPermit(600);
 * 
 * // Get safe available points for batch sizing
 * const safePoints = rl.getSafeAvailablePoints();
 * 
 * // Update from server response
 * rl.updateFromHeaders(response.headers);
 * ```
 * 
 * LEARNING FLOW:
 * ```typescript
 * // First request - no state exists
 * rl.getSafeAvailablePoints(); // Returns 0
 * 
 * // Send probe batch
 * const response = await sendBatch(10 records);
 * 
 * // Learn from response
 * rl.updateFromHeaders(response.headers);
 * // Logs: "🎓 LEARNED: 5000 points/3600s"
 * 
 * // Now we know the capacity
 * rl.getSafeAvailablePoints(); // Returns 4250 (5000 - 750 headroom)
 * ```
 */
export class RateLimiter {
  /** Path to persisted state file */
  private stateFile: string;
  
  /** Headroom threshold as fraction of limit (default 0.15 = 15%) */
  private headroomThreshold: number;
  
  /** Flag tracking whether we've learned server capacity yet */
  private hasLearnedFromServer: boolean = false;
  
  /**
   * Initialize rate limiter with optional custom headroom.
   * 
   * INITIALIZATION STEPS:
   * 1. Set headroom threshold (default 15%)
   * 2. Determine state file path (~/.malachite/state/rate-limit.json)
   * 3. Ensure state directory exists
   * 4. Check for existing state (from previous runs)
   * 5. Set hasLearnedFromServer flag if state exists
   * 
   * @param opts Configuration options
   * @param opts.headroom Headroom threshold (0.0-1.0, default 0.15 = 15%)
   */
  constructor(opts?: { headroom?: number }) {
    this.headroomThreshold = opts?.headroom ?? 0.15; // Default 15% headroom
    const stateDir = path.join(getMalachiteStateDir(), 'state');
    this.stateFile = path.join(stateDir, 'rate-limit.json');
    
    log.info(`[RateLimiter] 💾 State file: ${this.stateFile}`);
    log.info(`[RateLimiter] 🛡️ Headroom: ${(this.headroomThreshold * 100).toFixed(0)}% buffer before pausing`);
    this.ensureStateDir();
    
    // Check if we already have server info from previous runs
    const state = this.readState();
    if (state && state.limit > 0) {
      this.hasLearnedFromServer = true;
      log.info(`[RateLimiter] ℹ️  Using saved state: ${state.limit} points/${state.windowSeconds}s window`);
    } else {
      log.info(`[RateLimiter] 🔍 No saved state - will learn from first server response`);
    }
  }
  
  /**
   * Ensure state directory exists, creating it if necessary.
   * Called during initialization and before writing state.
   */
  private ensureStateDir(): void {
    const dir = path.dirname(this.stateFile);
    try {
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
        log.debug(`[RateLimiter] Created state directory: ${dir}`);
      }
    } catch (error) {
      log.error(`[RateLimiter] ❌ Failed to create state directory: ${error}`);
      throw error;
    }
  }
  
  /**
   * Read rate limit state from disk.
   * 
   * RETURNS:
   * - RateLimitState object if file exists and is valid JSON
   * - null if file doesn't exist or can't be parsed
   * 
   * @returns Persisted state or null
   */
  private readState(): RateLimitState | null {
    try {
      const raw = fs.readFileSync(this.stateFile, 'utf8');
      const state = JSON.parse(raw) as RateLimitState;
      log.debug(`[RateLimiter] Loaded state: limit=${state.limit}, remaining=${state.remaining}, window=${state.windowSeconds}s`);
      return state;
    } catch (e) {
      log.debug(`[RateLimiter] No existing state file`);
      return null;
    }
  }
  
  /**
   * Write rate limit state to disk.
   * 
   * EFFECTS:
   * - Ensures state directory exists
   * - Writes JSON to state file
   * - Sets hasLearnedFromServer flag on first successful write with capacity
   * - Logs detailed state information
   * 
   * @param state State to persist
   * @throws Error if write fails
   */
  private writeState(state: RateLimitState): void {
    try {
      this.ensureStateDir();
      const stateJson = JSON.stringify(state, null, 2);
      fs.writeFileSync(this.stateFile, stateJson, 'utf8');
      
      // Log when we first learn server capacity
      if (!this.hasLearnedFromServer && state.limit > 0) {
        this.hasLearnedFromServer = true;
        log.info(`[RateLimiter] ✅ Learned from server: ${state.limit} points/${state.windowSeconds}s window`);
      }
      
      log.debug(`[RateLimiter] State saved: limit=${state.limit}, remaining=${state.remaining}, resets=${new Date(state.resetAt * 1000).toISOString()}`);
    } catch (error) {
      log.error(`[RateLimiter] ❌ Failed to write state: ${error}`);
      throw error;
    }
  }
  
  /**
   * Update rate limit state from server response headers.
   * This is how we LEARN the server's configuration dynamically.
   * 
   * LEARNING PROCESS:
   * 1. Parse headers (limit, remaining, reset, window)
   * 2. Validate required fields (limit and remaining must exist)
   * 3. Calculate reset time (from header or estimated)
   * 4. Create new state with actual server values
   * 5. Log learning message on first discovery
   * 6. Save state to disk
   * 
   * IMPORTANT: We store the ACTUAL remaining from the server,
   * not adjusted by headroom. Headroom is applied only when
   * checking or reserving quota, not when storing state.
   * 
   * HEADER FORMATS SUPPORTED:
   * - Standard: ratelimit-limit, ratelimit-remaining, ratelimit-reset
   * - X-Prefixed: x-ratelimit-limit, x-ratelimit-remaining, x-ratelimit-reset
   * - Policy: ratelimit-policy (format: "5000;w=3600")
   * 
   * EXAMPLE:
   * ```typescript
   * const headers = {
   *   'ratelimit-limit': '5000',
   *   'ratelimit-remaining': '4970',
   *   'ratelimit-reset': '1738938600',
   *   'ratelimit-policy': '5000;w=3600'
   * };
   * rl.updateFromHeaders(headers);
   * // Logs: "🎓 LEARNED: 5000 points/3600s, currently 4970 remaining (99.4%)"
   * ```
   * 
   * @param headers Response headers from server (should be normalized)
   */
  updateFromHeaders(headers: Record<string, string>): void {
    const normalizedHeaders = normalizeHeaders(headers);
    const parsed = parseRateLimitHeaders(normalizedHeaders);
    
    if (!parsed.limit || parsed.remaining === undefined) {
      log.warn('[RateLimiter] Headers missing limit/remaining - cannot learn from server');
      return;
    }
    
    const now = Math.floor(Date.now() / 1000);
    const resetAt = parsed.reset || (now + (parsed.windowSeconds || 3600));
    const windowSeconds = parsed.windowSeconds || 3600;
    
    const state: RateLimitState = {
      limit: parsed.limit,
      remaining: parsed.remaining, // Store actual server value
      resetAt,
      windowSeconds,
      updatedAt: now,
      headroomThreshold: this.headroomThreshold
    };
    
    const headroomPoints = Math.floor(parsed.limit * this.headroomThreshold);
    const percentRemaining = ((parsed.remaining / parsed.limit) * 100).toFixed(1);
    
    // Special logging for first-time discovery
    if (!this.hasLearnedFromServer) {
      log.info(`[RateLimiter] 🎓 LEARNED: ${parsed.limit} points/${windowSeconds}s, currently ${parsed.remaining} remaining (${percentRemaining}%)`);
    } else {
      log.debug(`[RateLimiter] Updated: ${parsed.remaining}/${parsed.limit} (${percentRemaining}%), resets ${new Date(resetAt * 1000).toISOString()}`);
    }
    
    // Warn if approaching headroom threshold
    if (parsed.remaining <= headroomPoints) {
      log.warn(`[RateLimiter] ⚠️  Near headroom threshold! ${parsed.remaining} ≤ ${headroomPoints} (${(this.headroomThreshold * 100).toFixed(0)}%)`);
    }
    
    this.writeState(state);
  }
  
  /**
   * Check if we have enough quota for the requested points.
   * Does not modify state - use reserveQuota to actually reserve points.
   * 
   * ALGORITHM:
   * 1. Read current state
   * 2. Check if window has reset (auto-restore quota)
   * 3. Calculate effective remaining (actual - headroom)
   * 4. Compare with requested points
   * 
   * HEADROOM APPLICATION:
   * effective = remaining - (limit × threshold)
   * Example: 800 - (5000 × 0.15) = 800 - 750 = 50
   * 
   * RETURNS:
   * - true if effective remaining >= pointsNeeded
   * - true if no state yet (first request)
   * - false if insufficient quota
   * 
   * @param pointsNeeded Number of points required
   * @returns Whether quota is available
   */
  async checkQuota(pointsNeeded: number): Promise<boolean> {
    const state = this.readState();
    if (!state) {
      // No state yet - let first request through so we can learn
      log.debug('[RateLimiter] No state - allowing first request to learn from server');
      return true;
    }
    
    const now = Math.floor(Date.now() / 1000);
    
    // Auto-restore if window has reset
    if (now >= state.resetAt) {
      log.info(`[RateLimiter] 🔄 Window reset! Quota restored to ${state.limit}`);
      state.remaining = state.limit;
      state.resetAt = now + state.windowSeconds;
      state.updatedAt = now;
      this.writeState(state);
      return true;
    }
    
    // Calculate effective remaining with headroom buffer
    const headroomPoints = Math.floor(state.limit * this.headroomThreshold);
    const effectiveRemaining = state.remaining - headroomPoints;
    const hasQuota = effectiveRemaining >= pointsNeeded;
    
    if (!hasQuota) {
      const percentRemaining = ((state.remaining / state.limit) * 100).toFixed(1);
      log.debug(`[RateLimiter] Insufficient quota: need ${pointsNeeded}, have ${state.remaining} (${percentRemaining}%), preserving ${headroomPoints} headroom`);
    }
    
    return hasQuota;
  }
  
  /**
   * Reserve quota points before making a request.
   * Actually decrements the remaining count in state.
   * 
   * ALGORITHM:
   * 1. Read current state
   * 2. Check if window has reset (auto-restore)
   * 3. Calculate effective remaining (with headroom)
   * 4. If insufficient: log warning and return false
   * 5. If sufficient: decrement remaining and save state
   * 
   * RETURNS:
   * - true if quota reserved successfully
   * - true if no state yet (first request)
   * - false if insufficient quota
   * 
   * IMPORTANT: This MODIFIES state by decrementing remaining.
   * Only call this when you're about to make the request.
   * 
   * @param pointsNeeded Number of points to reserve
   * @returns Whether reservation succeeded
   */
  async reserveQuota(pointsNeeded: number): Promise<boolean> {
    const state = this.readState();
    if (!state) {
      // No state yet - let it through so we can learn
      log.debug('[RateLimiter] No state - allowing request to establish baseline');
      return true;
    }
    
    const now = Math.floor(Date.now() / 1000);
    
    // Auto-restore if window reset
    if (now >= state.resetAt) {
      log.info(`[RateLimiter] 🔄 Window reset detected during reservation`);
      state.remaining = state.limit;
      state.resetAt = now + state.windowSeconds;
      state.updatedAt = now;
      this.writeState(state);
    }
    
    // Check quota with headroom
    const headroomPoints = Math.floor(state.limit * this.headroomThreshold);
    const effectiveRemaining = state.remaining - headroomPoints;
    
    if (effectiveRemaining < pointsNeeded) {
      const waitTime = state.resetAt - now;
      const percentRemaining = ((state.remaining / state.limit) * 100).toFixed(1);
      
      if (state.remaining > 0) {
        log.warn(`[RateLimiter] ⚠️  Approaching headroom! Need ${pointsNeeded}, have ${state.remaining} (${percentRemaining}%), preserving ${headroomPoints} buffer`);
      } else {
        log.warn(`[RateLimiter] ❌ Quota exhausted! Need ${pointsNeeded}, have ${state.remaining}`);
      }
      
      log.warn(`[RateLimiter] ⏳ Must wait ${waitTime}s until ${new Date(state.resetAt * 1000).toISOString()}`);
      return false;
    }
    
    // Reserve the quota by decrementing remaining
    state.remaining -= pointsNeeded;
    state.updatedAt = now;
    this.writeState(state);
    
    log.debug(`[RateLimiter] Reserved ${pointsNeeded} points, ${state.remaining} remaining`);
    return true;
  }
  
  /**
   * Wait until the rate limit window resets.
   * Sleeps until resetAt timestamp + 1 second buffer.
   * 
   * USAGE:
   * Called when quota is exhausted and we need to wait.
   * Displays countdown timer to user.
   * 
   * @returns Promise that resolves after wait completes
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
      log.info('[RateLimiter] Window already reset');
      return;
    }
    
    const minutes = Math.floor(waitTime / 60);
    const seconds = waitTime % 60;
    
    log.warn(`[RateLimiter] ⏳ Waiting ${minutes}m ${seconds}s for quota reset...`);
    log.warn(`[RateLimiter] Reset at: ${new Date(state.resetAt * 1000).toISOString()}`);
    
    // Wait with 1 second buffer
    await new Promise(resolve => setTimeout(resolve, (waitTime + 1) * 1000));
    
    log.info('[RateLimiter] ✅ Wait complete - quota restored');
  }
  
  /**
   * Wait for a permit with the given number of points.
   * Combines reserveQuota and waitForReset - loops until permit granted.
   * 
   * ALGORITHM:
   * 1. Try to reserve quota
   * 2. If successful: return true
   * 3. If failed: wait for reset
   * 4. Loop back to step 1
   * 
   * This ensures we ALWAYS get the permit eventually, even if
   * we have to wait for quota to reset.
   * 
   * USAGE:
   * ```typescript
   * // This will wait if needed
   * await rl.waitForPermit(600);
   * // Quota is now reserved, safe to proceed
   * await sendBatch();
   * ```
   * 
   * @param pointsNeeded Number of points required
   * @returns Promise<true> when permit is granted
   */
  async waitForPermit(pointsNeeded: number): Promise<boolean> {
    while (true) {
      const reserved = await this.reserveQuota(pointsNeeded);
      if (reserved) {
        return true;
      }
      
      log.info(`[RateLimiter] Quota exhausted, waiting for reset...`);
      await this.waitForReset();
      log.info(`[RateLimiter] Retrying reservation...`);
    }
  }
  
  /**
   * Get safe available points (remaining - headroom buffer).
   * This is the amount we can safely use without hitting the threshold.
   * 
   * ALGORITHM:
   * 1. Read state
   * 2. If no state: return 0 (forces probe batch)
   * 3. Check if window reset (auto-restore)
   * 4. Calculate: remaining - (limit × threshold)
   * 5. Return max(0, safe_points)
   * 
   * EXAMPLE:
   * State: { limit: 5000, remaining: 4200 }
   * Headroom: 15% = 750 points
   * Safe: 4200 - 750 = 3450 points
   * 
   * USE CASE:
   * Call this to determine optimal batch size:
   * ```typescript
   * const safePoints = rl.getSafeAvailablePoints();
   * const batchSize = Math.floor(safePoints / 3); // 3 points per record
   * ```
   * 
   * @returns Safe available quota points (0 if no state or quota exhausted)
   */
  getSafeAvailablePoints(): number {
    const state = this.readState();
    if (!state) {
      // No state yet - return 0 to force conservative start
      // After first request, we'll learn the real limits
      return 0;
    }
    
    const now = Math.floor(Date.now() / 1000);
    
    // Auto-restore if window reset
    if (now >= state.resetAt) {
      log.debug(`[RateLimiter] Window reset detected, full quota: ${state.limit}`);
      return state.limit;
    }
    
    const headroomPoints = Math.floor(state.limit * this.headroomThreshold);
    const safePoints = Math.max(0, state.remaining - headroomPoints);
    
    log.debug(`[RateLimiter] Safe quota: ${safePoints} (${state.remaining} - ${headroomPoints} headroom)`);
    return safePoints;
  }
  
  /**
   * Get server capacity information (learned dynamically).
   * 
   * RETURNS:
   * - { limit, windowSeconds } if we've learned from server
   * - null if we haven't received headers yet
   * 
   * USE CASE:
   * Called by DynamicBatchCalculator to calculate initial batch size.
   * 
   * @returns Server capacity info or null
   */
  getServerCapacity(): { limit: number; windowSeconds: number } | null {
    const state = this.readState();
    if (!state || state.limit === 0) {
      return null;
    }
    return {
      limit: state.limit,
      windowSeconds: state.windowSeconds
    };
  }
  
  /**
   * Check if we've learned from the server yet.
   * 
   * @returns True if we have server capacity info
   */
  hasServerInfo(): boolean {
    return this.hasLearnedFromServer;
  }
  
  /**
   * Get current rate limit status for monitoring/debugging.
   * 
   * RETURNS:
   * Object with:
   * - hasState: Whether state exists
   * - limit: Server capacity (if known)
   * - remaining: Current quota (if known)
   * - remainingPercent: Quota as percentage (if known)
   * - headroomPoints: Absolute headroom in points (if known)
   * - effectiveRemaining: Safe quota after headroom (if known)
   * - resetAt: Reset timestamp (if known)
   * - secondsUntilReset: Time until reset (if known)
   * - windowSeconds: Window duration (if known)
   * 
   * @returns Current status object
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
