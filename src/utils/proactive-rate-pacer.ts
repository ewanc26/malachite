/**
 * @fileoverview Proactive Rate Pacer - Never Hit Rate Limits
 * 
 * This module implements PROACTIVE rate limiting that calculates optimal delays
 * to maintain steady throughput without ever hitting rate limit thresholds.
 * 
 * KEY INSIGHT: ATProto uses SLIDING WINDOW rate limits
 * - Points used at time T become available at time T + window_duration
 * - The window slides continuously (not fixed resets)
 * - We should spread requests evenly to maintain consistent rate
 * 
 * PHILOSOPHY:
 * - PROACTIVE not REACTIVE: Calculate delays to prevent hitting limits
 * - SMOOTH DISTRIBUTION: Spread requests evenly over time
 * - SUSTAINABLE RATE: Stay below limit with comfortable margin
 * - ADAPTIVE PACING: Adjust to current quota and usage patterns
 * 
 * EXAMPLE:
 * Server: 5000 points/hour (3600s) = 1.39 points/second
 * At 3 points/record: 0.46 records/second max
 * Target 80% of max: 0.37 records/second = 2.7s between records
 * With batch of 50: 150 points = need to wait 108s before next batch
 * 
 * This keeps us well below the limit while maintaining steady progress.
 * 
 * @module proactive-rate-pacer
 */

import { log } from './logger.js';

/**
 * Rate pacing calculation result
 */
export interface PacingCalculation {
  /** Recommended delay in milliseconds before next batch */
  delayMs: number;
  
  /** Target sustainable rate (records/second) */
  sustainableRate: number;
  
  /** Percentage of maximum rate being used (0-100) */
  utilizationPercent: number;
  
  /** Human-readable explanation */
  reason: string;
}

/**
 * ProactiveRatePacer - Calculate optimal delays to never hit rate limits
 * 
 * CORE ALGORITHM:
 * 
 * 1. Calculate maximum sustainable rate:
 *    max_rate = (limit / window_seconds) / points_per_record
 *    
 * 2. Apply target utilization (default 80%):
 *    target_rate = max_rate × 0.80
 *    
 * 3. Calculate time needed for points to replenish:
 *    points_used_this_batch × window_seconds / limit
 *    
 * 4. Add safety margin based on quota health:
 *    - High quota (>60%): minimal margin
 *    - Medium quota (30-60%): moderate margin  
 *    - Low quota (<30%): aggressive margin
 * 
 * SLIDING WINDOW BEHAVIOR:
 * - Points used now will become available in exactly `window_seconds`
 * - By spacing requests appropriately, old points become available
 *   as we need new ones
 * - This creates a steady-state where we never exhaust quota
 * 
 * EXAMPLE FLOW:
 * ```
 * Server: 5000 points/3600s (1 hour window)
 * Max rate: 5000/3600/3 = 0.46 records/s
 * Target rate (80%): 0.37 records/s
 * 
 * Batch 1 (50 records = 150 points):
 *   Time: 00:00
 *   Quota: 5000 → 4850
 *   Next batch in: 150/5000 × 3600 = 108s
 * 
 * Batch 2 (50 records = 150 points):
 *   Time: 00:01:48 (108s later)
 *   Points recovered: 150 (from 108s ago usage sliding out)
 *   Quota: 4850 + 150 - 150 = 4850 (steady state!)
 *   Next batch in: 108s
 * 
 * Result: Maintains 4850 points forever, never hits headroom
 * ```
 */
export class ProactiveRatePacer {
  /** Points per record in ATProto (constant) */
  private readonly POINTS_PER_RECORD = 3;
  
  /** Target utilization of maximum rate (80% = comfortable margin) */
  private readonly TARGET_UTILIZATION = 0.80;
  
  /** Minimum delay between batches (ms) - prevents hammering */
  private readonly MIN_DELAY_MS = 100;
  
  /** Maximum delay between batches (ms) - prevents excessive waiting */
  private readonly MAX_DELAY_MS = 300000; // 5 minutes
  
  /**
   * Calculate optimal delay before next batch to maintain sustainable rate.
   * 
   * This is the CORE of proactive pacing. Instead of using quota until
   * exhausted, we calculate exactly how long to wait so that points from
   * previous batches become available as we need them.
   * 
   * ALGORITHM:
   * 1. Calculate points we just used
   * 2. Determine how long until those points are available again (sliding window)
   * 3. Adjust based on current quota health
   * 4. Apply min/max bounds
   * 
   * QUOTA HEALTH ADJUSTMENT:
   * - >60% quota: Use target rate (80% of max)
   * - 30-60% quota: Slow down to 60% of max
   * - <30% quota: Conservative 40% of max
   * 
   * This creates automatic backpressure as quota depletes while
   * still maintaining progress.
   * 
   * @param batchSize Number of records in batch we just sent
   * @param serverLimit Total server capacity (e.g., 5000)
   * @param windowSeconds Window duration (e.g., 3600 = 1 hour)
   * @param currentRemaining Current quota remaining
   * @returns Optimal delay calculation
   */
  calculateDelay(
    batchSize: number,
    serverLimit: number,
    windowSeconds: number,
    currentRemaining: number
  ): PacingCalculation {
    // Calculate maximum sustainable rate (records per second)
    const pointsPerSecond = serverLimit / windowSeconds;
    const maxRecordsPerSecond = pointsPerSecond / this.POINTS_PER_RECORD;
    
    // Calculate quota health (percentage of limit remaining)
    const quotaHealthPercent = (currentRemaining / serverLimit) * 100;
    
    // Adjust target utilization based on quota health
    let targetUtilization = this.TARGET_UTILIZATION;
    let reason = `Target rate: ${(targetUtilization * 100).toFixed(0)}% of maximum`;
    
    if (quotaHealthPercent < 30) {
      // Low quota: be very conservative
      targetUtilization = 0.40;
      reason = `Low quota (${quotaHealthPercent.toFixed(0)}%): conservative 40% rate`;
    } else if (quotaHealthPercent < 60) {
      // Medium quota: moderate slowdown
      targetUtilization = 0.60;
      reason = `Medium quota (${quotaHealthPercent.toFixed(0)}%): moderate 60% rate`;
    }
    
    // Calculate target sustainable rate
    const sustainableRecordsPerSecond = maxRecordsPerSecond * targetUtilization;
    
    // Calculate how long this batch should take at target rate
    const idealBatchDurationSeconds = batchSize / sustainableRecordsPerSecond;
    
    // Convert to milliseconds
    let delayMs = Math.floor(idealBatchDurationSeconds * 1000);
    
    // Apply bounds
    delayMs = Math.max(this.MIN_DELAY_MS, Math.min(delayMs, this.MAX_DELAY_MS));
    
    log.debug(`[ProactiveRatePacer] Batch ${batchSize} records: target rate ${sustainableRecordsPerSecond.toFixed(3)} rec/s = ${delayMs}ms delay`);
    log.debug(`[ProactiveRatePacer] Quota health: ${quotaHealthPercent.toFixed(1)}%, utilization: ${(targetUtilization * 100).toFixed(0)}%`);
    
    return {
      delayMs,
      sustainableRate: sustainableRecordsPerSecond,
      utilizationPercent: targetUtilization * 100,
      reason
    };
  }
  
  /**
   * Calculate optimal batch size for current conditions.
   * 
   * Unlike the quota-based calculator which maximizes batch size,
   * this calculator aims for CONSISTENCY. We want batches that maintain
   * our target rate without exhausting quota.
   * 
   * STRATEGY:
   * - Calculate sustainable rate
   * - Determine batch size that takes ~30-60 seconds at that rate
   * - This provides good balance between:
   *   - Progress (not too small)
   *   - Smoothness (not too large/bursty)
   *   - Visibility (user sees progress regularly)
   * 
   * @param serverLimit Total server capacity
   * @param windowSeconds Window duration
   * @param currentRemaining Current quota remaining
   * @param maxBatchSize Hard limit (PDS max = 200)
   * @returns Recommended batch size
   */
  calculateOptimalBatchSize(
    serverLimit: number,
    windowSeconds: number,
    currentRemaining: number,
    maxBatchSize: number = 200
  ): number {
    // Calculate sustainable rate
    const pointsPerSecond = serverLimit / windowSeconds;
    const maxRecordsPerSecond = pointsPerSecond / this.POINTS_PER_RECORD;
    
    // Quota health determines target rate
    const quotaHealthPercent = (currentRemaining / serverLimit) * 100;
    let targetUtilization = this.TARGET_UTILIZATION;
    
    if (quotaHealthPercent < 30) {
      targetUtilization = 0.40;
    } else if (quotaHealthPercent < 60) {
      targetUtilization = 0.60;
    }
    
    const sustainableRecordsPerSecond = maxRecordsPerSecond * targetUtilization;
    
    // Target batch duration: 30-60 seconds is good balance
    // - Not too frequent (overhead, spam)
    // - Not too rare (poor progress visibility)
    const targetBatchDurationSeconds = 45; // sweet spot
    
    const optimalSize = Math.floor(sustainableRecordsPerSecond * targetBatchDurationSeconds);
    
    // Clamp to reasonable bounds
    const clampedSize = Math.max(1, Math.min(optimalSize, maxBatchSize));
    
    log.debug(`[ProactiveRatePacer] Optimal batch size: ${clampedSize} (${targetBatchDurationSeconds}s at ${sustainableRecordsPerSecond.toFixed(3)} rec/s)`);
    
    return clampedSize;
  }
  
  /**
   * Estimate time to completion given current conditions.
   * 
   * Provides realistic estimate accounting for:
   * - Sustainable rate (not maximum burst rate)
   * - Current quota health
   * - Sliding window recovery
   * 
   * @param remainingRecords Records left to import
   * @param serverLimit Server capacity
   * @param windowSeconds Window duration
   * @param currentQuota Current quota remaining
   * @returns Estimated seconds to completion
   */
  estimateTimeToCompletion(
    remainingRecords: number,
    serverLimit: number,
    windowSeconds: number,
    currentQuota: number
  ): number {
    // Calculate sustainable rate
    const pointsPerSecond = serverLimit / windowSeconds;
    const maxRecordsPerSecond = pointsPerSecond / this.POINTS_PER_RECORD;
    
    // Use current quota health to estimate average utilization
    const quotaHealthPercent = (currentQuota / serverLimit) * 100;
    let avgUtilization = this.TARGET_UTILIZATION;
    
    if (quotaHealthPercent < 30) {
      avgUtilization = 0.40;
    } else if (quotaHealthPercent < 60) {
      avgUtilization = 0.60;
    }
    
    const sustainableRecordsPerSecond = maxRecordsPerSecond * avgUtilization;
    
    // Time = records / rate
    const estimatedSeconds = remainingRecords / sustainableRecordsPerSecond;
    
    return estimatedSeconds;
  }
  
  /**
   * Calculate buffer time needed before we can safely start
   * if quota is currently too low.
   * 
   * In a sliding window, points become available gradually.
   * If quota is very low, we may need to wait for some points
   * to recover before starting.
   * 
   * @param pointsNeeded Points we need for next batch
   * @param currentRemaining Current quota
   * @param serverLimit Server capacity  
   * @param windowSeconds Window duration
   * @returns Seconds to wait (0 if ready now)
   */
  calculateRecoveryWaitTime(
    pointsNeeded: number,
    currentRemaining: number,
    serverLimit: number,
    windowSeconds: number
  ): number {
    if (currentRemaining >= pointsNeeded) {
      return 0; // Ready now
    }
    
    // How many points do we need to recover?
    const pointsToRecover = pointsNeeded - currentRemaining;
    
    // At what rate do points recover? (in sliding window, same as usage rate)
    const pointsPerSecond = serverLimit / windowSeconds;
    
    // Time needed for recovery
    const secondsNeeded = pointsToRecover / pointsPerSecond;
    
    log.debug(`[ProactiveRatePacer] Need ${pointsToRecover} points to recover, ETA ${secondsNeeded.toFixed(0)}s`);
    
    return Math.ceil(secondsNeeded);
  }
}
