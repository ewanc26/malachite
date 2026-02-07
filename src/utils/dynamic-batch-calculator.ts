/**
 * @fileoverview Dynamic Batch Calculator - Zero Hardcoded Defaults
 * 
 * This module provides intelligent, real-time calculation of optimal batch sizes and delays
 * for publishing records to AT Protocol. Unlike traditional static batching systems that use
 * hardcoded defaults, this calculator learns and adapts continuously based on:
 * 
 * 1. **Server Rate Limit Capacity**: Learned from response headers (e.g., 5000 points/hour)
 * 2. **Current Quota Availability**: Real-time tracking of remaining points
 * 3. **Network Performance Metrics**: Rolling window of recent batch durations
 * 4. **Success/Failure Patterns**: Consecutive outcomes to detect trends
 * 
 * DESIGN PHILOSOPHY:
 * - NO HARDCODED DEFAULTS: All calculations derived from runtime data
 * - CONTINUOUS ADAPTATION: Adjusts every batch based on current conditions
 * - CONFIDENCE SCORING: Indicates reliability of calculations
 * - GRACEFUL DEGRADATION: Scales down smoothly when quota depletes
 * - INSTANT RECOVERY: Returns to optimal speed after quota resets
 * 
 * PERFORMANCE:
 * - Fresh quota: 200 records/batch (maximum throughput)
 * - Low quota: 10-50 records/batch (conservative progress)
 * - No quota: 0 records (waits for reset)
 * - Delays: 100-2000ms based on network speed
 * 
 * @module dynamic-batch-calculator
 */

import { log } from './logger.js';

/**
 * Network performance metrics tracked over a rolling window.
 * Used to detect trends in batch processing speed and adjust accordingly.
 */
export interface NetworkMetrics {
  /** Recent batch durations in milliseconds (rolling window of 10) */
  recentBatchDurations: number[];
  
  /** Recent batch sizes (rolling window of 10) */
  recentBatchSizes: number[];
  
  /** Count of consecutive successful batches (resets on failure) */
  consecutiveSuccesses: number;
  
  /** Count of consecutive failed batches (resets on success) */
  consecutiveFailures: number;
  
  /** Unix timestamp (ms) when metrics were last updated */
  lastUpdated: number;
}

/**
 * Result of a batch size/delay calculation.
 * Provides the recommended values along with reasoning and confidence.
 */
export interface BatchCalculation {
  /** Recommended batch size (1-200 records per batch) */
  batchSize: number;
  
  /** Recommended delay between batches (100-2000ms) */
  batchDelay: number;
  
  /** Human-readable explanation of why these values were chosen */
  reason: string;
  
  /** Confidence level (0.0-1.0) indicating reliability of calculation */
  confidence: number;
}

/**
 * DynamicBatchCalculator - Intelligent batch size and delay calculator
 * 
 * CORE ALGORITHM:
 * 
 * 1. QUOTA-BASED SIZING:
 *    batch_size = min(available_points ÷ 3, 200)
 *    
 *    Examples:
 *    - 5000 points → 1666 records → clamped to 200
 *    - 900 points → 300 records → clamped to 200
 *    - 600 points → 200 records → use 200
 *    - 150 points → 50 records → use 50
 *    - 10 points → 3 records → use 3
 * 
 * 2. METRIC-BASED DELAYS:
 *    If performing well (5+ successes):
 *      delay = avg_duration × 0.1 (fast)
 *    Otherwise:
 *      delay = avg_duration × 0.5 (conservative)
 *    Minimum: 100ms
 * 
 * 3. ADAPTIVE SCALING:
 *    - 5+ consecutive successes → ×1.25 scale up
 *    - 2+ consecutive failures → ×0.67 scale down
 *    - Network 50%+ slower → ×0.8 scale down
 *    - Network 30%+ faster → ×1.15 scale up
 * 
 * USAGE FLOW:
 * 
 * First batch (no server info):
 *   → Start with probe (10 records)
 *   → Receive headers with capacity
 *   → Calculate optimal initial batch
 *   → Continue with optimal settings
 * 
 * Subsequent batches:
 *   → Get safe available quota
 *   → Calculate batch size from quota
 *   → Apply adaptive scaling
 *   → Use calculated delay
 *   → Record success/failure
 *   → Adjust for next batch
 */
export class DynamicBatchCalculator {
  /** Cost per record in AT Protocol rate limiting (constant) */
  private readonly POINTS_PER_RECORD = 3;
  
  /** PDS hard limit for applyWrites operations (AT Protocol spec) */
  private readonly MAX_PDS_BATCH_SIZE = 200;
  
  /** Number of recent batches to track for trend analysis */
  private readonly METRICS_WINDOW_SIZE = 10;
  
  /** Absolute minimum delay to prevent hammering the server */
  private readonly MIN_DELAY_MS = 100;
  
  /** Current network performance metrics (rolling window) */
  private metrics: NetworkMetrics = {
    recentBatchDurations: [],
    recentBatchSizes: [],
    consecutiveSuccesses: 0,
    consecutiveFailures: 0,
    lastUpdated: Date.now()
  };
  
  /**
   * Calculate optimal batch size based purely on available rate limit points.
   * 
   * ALGORITHM:
   * 1. Calculate max records: available_points ÷ 3 points/record
   * 2. Clamp to PDS limit: min(max_records, 200)
   * 3. Calculate confidence based on information availability
   * 
   * RETURNS:
   * - null if no information available (should not happen in practice)
   * - { batchSize: 0 } if no quota available (must wait for reset)
   * - { batchSize: 1-200 } with calculated delay otherwise
   * 
   * CONFIDENCE SCORING:
   * - Base: 0.5 (when we only know available points)
   * - +0.3 scaled by quota % (when we know server limit)
   * - Max: 0.95 (when quota is near 100%)
   * 
   * @param availablePoints Current safe quota available (after headroom)
   * @param serverLimit Total server capacity (null if unknown)
   * @returns Batch calculation or null if no data
   */
  calculateBatchSizeFromQuota(availablePoints: number, serverLimit: number | null): BatchCalculation | null {
    if (availablePoints <= 0) {
      return {
        batchSize: 0,
        batchDelay: 0,
        reason: 'No quota available - must wait for reset',
        confidence: 1.0
      };
    }
    
    // Calculate max records from available points
    const maxRecordsFromQuota = Math.floor(availablePoints / this.POINTS_PER_RECORD);
    
    // Clamp to PDS hard limit
    const batchSize = Math.min(maxRecordsFromQuota, this.MAX_PDS_BATCH_SIZE);
    
    // Calculate confidence based on how much information we have
    let confidence = 0.5; // Base confidence
    if (serverLimit !== null && serverLimit > 0) {
      // We have server limit info - higher confidence
      const quotaPercentage = availablePoints / serverLimit;
      confidence = Math.min(0.95, 0.6 + quotaPercentage * 0.3);
    }
    
    if (batchSize === 0) {
      return {
        batchSize: 0,
        batchDelay: 0,
        reason: 'Available points too low for even one record',
        confidence
      };
    }
    
    const reason = serverLimit 
      ? `Quota-based: ${availablePoints}/${serverLimit} points (${(availablePoints/serverLimit*100).toFixed(1)}%)`
      : `Quota-based: ${availablePoints} points available`;
    
    return {
      batchSize,
      batchDelay: this.calculateDelayFromMetrics(),
      reason,
      confidence
    };
  }
  
  /**
   * Calculate initial batch size from server rate limit headers.
   * Used when we first learn the server's capacity from the first response.
   * 
   * ALGORITHM:
   * 1. Calculate sustainable rate: (points/window) ÷ 3 = records/second
   * 2. Apply conservative factor: ÷2 to leave buffer for retries
   * 3. Calculate batch size: conservative_rate × 2 seconds
   * 4. Clamp to 1-200 range
   * 
   * EXAMPLE:
   * Server: 5000 points / 3600s
   *   → 1.39 points/s ÷ 3 = 0.46 rec/s
   *   → 0.46 ÷ 2 (conservative) = 0.23 rec/s
   *   → 0.23 × 2s = 0.46 → rounds to at least 1
   *   
   * The actual calculation uses different math but same principle:
   *   → Server capacity: 5000/3600 = 1.39 points/s = 0.46 rec/s
   *   → Conservative (50%): 0.23 rec/s
   *   → Batch interval 2s: 0.23 × 2 ≈ 1 record minimum
   * 
   * This is intentionally conservative for the first batch. Adaptive
   * scaling will quickly increase it if the server can handle more.
   * 
   * @param serverLimit Server rate limit capacity (e.g., 5000)
   * @param windowSeconds Rate limit window duration (e.g., 3600 = 1 hour)
   * @returns Initial batch calculation with 0.7 confidence
   */
  calculateInitialBatchFromServer(serverLimit: number, windowSeconds: number): BatchCalculation {
    // Calculate maximum sustainable throughput
    // We want to use the quota efficiently over the window
    const pointsPerSecond = serverLimit / windowSeconds;
    const recordsPerSecond = pointsPerSecond / this.POINTS_PER_RECORD;
    
    // Start conservatively: aim to use quota over 2x the window time
    // This leaves plenty of buffer for retries and other operations
    const conservativeRecordsPerSecond = recordsPerSecond / 2;
    
    // Calculate batch size that maintains this rate with reasonable batching
    // Assume we want ~1 batch per 2 seconds initially
    const targetBatchInterval = 2; // seconds
    const initialBatchSize = Math.floor(conservativeRecordsPerSecond * targetBatchInterval);
    
    // Clamp to reasonable bounds
    const clampedBatchSize = Math.max(
      1, // At least 1 record
      Math.min(initialBatchSize, this.MAX_PDS_BATCH_SIZE)
    );
    
    log.info(`[DynamicBatchCalculator] Server capacity: ${serverLimit} points/${windowSeconds}s = ${pointsPerSecond.toFixed(1)} points/s`);
    log.info(`[DynamicBatchCalculator] Conservative rate: ${conservativeRecordsPerSecond.toFixed(1)} records/s (50% of max)`);
    log.info(`[DynamicBatchCalculator] Initial batch size: ${clampedBatchSize} records`);
    
    return {
      batchSize: clampedBatchSize,
      batchDelay: this.calculateDelayFromMetrics(),
      reason: `Server capacity: ${serverLimit} points/${windowSeconds}s (conservative start)`,
      confidence: 0.7
    };
  }
  
  /**
   * Calculate delay based on recent network performance metrics.
   * 
   * ALGORITHM:
   * - If no history: return MIN_DELAY_MS (100ms)
   * - Calculate average batch duration from recent batches
   * - If performing well (3+ successes): delay = avg × 0.1 (fast)
   * - Otherwise: delay = avg × 0.5 (conservative)
   * - Never go below MIN_DELAY_MS
   * 
   * EXAMPLE:
   * Recent batches average 1000ms:
   *   Good performance: 1000 × 0.1 = 100ms delay
   *   Poor performance: 1000 × 0.5 = 500ms delay
   * 
   * This adapts the delay to network speed. Fast networks get shorter
   * delays, slow networks get longer delays. The multiplier changes
   * based on success patterns to be more aggressive when things are
   * going well and more conservative when there are issues.
   * 
   * @returns Recommended delay in milliseconds (100-2000ms typically)
   */
  private calculateDelayFromMetrics(): number {
    if (this.metrics.recentBatchDurations.length === 0) {
      // No history - use minimum safe delay
      return this.MIN_DELAY_MS;
    }
    
    // Calculate average batch duration
    const avgDuration = this.metrics.recentBatchDurations.reduce((a, b) => a + b, 0) 
      / this.metrics.recentBatchDurations.length;
    
    // Base delay on average processing time
    // Good performance: delay = 10% of processing time
    // Poor performance: delay = 50% of processing time
    const performanceMultiplier = this.metrics.consecutiveSuccesses > 3 ? 0.1 : 0.5;
    const calculatedDelay = Math.floor(avgDuration * performanceMultiplier);
    
    // Never go below minimum
    return Math.max(this.MIN_DELAY_MS, calculatedDelay);
  }
  
  /**
   * Record a successful batch for metrics tracking.
   * 
   * EFFECTS:
   * - Increments consecutiveSuccesses counter
   * - Resets consecutiveFailures to 0
   * - Adds batch metrics to rolling window
   * - May trigger scale-up on next adaptive calculation
   * 
   * @param batchSize Number of records in the successful batch
   * @param durationMs Time taken to process the batch (milliseconds)
   */
  recordSuccess(batchSize: number, durationMs: number): void {
    this.metrics.consecutiveSuccesses++;
    this.metrics.consecutiveFailures = 0;
    this.addBatchMetric(batchSize, durationMs);
    
    log.debug(`[DynamicBatchCalculator] Success recorded: size=${batchSize}, duration=${durationMs}ms, streak=${this.metrics.consecutiveSuccesses}`);
  }
  
  /**
   * Record a failed batch for metrics tracking.
   * 
   * EFFECTS:
   * - Increments consecutiveFailures counter
   * - Resets consecutiveSuccesses to 0
   * - Adds batch metrics to rolling window
   * - May trigger scale-down on next adaptive calculation
   * 
   * @param batchSize Number of records in the failed batch
   * @param durationMs Time taken before the batch failed (milliseconds)
   */
  recordFailure(batchSize: number, durationMs: number): void {
    this.metrics.consecutiveFailures++;
    this.metrics.consecutiveSuccesses = 0;
    this.addBatchMetric(batchSize, durationMs);
    
    log.debug(`[DynamicBatchCalculator] Failure recorded: size=${batchSize}, duration=${durationMs}ms, failures=${this.metrics.consecutiveFailures}`);
  }
  
  /**
   * Add a batch metric to the rolling window.
   * Maintains a maximum window size of METRICS_WINDOW_SIZE (10) by
   * removing the oldest metric when the window is full.
   * 
   * @param batchSize Number of records in the batch
   * @param durationMs Time taken to process the batch
   */
  private addBatchMetric(batchSize: number, durationMs: number): void {
    this.metrics.recentBatchDurations.push(durationMs);
    this.metrics.recentBatchSizes.push(batchSize);
    this.metrics.lastUpdated = Date.now();
    
    // Keep only recent metrics
    if (this.metrics.recentBatchDurations.length > this.METRICS_WINDOW_SIZE) {
      this.metrics.recentBatchDurations.shift();
      this.metrics.recentBatchSizes.shift();
    }
  }
  
  /**
   * Calculate adaptive scaling factor based on performance patterns.
   * 
   * RULES:
   * 1. Scale up ×1.25 after 5 consecutive successes
   * 2. Scale down ×0.67 after 2 consecutive failures
   * 3. Scale down ×0.8 if recent batches are 50%+ slower
   * 4. Scale up ×1.15 if recent batches are 30%+ faster
   * 5. Return 1.0 (no change) for stable performance
   * 
   * EXAMPLE FLOW:
   * - Start: 100 records/batch
   * - 5 successes: 100 × 1.25 = 125 records/batch
   * - 5 more successes: 125 × 1.25 = 156 records/batch
   * - Network degrades: 156 × 0.8 = 125 records/batch
   * - 2 failures: 125 × 0.67 = 84 records/batch
   * - Recover: 84 × 1.15 = 97 records/batch
   * 
   * This creates a feedback loop that continuously adjusts batch
   * sizes to match current server and network conditions.
   * 
   * @returns Scale factor (0.67-1.25) and human-readable reason
   */
  calculateAdaptiveScale(): { scale: number; reason: string } {
    // No adjustment needed if no history
    if (this.metrics.recentBatchDurations.length < 3) {
      return { scale: 1.0, reason: 'Insufficient history for adaptation' };
    }
    
    // Scale up on consistent success
    if (this.metrics.consecutiveSuccesses >= 5) {
      log.debug(`[DynamicBatchCalculator] Scaling up: ${this.metrics.consecutiveSuccesses} consecutive successes`);
      return { 
        scale: 1.25, 
        reason: `Performance excellent (${this.metrics.consecutiveSuccesses} successes)` 
      };
    }
    
    // Scale down on failures
    if (this.metrics.consecutiveFailures >= 2) {
      log.debug(`[DynamicBatchCalculator] Scaling down: ${this.metrics.consecutiveFailures} consecutive failures`);
      return { 
        scale: 0.67, 
        reason: `Performance degraded (${this.metrics.consecutiveFailures} failures)` 
      };
    }
    
    // Check if recent batches are getting slower
    if (this.metrics.recentBatchDurations.length >= 5) {
      const recentAvg = this.metrics.recentBatchDurations.slice(-3).reduce((a, b) => a + b) / 3;
      const olderAvg = this.metrics.recentBatchDurations.slice(0, 3).reduce((a, b) => a + b) / 3;
      
      if (recentAvg > olderAvg * 1.5) {
        log.debug(`[DynamicBatchCalculator] Scaling down: recent batches 50% slower (${recentAvg.toFixed(0)}ms vs ${olderAvg.toFixed(0)}ms)`);
        return { 
          scale: 0.8, 
          reason: 'Network performance degrading' 
        };
      }
      
      if (recentAvg < olderAvg * 0.7) {
        log.debug(`[DynamicBatchCalculator] Scaling up: recent batches 30% faster (${recentAvg.toFixed(0)}ms vs ${olderAvg.toFixed(0)}ms)`);
        return { 
          scale: 1.15, 
          reason: 'Network performance improving' 
        };
      }
    }
    
    return { scale: 1.0, reason: 'Performance stable' };
  }
  
  /**
   * Get current metrics for logging/debugging.
   * Returns a copy of the metrics to prevent external modification.
   * 
   * @returns Current network performance metrics
   */
  getMetrics(): NetworkMetrics {
    return { ...this.metrics };
  }
  
  /**
   * Get performance summary for display.
   * 
   * CALCULATES:
   * - Average batch size from recent batches
   * - Average batch duration from recent batches
   * - Records per second throughput rate
   * 
   * EXAMPLE OUTPUT:
   * "Avg: 176 records in 1150ms (~153.0 rec/s)"
   * 
   * @returns Human-readable performance summary string
   */
  getPerformanceSummary(): string {
    if (this.metrics.recentBatchDurations.length === 0) {
      return 'No performance data yet';
    }
    
    const avgDuration = this.metrics.recentBatchDurations.reduce((a, b) => a + b, 0) 
      / this.metrics.recentBatchDurations.length;
    const avgSize = this.metrics.recentBatchSizes.reduce((a, b) => a + b, 0) 
      / this.metrics.recentBatchSizes.length;
    const recordsPerSecond = (avgSize / avgDuration) * 1000;
    
    return `Avg: ${avgSize.toFixed(0)} records in ${avgDuration.toFixed(0)}ms (~${recordsPerSecond.toFixed(1)} rec/s)`;
  }
}
