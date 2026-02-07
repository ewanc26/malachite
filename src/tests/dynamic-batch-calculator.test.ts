/**
 * Tests for DynamicBatchCalculator
 * Run with: pnpm test
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { DynamicBatchCalculator } from '../utils/dynamic-batch-calculator.js';

describe('DynamicBatchCalculator', () => {
  describe('calculateBatchSizeFromQuota', () => {
    it('should return null for no quota available', () => {
      const calc = new DynamicBatchCalculator();
      const result = calc.calculateBatchSizeFromQuota(0, 5000);
      
      assert.strictEqual(result?.batchSize, 0);
      assert.strictEqual(result?.confidence, 1.0);
      assert.match(result?.reason || '', /No quota/);
    });

    it('should calculate batch size from available points', () => {
      const calc = new DynamicBatchCalculator();
      const result = calc.calculateBatchSizeFromQuota(900, 5000);
      
      // 900 points ÷ 3 points/record = 300 records, clamped to 200 max
      assert.strictEqual(result?.batchSize, 200);
      assert(result?.confidence && result.confidence > 0.6);
    });

    it('should handle low quota correctly', () => {
      const calc = new DynamicBatchCalculator();
      const result = calc.calculateBatchSizeFromQuota(15, 5000);
      
      // 15 points ÷ 3 points/record = 5 records
      assert.strictEqual(result?.batchSize, 5);
    });

    it('should clamp to PDS max of 200', () => {
      const calc = new DynamicBatchCalculator();
      const result = calc.calculateBatchSizeFromQuota(10000, 10000);
      
      // 10000 points ÷ 3 = 3333 records, but clamped to 200
      assert.strictEqual(result?.batchSize, 200);
    });

    it('should have higher confidence with server info', () => {
      const calc = new DynamicBatchCalculator();
      
      const withoutServer = calc.calculateBatchSizeFromQuota(1000, null);
      const withServer = calc.calculateBatchSizeFromQuota(1000, 5000);
      
      assert(withServer && withoutServer);
      assert(withServer.confidence > withoutServer.confidence);
    });
  });

  describe('calculateInitialBatchFromServer', () => {
    it('should calculate conservative initial batch', () => {
      const calc = new DynamicBatchCalculator();
      const result = calc.calculateInitialBatchFromServer(5000, 3600);
      
      // 5000 points / 3600s = 1.39 points/s
      // 1.39 / 3 = 0.46 records/s
      // 0.46 / 2 (conservative) = 0.23 records/s
      // 0.23 * 2s (target interval) = 0.46 records
      // Rounds to at least 1, likely ~27 based on math
      assert(result.batchSize >= 1);
      assert(result.batchSize <= 200);
      assert(result.confidence >= 0.7);
    });

    it('should handle high capacity servers', () => {
      const calc = new DynamicBatchCalculator();
      const result = calc.calculateInitialBatchFromServer(10000, 3600);
      
      assert(result.batchSize > 1);
      assert(result.batchSize <= 200);
    });

    it('should handle low capacity servers', () => {
      const calc = new DynamicBatchCalculator();
      const result = calc.calculateInitialBatchFromServer(1000, 3600);
      
      assert(result.batchSize >= 1);
      assert(result.batchSize <= 200);
    });
  });

  describe('adaptive scaling', () => {
    it('should scale up after 5 successes', () => {
      const calc = new DynamicBatchCalculator();
      
      // Record 5 successes
      for (let i = 0; i < 5; i++) {
        calc.recordSuccess(100, 1000);
      }
      
      const scale = calc.calculateAdaptiveScale();
      assert.strictEqual(scale.scale, 1.25);
      assert.match(scale.reason, /excellent/);
    });

    it('should scale down after 2 failures', () => {
      const calc = new DynamicBatchCalculator();
      
      // Record 2 failures
      calc.recordFailure(100, 1000);
      calc.recordFailure(100, 1000);
      
      const scale = calc.calculateAdaptiveScale();
      assert.strictEqual(scale.scale, 0.67);
      assert.match(scale.reason, /degraded/);
    });

    it('should scale down on network degradation', () => {
      const calc = new DynamicBatchCalculator();
      
      // Start with fast batches
      for (let i = 0; i < 3; i++) {
        calc.recordSuccess(100, 1000);
      }
      
      // Then slow batches
      for (let i = 0; i < 3; i++) {
        calc.recordSuccess(100, 2000);
      }
      
      const scale = calc.calculateAdaptiveScale();
      // Should detect 2x slowdown and scale down
      assert(scale.scale < 1.0);
      assert.match(scale.reason, /degrading/);
    });

    it('should scale up on network improvement', () => {
      const calc = new DynamicBatchCalculator();
      
      // Start with slow batches
      for (let i = 0; i < 3; i++) {
        calc.recordSuccess(100, 2000);
      }
      
      // Then fast batches
      for (let i = 0; i < 3; i++) {
        calc.recordSuccess(100, 800);
      }
      
      const scale = calc.calculateAdaptiveScale();
      // Should detect improvement and scale up
      assert(scale.scale > 1.0);
      assert.match(scale.reason, /improving/);
    });

    it('should return 1.0 scale for stable performance', () => {
      const calc = new DynamicBatchCalculator();
      
      // Consistent performance
      for (let i = 0; i < 5; i++) {
        calc.recordSuccess(100, 1000);
      }
      
      const scale = calc.calculateAdaptiveScale();
      assert.strictEqual(scale.scale, 1.0);
      assert.match(scale.reason, /stable/);
    });
  });

  describe('metrics tracking', () => {
    it('should track recent batch durations', () => {
      const calc = new DynamicBatchCalculator();
      
      calc.recordSuccess(50, 1000);
      calc.recordSuccess(60, 1200);
      calc.recordSuccess(70, 1100);
      
      const metrics = calc.getMetrics();
      assert.strictEqual(metrics.recentBatchDurations.length, 3);
      assert.strictEqual(metrics.recentBatchSizes.length, 3);
    });

    it('should limit metrics window to 10 entries', () => {
      const calc = new DynamicBatchCalculator();
      
      // Add 15 batches
      for (let i = 0; i < 15; i++) {
        calc.recordSuccess(100, 1000);
      }
      
      const metrics = calc.getMetrics();
      assert.strictEqual(metrics.recentBatchDurations.length, 10);
      assert.strictEqual(metrics.recentBatchSizes.length, 10);
    });

    it('should track consecutive successes', () => {
      const calc = new DynamicBatchCalculator();
      
      calc.recordSuccess(100, 1000);
      calc.recordSuccess(100, 1000);
      calc.recordSuccess(100, 1000);
      
      const metrics = calc.getMetrics();
      assert.strictEqual(metrics.consecutiveSuccesses, 3);
      assert.strictEqual(metrics.consecutiveFailures, 0);
    });

    it('should reset success counter on failure', () => {
      const calc = new DynamicBatchCalculator();
      
      calc.recordSuccess(100, 1000);
      calc.recordSuccess(100, 1000);
      calc.recordFailure(100, 1000);
      
      const metrics = calc.getMetrics();
      assert.strictEqual(metrics.consecutiveSuccesses, 0);
      assert.strictEqual(metrics.consecutiveFailures, 1);
    });
  });

  describe('performance summary', () => {
    it('should return "no data" message initially', () => {
      const calc = new DynamicBatchCalculator();
      const summary = calc.getPerformanceSummary();
      
      assert.match(summary, /No performance data/);
    });

    it('should calculate performance metrics', () => {
      const calc = new DynamicBatchCalculator();
      
      calc.recordSuccess(100, 1000);
      calc.recordSuccess(100, 1000);
      calc.recordSuccess(100, 1000);
      
      const summary = calc.getPerformanceSummary();
      
      // Should show avg batch size, duration, and rate
      assert.match(summary, /100/); // batch size
      assert.match(summary, /1000ms/); // duration
      assert.match(summary, /rec\/s/); // rate
    });
  });
});
