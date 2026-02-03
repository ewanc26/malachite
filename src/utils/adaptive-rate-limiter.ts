import { log } from './logger.js';
import type { Config } from '../types.js';
import {
  MAX_CONSECUTIVE_FAILURES,
  CIRCUIT_BREAKER_THRESHOLD,
  CIRCUIT_BREAKER_TIMEOUT,
  SPEEDUP_THRESHOLD,
  MAX_DELAY,
  RESPONSE_TIME_WINDOW,
  ERROR_RATE_WINDOW,
  HIGH_ERROR_RATE_THRESHOLD,
  SLOW_RESPONSE_THRESHOLD,
} from '../constants.js';

/**
 * Rate limiter state and metrics
 */
interface RateLimiterState {
  currentBatchSize: number;
  currentDelay: number;
  consecutiveSuccesses: number;
  consecutiveFailures: number;
  totalRequests: number;
  totalFailures: number;
  totalRateLimits: number;
  lastRateLimitTime: number | null;
  circuitBreakerOpen: boolean;
  circuitBreakerOpenedAt: number | null;
  recentResponseTimes: number[];
  recentErrorRates: number[];
}

/**
 * Rate limit detection result
 */
interface RateLimitDetection {
  isRateLimited: boolean;
  resetTime: number | null;
  retryAfter: number | null;
  confidence: 'high' | 'medium' | 'low';
}

/**
 * Adaptive rate limiter with exponential backoff and dynamic adjustments
 * 
 * Features:
 * - Exponential backoff with jitter for rate limit errors
 * - Dynamic batch size adjustment based on success/failure patterns
 * - Circuit breaker pattern to prevent cascading failures
 * - Response time monitoring for proactive slowdown
 * - Rate limit detection from multiple signals (status codes, headers, error patterns)
 * - Automatic recovery and speed-up when conditions improve
 */
export class AdaptiveRateLimiter {
  private state: RateLimiterState;
  private config: Config;
  private maxBatchSize: number;
  private minBatchSize: number;
  
  constructor(config: Config, initialBatchSize: number, initialDelay: number, maxBatchSize: number = 200) {
    this.config = config;
    this.maxBatchSize = Math.min(maxBatchSize, config.MAX_BATCH_SIZE);
    this.minBatchSize = 10; // Minimum 10 records per batch
    
    this.state = {
      currentBatchSize: Math.min(initialBatchSize, this.maxBatchSize),
      currentDelay: initialDelay,
      consecutiveSuccesses: 0,
      consecutiveFailures: 0,
      totalRequests: 0,
      totalFailures: 0,
      totalRateLimits: 0,
      lastRateLimitTime: null,
      circuitBreakerOpen: false,
      circuitBreakerOpenedAt: null,
      recentResponseTimes: [],
      recentErrorRates: [],
    };
  }
  
  /**
   * Get current batch size
   */
  getCurrentBatchSize(): number {
    return this.state.currentBatchSize;
  }
  
  /**
   * Get current delay
   */
  getCurrentDelay(): number {
    return this.state.currentDelay;
  }
  
  /**
   * Get current statistics
   */
  getStats() {
    const successRate = this.state.totalRequests > 0 
      ? ((this.state.totalRequests - this.state.totalFailures) / this.state.totalRequests) * 100
      : 0;
    
    const avgResponseTime = this.state.recentResponseTimes.length > 0
      ? this.state.recentResponseTimes.reduce((a, b) => a + b, 0) / this.state.recentResponseTimes.length
      : 0;
    
    return {
      batchSize: this.state.currentBatchSize,
      delay: this.state.currentDelay,
      totalRequests: this.state.totalRequests,
      successRate: successRate.toFixed(1),
      rateLimits: this.state.totalRateLimits,
      avgResponseTime: Math.round(avgResponseTime),
      circuitBreakerOpen: this.state.circuitBreakerOpen,
    };
  }
  
  /**
   * Detect rate limiting from error and headers
   */
  detectRateLimit(error: any): RateLimitDetection {
    let isRateLimited = false;
    let resetTime: number | null = null;
    let retryAfter: number | null = null;
    let confidence: 'high' | 'medium' | 'low' = 'low';
    
    // Check HTTP status code
    if (error.status === 429) {
      isRateLimited = true;
      confidence = 'high';
    }
    
    // Check error message patterns
    const message = error.message?.toLowerCase() || '';
    if (message.includes('rate limit') || 
        message.includes('too many requests') ||
        message.includes('throttle') ||
        message.includes('quota exceeded')) {
      isRateLimited = true;
      confidence = confidence === 'high' ? 'high' : 'medium';
    }
    
    // Check rate limit headers
    const headers = error.headers || {};
    
    // ratelimit-reset header (Unix timestamp)
    if (headers['ratelimit-reset']) {
      resetTime = parseInt(headers['ratelimit-reset'], 10) * 1000;
      isRateLimited = true;
      confidence = 'high';
    }
    
    // Retry-After header (seconds or HTTP date)
    if (headers['retry-after']) {
      const retryAfterValue = headers['retry-after'];
      if (/^\d+$/.test(retryAfterValue)) {
        // Numeric value in seconds
        retryAfter = parseInt(retryAfterValue, 10) * 1000;
      } else {
        // HTTP date
        const retryDate = new Date(retryAfterValue);
        if (!isNaN(retryDate.getTime())) {
          retryAfter = retryDate.getTime() - Date.now();
        }
      }
      
      if (retryAfter) {
        isRateLimited = true;
        confidence = 'high';
      }
    }
    
    // X-RateLimit-* headers
    if (headers['x-ratelimit-remaining'] === '0' || headers['x-ratelimit-remaining'] === 0) {
      isRateLimited = true;
      confidence = 'high';
      
      if (headers['x-ratelimit-reset']) {
        resetTime = parseInt(headers['x-ratelimit-reset'], 10) * 1000;
      }
    }
    
    return {
      isRateLimited,
      resetTime,
      retryAfter,
      confidence,
    };
  }
  
  /**
   * Calculate exponential backoff with jitter
   */
  private calculateBackoff(attempt: number): number {
    // Base exponential backoff: 2^attempt * initial delay
    const exponentialDelay = Math.pow(2, attempt) * this.state.currentDelay;
    
    // Cap at maximum delay
    const cappedDelay = Math.min(exponentialDelay, MAX_DELAY);
    
    // Add jitter (±25%) to prevent thundering herd
    const jitter = cappedDelay * 0.25 * (Math.random() * 2 - 1);
    
    return Math.floor(cappedDelay + jitter);
  }
  
  /**
   * Handle successful batch
   */
  async onSuccess(responseTime: number): Promise<void> {
    this.state.consecutiveSuccesses++;
    this.state.consecutiveFailures = 0;
    this.state.totalRequests++;
    
    // Track response time
    this.state.recentResponseTimes.push(responseTime);
    if (this.state.recentResponseTimes.length > RESPONSE_TIME_WINDOW) {
      this.state.recentResponseTimes.shift();
    }
    
    // Track success in error rate window
    this.state.recentErrorRates.push(0);
    if (this.state.recentErrorRates.length > ERROR_RATE_WINDOW) {
      this.state.recentErrorRates.shift();
    }
    
    // Close circuit breaker if it was open and enough time has passed
    if (this.state.circuitBreakerOpen && this.state.circuitBreakerOpenedAt) {
      if (Date.now() - this.state.circuitBreakerOpenedAt > CIRCUIT_BREAKER_TIMEOUT) {
        this.state.circuitBreakerOpen = false;
        this.state.circuitBreakerOpenedAt = null;
        log.info('🔓 Circuit breaker closed - resuming normal operations');
      }
    }
    
    // Speed up if we're doing well
    if (this.state.consecutiveSuccesses >= SPEEDUP_THRESHOLD) {
      this.speedUp();
      this.state.consecutiveSuccesses = 0; // Reset counter
    }
    
    // Also check if response times are good - we can be more aggressive
    const avgResponseTime = this.state.recentResponseTimes.reduce((a, b) => a + b, 0) / 
                           this.state.recentResponseTimes.length;
    
    if (avgResponseTime < SLOW_RESPONSE_THRESHOLD / 2 && 
        this.state.currentDelay > this.config.MIN_BATCH_DELAY) {
      // Responses are very fast, can reduce delay slightly
      const oldDelay = this.state.currentDelay;
      this.state.currentDelay = Math.max(
        this.config.MIN_BATCH_DELAY,
        Math.floor(this.state.currentDelay * 0.9)
      );
      
      if (oldDelay !== this.state.currentDelay) {
        log.debug(`⚡ Fast responses detected, reducing delay: ${oldDelay}ms → ${this.state.currentDelay}ms`);
      }
    }
  }
  
  /**
   * Handle failed batch with rate limit
   */
  async onRateLimit(error: any): Promise<number> {
    this.state.consecutiveFailures++;
    this.state.consecutiveSuccesses = 0;
    this.state.totalRequests++;
    this.state.totalFailures++;
    this.state.totalRateLimits++;
    this.state.lastRateLimitTime = Date.now();
    
    // Track failure in error rate window
    this.state.recentErrorRates.push(1);
    if (this.state.recentErrorRates.length > ERROR_RATE_WINDOW) {
      this.state.recentErrorRates.shift();
    }
    
    // Detect rate limit details
    const detection = this.detectRateLimit(error);
    
    log.warn(`⚠️  Rate limit detected (confidence: ${detection.confidence})`);
    
    // Calculate wait time
    let waitTime: number;
    
    if (detection.resetTime) {
      waitTime = Math.max(detection.resetTime - Date.now() + 2000, 0); // +2s buffer
      log.info(`📊 Rate limit reset time: ${new Date(detection.resetTime).toLocaleTimeString()}`);
    } else if (detection.retryAfter) {
      waitTime = detection.retryAfter + 1000; // +1s buffer
      log.info(`📊 Retry after: ${Math.ceil(detection.retryAfter / 1000)}s`);
    } else {
      // Use exponential backoff
      waitTime = this.calculateBackoff(this.state.consecutiveFailures);
      log.info(`📊 Using exponential backoff: ${Math.ceil(waitTime / 1000)}s`);
    }
    
    // Apply exponential backoff to delay
    const oldDelay = this.state.currentDelay;
    const backoffMultiplier = Math.pow(2, Math.min(this.state.consecutiveFailures, 5));
    this.state.currentDelay = Math.min(
      this.state.currentDelay * backoffMultiplier,
      MAX_DELAY
    );
    
    // Reduce batch size aggressively
    const oldBatchSize = this.state.currentBatchSize;
    this.state.currentBatchSize = Math.max(
      Math.floor(this.state.currentBatchSize / 2),
      this.minBatchSize
    );
    
    log.info(`📉 Backing off - Batch: ${oldBatchSize} → ${this.state.currentBatchSize}, Delay: ${oldDelay}ms → ${this.state.currentDelay}ms`);
    
    // Open circuit breaker if too many consecutive rate limits
    if (this.state.consecutiveFailures >= CIRCUIT_BREAKER_THRESHOLD) {
      this.state.circuitBreakerOpen = true;
      this.state.circuitBreakerOpenedAt = Date.now();
      log.error(`🔒 Circuit breaker opened after ${this.state.consecutiveFailures} consecutive failures`);
      log.warn(`⏸️  Entering cooldown period for ${CIRCUIT_BREAKER_TIMEOUT / 1000}s`);
      
      // Return extended wait time for circuit breaker
      return Math.max(waitTime, CIRCUIT_BREAKER_TIMEOUT);
    }
    
    return waitTime;
  }
  
  /**
   * Handle general error (non-rate-limit)
   */
  async onError(_error: any): Promise<void> {
    this.state.consecutiveFailures++;
    this.state.consecutiveSuccesses = 0;
    this.state.totalRequests++;
    this.state.totalFailures++;
    
    // Track failure in error rate window
    this.state.recentErrorRates.push(1);
    if (this.state.recentErrorRates.length > ERROR_RATE_WINDOW) {
      this.state.recentErrorRates.shift();
    }
    
    // Calculate error rate
    if (this.state.recentErrorRates.length >= ERROR_RATE_WINDOW) {
      const errorRate = this.state.recentErrorRates.reduce((a, b) => a + b, 0) / 
                       this.state.recentErrorRates.length;
      
      if (errorRate > HIGH_ERROR_RATE_THRESHOLD) {
        // High error rate detected, slow down
        const oldDelay = this.state.currentDelay;
        const oldBatchSize = this.state.currentBatchSize;
        
        this.state.currentDelay = Math.min(this.state.currentDelay * 1.5, MAX_DELAY);
        this.state.currentBatchSize = Math.max(
          Math.floor(this.state.currentBatchSize * 0.75),
          this.minBatchSize
        );
        
        log.warn(`⚠️  High error rate (${(errorRate * 100).toFixed(1)}%) - slowing down`);
        log.info(`📉 Adjusted: Batch ${oldBatchSize} → ${this.state.currentBatchSize}, Delay ${oldDelay}ms → ${this.state.currentDelay}ms`);
      }
    }
    
    // Slow down if multiple consecutive failures
    if (this.state.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
      const oldDelay = this.state.currentDelay;
      const oldBatchSize = this.state.currentBatchSize;
      
      this.state.currentDelay = Math.min(this.state.currentDelay * 2, MAX_DELAY);
      this.state.currentBatchSize = Math.max(
        Math.floor(this.state.currentBatchSize / 2),
        this.minBatchSize
      );
      
      log.warn(`⚠️  ${this.state.consecutiveFailures} consecutive failures - slowing down`);
      log.info(`📉 Adjusted: Batch ${oldBatchSize} → ${this.state.currentBatchSize}, Delay ${oldDelay}ms → ${this.state.currentDelay}ms`);
    }
  }
  
  /**
   * Check if circuit breaker is open
   */
  isCircuitBreakerOpen(): boolean {
    // Check if enough time has passed to try closing the circuit breaker
    if (this.state.circuitBreakerOpen && this.state.circuitBreakerOpenedAt) {
      if (Date.now() - this.state.circuitBreakerOpenedAt > CIRCUIT_BREAKER_TIMEOUT) {
        this.state.circuitBreakerOpen = false;
        this.state.circuitBreakerOpenedAt = null;
        this.state.consecutiveFailures = 0; // Reset failure counter
        log.info('🔓 Circuit breaker timeout elapsed - attempting to resume');
        return false;
      }
    }
    
    return this.state.circuitBreakerOpen;
  }
  
  /**
   * Speed up processing when conditions are good
   */
  private speedUp(): void {
    const oldDelay = this.state.currentDelay;
    const oldBatchSize = this.state.currentBatchSize;
    
    // Reduce delay by 20%
    if (this.state.currentDelay > this.config.MIN_BATCH_DELAY) {
      this.state.currentDelay = Math.max(
        this.config.MIN_BATCH_DELAY,
        Math.floor(this.state.currentDelay * 0.8)
      );
    }
    
    // Increase batch size by 25% (but not above max)
    if (this.state.currentBatchSize < this.maxBatchSize) {
      this.state.currentBatchSize = Math.min(
        Math.floor(this.state.currentBatchSize * 1.25),
        this.maxBatchSize
      );
    }
    
    // Only log if something changed
    if (oldDelay !== this.state.currentDelay || oldBatchSize !== this.state.currentBatchSize) {
      log.info(`⚡ Speeding up! Batch: ${oldBatchSize} → ${this.state.currentBatchSize}, Delay: ${oldDelay}ms → ${this.state.currentDelay}ms`);
    }
  }
  
  /**
   * Wait for the specified delay with cancellation support
   */
  async wait(checkCancellation?: () => boolean): Promise<boolean> {
    const delay = this.state.currentDelay;
    const checkInterval = 100; // Check cancellation every 100ms
    const checks = Math.ceil(delay / checkInterval);
    
    for (let i = 0; i < checks; i++) {
      if (checkCancellation && checkCancellation()) {
        return false; // Cancelled
      }
      
      await new Promise(resolve => setTimeout(resolve, Math.min(checkInterval, delay - (i * checkInterval))));
    }
    
    return true; // Completed
  }
  
  /**
   * Reset to initial state (useful for testing or manual reset)
   */
  reset(initialBatchSize?: number, initialDelay?: number): void {
    this.state = {
      currentBatchSize: initialBatchSize ? Math.min(initialBatchSize, this.maxBatchSize) : this.state.currentBatchSize,
      currentDelay: initialDelay || this.state.currentDelay,
      consecutiveSuccesses: 0,
      consecutiveFailures: 0,
      totalRequests: 0,
      totalFailures: 0,
      totalRateLimits: 0,
      lastRateLimitTime: null,
      circuitBreakerOpen: false,
      circuitBreakerOpenedAt: null,
      recentResponseTimes: [],
      recentErrorRates: [],
    };
    
    log.info('🔄 Rate limiter reset to initial state');
  }
}
