/**
 * TID (Timestamp Identifier) Clock for ATProto
 * 
 * Implements spec-compliant, monotonic TID generation with:
 * - AT-Protocol format validation (13 chars, base32 alphabet)
 * - Strict monotonicity guarantees (even under clock drift)
 * - Concurrency safety (mutex-protected state)
 * - Deterministic mode for dry-runs
 * - Full observability (structured JSON logging)
 * - Collision resistance
 * 
 * Based on AT-Protocol spec: https://atproto.com/specs/tid
 * Reference implementation: @atproto/common-web
 */

import { s32decode, s32encode } from '@atproto/common-web/dist/util.js';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

const TID_LENGTH = 13;
const TID_REGEX = /^[234567abcdefghij][234567abcdefghijklmnopqrstuvwxyz]{12}$/;

/**
 * TID validation error
 */
export class InvalidTidError extends Error {
  constructor(message: string, public tid?: string) {
    super(message);
    this.name = 'InvalidTidError';
  }
}

/**
 * TID generation modes
 */
export enum TidMode {
  PRODUCTION = 'production',   // Real wall-clock time
  DRY_RUN = 'dry-run',         // Deterministic with fixed seed
  REPLAY = 'replay',           // Replay from logged state
}

/**
 * Clock source abstraction for testability
 */
export interface ClockSource {
  now(): number; // Returns microseconds since epoch
}

/**
 * Real wall-clock implementation
 */
export class RealClock implements ClockSource {
  now(): number {
    return Date.now() * 1000; // Convert ms to µs
  }
}

/**
 * Deterministic fake clock for testing/dry-runs
 */
export class FakeClock implements ClockSource {
  constructor(private timestamp: number) {}
  
  now(): number {
    return this.timestamp;
  }
  
  advance(microseconds: number): void {
    this.timestamp += microseconds;
  }
  
  set(microseconds: number): void {
    this.timestamp = microseconds;
  }
}

/**
 * TID generator state (for persistence and logging)
 */
export interface TidState {
  lastTimestampUs: number;  // Last generated timestamp in microseconds
  clockId: number;          // Clock identifier (0-31 for this implementation)
  generatedCount: number;   // Total TIDs generated
}

/**
 * Metadata for each generated TID
 */
export interface TidMetadata {
  tid: string;
  timestampUs: number;
  clockId: number;
  generatedAt: string;      // ISO8601 with microseconds
  validated: boolean;
  context?: string;
}

/**
 * Logger interface for TID operations
 */
export interface TidLogger {
  logGenerated(metadata: TidMetadata, opId: string): void;
  logWarning(message: string, details?: any): void;
  logError(message: string, details?: any): void;
}

/**
 * Default console-based logger
 */
export class ConsoleTidLogger implements TidLogger {
  logGenerated(metadata: TidMetadata, opId: string): void {
    const entry = {
      level: 'INFO',
      op_id: opId,
      ts: metadata.generatedAt,
      event: 'tid.generated',
      tid: metadata.tid,
      clock_id: metadata.clockId,
      wall_ts_us: metadata.timestampUs,
      generator: 'tid-clock-v1',
      context: metadata.context || 'unknown',
      validated: metadata.validated,
    };
    console.log(JSON.stringify(entry));
  }

  logWarning(message: string, details?: any): void {
    console.warn(JSON.stringify({
      level: 'WARN',
      event: 'tid.warning',
      message,
      ...details,
    }));
  }

  logError(message: string, details?: any): void {
    console.error(JSON.stringify({
      level: 'ERROR',
      event: 'tid.error',
      message,
      ...details,
    }));
  }
}

/**
 * Silent logger (for production when structured logging is handled elsewhere)
 */
export class SilentTidLogger implements TidLogger {
  logGenerated(): void {}
  logWarning(): void {}
  logError(): void {}
}

/**
 * Main TID Clock generator
 */
export class TidClock {
  private state: TidState;
  private clock: ClockSource;
  private logger: TidLogger;
  private statePath: string | null;
  private mutex: Promise<void> = Promise.resolve();

  constructor(
    clock: ClockSource = new RealClock(),
    logger: TidLogger = new SilentTidLogger(),
    options: {
      statePath?: string;
      clockId?: number;
      initialState?: Partial<TidState>;
    } = {}
  ) {
    this.clock = clock;
    this.logger = logger;
    this.statePath = options.statePath || null;

    // Initialize or load state
    if (this.statePath && fs.existsSync(this.statePath)) {
      this.state = this.loadState();
    } else {
      this.state = {
        lastTimestampUs: 0,
        clockId: options.clockId ?? this.generateClockId(),
        generatedCount: 0,
        ...options.initialState,
      };
      if (this.statePath) {
        this.saveState();
      }
    }
  }

  /**
   * Generate a cryptographically random clock ID (0-31)
   */
  private generateClockId(): number {
    return crypto.randomInt(0, 32);
  }

  /**
   * Generate next TID with monotonicity guarantees
   * 
   * Per AT-Protocol spec and reference implementation:
   * - Use max(currentTime, lastTimestamp) to handle clock drift
   * - If same as last timestamp, increment the timestamp itself (acts as sequence)
   * - This ensures monotonicity while keeping TID format simple
   */
  async next(context?: string): Promise<string> {
    return this.withMutex(async () => {
      const currentTime = this.clock.now();
      
      // Take max of current time and last timestamp (handles backwards clock drift)
      let timestamp = Math.max(currentTime, this.state.lastTimestampUs);
      
      // If we're at the same timestamp, increment by 1 microsecond
      // This acts as our sequence counter while maintaining monotonicity
      if (timestamp === this.state.lastTimestampUs) {
        timestamp = this.state.lastTimestampUs + 1;
      }

      if (currentTime < this.state.lastTimestampUs) {
        this.logger.logWarning('Clock moved backwards', {
          current: currentTime,
          last: this.state.lastTimestampUs,
          delta: this.state.lastTimestampUs - currentTime,
          action: 'using_incremented_timestamp',
        });
      }

      // Generate TID
      const tid = this.encodeTid(timestamp, this.state.clockId);

      // Validate format
      const validated = this.validateTid(tid);
      if (!validated) {
        const error = new InvalidTidError('Generated invalid TID', tid);
        this.logger.logError('TID validation failed', {
          tid,
          timestamp,
          clockId: this.state.clockId,
        });
        throw error;
      }

      // Update state
      this.state.lastTimestampUs = timestamp;
      this.state.generatedCount++;

      // Persist state if configured
      if (this.statePath) {
        this.saveState();
      }

      // Log metadata
      const opId = crypto.randomUUID();
      const metadata: TidMetadata = {
        tid,
        timestampUs: timestamp,
        clockId: this.state.clockId,
        generatedAt: this.formatMicrosecondTimestamp(timestamp),
        validated,
        context,
      };
      this.logger.logGenerated(metadata, opId);

      return tid;
    });
  }

  /**
   * Generate TID from a specific Date (for historical records)
   * Maintains monotonicity relative to previously generated TIDs
   */
  async fromDate(date: Date, context?: string): Promise<string> {
    const timestamp = date.getTime() * 1000; // Convert ms to µs
    
    return this.withMutex(async () => {
      // Ensure monotonicity: use max of input timestamp and last generated
      let finalTimestamp = Math.max(timestamp, this.state.lastTimestampUs);
      
      // If we're at the same timestamp, increment by 1 microsecond
      if (finalTimestamp === this.state.lastTimestampUs) {
        finalTimestamp = this.state.lastTimestampUs + 1;
      }

      const tid = this.encodeTid(finalTimestamp, this.state.clockId);
      const validated = this.validateTid(tid);

      if (!validated) {
        throw new InvalidTidError('Generated invalid TID from date', tid);
      }

      // Update state
      this.state.lastTimestampUs = finalTimestamp;
      this.state.generatedCount++;

      if (this.statePath) {
        this.saveState();
      }

      const opId = crypto.randomUUID();
      const metadata: TidMetadata = {
        tid,
        timestampUs: finalTimestamp,
        clockId: this.state.clockId,
        generatedAt: this.formatMicrosecondTimestamp(finalTimestamp),
        validated,
        context,
      };
      this.logger.logGenerated(metadata, opId);

      return tid;
    });
  }

  /**
   * Encode timestamp and clock ID into TID format
   */
  private encodeTid(timestampUs: number, clockId: number): string {
    const timestampStr = s32encode(timestampUs).padStart(11, '2');
    const clockIdStr = s32encode(clockId).padStart(2, '2');
    return timestampStr + clockIdStr;
  }

  /**
   * Validate TID format per AT-Protocol spec
   */
  private validateTid(tid: string): boolean {
    if (tid.length !== TID_LENGTH) {
      return false;
    }
    if (!TID_REGEX.test(tid)) {
      return false;
    }
    return true;
  }

  /**
   * Format microsecond timestamp as ISO8601 with microsecond precision
   */
  private formatMicrosecondTimestamp(timestampUs: number): string {
    const ms = Math.floor(timestampUs / 1000);
    const us = timestampUs % 1000;
    const date = new Date(ms);
    const iso = date.toISOString();
    // Replace milliseconds with microseconds
    return iso.replace(/\.(\d{3})Z$/, `.${us.toString().padStart(3, '0')}000Z`);
  }

  /**
   * Load state from disk
   */
  private loadState(): TidState {
    if (!this.statePath) {
      throw new Error('State path not configured');
    }
    const data = fs.readFileSync(this.statePath, 'utf-8');
    return JSON.parse(data);
  }

  /**
   * Save state to disk
   */
  private saveState(): void {
    if (!this.statePath) {
      return;
    }
    const dir = path.dirname(this.statePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(this.statePath, JSON.stringify(this.state, null, 2), 'utf-8');
  }

  /**
   * Get current state (for inspection/debugging)
   */
  getState(): Readonly<TidState> {
    return { ...this.state };
  }

  /**
   * Reset state (for testing)
   */
  reset(preserveClockId: boolean = false): void {
    const clockId = preserveClockId ? this.state.clockId : this.generateClockId();
    this.state = {
      lastTimestampUs: 0,
      clockId,
      generatedCount: 0,
    };
    if (this.statePath) {
      this.saveState();
    }
  }

  /**
   * Mutex for concurrent access protection
   */
  private async withMutex<T>(fn: () => Promise<T>): Promise<T> {
    const currentMutex = this.mutex;
    let releaseMutex: () => void;
    this.mutex = new Promise((resolve) => {
      releaseMutex = resolve;
    });

    try {
      await currentMutex;
      return await fn();
    } finally {
      releaseMutex!();
    }
  }
}

/**
 * Validate a TID string
 */
export function validateTid(tid: string): boolean {
  return tid.length === TID_LENGTH && TID_REGEX.test(tid);
}

/**
 * Ensure TID is valid (throws on invalid)
 */
export function ensureValidTid(tid: string): asserts tid is string {
  if (!validateTid(tid)) {
    throw new InvalidTidError(`Invalid TID format: ${tid}`, tid);
  }
}

/**
 * Decode TID to get timestamp
 */
export function decodeTidTimestamp(tid: string): number {
  ensureValidTid(tid);
  return s32decode(tid.slice(0, 11));
}

/**
 * Decode TID to get clock ID
 */
export function decodeTidClockId(tid: string): number {
  ensureValidTid(tid);
  return s32decode(tid.slice(11, 13));
}

/**
 * Compare two TIDs (for sorting)
 * Returns: -1 if a < b, 0 if equal, 1 if a > b
 */
export function compareTids(a: string, b: string): number {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

/**
 * Check if TIDs are in strictly increasing order
 */
export function areMonotonic(tids: string[]): boolean {
  for (let i = 1; i < tids.length; i++) {
    if (tids[i] <= tids[i - 1]) {
      return false;
    }
  }
  return true;
}
