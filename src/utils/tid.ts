/**
 * TID (Timestamp Identifier) generation for ATProto
 * 
 * This module provides a high-level API for TID generation.
 * For the full implementation with monotonicity guarantees, see tid-clock.ts
 * 
 * Based on: https://atproto.com/specs/tid
 */

import { TidClock, RealClock, FakeClock, validateTid } from './tid-clock.js';

// Global TID clock instance
let globalClock: TidClock | null = null;

/**
 * Initialize the global TID clock
 * Should be called once at application startup
 */
export function initTidClock(options: {
  mode?: 'production' | 'dry-run';
  statePath?: string;
  seed?: number;
  clockId?: number;
} = {}): void {
  const { mode = 'production', statePath, seed, clockId } = options;

  if (mode === 'dry-run' && seed !== undefined) {
    // Deterministic clock for dry-run with fixed clockId for reproducibility
    globalClock = new TidClock(new FakeClock(seed), undefined, { 
      statePath,
      clockId: clockId ?? 0  // Use fixed clockId for deterministic TIDs
    });
  } else {
    // Production real-time clock
    globalClock = new TidClock(new RealClock(), undefined, { statePath, clockId });
  }
}

/**
 * Get or create the global TID clock
 */
function getClock(): TidClock {
  if (!globalClock) {
    // Auto-initialize with production settings if not explicitly initialized
    globalClock = new TidClock(new RealClock());
  }
  return globalClock;
}

/**
 * Generate a TID from a Date object with monotonicity guarantees
 * 
 * This is the recommended way to generate TIDs for historical records.
 * TIDs are guaranteed to be:
 * - Spec-compliant (13 chars, valid base32)
 * - Strictly monotonic (even if dates are out of order)
 * - Collision-free (duplicate detection)
 * 
 * @param date - The date to generate a TID from
 * @param context - Optional context for logging (e.g., "inject:playlist")
 * @returns A valid 13-character TID string
 */
export async function generateTID(date: Date, context?: string): Promise<string> {
  const clock = getClock();
  return await clock.fromDate(date, context);
}

/**
 * Generate a TID from an ISO 8601 timestamp string
 * 
 * @param isoString - ISO 8601 formatted datetime string
 * @param context - Optional context for logging
 * @returns A valid 13-character TID string
 */
export async function generateTIDFromISO(isoString: string, context?: string): Promise<string> {
  return await generateTID(new Date(isoString), context);
}

/**
 * Generate next TID using current time
 * 
 * @param context - Optional context for logging
 * @returns A valid 13-character TID string
 */
export async function generateNextTID(context?: string): Promise<string> {
  const clock = getClock();
  return await clock.next(context);
}

/**
 * Validate a TID string
 * 
 * @param tid - The TID to validate
 * @returns true if valid, false otherwise
 */
export function isValidTID(tid: string): boolean {
  return validateTid(tid);
}

/**
 * Reset the global clock state (for testing only)
 */
export function resetTidClock(): void {
  if (globalClock) {
    globalClock.reset();
  }
}

/**
 * Get the current clock state (for debugging/inspection)
 */
export function getTidClockState() {
  const clock = getClock();
  return clock.getState();
}

// Re-export types and utilities from tid-clock
export { TidClock, RealClock, FakeClock, validateTid } from './tid-clock.js';
export type { TidState, TidMetadata } from './tid-clock.js';
