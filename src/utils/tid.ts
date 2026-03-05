/**
 * TID generation — re-exported from @ewanc26/tid.
 *
 * The canonical implementation lives in packages/tid.
 * Spec: https://atproto.com/specs/tid
 */

export {
  generateTID,
  generateNextTID,
  validateTid,
  decodeTid,
  decodeTidTimestamp,
  decodeTidClockId,
  ensureValidTid,
  InvalidTidError,
  areMonotonic,
  compareTids,
  getTidClockState,
  resetTidClock,
  seedTidClock,
} from '@ewanc26/tid';
export type { DecodedTid, TidClockState } from '@ewanc26/tid';

import { generateTID } from '@ewanc26/tid';

/**
 * Generate a TID from an ISO 8601 string.
 * Synchronous wrapper kept for call-site compatibility.
 */
export function generateTIDFromISO(isoString: string, _context?: string): string {
  return generateTID(isoString);
}

/**
 * Generate a TID from a Date object.
 * Convenience wrapper kept for call-site compatibility.
 */
export function generateTIDFromDate(date: Date, _context?: string): string {
  return generateTID(date);
}

// No-op shims for callers that used the old initTidClock / getTidClockState API.
// The package manages its own module-level clock; these are no longer needed.
export function initTidClock(_options?: {
  mode?: 'production' | 'dry-run';
  statePath?: string;
  seed?: number;
  clockId?: number;
}): void {
  // No-op: use seedTidClock() in tests instead.
}
