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
